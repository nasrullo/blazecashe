use crate::transports::common::{Command, ProtocolClient, ProtocolServer, Response};
use crate::transports::{
    handle_command, handle_get_response, handle_ping_response, handle_put_response, Serializer,
};
use crate::utils::persistence::PersistenceManager;
use crate::Group;
use async_trait::async_trait;
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::timeout;
use tracing::info;

pub struct UdpServer<S> {
    group: Arc<Group>,
    serializer: std::marker::PhantomData<S>,
    /// Optional persistence manager for WAL logging on UDP commands.
    persistence: Option<Arc<AsyncMutex<PersistenceManager>>>,
}

impl<S> UdpServer<S> {
    pub fn new(group: Arc<Group>) -> Self {
        Self {
            group,
            serializer: std::marker::PhantomData,
            persistence: None,
        }
    }

    /// Creates a new UDP server with optional persistence.
    ///
    /// When a persistence manager is provided, successful PUT and DELETE
    /// operations received over UDP will be logged to the WAL.
    pub fn with_persistence(
        group: Arc<Group>,
        persistence: Option<Arc<AsyncMutex<PersistenceManager>>>,
    ) -> Self {
        Self {
            group,
            serializer: std::marker::PhantomData,
            persistence,
        }
    }
}


const MAGIC: u16 = 0xBC01;
const VERSION: u8 = 1;
const FLAG_RESPONSE: u8 = 0b0000_0001;

const MAX_DATAGRAM: usize = 1200;
const HEADER_LEN: usize = 14;
const MAX_PAYLOAD: usize = MAX_DATAGRAM - HEADER_LEN;

const MAX_MESSAGE_BYTES: usize =  4 << 20;
const REASSEMBLY_TIMEOUT: Duration = Duration::from_secs(2);
const CLIENT_RETRIES: usize = 2; // total attempts = 1 + CLIENT_RETRIES

static NEXT_REQUEST_ID: AtomicU32 = AtomicU32::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MsgType {
    Request = 0,
    Response = 1,
}

#[derive(Debug)]
struct FragHeader {
    msg_type: MsgType,
    request_id: u32,
    seq_no: u16,
    frag_count: u16,
    payload_len: u16,
}

fn encode_fragment(h: FragHeader, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());

    out.extend_from_slice(&MAGIC.to_be_bytes());
    out.push(VERSION);

    let mut flags = 0u8;
    if h.msg_type == MsgType::Response {
        flags |= FLAG_RESPONSE;
    }
    out.push(flags);

    out.extend_from_slice(&h.request_id.to_be_bytes());
    out.extend_from_slice(&h.seq_no.to_be_bytes());
    out.extend_from_slice(&h.frag_count.to_be_bytes());
    out.extend_from_slice(&h.payload_len.to_be_bytes());
    out.extend_from_slice(payload);

    out
}

fn decode_fragment(buf: &[u8]) -> Result<(FragHeader, &[u8]), Box<dyn Error + Send + Sync>> {
    if buf.len() < HEADER_LEN {
        return Err("udp: datagram too short".into());
    }

    let magic = u16::from_be_bytes([buf[0], buf[1]]);
    if magic != MAGIC {
        return Err("udp: bad magic".into());
    }

    let version = buf[2];
    if version != VERSION {
        return Err("udp: unsupported version".into());
    }

    let flags = buf[3];
    let msg_type = if (flags & FLAG_RESPONSE) != 0 {
        MsgType::Response
    } else {
        MsgType::Request
    };

    let request_id = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    let seq_no = u16::from_be_bytes([buf[8], buf[9]]);
    let frag_count = u16::from_be_bytes([buf[10], buf[11]]);
    let payload_len = u16::from_be_bytes([buf[12], buf[13]]);

    if frag_count == 0 {
        return Err("udp: frag_count=0".into());
    }
    if seq_no >= frag_count {
        return Err("udp: seq_no out of range".into());
    }
    if payload_len as usize > MAX_PAYLOAD {
        return Err("udp: payload too large for datagram".into());
    }

    let expected_total = HEADER_LEN + payload_len as usize;
    if buf.len() < expected_total {
        return Err("udp: truncated payload".into());
    }

    // Coarse message-size cap (worst-case)
    let worst_case = (frag_count as usize).saturating_mul(MAX_PAYLOAD);
    if worst_case > MAX_MESSAGE_BYTES + MAX_PAYLOAD {
        return Err("udp: message exceeds size cap".into());
    }

    let payload = &buf[HEADER_LEN..expected_total];
    Ok((
        FragHeader {
            msg_type,
            request_id,
            seq_no,
            frag_count,
            payload_len,
        },
        payload,
    ))
}

fn fragment_bytes(
    msg_type: MsgType,
    request_id: u32,
    bytes: &[u8],
) -> Result<Vec<Vec<u8>>, Box<dyn Error + Send + Sync>> {
    if bytes.len() > MAX_MESSAGE_BYTES {
        return Err("udp: message too large".into());
    }

    let frag_count = ((bytes.len() + MAX_PAYLOAD - 1) / MAX_PAYLOAD).max(1);
    if frag_count > u16::MAX as usize {
        return Err("udp: too many fragments".into());
    }
    let frag_count_u16 = frag_count as u16;

    let mut out = Vec::with_capacity(frag_count);

    for i in 0..frag_count {
        let start = i * MAX_PAYLOAD;
        let end = (start + MAX_PAYLOAD).min(bytes.len());
        let payload = &bytes[start..end];

        let hdr = FragHeader {
            msg_type,
            request_id,
            seq_no: i as u16,
            frag_count: frag_count_u16,
            payload_len: payload.len() as u16,
        };

        out.push(encode_fragment(hdr, payload));
    }

    Ok(out)
}

#[derive(Debug)]
struct Reassembly {
    created_at: Instant,
    frag_count: u16,
    received: Vec<bool>,
    parts: Vec<Vec<u8>>,
    received_count: u16,
    total_len: usize,
}

impl Reassembly {
    fn new(frag_count: u16) -> Self {
        Self {
            created_at: Instant::now(),
            frag_count,
            received: vec![false; frag_count as usize],
            parts: vec![Vec::new(); frag_count as usize],
            received_count: 0,
            total_len: 0,
        }
    }

    fn insert(&mut self, seq_no: u16, payload: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
        let idx = seq_no as usize;
        if idx >= self.parts.len() {
            return Err("udp: reassembly seq out of range".into());
        }

        if self.received[idx] {
            // duplicate; ignore
            return Ok(());
        }

        self.received[idx] = true;
        self.parts[idx] = payload.to_vec();
        self.received_count = self.received_count.saturating_add(1);
        self.total_len = self.total_len.saturating_add(payload.len());

        if self.total_len > MAX_MESSAGE_BYTES {
            return Err("udp: reassembly exceeded size cap".into());
        }

        Ok(())
    }

    fn is_complete(&self) -> bool {
        self.received_count == self.frag_count
    }

    fn assemble(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.total_len);
        for p in self.parts {
            out.extend_from_slice(&p);
        }
        out
    }
}

// ---- Server: reassemble request, process, fragment response ----

type ReassemblyKey = (SocketAddr, u32);

#[async_trait]
impl<S: Serializer + 'static> ProtocolServer for UdpServer<S> {
    async fn start(&self, port: u16) -> Result<(), Box<dyn Error + Send + Sync>> {
        let socket = Arc::new(UdpSocket::bind(format!("0.0.0.0:{port}")).await?);
        info!(port = port, "UDP server listening");

        let inflight: Arc<Mutex<HashMap<ReassemblyKey, Reassembly>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let mut buffer = [0u8; MAX_DATAGRAM];

        loop {
            let (len, addr) = socket.recv_from(&mut buffer).await?;
            let packet = buffer[..len].to_vec();

            let socket_clone = Arc::clone(&socket);
            let group = Arc::clone(&self.group);
            let inflight = Arc::clone(&inflight);
            let persistence = self.persistence.clone();

            tokio::spawn(async move {
                let (hdr, payload) = match decode_fragment(&packet) {
                    Ok(v) => v,
                    Err(_) => return, // ignore junk
                };

                if hdr.msg_type != MsgType::Request {
                    return;
                }

                let key = (addr, hdr.request_id);
                let mut maybe_complete: Option<Vec<u8>> = None;

                {
                    let mut map = inflight.lock().await;

                    // Clean up expired entries opportunistically
                    let now = Instant::now();
                    map.retain(|_, r| now.duration_since(r.created_at) <= REASSEMBLY_TIMEOUT);

                    let entry = map
                        .entry(key)
                        .or_insert_with(|| Reassembly::new(hdr.frag_count));

                    // If frag_count mismatched, reset (a client reused request_id too quickly)
                    if entry.frag_count != hdr.frag_count {
                        *entry = Reassembly::new(hdr.frag_count);
                    }

                    if entry.insert(hdr.seq_no, payload).is_err() {
                        map.remove(&key);
                        return;
                    }

                    if entry.is_complete() {
                        if let Some(done) = map.remove(&key) {
                            maybe_complete = Some(done.assemble());
                        }
                    }
                }

                let request_bytes = match maybe_complete {
                    Some(b) => b,
                    None => return,
                };

                // Process request
                let cmd = match S::deserialize_command(&request_bytes) {
                    Ok(c) => c,
                    Err(_) => {
                        let resp = Response::Error("udp_bad_request".to_string());
                        let resp_bytes = S::serialize_response(&resp);
                        if let Ok(frags) =
                            fragment_bytes(MsgType::Response, hdr.request_id, &resp_bytes)
                        {
                            for f in frags {
                                let _ = socket_clone.send_to(&f, addr).await;
                            }
                        }
                        return;
                    }
                };

                // Use shared command handler; pass through optional persistence for WAL
                let resp = handle_command(&group, cmd, persistence).await;

                let resp_bytes = S::serialize_response(&resp);
                let frags = match fragment_bytes(MsgType::Response, hdr.request_id, &resp_bytes) {
                    Ok(f) => f,
                    Err(_) => {
                        let resp = Response::Error("udp_response_too_large".to_string());
                        let resp_bytes = S::serialize_response(&resp);
                        if let Ok(frags) =
                            fragment_bytes(MsgType::Response, hdr.request_id, &resp_bytes)
                        {
                            for f in frags {
                                let _ = socket_clone.send_to(&f, addr).await;
                            }
                        }
                        return;
                    }
                };

                for f in frags {
                    let _ = socket_clone.send_to(&f, addr).await;
                }
            });
        }
    }
}

pub struct UdpClient<S> {
    socket: UdpSocket,
    server_addr: String,
    serializer: std::marker::PhantomData<S>,
}

impl<S> UdpClient<S>
where
    S: Serializer + 'static,
{
    async fn send_fragmented(
        &self,
        request_id: u32,
        cmd_bytes: &[u8],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let frags = fragment_bytes(MsgType::Request, request_id, cmd_bytes)?;
        for f in frags {
            self.socket.send_to(&f, &self.server_addr).await?;
        }
        Ok(())
    }

    async fn recv_reassembled_response(
        &self,
        request_id: u32,
    ) -> Result<Response, Box<dyn Error + Send + Sync>> {
        let deadline = Instant::now() + REASSEMBLY_TIMEOUT;

        let mut r: Option<Reassembly> = None;
        let mut buf = [0u8; MAX_DATAGRAM];

        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "udp response timed out",
                )
                .into());
            }

            let remaining = deadline.duration_since(now);
            let (len, _addr) = timeout(remaining, self.socket.recv_from(&mut buf)).await??;

            let (hdr, payload) = decode_fragment(&buf[..len])?;
            if hdr.msg_type != MsgType::Response {
                continue;
            }
            if hdr.request_id != request_id {
                continue;
            }

            if r.is_none() {
                r = Some(Reassembly::new(hdr.frag_count));
            }

            let mut rr = r.take().unwrap();

            if rr.frag_count != hdr.frag_count {
                rr = Reassembly::new(hdr.frag_count);
            }

            rr.insert(hdr.seq_no, payload)?;

            if rr.is_complete() {
                let bytes = rr.assemble();
                return Ok(S::deserialize_response(&bytes)?);
            }

            r = Some(rr);
        }
    }

    async fn round_trip_with_retries(
        &self,
        cmd: &Command,
    ) -> Result<Response, Box<dyn Error + Send + Sync>> {
        let cmd_data = S::serialize_command(cmd);
        let mut last_err: Option<Box<dyn Error + Send + Sync>> = None;

        for _attempt in 0..=CLIENT_RETRIES {
            let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);

            if let Err(e) = self.send_fragmented(request_id, &cmd_data).await {
                last_err = Some(e);
                continue;
            }

            match self.recv_reassembled_response(request_id).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            }
        }

        Err(last_err.unwrap_or_else(|| "udp request failed".into()))
    }
}

#[async_trait]
impl<S: Serializer + 'static> ProtocolClient for UdpClient<S> {
    async fn connect(addr: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        Ok(Self {
            socket,
            server_addr: addr.to_string(),
            serializer: std::marker::PhantomData,
        })
    }

    async fn ping(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let resp = self.round_trip_with_retries(&Command::Ping).await?;
        handle_ping_response(resp)
    }

    async fn get(&mut self, key: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let resp = self
            .round_trip_with_retries(&Command::Get(key.to_string()))
            .await?;
        handle_get_response(resp)
    }

    async fn put(&mut self, key: &str, value: &[u8], ttl:u32) -> Result<(), Box<dyn Error + Send + Sync>> {
        let resp = self
            .round_trip_with_retries(&Command::Put(key.to_string(), value.to_vec(), ttl))
            .await?;
        handle_put_response(resp)
    }

    async fn delete(&mut self, key: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let resp = self
            .round_trip_with_retries(&Command::Delete(key.to_string()))
            .await?;
        match resp {
            Response::Ok(_) => Ok(true),
            Response::Error(msg) if msg == "Not found" => Ok(false),
            Response::Error(msg) => Err(msg.into()),
            _ => Err("Invalid delete response".into()),
        }
    }
}
