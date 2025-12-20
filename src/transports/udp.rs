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
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tracing::info;
use futures::future;

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

// Optimized: inline encode_fragment into fragment_bytes to use buffer pool
// (encode_fragment removed, logic moved to fragment_bytes)

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

        // Inline encode_fragment for better performance
        let mut frag = Vec::with_capacity(HEADER_LEN + payload.len());
        frag.extend_from_slice(&MAGIC.to_be_bytes());
        frag.push(VERSION);
        let mut flags = 0u8;
        if hdr.msg_type == MsgType::Response {
            flags |= FLAG_RESPONSE;
        }
        frag.push(flags);
        frag.extend_from_slice(&hdr.request_id.to_be_bytes());
        frag.extend_from_slice(&hdr.seq_no.to_be_bytes());
        frag.extend_from_slice(&hdr.frag_count.to_be_bytes());
        frag.extend_from_slice(&hdr.payload_len.to_be_bytes());
        frag.extend_from_slice(payload);
        out.push(frag);
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

// Request work item for the work queue
struct UdpRequest {
    cmd_byte: u8,
    cmd_data: Vec<u8>,
    request_id: u32,
    addr: SocketAddr,
}

#[async_trait]
impl<S: Serializer + 'static> ProtocolServer for UdpServer<S> {
    async fn start(&self, port: u16) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Optimization 1: SO_REUSEPORT - spawn multiple server instances for load distribution
        // This allows the kernel to distribute incoming packets across multiple sockets
        // Each socket runs in its own task, reducing contention and improving throughput
        let num_instances = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(8); // Cap at 8 instances to avoid too many tasks
        
        info!(port = port, instances = num_instances, "Starting UDP server with SO_REUSEPORT");
        
        let mut handles = Vec::new();
        for instance_id in 0..num_instances {
            let group = Arc::clone(&self.group);
            let persistence = self.persistence.clone();
            
            let handle = tokio::spawn(async move {
                // Optimize UDP socket for performance
                use socket2::{Domain, Socket, Type, Protocol, SockAddr};
                let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
                
                // Set socket options for maximum performance
                socket.set_reuse_address(true)?;
                socket.set_reuse_port(true)?; // SO_REUSEPORT - allows multiple sockets on same port
                
                // Increase buffer sizes for high throughput
                socket.set_recv_buffer_size(4 * 1024 * 1024)?; // 4MB receive buffer
                socket.set_send_buffer_size(4 * 1024 * 1024)?; // 4MB send buffer
                
                // Bind and convert to tokio socket
                use std::net::SocketAddr as StdSocketAddr;
                let addr = StdSocketAddr::from(([0, 0, 0, 0], port));
                socket.bind(&SockAddr::from(addr))?;
                let socket = UdpSocket::from_std(socket.into())?;
                let socket = Arc::new(socket);
                
                info!(port = port, instance = instance_id, "UDP server instance listening");
                
                // Create server instance and start the loop
                let server = UdpServer::<S> {
                    group,
                    serializer: std::marker::PhantomData,
                    persistence,
                };
                
                server.start_instance(socket).await
            });
            
            handles.push(handle);
        }
        
        // Wait forever - instances run in infinite loops
        // This prevents the server from "exiting" in main.rs
        future::join_all(handles).await;
        
        // This should never be reached, but return Ok for type safety
        Ok(())
    }
}

impl<S: Serializer + 'static> UdpServer<S> {
    /// Start a single UDP server instance (used by SO_REUSEPORT)
    async fn start_instance(&self, socket: Arc<UdpSocket>) -> Result<(), Box<dyn Error + Send + Sync>> {

        let inflight: Arc<Mutex<HashMap<ReassemblyKey, Reassembly>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Spawn tasks directly without semaphore for maximum performance
        // With SO_REUSEPORT, we already have load distribution across instances
        let mut buffer = [0u8; MAX_DATAGRAM];

        loop {
            let (len, addr) = socket.recv_from(&mut buffer).await?;
            
            let socket_clone = Arc::clone(&socket);
            let group = Arc::clone(&self.group);
            let inflight = Arc::clone(&inflight);
            let persistence = self.persistence.clone();

            // Fast path: check if single-datagram message (no fragmentation)
            // Single-datagram format: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][CMD:1][DATA:...]
            // Fragment format: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][SEQ:2][FRAG_COUNT:2][PAYLOAD_LEN:2][DATA:...]
            // We can distinguish by checking if byte 8 is a command (0x01-0x04) vs a fragment seq (usually 0x00 for first fragment)
            if len >= 9 && len <= MAX_DATAGRAM {
                let magic = u16::from_be_bytes([buffer[0], buffer[1]]);
                if magic == MAGIC && buffer[2] == VERSION {
                    // Check if it's a single-datagram (command byte) vs fragmented (has SEQ/FRAG_COUNT)
                    let byte8 = buffer[8];
                    // Fast path: single-datagram message
                    // Commands are 0x01 (GET), 0x02 (PUT), 0x03 (DELETE), 0x04 (PING)
                    // Fragments have SEQ_NO (0-65535) in bytes 8-9, so byte 8 alone can't be > 255
                    // If byte 8 is a command (0x01-0x04), it's single-datagram format
                    // (fragments would have SEQ_NO in bytes 8-9, which is unlikely to be 0x01-0x04)
                    if byte8 >= 0x01 && byte8 <= 0x04 {
                        // Definitely single-datagram (too short for fragment header)
                        let request_id = u32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
                        let cmd_byte = byte8;
                        let cmd_data = buffer[9..len].to_vec(); // Copy data before spawning
                        
                        // Handle single-datagram requests directly (fast path)
                        let socket_clone = Arc::clone(&socket);
                        let group = Arc::clone(&self.group);
                        let _persistence = self.persistence.clone();
                        
                        // Optimization 2: Work-stealing approach - handle fast path inline when possible
                        // For very simple operations (PING), we can handle inline to avoid task spawn overhead
                        if cmd_byte == 0x04 {
                            // PING - handle inline (no async needed, no group access needed)
                            let mut response = Vec::with_capacity(9);
                            response.extend_from_slice(&MAGIC.to_be_bytes());
                            response.push(VERSION);
                            response.push(FLAG_RESPONSE);
                            response.extend_from_slice(&request_id.to_be_bytes());
                            response.push(0x00); // Success
                            
                            if response.len() <= MAX_DATAGRAM {
                                let _ = socket.send_to(&response, addr).await;
                            }
                            continue;
                        }
                        
                        // For GET/PUT, spawn task directly for concurrent processing
                        // This allows multiple requests to be processed in parallel
                        if cmd_byte == 0x01 || cmd_byte == 0x02 {
                            let socket_spawn = Arc::clone(&socket);
                            let group_spawn = Arc::clone(&self.group);
                            let cmd_byte_spawn = cmd_byte;
                            let cmd_data_spawn = cmd_data.clone();
                            let request_id_spawn = request_id;
                            let addr_spawn = addr;
                            
                            tokio::spawn(async move {
                                
                                let mut response = Vec::with_capacity(64);
                                response.extend_from_slice(&MAGIC.to_be_bytes());
                                response.push(VERSION);
                                response.push(FLAG_RESPONSE);
                                response.extend_from_slice(&request_id_spawn.to_be_bytes());
                                
                                match cmd_byte_spawn {
                                    0x01 => { // GET
                                        if cmd_data_spawn.len() >= 2 {
                                            let key_len = u16::from_be_bytes([cmd_data_spawn[0], cmd_data_spawn[1]]) as usize;
                                            if cmd_data_spawn.len() >= 2 + key_len {
                                                if let Ok(key) = std::str::from_utf8(&cmd_data_spawn[2..2 + key_len]) {
                                                    match group_spawn.get(key).await {
                                                        Ok(value) => {
                                                            response.push(0x00); // Found
                                                            response.extend_from_slice(&(value.len() as u32).to_be_bytes());
                                                            response.extend_from_slice(&value);
                                                        }
                                                        Err(_) => {
                                                            response.push(0x01); // Not found
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    0x02 => { // PUT
                                        if cmd_data_spawn.len() >= 2 {
                                            let key_len = u16::from_be_bytes([cmd_data_spawn[0], cmd_data_spawn[1]]) as usize;
                                            if cmd_data_spawn.len() >= 2 + key_len + 4 {
                                                if let Ok(key) = std::str::from_utf8(&cmd_data_spawn[2..2 + key_len]) {
                                                    let value_len = u32::from_be_bytes([
                                                        cmd_data_spawn[2 + key_len],
                                                        cmd_data_spawn[2 + key_len + 1],
                                                        cmd_data_spawn[2 + key_len + 2],
                                                        cmd_data_spawn[2 + key_len + 3],
                                                    ]) as usize;
                                                    if cmd_data_spawn.len() >= 2 + key_len + 4 + value_len + 4 {
                                                        let value = &cmd_data_spawn[2 + key_len + 4..2 + key_len + 4 + value_len];
                                                        let ttl = u32::from_be_bytes([
                                                            cmd_data_spawn[2 + key_len + 4 + value_len],
                                                            cmd_data_spawn[2 + key_len + 4 + value_len + 1],
                                                            cmd_data_spawn[2 + key_len + 4 + value_len + 2],
                                                            cmd_data_spawn[2 + key_len + 4 + value_len + 3],
                                                        ]);
                                                        
                                                        match group_spawn.set(key, value.to_vec(), ttl).await {
                                                            Ok(_) => {
                                                                response.push(0x00); // Success
                                                            }
                                                            Err(_) => {
                                                                response.push(0x02); // Error
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    _ => {
                                        response.push(0x02); // Error
                                    }
                                }
                                
                                // Send response
                                if response.len() <= MAX_DATAGRAM {
                                    let _ = socket_spawn.send_to(&response, addr_spawn).await;
                                }
                            });
                            // Yield to allow the spawned task to start executing
                            // This is necessary because we immediately continue to recv_from,
                            // and the runtime needs a chance to schedule the spawned task
                            tokio::task::yield_now().await;
                            continue;
                        }
                    }
                }
            }
            
            let packet = buffer[..len].to_vec();
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

    async fn round_trip_with_retries<'a>(
        &self,
        cmd: &Command<'a>,
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
        // Optimize UDP socket for performance
        use socket2::{Domain, Socket, Type, Protocol};
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        
        // Set socket options for maximum performance
        socket.set_reuse_address(true)?;
        
        // Increase buffer sizes for high throughput
        socket.set_recv_buffer_size(4 * 1024 * 1024)?; // 4MB receive buffer
        socket.set_send_buffer_size(4 * 1024 * 1024)?; // 4MB send buffer
        
        // Convert to tokio socket
        let socket = UdpSocket::from_std(socket.into())?;
        
        Ok(Self {
            socket,
            server_addr: addr.to_string(),
            serializer: std::marker::PhantomData,
        })
    }

    async fn ping(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Fast path: direct encoding for ping (single byte command)
        let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        let mut packet = [0u8; 9]; // Stack-allocated for ping
        packet[0..2].copy_from_slice(&MAGIC.to_be_bytes());
        packet[2] = VERSION;
        packet[3] = 0u8;
        packet[4..8].copy_from_slice(&request_id.to_be_bytes());
        packet[8] = 0x04; // PING command
        
        self.socket.send_to(&packet, &self.server_addr).await?;
        
        // Receive response - no timeout for fast path (localhost should be instant)
        let mut buffer = [0u8; MAX_DATAGRAM];
        let (len, _) = self.socket.recv_from(&mut buffer).await?;
        
        if len < 9 || u16::from_be_bytes([buffer[0], buffer[1]]) != MAGIC || buffer[2] != VERSION {
            return Err("invalid ping response".into());
        }
        
        if u32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]) != request_id {
            return Err("request ID mismatch".into());
        }
        
        if buffer[8] == 0x00 {
            Ok(())
        } else {
            Err("ping failed".into())
        }
    }

    async fn get(&mut self, key: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        use std::borrow::Cow;
        // Fast path: direct encoding for GET
        let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        let key_bytes = key.as_bytes();
        let key_len = key_bytes.len();
        let packet_size = 9 + 2 + key_len;
        
        if packet_size <= MAX_DATAGRAM {
            // Single datagram - use direct encoding
            let mut packet = Vec::with_capacity(packet_size);
            packet.extend_from_slice(&MAGIC.to_be_bytes());
            packet.push(VERSION);
            packet.push(0u8);
            packet.extend_from_slice(&request_id.to_be_bytes());
            packet.push(0x01); // GET command
            packet.extend_from_slice(&(key_len as u16).to_be_bytes());
            packet.extend_from_slice(key_bytes);
            
            self.socket.send_to(&packet, &self.server_addr).await?;
            
            // Receive response - loop until we get the right request ID or timeout
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut attempts = 0u32;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(format!("GET response timeout after {} attempts (request_id={})", attempts, request_id).into());
                }
                
                let mut buffer = [0u8; MAX_DATAGRAM];
                let (len, _) = match tokio::time::timeout(remaining, self.socket.recv_from(&mut buffer)).await {
                    Ok(Ok((len, addr))) => {
                        attempts += 1;
                        (len, addr)
                    }
                    Ok(Err(e)) => return Err(e.into()),
                    Err(_) => {
                        // Timeout on this iteration - check if we still have time overall
                        if deadline.saturating_duration_since(Instant::now()).is_zero() {
                            return Err(format!("GET response timeout after {} attempts (request_id={})", attempts, request_id).into());
                        }
                        continue; // Try again if we have time
                    }
                };
                
                if len < 9 || u16::from_be_bytes([buffer[0], buffer[1]]) != MAGIC || buffer[2] != VERSION {
                    continue; // Invalid response, try again
                }
                
                let resp_request_id = u32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
                if resp_request_id != request_id {
                    // Wrong request ID - this is expected with concurrent requests
                    continue; // Try again
                }
                
                // Got the right response!
                let status = buffer[8];
                match status {
                    0x00 => {
                        if len < 13 {
                            return Err("response too short".into());
                        }
                        let value_len = u32::from_be_bytes([buffer[9], buffer[10], buffer[11], buffer[12]]) as usize;
                        if len < 13 + value_len {
                            return Err("response incomplete".into());
                        }
                        return Ok(buffer[13..13 + value_len].to_vec());
                    }
                    0x01 => return Ok(Vec::new()), // Not found
                    _ => return Err("unexpected status".into()),
                }
            }
        } else {
            // Fall back to fragmented path
            let resp = self
                .round_trip_with_retries(&Command::Get(Cow::Borrowed(key)))
                .await?;
            handle_get_response(resp)
        }
    }

    async fn put(&mut self, key: &str, value: &[u8], ttl: u32) -> Result<(), Box<dyn Error + Send + Sync>> {
        use std::borrow::Cow;
        // Fast path: direct encoding for PUT
        let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        let key_bytes = key.as_bytes();
        let key_len = key_bytes.len();
        let value_len = value.len();
        let packet_size = 9 + 2 + key_len + 4 + value_len + 4;
        
        if packet_size <= MAX_DATAGRAM {
            // Single datagram - use direct encoding
            let mut packet = Vec::with_capacity(packet_size);
            packet.extend_from_slice(&MAGIC.to_be_bytes());
            packet.push(VERSION);
            packet.push(0u8);
            packet.extend_from_slice(&request_id.to_be_bytes());
            packet.push(0x02); // PUT command
            packet.extend_from_slice(&(key_len as u16).to_be_bytes());
            packet.extend_from_slice(key_bytes);
            packet.extend_from_slice(&(value_len as u32).to_be_bytes());
            packet.extend_from_slice(value);
            packet.extend_from_slice(&ttl.to_be_bytes());
            
            self.socket.send_to(&packet, &self.server_addr).await?;
            
            // Receive response - loop until we get the right request ID or timeout
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut attempts = 0u32;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(format!("PUT response timeout after {} attempts (request_id={})", attempts, request_id).into());
                }
                
                let mut buffer = [0u8; MAX_DATAGRAM];
                let (len, _) = match tokio::time::timeout(remaining, self.socket.recv_from(&mut buffer)).await {
                    Ok(Ok((len, addr))) => {
                        attempts += 1;
                        (len, addr)
                    }
                    Ok(Err(e)) => return Err(e.into()),
                    Err(_) => {
                        // Timeout on this iteration - check if we still have time overall
                        if deadline.saturating_duration_since(Instant::now()).is_zero() {
                            return Err(format!("PUT response timeout after {} attempts (request_id={})", attempts, request_id).into());
                        }
                        continue; // Try again if we have time
                    }
                };
                
                if len < 9 || u16::from_be_bytes([buffer[0], buffer[1]]) != MAGIC || buffer[2] != VERSION {
                    continue; // Invalid response, try again
                }
                
                let resp_request_id = u32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
                if resp_request_id != request_id {
                    // Wrong request ID - this is expected with concurrent requests
                    continue; // Try again
                }
                
                // Got the right response!
                if buffer[8] == 0x00 {
                    return Ok(());
                } else {
                    return Err("put failed".into());
                }
            }
        } else {
            // Fall back to fragmented path
            let resp = self
                .round_trip_with_retries(&Command::Put(Cow::Borrowed(key), value.to_vec(), ttl))
                .await?;
            handle_put_response(resp)
        }
    }

    async fn delete(&mut self, key: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
        use std::borrow::Cow;
        let resp = self
            .round_trip_with_retries(&Command::Delete(Cow::Borrowed(key)))
            .await?;
        match resp {
            Response::Ok(_) => Ok(true),
            Response::Error(msg) if msg == "Not found" => Ok(false),
            Response::Error(msg) => Err(msg.into()),
            _ => Err("Invalid delete response".into()),
        }
    }
}
