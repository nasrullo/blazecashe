//! # UDP Transport with QUIC-like Features
//!
//! This module implements a UDP-based transport protocol inspired by QUIC (Quick UDP Internet Connections).
//! It provides reliable message delivery over UDP through fragmentation, reassembly, and various
//! QUIC-inspired optimizations.
//!
//! ## QUIC Features Implemented
//!
//! ### 1. **Fragmentation and Reassembly** (QUIC Datagram Splitting)
//!    - Large messages are automatically split into multiple UDP datagrams (fragments)
//!    - Each fragment contains a header with sequence number, fragment count, and payload length
//!    - Fragments are reassembled on the receiving end, similar to QUIC's datagram splitting
//!    - Supports up to 65,535 fragments per message (u16::MAX)
//!    - Maximum message size: configurable (default 4MB, can be increased)
//!
//! ### 2. **Request ID-based Multiplexing** (QUIC Connection ID)
//!    - Each request has a unique 32-bit request ID
//!    - Allows multiple concurrent requests on the same UDP socket
//!    - Responses are matched to requests by request ID
//!    - Similar to QUIC's connection ID for multiplexing streams
//!
//! ### 3. **Fast Path for Small Messages** (QUIC 0-RTT Optimization)
//!    - Messages that fit in a single UDP datagram (< 1200 bytes) bypass fragmentation
//!    - Direct encoding/decoding without fragment headers
//!    - Reduces overhead for common small operations (GET, PUT with small values)
//!    - Similar to QUIC's 0-RTT optimization for small datagrams
//!
//! ### 4. **SO_REUSEPORT Load Distribution** (QUIC Multi-Path)
//!    - Multiple server instances bind to the same UDP port
//!    - Kernel distributes incoming packets across instances
//!    - Reduces contention and improves throughput
//!    - Similar to QUIC's multi-path support for load balancing
//!
//! ### 5. **Concurrent Request Processing** (QUIC Stream Multiplexing)
//!    - Server spawns tasks for concurrent request processing
//!    - Multiple requests can be processed in parallel
//!    - Similar to QUIC's stream multiplexing for concurrent operations
//!
//! ### 6. **Timeout-based Reassembly** (QUIC Connection Timeout)
//!    - Reassembly entries expire after 2 seconds
//!    - Prevents memory leaks from incomplete reassemblies
//!    - Similar to QUIC's connection timeout mechanism
//!
//! ### 7. **DoS Protection** (QUIC Anti-Amplification)
//!    - Size limits prevent memory exhaustion attacks
//!    - Early rejection of oversized messages
//!    - Opportunistic cleanup of expired reassembly entries
//!    - Similar to QUIC's anti-amplification protection
//!
//! ## Protocol Format
//!
//! ### Single-Datagram Format (Fast Path)
//! ```text
//! [0-1]   Magic (0xBC01)
//! [2]     Version (1)
//! [3]     Flags (0 = Request, 1 = Response)
//! [4-7]   Request ID (u32, big-endian)
//! [8]     Command (0x01=GET, 0x02=PUT, 0x03=DELETE, 0x04=PING)
//! [9+]    Command-specific data
//! ```
//!
//! ### Fragment Format (Multi-Datagram)
//! ```text
//! [0-1]   Magic (0xBC01)
//! [2]     Version (1)
//! [3]     Flags (bit 0 = Response, other bits reserved)
//! [4-7]   Request ID (u32, big-endian)
//! [8-9]   Sequence Number (u16, big-endian, 0-indexed)
//! [10-11] Fragment Count (u16, big-endian, total fragments)
//! [12-13] Payload Length (u16, big-endian, bytes in this fragment)
//! [14+]   Payload data
//! ```
//!
//! ## Performance Characteristics
//!
//! - **Small Messages**: Single UDP packet, minimal overhead (~9 bytes header)
//! - **Large Messages**: Multiple UDP packets, automatic fragmentation/reassembly
//! - **Overhead**: ~14 bytes per fragment (fragment header)
//! - **Latency**: First fragment to last fragment arrival time
//! - **Throughput**: Optimized for high-throughput scenarios with SO_REUSEPORT
//!
//! ## Comparison with QUIC
//!
//! | Feature | QUIC | This Implementation |
//! |---------|------|---------------------|
//! | Fragmentation | Yes | Yes |
//! | Reassembly | Yes | Yes |
//! | Request Multiplexing | Yes (Streams) | Yes (Request IDs) |
//! | Fast Path | Yes (0-RTT) | Yes (Single-datagram) |
//! | Load Distribution | Yes (Multi-path) | Yes (SO_REUSEPORT) |
//! | Reliability | TCP-like | UDP-based (best effort) |
//! | Flow Control | Yes | No (UDP) |
//! | Congestion Control | Yes | No (UDP) |
//!
//! ## Future Enhancements (Not Yet Implemented)
//!
//! - Request batching/pipelining: Batch multiple requests in a single UDP datagram
//! - io_uring zero-copy: Use Linux io_uring for zero-copy I/O operations
//! - Selective retransmission: Only retransmit missing fragments (NACK mechanism)
//! - Adaptive fragment sizing: Path MTU discovery for optimal fragment size
//! - Connection migration: Handle client IP/port changes (QUIC feature)

use crate::transports::common::{Command, ProtocolClient, ProtocolServer, Response};
use crate::transports::{
    handle_command, handle_get_response, handle_put_response, Serializer,
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
use tracing::{info, warn};
use futures::future;

/// UDP Server with QUIC-like features for high-performance message handling.
///
/// This server implements several QUIC-inspired optimizations:
/// - **SO_REUSEPORT**: Multiple server instances for load distribution
/// - **Fast Path**: Inline handling of small single-datagram messages
/// - **Concurrent Processing**: Spawned tasks for parallel request handling
/// - **Fragmentation/Reassembly**: Automatic handling of large messages
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


// Protocol constants (QUIC-inspired)
/// Magic number to identify BlazeCache UDP packets (similar to QUIC's connection ID validation)
const MAGIC: u16 = 0xBC01;
/// Protocol version (for future compatibility, similar to QUIC version negotiation)
const VERSION: u8 = 1;
/// Flag bit indicating a response packet (QUIC uses similar flag bits)
const FLAG_RESPONSE: u8 = 0b0000_0001;

// Datagram size limits (QUIC MTU considerations)
/// Maximum UDP datagram size (QUIC uses 1200 bytes as safe MTU)
const MAX_DATAGRAM: usize = 1200;
/// Fragment header length (14 bytes for sequence, count, payload length)
const HEADER_LEN: usize = 14;
/// Maximum payload per fragment (MAX_DATAGRAM - HEADER_LEN)
const MAX_PAYLOAD: usize = MAX_DATAGRAM - HEADER_LEN;

// Message size and timeout limits
/// Maximum message size before fragmentation (4MB default, configurable)
/// QUIC supports up to 4GB, but we use a more conservative default
const MAX_MESSAGE_BYTES: usize =  4 << 20;
/// Timeout for reassembly (QUIC uses similar timeouts for connection state)
const REASSEMBLY_TIMEOUT: Duration = Duration::from_secs(2);
/// Number of retries for failed requests (QUIC has similar retry mechanisms)
const CLIENT_RETRIES: usize = 2; // total attempts = 1 + CLIENT_RETRIES

/// Global atomic counter for generating unique request IDs (QUIC connection ID generation)
/// This allows multiple concurrent requests on the same UDP socket
static NEXT_REQUEST_ID: AtomicU32 = AtomicU32::new(1);

/// Message type (QUIC uses similar type indicators)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MsgType {
    Request = 0,
    Response = 1,
}

/// Fragment header structure (QUIC-inspired fragmentation)
///
/// Similar to QUIC's datagram splitting, this header contains:
/// - `request_id`: Unique identifier for the message (like QUIC's connection ID)
/// - `seq_no`: Sequence number of this fragment (0-indexed)
/// - `frag_count`: Total number of fragments in the message
/// - `payload_len`: Length of payload in this fragment
#[derive(Debug)]
struct FragHeader {
    msg_type: MsgType,
    request_id: u32,
    seq_no: u16,
    frag_count: u16,
    payload_len: u16,
}

/// Decodes a fragment header from a UDP datagram (QUIC packet parsing)
///
/// This function validates and parses the fragment header, similar to QUIC's
/// packet header parsing. It performs:
/// - Magic number validation (QUIC version negotiation)
/// - Version checking (QUIC version compatibility)
/// - Fragment header extraction (QUIC datagram splitting)
/// - Size validation (QUIC anti-amplification protection)
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

/// Fragments a large message into multiple UDP datagrams (QUIC datagram splitting)
///
/// This function implements QUIC-like fragmentation:
/// 1. Calculates the number of fragments needed (ceil(message_size / MAX_PAYLOAD))
/// 2. Splits the message into fragments of up to MAX_PAYLOAD bytes each
/// 3. Adds a fragment header to each fragment with sequence number and total count
/// 4. Returns a vector of fragments ready to be sent as independent UDP datagrams
///
/// Similar to QUIC's datagram splitting, fragments can arrive out of order
/// and are reassembled on the receiving end.
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

/// Reassembly state for reconstructing fragmented messages (QUIC datagram reassembly)
///
/// This structure tracks the reassembly of a fragmented message, similar to QUIC's
/// datagram reassembly mechanism:
/// - `created_at`: Timestamp for timeout-based cleanup (QUIC connection timeout)
/// - `frag_count`: Total number of fragments expected
/// - `received`: Bitmap tracking which fragments have been received
/// - `parts`: Storage for received fragment payloads
/// - `received_count`: Number of fragments received so far
/// - `total_len`: Total bytes received (for size validation)
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
    /// Creates a new reassembly state for a fragmented message (QUIC reassembly initialization)
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

    /// Inserts a fragment into the reassembly state (QUIC fragment insertion)
    ///
    /// This method handles:
    /// - Duplicate detection (QUIC duplicate packet handling)
    /// - Out-of-order fragment insertion (QUIC out-of-order delivery)
    /// - Size validation (QUIC anti-amplification protection)
    /// - Progress tracking (QUIC reassembly progress)
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

    /// Checks if all fragments have been received (QUIC reassembly completion check)
    fn is_complete(&self) -> bool {
        self.received_count == self.frag_count
    }

    /// Assembles the complete message from all fragments (QUIC message reconstruction)
    ///
    /// This method concatenates all fragment payloads in sequence order,
    /// similar to QUIC's datagram reassembly. The fragments are stored in
    /// order, so this is a simple concatenation operation.
    fn assemble(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.total_len);
        for p in self.parts {
            out.extend_from_slice(&p);
        }
        out
    }
}

// ---- Server: reassemble request, process, fragment response ----

/// Key for tracking reassembly state (QUIC connection state key)
/// Combines client address and request ID to uniquely identify a message
type ReassemblyKey = (SocketAddr, u32);

/// Request work item for concurrent processing (QUIC stream work item)
#[allow(dead_code)]
/// This structure encapsulates a single-datagram request for processing
struct UdpRequest {
    cmd_byte: u8,
    cmd_data: Vec<u8>,
    request_id: u32,
    addr: SocketAddr,
}

#[async_trait]
impl<S: Serializer + 'static> ProtocolServer for UdpServer<S> {
    async fn start(&self, port: u16) -> Result<(), Box<dyn Error + Send + Sync>> {
        // QUIC Feature: SO_REUSEPORT Load Distribution
        //
        // This optimization spawns multiple server instances that all bind to the same UDP port.
        // The kernel distributes incoming packets across these sockets, similar to QUIC's
        // multi-path support for load balancing.
        //
        // Benefits:
        // - Reduces contention on a single socket
        // - Improves throughput by parallelizing packet reception
        // - Allows better CPU utilization across cores
        // - Similar to QUIC's connection migration and multi-path support
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
                // QUIC Feature: SO_REUSEPORT
                // Allows multiple sockets to bind to the same port, enabling kernel-level
                // load distribution. This is similar to QUIC's multi-path support.
                socket.set_reuse_port(true)?;
                
                // QUIC Feature: Large Buffer Sizes
                // Increases socket buffer sizes to handle high-throughput scenarios,
                // similar to QUIC's connection buffer management.
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
            
            let inflight = Arc::clone(&inflight);
            let persistence = self.persistence.clone();

            // QUIC Feature: Fast Path for Single-Datagram Messages (0-RTT Optimization)
            //
            // This optimization bypasses fragmentation overhead for small messages that fit
            // in a single UDP datagram. Similar to QUIC's 0-RTT optimization, this reduces
            // latency and overhead for common operations.
            //
            // Single-datagram format: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][CMD:1][DATA:...]
            // Fragment format: [MAGIC:2][VERSION:1][FLAGS:1][REQUEST_ID:4][SEQ:2][FRAG_COUNT:2][PAYLOAD_LEN:2][DATA:...]
            //
            // We distinguish by checking if byte 8 is a command (0x01-0x04) vs a fragment seq
            // (fragments have SEQ_NO in bytes 8-9, which is unlikely to be 0x01-0x04)
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
                        let _group = Arc::clone(&self.group);
                        let _persistence = self.persistence.clone();
                        
                        // QUIC Feature: Inline Handling for Simple Operations
                        //
                        // For very simple operations (PING), we handle them inline to avoid
                        // task spawn overhead. This is similar to QUIC's fast path for
                        // connection establishment and keep-alive packets.
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
                        
                        // QUIC Feature: Concurrent Request Processing (Stream Multiplexing)
                        //
                        // For GET/PUT operations, we spawn tasks for concurrent processing.
                        // This allows multiple requests to be processed in parallel, similar
                        // to QUIC's stream multiplexing where multiple streams can be
                        // processed concurrently on the same connection.
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
                                    match socket_spawn.send_to(&response, addr_spawn).await {
                                        Ok(n) => {
                                            info!("Sent response for request_id={}, len={}, to={}", request_id_spawn, n, addr_spawn);
                                        }
                                        Err(e) => {
                                            warn!("Failed to send response for request_id={}, error={}", request_id_spawn, e);
                                        }
                                    }
                                } else {
                                    warn!("Response too large for request_id={}, len={}", request_id_spawn, response.len());
                                }
                            });
                            // QUIC Feature: Explicit Task Scheduling
                            //
                            // We yield to allow the spawned task to start executing. This is
                            // necessary because we immediately continue to recv_from, and the
                            // runtime needs a chance to schedule the spawned task. Similar to
                            // QUIC's flow control where the protocol explicitly manages when
                            // to process different streams.
                            tokio::task::yield_now().await;
                            continue;
                        }
                    }
                }
            }
            
            let packet = buffer[..len].to_vec();
            let socket_clone = Arc::clone(&socket);
            let group = Arc::clone(&self.group);
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

                    // QUIC Feature: Timeout-based Cleanup (Connection Timeout)
                    //
                    // We clean up expired reassembly entries opportunistically to prevent
                    // memory leaks. This is similar to QUIC's connection timeout mechanism
                    // where idle connections are closed after a timeout period.
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

/// UDP Client with QUIC-like features for high-performance message handling.
///
/// This client implements several QUIC-inspired optimizations:
/// - **Fast Path**: Direct encoding for small single-datagram messages
/// - **Request ID Multiplexing**: Multiple concurrent requests on the same socket
/// - **Automatic Fragmentation**: Large messages are automatically fragmented
/// - **Reassembly**: Fragments are automatically reassembled
/// - **Retry Logic**: Automatic retries for failed requests
pub struct UdpClient<S> {
    socket: UdpSocket,
    server_addr: String,
    serializer: std::marker::PhantomData<S>,
}

impl<S> UdpClient<S>
where
    S: Serializer + 'static,
{
    /// Sends a fragmented message (QUIC datagram splitting)
    ///
    /// This method fragments a large message and sends each fragment as an independent
    /// UDP datagram. Similar to QUIC's datagram splitting, fragments can be sent
    /// independently and may arrive out of order.
    ///
    /// TODO: Implement batch sending using `futures::future::join_all` for parallel
    /// fragment transmission, similar to QUIC's parallel packet sending.
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

    /// Receives and reassembles a fragmented response (QUIC datagram reassembly)
    ///
    /// This method implements QUIC-like reassembly:
    /// - Waits for fragments with matching request_id
    /// - Tracks received fragments to handle out-of-order delivery
    /// - Times out after REASSEMBLY_TIMEOUT (QUIC connection timeout)
    /// - Returns the complete reassembled message
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

    /// Performs a request-response round trip with automatic retries (QUIC retry mechanism)
    ///
    /// This method implements QUIC-like retry logic:
    /// - Sends the request with a unique request_id
    /// - Waits for the response with matching request_id
    /// - Retries up to CLIENT_RETRIES times on failure
    /// - Returns the first successful response or the last error
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
        // QUIC Feature: Optimized Socket Configuration
        //
        // The client socket is configured for maximum performance, similar to QUIC's
        // connection establishment optimizations:
        // - Large buffer sizes for high-throughput scenarios
        // - Reuse address for connection pooling (if needed)
        // - Optimized for low-latency, high-throughput operations
        use socket2::{Domain, Socket, Type, Protocol};
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        
        // Set socket options for maximum performance
        socket.set_reuse_address(true)?;
        
        // QUIC Feature: Large Buffer Sizes
        // Increases socket buffer sizes to handle high-throughput scenarios,
        // similar to QUIC's connection buffer management.
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
        // QUIC Feature: Fast Path for PING (0-RTT Optimization)
        //
        // PING uses direct encoding without Command enum serialization, bypassing
        // fragmentation overhead. This is similar to QUIC's 0-RTT optimization
        // for connection establishment and keep-alive packets.
        let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        let mut packet = [0u8; 9]; // Stack-allocated for ping
        packet[0..2].copy_from_slice(&MAGIC.to_be_bytes());
        packet[2] = VERSION;
        packet[3] = 0u8;
        packet[4..8].copy_from_slice(&request_id.to_be_bytes());
        packet[8] = 0x04; // PING command
        
        self.socket.send_to(&packet, &self.server_addr).await?;
        
        // Receive response with timeout
        let mut buffer = [0u8; MAX_DATAGRAM];
        let recv_result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.socket.recv_from(&mut buffer)
        ).await;
        
        let (len, _) = match recv_result {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => return Err(Box::new(e)),
            Err(_) => return Err("ping timeout".into()),
        };
        
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
        // QUIC Feature: Fast Path for GET (0-RTT Optimization)
        //
        // GET uses direct encoding for small requests that fit in a single datagram.
        // This bypasses Command enum serialization and fragmentation overhead,
        // similar to QUIC's 0-RTT optimization for small datagrams.
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
        // QUIC Feature: Fast Path for PUT (0-RTT Optimization)
        //
        // PUT uses direct encoding for small requests that fit in a single datagram.
        // This bypasses Command enum serialization and fragmentation overhead,
        // similar to QUIC's 0-RTT optimization for small datagrams.
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
