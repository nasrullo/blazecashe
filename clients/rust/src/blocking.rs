// Blocking I/O client implementation (similar to Go's blocking I/O with goroutines)
// This should match Go's performance by avoiding async runtime overhead

use std::net::TcpStream;
use std::io::{Read, Write, Error as IOError};
use std::sync::{Arc, atomic::{AtomicPtr, AtomicI32, Ordering}};
use dashmap::DashMap;
use crossbeam_channel;
use crate::{ClientError, SelectionStrategy, ClientConsistentHash, build_ring};

// Optimized request encoding (bypasses Command enum to avoid allocations)
fn encode_get_request(key: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 2 + key.len());
    buf.push(0x01); // GET command
    buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
    buf.extend_from_slice(key.as_bytes());
    buf
}

fn encode_put_request(key: &str, value: &[u8], ttl: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 2 + key.len() + 4 + value.len() + 4);
    buf.push(0x02); // PUT command
    buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
    buf.extend_from_slice(key.as_bytes());
    buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
    buf.extend_from_slice(value);
    buf.extend_from_slice(&ttl.to_be_bytes());
    buf
}

fn encode_delete_request(key: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 2 + key.len());
    buf.push(0x03); // DELETE command
    buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
    buf.extend_from_slice(key.as_bytes());
    buf
}

// Server selection snapshot for lock-free reads (RCU pattern)
struct ServerSelection {
    strategy: SelectionStrategy,
    servers: Vec<String>,
    hash_ring: Option<ClientConsistentHash>,
}

// Pool structure to combine sender/receiver/count (reduces DashMap lookups)
struct ConnectionPool {
    sender: crossbeam_channel::Sender<TcpStream>,
    receiver: crossbeam_channel::Receiver<TcpStream>,
    count: Arc<AtomicI32>,
}

pub struct BlockingTcpClient {
    // Protected by RwLock for writes (strategy changes, peer discovery)
    servers: Arc<std::sync::RwLock<Vec<String>>>,
    strategy: Arc<std::sync::RwLock<SelectionStrategy>>,
    hash_ring: Arc<std::sync::RwLock<Option<crate::ClientConsistentHash>>>,
    seed: Option<String>,
    refresh_secs: Option<u64>,
    // Lock-free reads using RCU pattern
    selection: Arc<AtomicPtr<ServerSelection>>,
    current_index: Arc<std::sync::atomic::AtomicUsize>,
    // Connection pooling (optimized: single DashMap lookup gets all pool info)
    pools: Arc<DashMap<String, ConnectionPool>>,
    max_pool_size: i32,
}

const MAX_POOL_SIZE: i32 = 500;

impl BlockingTcpClient {
    pub fn new(servers: Vec<String>) -> Self {
        let strategy = SelectionStrategy::RoundRobin;
        let ring = build_ring(&servers, &strategy);
        let client = Self {
            servers: Arc::new(std::sync::RwLock::new(servers.clone())),
            strategy: Arc::new(std::sync::RwLock::new(strategy)),
            current_index: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            hash_ring: Arc::new(std::sync::RwLock::new(ring)),
            seed: None,
            refresh_secs: None,
            selection: Arc::new(AtomicPtr::new(std::ptr::null_mut())),
            pools: Arc::new(DashMap::new()),
            max_pool_size: MAX_POOL_SIZE,
        };
        // Initialize snapshot
        client.update_selection_snapshot();
        client
    }

    fn update_selection_snapshot(&self) {
        let servers = self.servers.read().unwrap().clone();
        let strategy = self.strategy.read().unwrap().clone();
        let hash_ring = self.hash_ring.read().unwrap().clone();
        
        let snapshot = Box::into_raw(Box::new(ServerSelection {
            strategy,
            servers,
            hash_ring,
        }));
        
        let old = self.selection.swap(snapshot, Ordering::Release);
        if !old.is_null() {
            unsafe {
                let _ = Box::from_raw(old);
            }
        }
    }

    fn select_server(&self, key: &str) -> Option<String> {
        // Lock-free read using RCU pattern
        let snapshot_ptr = self.selection.load(Ordering::Acquire);
        
        if snapshot_ptr.is_null() {
            // Fallback to locked read if snapshot not initialized
            return self.select_server_locked(key);
        }
        
        let snapshot = unsafe { &*snapshot_ptr };
        
        match &snapshot.strategy {
            SelectionStrategy::RoundRobin => {
                if snapshot.servers.is_empty() {
                    return None;
                }
                let index = self.current_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % snapshot.servers.len();
                // Clone is necessary but snapshot is stable (RCU pattern)
                Some(snapshot.servers[index].clone())
            }
            SelectionStrategy::ConsistentHashing => {
                if let Some(ref r) = snapshot.hash_ring {
                    if let Some(s) = r.pick_server(key) {
                        // pick_server returns &str, convert to String
                        return Some(s.to_string());
                    }
                }
                // Fallback if ring empty
                if snapshot.servers.is_empty() {
                    None
                } else {
                    let idx = self.current_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % snapshot.servers.len();
                    Some(snapshot.servers[idx].clone())
                }
            }
        }
    }

    fn select_server_locked(&self, key: &str) -> Option<String> {
        let strategy = self.strategy.read().unwrap().clone();
        match strategy {
            SelectionStrategy::RoundRobin => {
                let servers = self.servers.read().unwrap();
                if servers.is_empty() {
                    return None;
                }
                let index = self.current_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % servers.len();
                Some(servers[index].clone())
            }
            SelectionStrategy::ConsistentHashing => {
                let ring = self.hash_ring.read().unwrap();
                if let Some(ref r) = *ring {
                    if let Some(s) = r.pick_server(key) {
                        return Some(s.to_string());
                    }
                }
                // Fallback if ring empty
                let servers = self.servers.read().unwrap();
                if servers.is_empty() {
                    None
                } else {
                    let idx = self.current_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % servers.len();
                    Some(servers[idx].clone())
                }
            }
        }
    }

    fn connect_with_nodelay(addr: &str) -> Result<TcpStream, IOError> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }

    fn get_or_create_connection(&self, server: &str) -> Result<TcpStream, ClientError> {
        // Optimized: single DashMap lookup gets all pool info (like Go's single sync.Map.Load)
        // First try to get existing pool
        if let Some(pool) = self.pools.get(server) {
            // Pool exists - try non-blocking receive (like Go's select with default)
            match pool.receiver.try_recv() {
                Ok(stream) => return Ok(stream),
                Err(_) => {
                    // Channel empty, check if we can create new connection
                    let current = pool.count.load(Ordering::Relaxed);
                    if current < self.max_pool_size {
                        // Try to increment counter atomically (like Go's CAS)
                        if pool.count.compare_exchange(current, current + 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                            // Successfully claimed slot, create connection
                            match Self::connect_with_nodelay(server) {
                                Ok(stream) => {
                                    // Try channel one more time before returning new
                                    match pool.receiver.try_recv() {
                                        Ok(pooled_stream) => {
                                            // Got one from pool, close new one and return pooled
                                            pool.count.fetch_sub(1, Ordering::Relaxed);
                                            let _ = stream.shutdown(std::net::Shutdown::Both);
                                            return Ok(pooled_stream);
                                        }
                                        Err(_) => return Ok(stream),
                                    }
                                }
                                Err(e) => {
                                    pool.count.fetch_sub(1, Ordering::Relaxed);
                                    return Err(ClientError::Io(e));
                                }
                            }
                        }
                        // CAS failed, try channel again
                        match pool.receiver.try_recv() {
                            Ok(stream) => return Ok(stream),
                            Err(_) => {
                                // Still nothing, create new connection (allow overflow)
                                match Self::connect_with_nodelay(server) {
                                    Ok(stream) => {
                                        pool.count.fetch_add(1, Ordering::Relaxed);
                                        return Ok(stream);
                                    }
                                    Err(e) => return Err(ClientError::Io(e)),
                                }
                            }
                        }
                    } else {
                        // Pool at max size, try channel one more time
                        match pool.receiver.try_recv() {
                            Ok(stream) => return Ok(stream),
                            Err(_) => {
                                // Still no connection, create new one (allow overflow)
                                match Self::connect_with_nodelay(server) {
                                    Ok(stream) => {
                                        pool.count.fetch_add(1, Ordering::Relaxed);
                                        return Ok(stream);
                                    }
                                    Err(e) => return Err(ClientError::Io(e)),
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // Pool doesn't exist - initialize it (like Go's LoadOrStore)
        let (sender, receiver) = crossbeam_channel::bounded(self.max_pool_size as usize);
        let count = Arc::new(AtomicI32::new(0));
        let pool = ConnectionPool {
            sender,
            receiver,
            count,
        };
        
        // Use entry API to ensure only one thread initializes
        let pool_ref = self.pools
            .entry(server.to_string())
            .or_insert(pool);
        
        // Try channel one more time
        match pool_ref.receiver.try_recv() {
            Ok(stream) => return Ok(stream),
            Err(_) => {
                // Create new connection
                match Self::connect_with_nodelay(server) {
                    Ok(stream) => {
                        pool_ref.count.fetch_add(1, Ordering::Relaxed);
                        return Ok(stream);
                    }
                    Err(e) => return Err(ClientError::Io(e)),
                }
            }
        }
    }
    
    fn return_connection(&self, server: &str, stream: TcpStream) {
        // Optimized: single DashMap lookup
        if let Some(pool) = self.pools.get(server) {
            // Try non-blocking send first (like Go's select with default)
            match pool.sender.try_send(stream) {
                Ok(_) => {
                    // Successfully returned to pool (lock-free)
                    return;
                }
                Err(crossbeam_channel::TrySendError::Full(stream)) => {
                    // Channel full, close connection
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                    // Decrement counter atomically (lock-free)
                    pool.count.fetch_sub(1, Ordering::Relaxed);
                }
                Err(crossbeam_channel::TrySendError::Disconnected(stream)) => {
                    // Channel disconnected, close connection
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                    // Decrement counter atomically (lock-free)
                    pool.count.fetch_sub(1, Ordering::Relaxed);
                }
            }
        } else {
            // Pool doesn't exist, just close the connection
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }
    
    fn mark_connection_dead(&self, server: &str) {
        // Optimized: single DashMap lookup
        if let Some(pool) = self.pools.get(server) {
            pool.count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ClientError> {
        let server = self.select_server(key).ok_or(ClientError::Protocol("No servers available".into()))?;
        
        let mut stream = self.get_or_create_connection(&server)?;
        let mut should_return = true;
        
        // Optimized: encode directly without Command enum allocation
        let request = encode_get_request(key);
        let result = match stream.write_all(&request) {
            Err(e) => {
                should_return = false;
                self.mark_connection_dead(&server);
                Err(ClientError::Io(e))
            }
            Ok(_) => {
                // Read status byte first (like Go client with io.ReadFull)
                let mut status_buf = [0u8; 1];
                match stream.read_exact(&mut status_buf) {
                    Err(e) => {
                        should_return = false;
                        self.mark_connection_dead(&server);
                        Err(ClientError::Io(e))
                    }
                    Ok(_) => {
                        let status = status_buf[0];
                        match status {
                            0x00 => {
                                // OK - read data length and data
                                let mut len_buf = [0u8; 4];
                                match stream.read_exact(&mut len_buf) {
                                    Err(e) => {
                                        should_return = false;
                                        self.mark_connection_dead(&server);
                                        Err(ClientError::Io(e))
                                    }
                                    Ok(_) => {
                                        let data_len = u32::from_be_bytes(len_buf) as usize;
                                        if data_len == 0 {
                                            Ok(Some(Vec::new()))
                                        } else {
                                            let mut data = vec![0u8; data_len];
                                            match stream.read_exact(&mut data) {
                                                Err(e) => {
                                                    should_return = false;
                                                    self.mark_connection_dead(&server);
                                                    Err(ClientError::Io(e))
                                                }
                                                Ok(_) => Ok(Some(data))
                                            }
                                        }
                                    }
                                }
                            }
                            0x01 => {
                                // ERROR - read message length and message
                                let mut msg_len_buf = [0u8; 2];
                                match stream.read_exact(&mut msg_len_buf) {
                                    Err(e) => {
                                        should_return = false;
                                        self.mark_connection_dead(&server);
                                        Err(ClientError::Io(e))
                                    }
                                    Ok(_) => {
                                        let msg_len = u16::from_be_bytes(msg_len_buf) as usize;
                                        if msg_len == 0 {
                                            Ok(None)
                                        } else {
                                            let mut msg_bytes = vec![0u8; msg_len];
                                            match stream.read_exact(&mut msg_bytes) {
                                                Err(e) => {
                                                    should_return = false;
                                                    self.mark_connection_dead(&server);
                                                    Err(ClientError::Io(e))
                                                }
                                                Ok(_) => {
                                                    // Optimize: check "not found" case-insensitively
                                                    let msg_lower: Vec<u8> = msg_bytes.iter().map(|&b| b.to_ascii_lowercase()).collect();
                                                    if msg_lower.windows(9).any(|w| w == b"not found") {
                                                        Ok(None)
                                                    } else {
                                                        let msg = String::from_utf8_lossy(&msg_bytes);
                                                        Err(ClientError::Protocol(msg.to_string()))
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {
                                should_return = false;
                                self.mark_connection_dead(&server);
                                Err(ClientError::Protocol(format!("Unknown status: {}", status)))
                            }
                        }
                    }
                }
            }
        };
        
        // Return connection to pool if it's still good
        if should_return {
            self.return_connection(&server, stream);
        }
        
        result
    }

    pub fn set(&self, key: &str, value: Vec<u8>) -> Result<(), ClientError> {
        self.set_with_ttl(key, value, 0)
    }

    pub fn set_with_ttl(&self, key: &str, value: Vec<u8>, ttl_secs: u32) -> Result<(), ClientError> {
        let server = self.select_server(key).ok_or(ClientError::Protocol("No servers available".into()))?;
        
        let mut stream = self.get_or_create_connection(&server)?;
        let mut should_return = true;
        
        // Optimized: encode directly without Command enum allocation
        let request = encode_put_request(key, &value, ttl_secs);
        let write_result = stream.write_all(&request);
        
        let result = match write_result {
            Err(e) => {
                should_return = false;
                self.mark_connection_dead(&server);
                Err(ClientError::Io(e))
            }
            Ok(_) => {
                // Read status byte first (like Go client with io.ReadFull)
                let mut status_buf = [0u8; 1];
                match stream.read_exact(&mut status_buf) {
                    Err(e) => {
                        should_return = false;
                        self.mark_connection_dead(&server);
                        Err(ClientError::Io(e))
                    }
                    Ok(_) => {
                        let status = status_buf[0];
                        match status {
                            0x00 => {
                                // OK - read data length (should be 0 for PUT success)
                                let mut len_buf = [0u8; 4];
                                match stream.read_exact(&mut len_buf) {
                                    Err(e) => {
                                        should_return = false;
                                        self.mark_connection_dead(&server);
                                        Err(ClientError::Io(e))
                                    }
                                    Ok(_) => {
                                        let data_len = u32::from_be_bytes(len_buf) as usize;
                                        if data_len > 0 {
                                            // Read and discard data
                                            let mut discard = vec![0u8; data_len];
                                            match stream.read_exact(&mut discard) {
                                                Err(e) => {
                                                    should_return = false;
                                                    self.mark_connection_dead(&server);
                                                    Err(ClientError::Io(e))
                                                }
                                                Ok(_) => Ok(())
                                            }
                                        } else {
                                            Ok(())
                                        }
                                    }
                                }
                            }
                            0x01 => {
                                // ERROR - read message (connection is still good for protocol errors)
                                let mut msg_len_buf = [0u8; 2];
                                match stream.read_exact(&mut msg_len_buf) {
                                    Err(e) => {
                                        should_return = false;
                                        self.mark_connection_dead(&server);
                                        Err(ClientError::Io(e))
                                    }
                                    Ok(_) => {
                                        let msg_len = u16::from_be_bytes(msg_len_buf) as usize;
                                        if msg_len > 0 {
                                            let mut msg_bytes = vec![0u8; msg_len];
                                            match stream.read_exact(&mut msg_bytes) {
                                                Err(e) => {
                                                    should_return = false;
                                                    self.mark_connection_dead(&server);
                                                    Err(ClientError::Io(e))
                                                }
                                                Ok(_) => {
                                                    let msg = String::from_utf8_lossy(&msg_bytes);
                                                    Err(ClientError::Protocol(msg.to_string()))
                                                }
                                            }
                                        } else {
                                            Err(ClientError::Protocol("set failed".into()))
                                        }
                                    }
                                }
                            }
                            _ => {
                                should_return = false;
                                self.mark_connection_dead(&server);
                                Err(ClientError::Protocol(format!("Unexpected status: {}", status)))
                            }
                        }
                    }
                }
            }
        };
        
        // Return connection to pool if it's still good
        if should_return {
            self.return_connection(&server, stream);
        }
        
        result
    }
}
