// Blocking I/O client implementation (similar to Go's blocking I/O with goroutines)
// This should match Go's performance by avoiding async runtime overhead

use std::net::TcpStream;
use std::io::{Read, Write, Error as IOError};
use std::sync::{Arc, atomic::{AtomicPtr, AtomicU32, Ordering}};
use std::collections::HashMap;
use dashmap::DashMap;
use crossbeam::queue::SegQueue;
use crate::{ClientError, SelectionStrategy, ClientConsistentHash, build_ring};
use blazecache::serializers::BinarySerializer;
use blazecache::transports::common::{Command, Response};
use blazecache::transports::Serializer;

// Server selection snapshot for lock-free reads (RCU pattern)
struct ServerSelection {
    strategy: SelectionStrategy,
    servers: Vec<String>,
    hash_ring: Option<ClientConsistentHash>,
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
    // Connection pooling (lock-free using DashMap and SegQueue)
    connection_pools: Arc<DashMap<String, Arc<SegQueue<TcpStream>>>>,
    pool_counts: Arc<DashMap<String, Arc<AtomicU32>>>,
    max_pool_size: u32,
}

const MAX_POOL_SIZE: u32 = 500;

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
            connection_pools: Arc::new(DashMap::new()),
            pool_counts: Arc::new(DashMap::new()),
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
                Some(snapshot.servers[index].clone())
            }
            SelectionStrategy::ConsistentHashing => {
                if let Some(ref r) = snapshot.hash_ring {
                    if let Some(s) = r.pick_server(key) {
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
        // Fast path: try to get connection from pool (lock-free)
        if let Some(queue) = self.connection_pools.get(server) {
            // Try non-blocking pop
            if let Some(stream) = queue.value().pop() {
                return Ok(stream);
            }
            
            // Pool empty, check if we can create new connection
            let count = self.pool_counts
                .get(server)
                .map(|c| c.load(Ordering::Relaxed))
                .unwrap_or(0);
            
            if count < self.max_pool_size {
                // Try to increment counter atomically
                let pool_count = self.pool_counts
                    .entry(server.to_string())
                    .or_insert_with(|| Arc::new(AtomicU32::new(0)))
                    .clone();
                
                let current = pool_count.load(Ordering::Relaxed);
                if current < self.max_pool_size {
                    if pool_count.compare_exchange(current, current + 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                        // Successfully claimed slot, create connection
                        match Self::connect_with_nodelay(server) {
                            Ok(stream) => {
                                // Try to get from pool one more time before returning new
                                if let Some(pooled_stream) = queue.value().pop() {
                                    // Got one from pool, close new one and return pooled
                                    pool_count.fetch_sub(1, Ordering::Relaxed);
                                    return Ok(pooled_stream);
                                }
                                return Ok(stream);
                            }
                            Err(e) => {
                                pool_count.fetch_sub(1, Ordering::Relaxed);
                                return Err(ClientError::Io(e));
                            }
                        }
                    }
                }
            }
            
            // Pool full or CAS failed, try one more time
            if let Some(stream) = queue.value().pop() {
                return Ok(stream);
            }
            
            // Still nothing, create new connection (allow overflow)
            match Self::connect_with_nodelay(server) {
                Ok(stream) => {
                    let pool_count = self.pool_counts
                        .entry(server.to_string())
                        .or_insert_with(|| Arc::new(AtomicU32::new(0)))
                        .clone();
                    pool_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(stream);
                }
                Err(e) => return Err(ClientError::Io(e)),
            }
        }
        
        // Initialize pool for this server
        let queue = Arc::new(SegQueue::new());
        self.connection_pools.insert(server.to_string(), queue.clone());
        let pool_count = Arc::new(AtomicU32::new(0));
        self.pool_counts.insert(server.to_string(), pool_count.clone());
        
        // Create new connection
        match Self::connect_with_nodelay(server) {
            Ok(stream) => {
                pool_count.fetch_add(1, Ordering::Relaxed);
                Ok(stream)
            }
            Err(e) => Err(ClientError::Io(e)),
        }
    }
    
    fn return_connection(&self, server: &str, stream: TcpStream) {
        if let Some(queue) = self.connection_pools.get(server) {
            // Push to queue (lock-free, always succeeds)
            queue.value().push(stream);
        }
    }
    
    fn mark_connection_dead(&self, server: &str) {
        if let Some(count) = self.pool_counts.get(server) {
            count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ClientError> {
        let server = self.select_server(key).ok_or(ClientError::Protocol("No servers available".into()))?;
        
        let mut stream = self.get_or_create_connection(&server)?;
        let mut should_return = true;
        
        let request = <BinarySerializer as Serializer>::serialize_command(&Command::Get(key.to_string()));
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
        
        let request = <BinarySerializer as Serializer>::serialize_command(&Command::Put(key.to_string(), value, ttl_secs));
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

