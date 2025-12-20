use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tokio::time::Duration;
use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicPtr, AtomicU32, Ordering}};
use std::io::Error as IOError;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::error::Error as STDError;
use dashmap::DashMap;
use crossbeam::queue::SegQueue;

use blazecache::serializers::BinarySerializer;
use blazecache::transports::common::{Command, Response};
use blazecache::transports::Serializer;

#[derive(Debug)]
pub enum ClientError {
    Io(IOError),
    Protocol(String),
    NotFound,
    Timeout,
}

impl From<IOError> for ClientError {
    fn from(err: IOError) -> Self {
        ClientError::Io(err)
    }
}

impl Display for ClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            ClientError::Io(err) => write!(f, "IO error: {}", err),
            ClientError::Protocol(msg) => write!(f, "Protocol error: {}", msg),
            ClientError::NotFound => write!(f, "Key not found"),
            ClientError::Timeout => write!(f, "Operation timeout"),
        }
    }
}

impl STDError for ClientError {}

#[derive(Clone)]
pub enum SelectionStrategy {
    RoundRobin,
    ConsistentHashing,
}

// Server selection snapshot for lock-free reads (RCU pattern)
struct ServerSelection {
    strategy: SelectionStrategy,
    servers: Vec<String>,
    hash_ring: Option<ClientConsistentHash>,
}

pub struct TcpClient {
    // Protected by RwLock for writes (strategy changes, peer discovery)
    servers: Arc<RwLock<Vec<String>>>,
    strategy: Arc<RwLock<SelectionStrategy>>,
    hash_ring: Arc<RwLock<Option<ClientConsistentHash>>>,
    seed: Option<String>,
    refresh_secs: Option<u64>,
    // Lock-free reads using RCU pattern
    selection: Arc<AtomicPtr<ServerSelection>>,
    current_index: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    // Connection pooling (lock-free using DashMap and SegQueue)
    connection_pools: Arc<DashMap<String, Arc<SegQueue<TcpStream>>>>,
    pool_counts: Arc<DashMap<String, Arc<AtomicU32>>>,
    max_pool_size: u32,
}

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_POOL_SIZE: u32 = 500;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

impl TcpClient {
    // Connect with TCP_NODELAY set (using into_std/from_std - simpler and works)
    async fn connect_with_nodelay(addr: &str) -> Result<TcpStream, IOError> {
        let stream = TcpStream::connect(addr).await?;
        // Set TCP_NODELAY - only do conversion if needed
        if let Ok(std_stream) = stream.into_std() {
            let _ = std_stream.set_nodelay(true);
            TcpStream::from_std(std_stream).map_err(|e| IOError::new(std::io::ErrorKind::Other, e))
        } else {
            // If conversion fails, return original stream (nodelay might already be set)
            Err(IOError::new(std::io::ErrorKind::Other, "Failed to convert stream"))
        }
    }

    async fn update_selection_snapshot(&self) {
        let servers = self.servers.read().await.clone();
        let strategy = self.strategy.read().await.clone();
        let hash_ring = self.hash_ring.read().await.clone();
        
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

    pub fn new(servers: Vec<String>) -> Self {
        let strategy = SelectionStrategy::RoundRobin;
        let ring = build_ring(&servers, &strategy);
        let client = Self {
            servers: Arc::new(RwLock::new(servers.clone())),
            strategy: Arc::new(RwLock::new(strategy)),
            current_index: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            hash_ring: Arc::new(RwLock::new(ring)),
            seed: None,
            refresh_secs: None,
            selection: Arc::new(AtomicPtr::new(std::ptr::null_mut())),
            connection_pools: Arc::new(DashMap::new()),
            pool_counts: Arc::new(DashMap::new()),
            max_pool_size: MAX_POOL_SIZE,
        };
        // Initialize snapshot (spawn task since we're in sync context)
        let selection_clone = Arc::clone(&client.selection);
        let servers_clone = Arc::clone(&client.servers);
        let strategy_clone = Arc::clone(&client.strategy);
        let hash_ring_clone = Arc::clone(&client.hash_ring);
        tokio::spawn(async move {
            let servers = servers_clone.read().await.clone();
            let strategy = strategy_clone.read().await.clone();
            let hash_ring = hash_ring_clone.read().await.clone();
            
            let snapshot = Box::into_raw(Box::new(ServerSelection {
                strategy,
                servers,
                hash_ring,
            }));
            
            let old = selection_clone.swap(snapshot, Ordering::Release);
            if !old.is_null() {
                unsafe {
                    let _ = Box::from_raw(old);
                }
            }
        });
        client
    }

    pub fn with_strategy(servers: Vec<String>, strategy: SelectionStrategy) -> Self {
        let ring = build_ring(&servers, &strategy);
        let client = Self {
            servers: Arc::new(RwLock::new(servers.clone())),
            strategy: Arc::new(RwLock::new(strategy)),
            current_index: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            hash_ring: Arc::new(RwLock::new(ring)),
            seed: None,
            refresh_secs: None,
            selection: Arc::new(AtomicPtr::new(std::ptr::null_mut())),
            connection_pools: Arc::new(DashMap::new()),
            pool_counts: Arc::new(DashMap::new()),
            max_pool_size: MAX_POOL_SIZE,
        };
        // Initialize snapshot
        let selection_clone = Arc::clone(&client.selection);
        let servers_clone = Arc::clone(&client.servers);
        let strategy_clone = Arc::clone(&client.strategy);
        let hash_ring_clone = Arc::clone(&client.hash_ring);
        tokio::spawn(async move {
            let servers = servers_clone.read().await.clone();
            let strategy = strategy_clone.read().await.clone();
            let hash_ring = hash_ring_clone.read().await.clone();
            
            let snapshot = Box::into_raw(Box::new(ServerSelection {
                strategy,
                servers,
                hash_ring,
            }));
            
            let old = selection_clone.swap(snapshot, Ordering::Release);
            if !old.is_null() {
                unsafe {
                    let _ = Box::from_raw(old);
                }
            }
        });
        client
    }

    pub fn with_discovery(seed: String, refresh_secs: u64) -> Self {
        let client = Self {
            servers: Arc::new(RwLock::new(vec![seed.clone()])),
            strategy: Arc::new(RwLock::new(SelectionStrategy::ConsistentHashing)),
            current_index: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            hash_ring: Arc::new(RwLock::new(build_ring(&[seed.clone()], &SelectionStrategy::ConsistentHashing))),
            seed: Some(seed.clone()),
            refresh_secs: Some(refresh_secs),
            selection: Arc::new(AtomicPtr::new(std::ptr::null_mut())),
            connection_pools: Arc::new(DashMap::new()),
            pool_counts: Arc::new(DashMap::new()),
            max_pool_size: MAX_POOL_SIZE,
        };
        
        // Initialize snapshot
        let selection_clone = Arc::clone(&client.selection);
        let servers_clone = Arc::clone(&client.servers);
        let strategy_clone = Arc::clone(&client.strategy);
        let hash_ring_clone = Arc::clone(&client.hash_ring);
        tokio::spawn(async move {
            let servers = servers_clone.read().await.clone();
            let strategy = strategy_clone.read().await.clone();
            let hash_ring = hash_ring_clone.read().await.clone();
            
            let snapshot = Box::into_raw(Box::new(ServerSelection {
                strategy,
                servers,
                hash_ring,
            }));
            
            let old = selection_clone.swap(snapshot, Ordering::Release);
            if !old.is_null() {
                unsafe {
                    let _ = Box::from_raw(old);
                }
            }
        });
        
        // Spawn background task for peer discovery
        let servers_clone = Arc::clone(&client.servers);
        let strategy_clone = Arc::clone(&client.strategy);
        let ring_clone = Arc::clone(&client.hash_ring);
        let selection_clone2 = Arc::clone(&client.selection);
        let seed_clone = seed.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(refresh_secs));
            loop {
                interval.tick().await;
                if let Err(_) = refresh_peers_discovery(&seed_clone, &servers_clone, &ring_clone).await {
                    // Log error but continue
                } else {
                    // Update snapshot after successful refresh
                    let servers = servers_clone.read().await.clone();
                    let strategy = strategy_clone.read().await.clone();
                    let hash_ring = ring_clone.read().await.clone();
                    
                    let snapshot = Box::into_raw(Box::new(ServerSelection {
                        strategy,
                        servers,
                        hash_ring,
                    }));
                    
                    let old = selection_clone2.swap(snapshot, Ordering::Release);
                    if !old.is_null() {
                        unsafe {
                            let _ = Box::from_raw(old);
                        }
                    }
                }
            }
        });
        
        // Do initial refresh
        let servers_init = Arc::clone(&client.servers);
        let strategy_init = Arc::clone(&client.strategy);
        let ring_init = Arc::clone(&client.hash_ring);
        let selection_init = Arc::clone(&client.selection);
        let seed_init = seed.clone();
        tokio::spawn(async move {
            let _ = refresh_peers_discovery(&seed_init, &servers_init, &ring_init).await;
            // Update snapshot after refresh
            let servers = servers_init.read().await.clone();
            let strategy = strategy_init.read().await.clone();
            let hash_ring = ring_init.read().await.clone();
            
            let snapshot = Box::into_raw(Box::new(ServerSelection {
                strategy,
                servers,
                hash_ring,
            }));
            
            let old = selection_init.swap(snapshot, Ordering::Release);
            if !old.is_null() {
                unsafe {
                    let _ = Box::from_raw(old);
                }
            }
        });
        
        client
    }

    async fn select_server(&self, key: &str) -> Option<String> {
        // Lock-free read using RCU pattern
        let snapshot_ptr = self.selection.load(Ordering::Acquire);
        
        if snapshot_ptr.is_null() {
            // Fallback to locked read if snapshot not initialized
            return self.select_server_locked(key).await;
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
                        // pick_server returns &str from snapshot.servers, so we need to clone
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
    
    async fn select_server_locked(&self, key: &str) -> Option<String> {
        let strategy = self.strategy.read().await.clone();
        match strategy {
            SelectionStrategy::RoundRobin => {
                let servers = self.servers.read().await;
                if servers.is_empty() {
                    return None;
                }
                let index = self.current_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % servers.len();
                Some(servers[index].clone())
            }
            SelectionStrategy::ConsistentHashing => {
                let ring = self.hash_ring.read().await;
                if let Some(ref r) = *ring {
                    if let Some(s) = r.pick_server(key) {
                        return Some(s.to_string());
                    }
                }
                // Fallback if ring empty
                let servers = self.servers.read().await;
                if servers.is_empty() {
                    None
                } else {
                    let idx = self.current_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % servers.len();
                    Some(servers[idx].clone())
                }
            }
        }
    }

    async fn get_or_create_connection(&self, server: &str) -> Result<TcpStream, ClientError> {
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
                // Optimize: avoid to_string() by using entry API with &str
                let pool_count = self.pool_counts
                    .entry(server.to_string())
                    .or_insert_with(|| Arc::new(AtomicU32::new(0)))
                    .clone();
                
                let current = pool_count.load(Ordering::Relaxed);
                if current < self.max_pool_size {
                    if pool_count.compare_exchange(current, current + 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                        // Successfully claimed slot, create connection
                        match tokio::time::timeout(CONNECTION_TIMEOUT, Self::connect_with_nodelay(server)).await {
                            Ok(Ok(stream)) => {
                                // Try to get from pool one more time before returning new
                                if let Some(pooled_stream) = queue.value().pop() {
                                    // Got one from pool, close new one and return pooled
                                    pool_count.fetch_sub(1, Ordering::Relaxed);
                                    return Ok(pooled_stream);
                                }
                                return Ok(stream);
                            }
                            Ok(Err(e)) => {
                                pool_count.fetch_sub(1, Ordering::Relaxed);
                                return Err(ClientError::Io(e));
                            }
                            Err(_) => {
                                pool_count.fetch_sub(1, Ordering::Relaxed);
                                return Err(ClientError::Timeout);
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
            match tokio::time::timeout(CONNECTION_TIMEOUT, Self::connect_with_nodelay(server)).await {
                Ok(Ok(stream)) => {
                    let pool_count = self.pool_counts
                        .entry(server.to_string())
                        .or_insert_with(|| Arc::new(AtomicU32::new(0)))
                        .clone();
                    pool_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(stream);
                }
                Ok(Err(e)) => return Err(ClientError::Io(e)),
                Err(_) => return Err(ClientError::Timeout),
            }
        }
        
        // Initialize pool for this server
        let queue = Arc::new(SegQueue::new());
        self.connection_pools.insert(server.to_string(), queue.clone());
        let pool_count = Arc::new(AtomicU32::new(0));
        self.pool_counts.insert(server.to_string(), pool_count.clone());
        
        // Create new connection
        match tokio::time::timeout(CONNECTION_TIMEOUT, Self::connect_with_nodelay(server)).await {
            Ok(Ok(stream)) => {
                pool_count.fetch_add(1, Ordering::Relaxed);
                Ok(stream)
            }
            Ok(Err(e)) => Err(ClientError::Io(e)),
            Err(_) => Err(ClientError::Timeout),
        }
    }
    
    fn return_connection(&self, server: &str, stream: TcpStream) {
        if let Some(queue) = self.connection_pools.get(server) {
            // Push to queue (lock-free, always succeeds)
            // SegQueue is unbounded, so this always succeeds
            queue.value().push(stream);
        } else {
            // Pool doesn't exist, stream will be dropped and closed automatically
            // No need to explicitly close
        }
    }
    
    fn mark_connection_dead(&self, server: &str) {
        if let Some(count) = self.pool_counts.get(server) {
            count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ClientError> {
        let server = self.select_server(key).await.ok_or(ClientError::Protocol("No servers available".into()))?;
        
        let mut stream = self.get_or_create_connection(&server).await?;
        let mut should_return = true;
        
        let request = <BinarySerializer as Serializer>::serialize_command(&Command::Get(key.to_string()));
        let result = match stream.write_all(&request).await {
            Err(e) => {
                should_return = false;
                self.mark_connection_dead(&server);
                Err(ClientError::Io(e))
            }
            Ok(_) => {
                // Read status byte first (like Go client with io.ReadFull)
                let mut status_buf = [0u8; 1];
                match stream.read_exact(&mut status_buf).await {
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
                                match stream.read_exact(&mut len_buf).await {
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
                                            match stream.read_exact(&mut data).await {
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
                                match stream.read_exact(&mut msg_len_buf).await {
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
                                            match stream.read_exact(&mut msg_bytes).await {
                                                Err(e) => {
                                                    should_return = false;
                                                    self.mark_connection_dead(&server);
                                                    Err(ClientError::Io(e))
                                                }
                                                Ok(_) => {
                                                    // Optimize: check "not found" case-insensitively without allocating
                                                    let msg_lower: Vec<u8> = msg_bytes.iter().map(|&b| b.to_ascii_lowercase()).collect();
                                                    if msg_lower.windows(9).any(|w| w == b"not found") {
                                                        Ok(None)
                                                    } else {
                                                        // Only allocate string if we need to return error
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
        // If should_return is false, stream will be dropped and closed automatically
        
        result
    }

    pub async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), ClientError> {
        self.set_with_ttl(key, value, 0).await
    }

    pub async fn set_with_ttl(&self, key: &str, value: Vec<u8>, ttl_secs: u32) -> Result<(), ClientError> {
        let server = self.select_server(key).await.ok_or(ClientError::Protocol("No servers available".into()))?;
        
        let mut stream = self.get_or_create_connection(&server).await?;
        let mut should_return = true;
        
        let request = <BinarySerializer as Serializer>::serialize_command(&Command::Put(key.to_string(), value, ttl_secs));
        let write_result = stream.write_all(&request).await;
        
        let result = match write_result {
            Err(e) => {
                should_return = false;
                self.mark_connection_dead(&server);
                Err(ClientError::Io(e))
            }
            Ok(_) => {
                // Read status byte first (like Go client with io.ReadFull)
                let mut status_buf = [0u8; 1];
                match stream.read_exact(&mut status_buf).await {
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
                                match stream.read_exact(&mut len_buf).await {
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
                                            match stream.read_exact(&mut discard).await {
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
                                match stream.read_exact(&mut msg_len_buf).await {
                                    Err(e) => {
                                        should_return = false;
                                        self.mark_connection_dead(&server);
                                        Err(ClientError::Io(e))
                                    }
                                    Ok(_) => {
                                        let msg_len = u16::from_be_bytes(msg_len_buf) as usize;
                                        if msg_len > 0 {
                                            let mut msg_bytes = vec![0u8; msg_len];
                                            match stream.read_exact(&mut msg_bytes).await {
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

    pub async fn delete(&self, key: &str) -> Result<bool, ClientError> {
        let server = self.select_server(key).await.ok_or(ClientError::Protocol("No servers available".into()))?;
        
        let mut stream = self.get_or_create_connection(&server).await?;
        let mut should_return = true;
        
        let request = <BinarySerializer as Serializer>::serialize_command(&Command::Delete(key.to_string()));
        let write_result = stream.write_all(&request).await;
        
        let result = match write_result {
            Err(e) => {
                should_return = false;
                self.mark_connection_dead(&server);
                Err(ClientError::Io(e))
            }
            Ok(_) => {
                // Read status byte first (like Go client with io.ReadFull)
                let mut status_buf = [0u8; 1];
                match stream.read_exact(&mut status_buf).await {
                    Err(e) => {
                        should_return = false;
                        self.mark_connection_dead(&server);
                        Err(ClientError::Io(e))
                    }
                    Ok(_) => {
                        let status = status_buf[0];
                        match status {
                            0x00 => {
                                // OK - read data length (should be 0 for DELETE success)
                                let mut len_buf = [0u8; 4];
                                match stream.read_exact(&mut len_buf).await {
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
                                            match stream.read_exact(&mut discard).await {
                                                Err(e) => {
                                                    should_return = false;
                                                    self.mark_connection_dead(&server);
                                                    Err(ClientError::Io(e))
                                                }
                                                Ok(_) => Ok(true)
                                            }
                                        } else {
                                            Ok(true)
                                        }
                                    }
                                }
                            }
                            0x01 => {
                                // ERROR - read message
                                let mut msg_len_buf = [0u8; 2];
                                match stream.read_exact(&mut msg_len_buf).await {
                                    Err(e) => {
                                        should_return = false;
                                        self.mark_connection_dead(&server);
                                        Err(ClientError::Io(e))
                                    }
                                    Ok(_) => {
                                        let msg_len = u16::from_be_bytes(msg_len_buf) as usize;
                                        if msg_len > 0 {
                                            let mut msg_bytes = vec![0u8; msg_len];
                                            match stream.read_exact(&mut msg_bytes).await {
                                                Err(e) => {
                                                    should_return = false;
                                                    self.mark_connection_dead(&server);
                                                    Err(ClientError::Io(e))
                                                }
                                                Ok(_) => {
                                                    // Optimize: check "not found" case-insensitively
                                                    let msg_lower: Vec<u8> = msg_bytes.iter().map(|&b| b.to_ascii_lowercase()).collect();
                                                    if msg_lower.windows(9).any(|w| w == b"not found") {
                                                        Ok(false)
                                                    } else {
                                                        let msg = String::from_utf8_lossy(&msg_bytes);
                                                        Err(ClientError::Protocol(msg.to_string()))
                                                    }
                                                }
                                            }
                                        } else {
                                            Ok(false)
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

    pub async fn get_multi(&self, keys: &[&str]) -> Result<HashMap<String, Vec<u8>>, ClientError> {
        let mut results = HashMap::new();
        
        for &key in keys {
            if let Some(value) = self.get(key).await? {
                results.insert(key.to_string(), value);
            }
        }
        
        Ok(results)
    }

    pub async fn ping(&self) -> Result<(), ClientError> {
        let servers = self.servers.read().await;
        let server = servers.get(0).ok_or(ClientError::Protocol("No servers configured".into()))?;
        let server = server.clone();
        drop(servers);
        
        let mut stream = TcpStream::connect(&server).await?;
        let request = <BinarySerializer as Serializer>::serialize_command(&Command::Ping);
        stream.write_all(&request).await?;

        // Read response (like server's TcpClient - use read() not read_exact)
        let mut buffer = vec![0u8; 1024];
        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            return Err(ClientError::Io(IOError::new(std::io::ErrorKind::UnexpectedEof, "Connection closed")));
        }
        buffer.truncate(n);
        
        let resp = <BinarySerializer as Serializer>::deserialize_response(&buffer)
            .map_err(|e| ClientError::Protocol(e.to_string()))?;
        match resp {
            Response::Pong => Ok(()),
            Response::Error(msg) => Err(ClientError::Protocol(msg)),
            _ => Err(ClientError::Protocol("Ping failed".to_string())),
        }
    }
}

#[derive(Clone)]
struct ClientConsistentHash {
    sorted_hashes: Vec<u64>,
    hash_to_index: Vec<usize>,
    servers: Vec<String>,
    replicas: usize,
}

impl ClientConsistentHash {
    fn new(replicas: usize) -> Self {
        Self {
            sorted_hashes: Vec::new(),
            hash_to_index: Vec::new(),
            servers: Vec::new(),
            replicas,
        }
    }

    fn add_server(&mut self, server: &str) {
        let server_idx = self.servers.len();
        self.servers.push(server.to_string());
        
        for i in 0..self.replicas {
            let virtual_id = format!("{}-{}", server, i);
            let hash = fnv_hash(&virtual_id);
            self.sorted_hashes.push(hash);
            self.hash_to_index.push(server_idx);
        }
    }
    
    fn finalize(&mut self) {
        let mut pairs: Vec<(u64, usize)> = self.sorted_hashes.iter()
            .zip(self.hash_to_index.iter())
            .map(|(&h, &i)| (h, i))
            .collect();
        pairs.sort_by_key(|&(h, _)| h);
        
        self.sorted_hashes.clear();
        self.hash_to_index.clear();
        self.sorted_hashes.reserve(pairs.len());
        self.hash_to_index.reserve(pairs.len());
        for (h, i) in pairs {
            self.sorted_hashes.push(h);
            self.hash_to_index.push(i);
        }
    }

    fn pick_server(&self, key: &str) -> Option<&str> {
        if self.sorted_hashes.is_empty() {
            return None;
        }
        
        let h = fnv_hash(key);
        
        match self.sorted_hashes.binary_search(&h) {
            Ok(idx) => Some(self.servers[self.hash_to_index[idx]].as_str()),
            Err(idx) => {
                if idx < self.sorted_hashes.len() {
                    Some(self.servers[self.hash_to_index[idx]].as_str())
                } else {
                    Some(self.servers[self.hash_to_index[0]].as_str())
                }
            }
        }
    }
}

fn build_ring(servers: &[String], strategy: &SelectionStrategy) -> Option<ClientConsistentHash> {
    if !matches!(strategy, SelectionStrategy::ConsistentHashing) {
        return None;
    }
    let mut ring = ClientConsistentHash::new(150);
    for s in servers {
        ring.add_server(s);
    }
    ring.finalize();
    Some(ring)
}

fn fnv_hash(input: &str) -> u64 {
    use fnv::FnvHasher;
    use std::hash::{Hash, Hasher};
    let mut h = FnvHasher::default();
    input.hash(&mut h);
    h.finish()
}

async fn refresh_peers_discovery(seed: &str, servers: &Arc<RwLock<Vec<String>>>, ring: &Arc<RwLock<Option<ClientConsistentHash>>>) -> Result<(), ClientError> {
    let mut stream = TcpStream::connect(seed).await?;
    let request = <BinarySerializer as Serializer>::serialize_command(&Command::Peer);
    stream.write_all(&request).await?;

    // Read response (like server's TcpClient - use read() not read_exact)
    let mut buffer = vec![0u8; 4096];
    let n = stream.read(&mut buffer).await?;
    if n == 0 {
        return Err(ClientError::Io(IOError::new(std::io::ErrorKind::UnexpectedEof, "Connection closed")));
    }
    buffer.truncate(n);
    
    let resp = <BinarySerializer as Serializer>::deserialize_response(&buffer)
        .map_err(|e| ClientError::Protocol(e.to_string()))?;
    match resp {
        Response::Ok(data) => {
            let list = String::from_utf8_lossy(&data);
            let peers: Vec<String> = list
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !peers.is_empty() {
                {
                    let mut w = servers.write().await;
                    *w = peers.clone();
                }
                let mut new_ring = ClientConsistentHash::new(150);
                for p in &peers {
                    new_ring.add_server(p);
                }
                new_ring.finalize();
                let mut rw = ring.write().await;
                *rw = Some(new_ring);
            }
            Ok(())
        }
        Response::Error(msg) => Err(ClientError::Protocol(msg)),
        _ => Err(ClientError::Protocol("Invalid PEER response".into())),
    }
}
