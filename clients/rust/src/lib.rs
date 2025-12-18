use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tokio::time::{Duration, timeout};
use std::collections::HashMap;
use std::sync::Arc;
use std::io::Error as IOError;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::error::Error as STDError;

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

pub struct TcpClient {
    servers: Arc<RwLock<Vec<String>>>,
    strategy: SelectionStrategy,
    current_index: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    hash_ring: Arc<RwLock<Option<ClientConsistentHash>>>,
    seed: Option<String>,
    refresh_secs: Option<u64>,
}

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

impl TcpClient {
    pub fn new(servers: Vec<String>) -> Self {
        let strategy = SelectionStrategy::RoundRobin;
        let ring = build_ring(&servers, &strategy);
        Self {
            servers: Arc::new(RwLock::new(servers)),
            strategy,
            current_index: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            hash_ring: Arc::new(RwLock::new(ring)),
            seed: None,
            refresh_secs: None,
        }
    }

    pub fn with_strategy(servers: Vec<String>, strategy: SelectionStrategy) -> Self {
        let ring = build_ring(&servers, &strategy);
        Self {
            servers: Arc::new(RwLock::new(servers)),
            strategy,
            current_index: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            hash_ring: Arc::new(RwLock::new(ring)),
            seed: None,
            refresh_secs: None,
        }
    }

    pub fn with_discovery(seed: String, refresh_secs: u64) -> Self {
        let client = Self {
            servers: Arc::new(RwLock::new(vec![seed.clone()])),
            strategy: SelectionStrategy::ConsistentHashing,
            current_index: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            hash_ring: Arc::new(RwLock::new(build_ring(&[seed.clone()], &SelectionStrategy::ConsistentHashing))),
            seed: Some(seed.clone()),
            refresh_secs: Some(refresh_secs),
        };
        
        // Spawn background task for peer discovery
        let servers_clone = Arc::clone(&client.servers);
        let ring_clone = Arc::clone(&client.hash_ring);
        let seed_clone = seed.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(refresh_secs));
            loop {
                interval.tick().await;
                if let Err(_) = refresh_peers_discovery(&seed_clone, &servers_clone, &ring_clone).await {
                    // Log error but continue
                }
            }
        });
        
        // Do initial refresh
        let servers_init = Arc::clone(&client.servers);
        let ring_init = Arc::clone(&client.hash_ring);
        let seed_init = seed.clone();
        tokio::spawn(async move {
            let _ = refresh_peers_discovery(&seed_init, &servers_init, &ring_init).await;
        });
        
        client
    }

    async fn select_server(&self, key: &str) -> Option<String> {
        match &self.strategy {
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

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ClientError> {
        let server = self.select_server(key).await.ok_or(ClientError::Protocol("No servers available".into()))?;
        
        // Retry logic for transient connection errors
        let mut last_error = None;
        for attempt in 0..3 {
            match TcpStream::connect(&server).await {
                Ok(mut stream) => {
                    let request = <BinarySerializer as Serializer>::serialize_command(&Command::Get(key.to_string()));
                    stream.write_all(&request).await?;

                    // Read response (like server's TcpClient - use read() not read_exact)
                    let mut buffer = vec![0u8; 8192];
                    let n = stream.read(&mut buffer).await?;
                    if n == 0 {
                        return Err(ClientError::Io(IOError::new(std::io::ErrorKind::UnexpectedEof, "Connection closed")));
                    }
                    buffer.truncate(n);
                    
                    let result = <BinarySerializer as Serializer>::deserialize_response(&buffer)
                        .map_err(|e| ClientError::Protocol(e.to_string()))
                        .and_then(|resp| match resp {
                            Response::Ok(data) => Ok(Some(data)),
                            Response::Error(msg) if msg.to_lowercase().contains("not found") => Ok(None),
                            Response::Error(msg) => Err(ClientError::Protocol(msg)),
                            _ => Err(ClientError::Protocol("Unexpected response".into())),
                        });
                    
                    if result.is_ok() {
                        return result;
                    }
                    last_error = Some(result.unwrap_err());
                    // If it's not a connection error, don't retry
                    if !matches!(last_error.as_ref().unwrap(), ClientError::Io(_)) {
                        return Err(last_error.unwrap());
                    }
                }
                Err(e) => {
                    last_error = Some(ClientError::Io(e));
                    if attempt < 2 {
                        // Exponential backoff: 20ms, 40ms
                        tokio::time::sleep(Duration::from_millis(20 * (1 << attempt))).await;
                    }
                }
            }
        }
        Err(last_error.unwrap())
    }

    pub async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), ClientError> {
        self.set_with_ttl(key, value, 0).await
    }

    pub async fn set_with_ttl(&self, key: &str, value: Vec<u8>, ttl_secs: u32) -> Result<(), ClientError> {
        let server = self.select_server(key).await.ok_or(ClientError::Protocol("No servers available".into()))?;
        
        // Retry logic for transient connection errors
        let mut last_error = None;
        for attempt in 0..3 {
            match TcpStream::connect(&server).await {
                Ok(mut stream) => {
                    let request = <BinarySerializer as Serializer>::serialize_command(&Command::Put(key.to_string(), value.clone(), ttl_secs));
                    stream.write_all(&request).await?;

                    // Read response (like server's TcpClient - use read() not read_exact)
                    let mut buffer = vec![0u8; 1024];
                    let n = stream.read(&mut buffer).await?;
                    if n == 0 {
                        return Err(ClientError::Io(IOError::new(std::io::ErrorKind::UnexpectedEof, "Connection closed")));
                    }
                    buffer.truncate(n);
                    
                    let result = <BinarySerializer as Serializer>::deserialize_response(&buffer)
                        .map_err(|e| ClientError::Protocol(e.to_string()))
                        .and_then(|resp| match resp {
                            Response::Ok(_) => Ok(()),
                            Response::Error(msg) => Err(ClientError::Protocol(msg)),
                            _ => Err(ClientError::Protocol("Unexpected response".into())),
                        });
                    
                    if result.is_ok() {
                        return result;
                    }
                    last_error = Some(result.unwrap_err());
                    // If it's not a connection error, don't retry
                    if !matches!(last_error.as_ref().unwrap(), ClientError::Io(_)) {
                        return Err(last_error.unwrap());
                    }
                }
                Err(e) => {
                    last_error = Some(ClientError::Io(e));
                    if attempt < 2 {
                        // Exponential backoff: 20ms, 40ms
                        tokio::time::sleep(Duration::from_millis(20 * (1 << attempt))).await;
                    }
                }
            }
        }
        Err(last_error.unwrap())
    }

    pub async fn delete(&self, key: &str) -> Result<bool, ClientError> {
        let server = self.select_server(key).await.ok_or(ClientError::Protocol("No servers available".into()))?;
        
        // Retry logic for transient connection errors
        let mut last_error = None;
        for attempt in 0..3 {
            match TcpStream::connect(&server).await {
                Ok(mut stream) => {
                    let request = <BinarySerializer as Serializer>::serialize_command(&Command::Delete(key.to_string()));
                    stream.write_all(&request).await?;

                    // Read response (like server's TcpClient - use read() not read_exact)
                    let mut buffer = vec![0u8; 1024];
                    let n = stream.read(&mut buffer).await?;
                    if n == 0 {
                        return Err(ClientError::Io(IOError::new(std::io::ErrorKind::UnexpectedEof, "Connection closed")));
                    }
                    buffer.truncate(n);
                    
                    let result = <BinarySerializer as Serializer>::deserialize_response(&buffer)
                        .map_err(|e| ClientError::Protocol(e.to_string()))
                        .and_then(|resp| match resp {
                            Response::Ok(_) => Ok(true),
                            Response::Error(msg) if msg.to_lowercase().contains("not found") => Ok(false),
                            Response::Error(msg) => Err(ClientError::Protocol(msg)),
                            _ => Err(ClientError::Protocol("Unexpected response".into())),
                        });
                    
                    if result.is_ok() {
                        return result;
                    }
                    last_error = Some(result.unwrap_err());
                    // If it's not a connection error, don't retry
                    if !matches!(last_error.as_ref().unwrap(), ClientError::Io(_)) {
                        return Err(last_error.unwrap());
                    }
                }
                Err(e) => {
                    last_error = Some(ClientError::Io(e));
                    if attempt < 2 {
                        // Exponential backoff: 20ms, 40ms
                        tokio::time::sleep(Duration::from_millis(20 * (1 << attempt))).await;
                    }
                }
            }
        }
        Err(last_error.unwrap())
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
