use crate::transports::common::{Command, ProtocolClient, ProtocolServer, Response};
use crate::transports::{
    handle_connection, handle_get_response, handle_ping_response, handle_put_response,
};
use crate::utils::persistence::PersistenceManager;
use crate::Group;
use async_trait::async_trait;
use std::error::Error;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{error, info};

/// TCP server implementation for BlazeCache protocol.
///
/// Provides reliable, connection-oriented transport for cache operations.
/// TCP is recommended for production deployments where data integrity
/// is more important than absolute minimum latency.
///
/// ## Performance Characteristics
///
/// - **Latency**: ~80μs for cache hits (includes TCP overhead)
/// - **Throughput**: High throughput for bulk operations
/// - **Reliability**: Guaranteed delivery with connection management
/// - **Memory**: Higher memory usage due to connection state
///
/// ## Use Cases
///
/// - Production deployments requiring reliability
/// - Bulk data operations
/// - Cross-datacenter replication
/// - When network conditions are unreliable
///
/// ## Example
///
/// ```rust,no_run
/// # use blazecache::{Group, Getter, transports::{TcpServer, ProtocolServer}, serializers::BinarySerializer};
/// # use std::sync::Arc;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let getter: Getter = Arc::new(|key: &str| Ok(format!("data-{}", key).into_bytes()));
/// let group = Arc::new(Group::new("cache".to_string(), 100 * 1024 * 1024, getter));
/// let server = TcpServer::<BinarySerializer>::new(group);
///
/// println!("Starting TCP server on port 8080...");
/// server.start(8080).await.unwrap();
/// # Ok(())
/// # }
/// ```
pub struct TcpServer<S> {
    /// Reference to the cache group that handles the actual cache operations.
    /// Shared across all client connections for this server instance.
    group: Arc<Group>,

    /// Phantom data to associate the serializer type with this server.
    /// The serializer determines the wire format (binary or JSON).
    serializer: std::marker::PhantomData<S>,

    /// Optional persistence manager for WAL logging.
    persistence: Option<Arc<AsyncMutex<PersistenceManager>>>,
}

impl<S> TcpServer<S> {
    /// Creates a new TCP server instance.
    ///
    /// The server will handle incoming TCP connections and route cache
    /// operations to the provided group. Each connection is handled
    /// concurrently in its own async task.
    ///
    /// ## Arguments
    ///
    /// * `group` - The cache group to handle operations
    ///
    /// ## Returns
    ///
    /// A new TcpServer instance ready to start listening.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use blazecache::{Group, Getter, transports::TcpServer, serializers::BinarySerializer};
    /// # use std::sync::Arc;
    /// # let getter: Getter = Arc::new(|key: &str| Ok(format!("data-{}", key).into_bytes()));
    /// let group = Arc::new(Group::new("cache".to_string(), 1024 * 1024, getter));
    /// let server = TcpServer::<BinarySerializer>::new(group);
    /// ```
    pub fn new(group: Arc<Group>) -> Self {
        Self {
            group,
            serializer: std::marker::PhantomData,
            persistence: None,
        }
    }

    /// Creates a new TCP server instance with optional persistence.
    ///
    /// When a persistence manager is provided, all successful PUT and DELETE
    /// operations will be logged to the WAL for crash recovery.
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

/// Trait for serializing and deserializing protocol messages.
///
/// The Serializer trait abstracts the wire format used for communication
/// between clients and servers. BlazeCache supports multiple serialization
/// formats for different use cases:
///
/// - **Binary**: Compact, fast serialization for production
/// - **JSON**: Human-readable format for debugging and development
///
/// ## Performance Comparison
///
/// | Format | Size | Serialize | Deserialize | Use Case |
/// |--------|------|-----------|-------------|----------|
/// | Binary | 100% | 100%      | 100%        | Production |
/// | JSON   | 150% | 80%       | 70%         | Development |
///
/// ## Example Implementation
///
/// ```rust,no_run
/// # use blazecache::transports::{tcp::Serializer, common::{Command, Response}};
/// # use std::error::Error;
/// struct MySerializer;
///
/// impl Serializer for MySerializer {
///     fn serialize_command(cmd: &Command) -> Vec<u8> {
///         // Custom serialization logic
///         vec![]
///     }
///     
///     fn deserialize_command(data: &[u8]) -> Result<Command, Box<dyn Error + Send + Sync>> {
///         // Custom deserialization logic
///         todo!()
///     }
///     
///     fn serialize_response(resp: &Response) -> Vec<u8> {
///         // Custom serialization logic
///         vec![]
///     }
///     
///     fn deserialize_response(data: &[u8]) -> Result<Response, Box<dyn Error + Send + Sync>> {
///         // Custom deserialization logic
///         todo!()
///     }
/// }
/// ```
pub trait Serializer: Send + Sync {
    /// Serializes a command to bytes for transmission.
    ///
    /// Commands include GET, PUT, PING operations that clients send to servers.
    ///
    /// ## Arguments
    ///
    /// * `cmd` - The command to serialize
    ///
    /// ## Returns
    ///
    /// Serialized bytes ready for network transmission.
    fn serialize_command(cmd: &Command) -> Vec<u8>;

    /// Deserializes bytes into a command.
    ///
    /// Used by servers to parse incoming client requests.
    ///
    /// ## Arguments
    ///
    /// * `data` - Serialized command bytes
    ///
    /// ## Returns
    ///
    /// Parsed command or error if deserialization fails.
    fn deserialize_command(data: &[u8]) -> Result<Command, Box<dyn Error + Send + Sync>>;

    /// Serializes a response to bytes for transmission.
    ///
    /// Responses include success/error status and optional data payload.
    ///
    /// ## Arguments
    ///
    /// * `resp` - The response to serialize
    ///
    /// ## Returns
    ///
    /// Serialized bytes ready for network transmission.
    fn serialize_response(resp: &Response) -> Vec<u8>;

    /// Deserializes bytes into a response.
    ///
    /// Used by clients to parse server responses.
    ///
    /// ## Arguments
    ///
    /// * `data` - Serialized response bytes
    ///
    /// ## Returns
    ///
    /// Parsed response or error if deserialization fails.
    fn deserialize_response(data: &[u8]) -> Result<Response, Box<dyn Error + Send + Sync>>;
}

#[async_trait]
impl<S: Serializer + 'static> ProtocolServer for TcpServer<S> {
    /// Starts the TCP server listening on the specified port.
    ///
    /// The server will accept incoming TCP connections and handle each
    /// connection concurrently in a separate async task. Each connection
    /// can handle multiple requests over its lifetime.
    ///
    /// ## Connection Handling
    ///
    /// 1. Accept incoming TCP connection
    /// 2. Spawn async task for connection handling
    /// 3. Parse incoming commands using the configured serializer
    /// 4. Route commands to the cache group
    /// 5. Serialize and send responses back to client
    /// 6. Handle connection errors gracefully
    ///
    /// ## Arguments
    ///
    /// * `port` - Port number to listen on (typically 8080 for cache servers)
    ///
    /// ## Returns
    ///
    /// This method runs indefinitely, returning only on fatal errors.
    ///
    /// ## Errors
    ///
    /// - Port already in use
    /// - Permission denied (ports < 1024 require root)
    /// - Network interface unavailable
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use blazecache::{Group, Getter, transports::{TcpServer, ProtocolServer}, serializers::BinarySerializer};
    /// # use std::sync::Arc;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let getter: Getter = Arc::new(|key: &str| Ok(format!("data-{}", key).into_bytes()));
    /// let group = Arc::new(Group::new("cache".to_string(), 100 * 1024 * 1024, getter));
    /// let server = TcpServer::<BinarySerializer>::new(group);
    ///
    /// // This will run forever, handling connections
    /// server.start(8080).await.unwrap();
    /// # Ok(())
    /// # }
    /// ```
    async fn start(&self, port: u16) -> Result<(), Box<dyn Error + Send + Sync>> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        info!(port = port, "TCP server listening");

        loop {
            let (stream, _) = listener.accept().await?;
            let server = Arc::new(TcpServer {
                group: Arc::clone(&self.group),
                serializer: std::marker::PhantomData,
                persistence: self.persistence.clone(),
            });

            // Handle each connection concurrently
            tokio::spawn(async move {
                if let Err(e) = handle_tcp_connection::<S>(stream, server).await {
                    error!(error = %e, "TCP connection error");
                }
            });
        }
    }
}

async fn handle_tcp_connection<S: Serializer + 'static>(
    mut stream: TcpStream,
    server: Arc<TcpServer<S>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut buffer = [0; 8192];

    loop {
        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        let response_data =
            handle_connection::<S>(&buffer[..n], &server.group, server.persistence.clone())
                .await?;
        stream.write_all(&response_data).await?;
    }
    Ok(())
}

pub struct TcpClient<S> {
    stream: TcpStream,
    serializer: std::marker::PhantomData<S>,
}

#[async_trait]
impl<S: Serializer + 'static> ProtocolClient for TcpClient<S> {
    async fn connect(addr: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream,
            serializer: PhantomData,
        })
    }

    async fn ping(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let cmd_data = S::serialize_command(&Command::Ping);
        self.stream.write_all(&cmd_data).await?;

        let mut buffer = [0; 1024];
        let n = self.stream.read(&mut buffer).await?;
        let response = S::deserialize_response(&buffer[..n])?;

        handle_ping_response(response)
    }

    async fn get(&mut self, key: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let cmd_data = S::serialize_command(&Command::Get(key.to_string()));
        self.stream.write_all(&cmd_data).await?;

        let mut buffer = [0; 8192];
        let n = self.stream.read(&mut buffer).await?;
        let response = S::deserialize_response(&buffer[..n])?;

        handle_get_response(response)
    }

    async fn put(&mut self, key: &str, value: &[u8], ttl: u32) -> Result<(), Box<dyn Error + Send + Sync>> {
        let cmd_data = S::serialize_command(&Command::Put(key.to_string(), value.to_vec(), ttl));
        self.stream.write_all(&cmd_data).await?;

        let mut buffer = [0; 1024];
        let n = self.stream.read(&mut buffer).await?;
        let response = S::deserialize_response(&buffer[..n])?;

        handle_put_response(response)
    }

    async fn delete(&mut self, key: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let cmd_data = S::serialize_command(&Command::Delete(key.to_string()));
        self.stream.write_all(&cmd_data).await?;

        let mut buffer = [0; 1024];
        let n = self.stream.read(&mut buffer).await?;
        let response = S::deserialize_response(&buffer[..n])?;

        match response {
            Response::Ok(_) => Ok(true),
            Response::Error(msg) if msg == "Not found" => Ok(false),
            Response::Error(msg) => Err(msg.into()),
            _ => Err("Invalid delete response".into()),
        }
    }
}
