//! # TCP Transport Module
//!
//! This module provides both plain TCP and TLS-encrypted TCP transport for BlazeCache.
//!
//! ## Plain TCP
//!
//! Provides reliable, connection-oriented transport for cache operations.
//! TCP is recommended for production deployments where data integrity
//! is more important than absolute minimum latency.
//!
//! ## TLS-Encrypted TCP
//!
//! Provides the same interface as plain TCP but with TLS encryption.
//! All client connections are encrypted using TLS 1.2+.
//!
//! - **Encryption**: All data in transit is encrypted
//! - **Authentication**: Server certificate verification
//! - **Integrity**: Protection against tampering
//! - **Forward Secrecy**: Perfect forward secrecy with ephemeral keys

use crate::transports::common::{Command, ProtocolClient, ProtocolServer, Response};
use crate::transports::{
    handle_connection, handle_get_response, handle_peer_response, handle_ping_response, handle_put_response, PersistenceManagerHandle,
};
use crate::Group;
use async_trait::async_trait;
use std::borrow::Cow;
use std::error::Error;
use std::fs;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::{pki_types::CertificateDer, pki_types::PrivateKeyDer, ServerConfig};
use tokio_rustls::rustls::{
    pki_types::ServerName,
    ClientConfig, RootCertStore,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_rustls::server::TlsStream as ServerTlsStream;
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tracing::{error, info, warn};

// ============================================================================
// Plain TCP Implementation
// ============================================================================

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
/// let group = Arc::new(Group::new("cache".to_string(), 100 * 1024 * 1024, getter, "127.0.0.1:8080".to_string()));
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
    persistence: PersistenceManagerHandle,
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
    /// let group = Arc::new(Group::new("cache".to_string(), 1024 * 1024, getter, "127.0.0.1:8080".to_string()));
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
        persistence: PersistenceManagerHandle,
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
/// # use std::borrow::Cow;
/// struct MySerializer;
///
/// impl Serializer for MySerializer {
///     fn serialize_command(cmd: &Command) -> Vec<u8> {
///         // Custom serialization logic
///         vec![]
///     }
///     
///     fn deserialize_command(data: &[u8]) -> Result<Command<'static>, Box<dyn Error + Send + Sync>> {
///         // Custom deserialization logic
///         // Example: use CBOR for binary format
///         ciborium::de::from_reader(data).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
///     }
///     
///     fn serialize_response(resp: &Response) -> Vec<u8> {
///         // Custom serialization logic
///         // Example: use CBOR for binary format
///         let mut buf = Vec::new();
///         ciborium::ser::into_writer(resp, &mut buf).unwrap_or_default();
///     }
///     
///     fn deserialize_response(data: &[u8]) -> Result<Response, Box<dyn Error + Send + Sync>> {
///         // Custom deserialization logic
///         // Example: use CBOR for binary format
///         ciborium::de::from_reader(data).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
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
    fn serialize_command<'a>(cmd: &Command<'a>) -> Vec<u8>;

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
    fn deserialize_command(data: &[u8]) -> Result<Command<'static>, Box<dyn Error + Send + Sync>>;

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
    /// let group = Arc::new(Group::new("cache".to_string(), 100 * 1024 * 1024, getter, "127.0.0.1:8080".to_string()));
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

/// Generic connection handler that works with any AsyncRead + AsyncWrite stream.
/// This reduces code duplication between TCP and TLS TCP handlers.
async fn handle_stream_connection<S, T>(
    mut stream: T,
    group: Arc<Group>,
    persistence: PersistenceManagerHandle,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    S: Serializer + 'static,
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut buffer = [0; 8192];

    loop {
        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        let response_data =
            handle_connection::<S>(&buffer[..n], &group, persistence.clone())
                .await?;
        stream.write_all(&response_data).await?;
    }
    Ok(())
}

async fn handle_tcp_connection<S: Serializer + 'static>(
    stream: TcpStream,
    server: Arc<TcpServer<S>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    handle_stream_connection::<S, _>(
        stream,
        server.group.clone(),
        server.persistence.clone(),
    )
    .await
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
        let cmd_data = S::serialize_command(&Command::Get(Cow::Borrowed(key)));
        self.stream.write_all(&cmd_data).await?;

        let mut buffer = [0; 8192];
        let n = self.stream.read(&mut buffer).await?;
        let response = S::deserialize_response(&buffer[..n])?;

        handle_get_response(response)
    }

    async fn put(&mut self, key: &str, value: &[u8], ttl: u32) -> Result<(), Box<dyn Error + Send + Sync>> {
        let cmd_data = S::serialize_command(&Command::Put(Cow::Borrowed(key), value.to_vec(), ttl));
        self.stream.write_all(&cmd_data).await?;

        let mut buffer = [0; 1024];
        let n = self.stream.read(&mut buffer).await?;
        let response = S::deserialize_response(&buffer[..n])?;

        handle_put_response(response)
    }

    async fn delete(&mut self, key: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let cmd_data = S::serialize_command(&Command::Delete(Cow::Borrowed(key)));
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

    async fn stats(&mut self) -> Result<String, Box<dyn Error + Send + Sync>> {
        let cmd_data = S::serialize_command(&Command::Stats);
        self.stream.write_all(&cmd_data).await?;

        let mut buffer = [0; 4096];
        let n = self.stream.read(&mut buffer).await?;
        let response = S::deserialize_response(&buffer[..n])?;

        match response {
            Response::Ok(data) => {
                String::from_utf8(data)
                    .map_err(|e| format!("Invalid UTF-8 in stats response: {}", e).into())
            }
            Response::Error(msg) => Err(msg.into()),
            _ => Err("Invalid stats response".into()),
        }
    }

    async fn peer(&mut self) -> Result<String, Box<dyn Error + Send + Sync>> {
        let cmd_data = S::serialize_command(&Command::Peer);
        self.stream.write_all(&cmd_data).await?;

        let mut buffer = [0; 4096];
        let n = self.stream.read(&mut buffer).await?;
        let response = S::deserialize_response(&buffer[..n])?;

        handle_peer_response(response)
    }

    async fn clear(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let cmd_data = S::serialize_command(&Command::Clear);
        self.stream.write_all(&cmd_data).await?;

        let mut buffer = [0; 4096];
        let n = self.stream.read(&mut buffer).await?;
        let response = S::deserialize_response(&buffer[..n])?;

        match response {
            Response::Ok(_) => Ok(()),
            Response::Error(msg) => Err(msg.into()),
            _ => Err("Invalid clear response".into()),
        }
    }
}

// ============================================================================
// TLS-Encrypted TCP Implementation
// ============================================================================

/// TLS-enabled TCP server for BlazeCache.
///
/// Provides the same interface as `TcpServer` but with TLS encryption.
/// All client connections are encrypted using TLS 1.2+.
///
/// ## Example
///
/// ```rust,no_run
/// # use blazecache::{Group, Getter, transports::{TlsTcpServer, ProtocolServer}, serializers::BinarySerializer};
/// # use std::sync::Arc;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let getter: Getter = Arc::new(|key: &str| Ok(format!("data-{}", key).into_bytes()));
/// let group = Arc::new(Group::new("cache".to_string(), 100 * 1024 * 1024, getter, "127.0.0.1:8080".to_string()));
/// let server = TlsTcpServer::<BinarySerializer>::new(group)
///     .with_certificate("cert.pem", "key.pem")?;
///
/// server.start(8080).await?;
/// # Ok(())
/// # }
/// ```
pub struct TlsTcpServer<S> {
    /// Reference to the cache group that handles the actual cache operations.
    group: Arc<Group>,

    /// Phantom data to associate the serializer type with this server.
    serializer: std::marker::PhantomData<S>,

    /// Optional persistence manager for WAL logging.
    persistence: PersistenceManagerHandle,

    /// TLS server configuration.
    tls_config: Arc<ServerConfig>,
}

impl<S> TlsTcpServer<S> {
    /// Creates a new TLS TCP server instance.
    ///
    /// The server will handle incoming TLS-encrypted TCP connections.
    /// You must call `with_certificate()` or `with_auto_certificate()` before starting.
    pub fn new(group: Arc<Group>) -> Self {
        // Install default crypto provider for rustls (must be called before using rustls)
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
        
        // Generate a temporary self-signed cert for initialization
        // This will be replaced by with_certificate() or with_auto_certificate()
        let tls_config = match Self::create_default_config() {
            Ok(config) => config,
            Err(_) => {
                warn!("Failed to create default TLS config, will need to call with_certificate() or with_auto_certificate()");
                // This is a placeholder - start() will fail if not properly configured
                panic!("TLS server must be configured with a certificate before starting");
            }
        };

        Self {
            group,
            serializer: std::marker::PhantomData,
            persistence: None,
            tls_config,
        }
    }

    fn create_default_config() -> Result<Arc<ServerConfig>, Box<dyn Error + Send + Sync>> {
        // Install default crypto provider for rustls (must be called before using rustls)
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
        
        use rcgen::{CertificateParams, KeyPair};

        let params = CertificateParams::new(vec!["localhost".to_string()])?;
        let key_pair = KeyPair::generate()?;
        let cert = params.self_signed(&key_pair)?;

        // Get DER bytes directly from certificate using der() method
        let cert_der = cert.der();
        let key_der = key_pair.serialize_der();

        let certs = vec![cert_der.clone()];
        let key = PrivateKeyDer::Pkcs8(key_der.into());

        Ok(Arc::new(
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| format!("Failed to create TLS config: {}", e))?,
        ))
    }

    /// Creates a new TLS TCP server instance with optional persistence.
    pub fn with_persistence(
        group: Arc<Group>,
        persistence: PersistenceManagerHandle,
    ) -> Self {
        let mut server = Self::new(group);
        server.persistence = persistence;
        server
    }

    /// Configures the server with a certificate and private key from files.
    ///
    /// ## Arguments
    ///
    /// * `cert_path` - Path to the server certificate file (PEM format)
    /// * `key_path` - Path to the private key file (PEM format)
    ///
    /// ## Returns
    ///
    /// Self for method chaining, or error if certificate/key cannot be loaded.
    pub fn with_certificate(
        mut self,
        cert_path: &str,
        key_path: &str,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Install default crypto provider for rustls
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
        
        let cert_data = fs::read(cert_path)?;
        let key_data = fs::read(key_path)?;

        let certs = rustls_pemfile::certs(&mut cert_data.as_slice())
            .collect::<Result<Vec<_>, _>>()?;
        let mut keys = rustls_pemfile::pkcs8_private_keys(&mut key_data.as_slice())
            .collect::<Result<Vec<_>, _>>()?;

        if certs.is_empty() {
            return Err("No certificates found in certificate file".into());
        }
        if keys.is_empty() {
            return Err("No private keys found in key file".into());
        }

        let certs: Vec<CertificateDer> = certs.into_iter().collect();
        let key = PrivateKeyDer::Pkcs8(keys.remove(0));

        let tls_config = Arc::new(
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| format!("Failed to create TLS config: {}", e))?,
        );

        self.tls_config = tls_config;
        Ok(self)
    }

    /// Generates a self-signed certificate automatically for development/testing.
    ///
    /// **Warning**: Self-signed certificates are not suitable for production.
    /// Clients will need to disable certificate verification or add the CA.
    ///
    /// ## Returns
    ///
    /// Self for method chaining, or error if certificate generation fails.
    pub fn with_auto_certificate(mut self) -> Result<Self, Box<dyn Error + Send + Sync>> {
        use rcgen::{CertificateParams, KeyPair};

        let params = CertificateParams::new(vec!["localhost".to_string()])?;
        let key_pair = KeyPair::generate()?;
        let cert = params.self_signed(&key_pair)?;

        // Get DER bytes directly from certificate using der() method
        let cert_der = cert.der();
        let key_der = key_pair.serialize_der();

        let certs = vec![cert_der.clone()];
        let key = PrivateKeyDer::Pkcs8(key_der.into());

        let tls_config = Arc::new(
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| format!("Failed to create TLS config: {}", e))?,
        );

        self.tls_config = tls_config;
        info!("Generated self-signed certificate for TLS server");
        Ok(self)
    }
}

#[async_trait]
impl<S: Serializer + 'static> ProtocolServer for TlsTcpServer<S> {
    /// Starts the TLS TCP server listening on the specified port.
    ///
    /// All connections will be encrypted using TLS. The server will accept
    /// incoming connections and handle each connection concurrently.
    async fn start(&self, port: u16) -> Result<(), Box<dyn Error + Send + Sync>> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        let acceptor = TlsAcceptor::from(self.tls_config.clone());
        info!(port = port, "TLS TCP server listening");

        loop {
            let (stream, _) = listener.accept().await?;
            let server = Arc::new(TlsTcpServer {
                group: Arc::clone(&self.group),
                serializer: std::marker::PhantomData,
                persistence: self.persistence.clone(),
                tls_config: self.tls_config.clone(),
            });
            let acceptor = acceptor.clone();

            // Handle each connection concurrently
            tokio::spawn(async move {
                match acceptor.accept(stream).await {
                    Ok(tls_stream) => {
                        if let Err(e) = handle_tls_tcp_connection::<S>(tls_stream, server).await {
                            error!(error = %e, "TLS TCP connection error");
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "TLS handshake failed");
                    }
                }
            });
        }
    }
}

async fn handle_tls_tcp_connection<S: Serializer + 'static>(
    stream: ServerTlsStream<TcpStream>,
    server: Arc<TlsTcpServer<S>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    handle_stream_connection::<S, _>(
        stream,
        server.group.clone(),
        server.persistence.clone(),
    )
    .await
}

/// TLS-enabled TCP client for BlazeCache.
///
/// Provides the same interface as `TcpClient` but with TLS encryption.
/// All communication with the server is encrypted using TLS 1.2+.
pub struct TlsTcpClient<S> {
    stream: ClientTlsStream<TcpStream>,
    serializer: std::marker::PhantomData<S>,
}

impl<S: Serializer + 'static> TlsTcpClient<S> {
    /// Connects to a TLS server without certificate verification.
    ///
    /// **Warning**: This should only be used for development/testing with self-signed certificates.
    /// Production code should always verify certificates.
    pub async fn connect_insecure(addr: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Create a custom verifier that accepts all certificates
        #[derive(Debug)]
        struct AcceptAllVerifier;
        impl tokio_rustls::rustls::client::danger::ServerCertVerifier for AcceptAllVerifier {
            fn verify_server_cert(
                &self,
                _end_entity: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
                _intermediates: &[tokio_rustls::rustls::pki_types::CertificateDer<'_>],
                _server_name: &tokio_rustls::rustls::pki_types::ServerName<'_>,
                _ocsp_response: &[u8],
                _now: tokio_rustls::rustls::pki_types::UnixTime,
            ) -> Result<tokio_rustls::rustls::client::danger::ServerCertVerified, tokio_rustls::rustls::Error> {
                Ok(tokio_rustls::rustls::client::danger::ServerCertVerified::assertion())
            }

            fn verify_tls12_signature(
                &self,
                _message: &[u8],
                _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
                _dss: &tokio_rustls::rustls::DigitallySignedStruct,
            ) -> Result<tokio_rustls::rustls::client::danger::HandshakeSignatureValid, tokio_rustls::rustls::Error> {
                Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
            }

            fn verify_tls13_signature(
                &self,
                _message: &[u8],
                _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
                _dss: &tokio_rustls::rustls::DigitallySignedStruct,
            ) -> Result<tokio_rustls::rustls::client::danger::HandshakeSignatureValid, tokio_rustls::rustls::Error> {
                Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
            }

            fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> {
                vec![
                    tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA256,
                    tokio_rustls::rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
                    tokio_rustls::rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
                    tokio_rustls::rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
                    tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA384,
                    tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA512,
                    tokio_rustls::rustls::SignatureScheme::RSA_PSS_SHA256,
                    tokio_rustls::rustls::SignatureScheme::RSA_PSS_SHA384,
                    tokio_rustls::rustls::SignatureScheme::RSA_PSS_SHA512,
                    tokio_rustls::rustls::SignatureScheme::ED25519,
                ]
            }
        }
        
        let client_config = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAllVerifier))
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(client_config));

        let stream = TcpStream::connect(addr).await?;
        let server_name = ServerName::try_from(
            addr.split(':')
                .next()
                .unwrap_or("localhost")
                .to_string(),
        )
        .map_err(|_| "Invalid server name")?;

        let tls_stream = connector.connect(server_name, stream).await?;

        Ok(Self {
            stream: tls_stream,
            serializer: PhantomData,
        })
    }
}

#[async_trait]
impl<S: Serializer + 'static> ProtocolClient for TlsTcpClient<S> {
    /// Connects to a TLS-enabled TCP server.
    ///
    /// ## Arguments
    ///
    /// * `addr` - Server address in format "hostname:port"
    ///
    /// ## Returns
    ///
    /// A connected TLS client, or error if connection/handshake fails.
    ///
    /// ## Certificate Validation
    ///
    /// By default, the client verifies the server's certificate. For self-signed
    /// certificates in development, you may need to disable verification or add
    /// the CA certificate to the trust store.
    async fn connect(addr: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Create TLS client config with default root certificates
        let mut root_store = RootCertStore::empty();
        for cert in rustls_native_certs::load_native_certs()? {
            root_store.add(cert)?;
        }

        let client_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(client_config));

        // Parse address and establish connection
        let stream = TcpStream::connect(addr).await?;
        let server_name = ServerName::try_from(
            addr.split(':')
                .next()
                .unwrap_or("localhost")
                .to_string(),
        )
        .map_err(|_| "Invalid server name")?;

        let tls_stream = connector.connect(server_name, stream).await?;

        Ok(Self {
            stream: tls_stream,
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
        let cmd_data = S::serialize_command(&Command::Get(Cow::Borrowed(key)));
        self.stream.write_all(&cmd_data).await?;

        let mut buffer = [0; 8192];
        let n = self.stream.read(&mut buffer).await?;
        let response = S::deserialize_response(&buffer[..n])?;

        handle_get_response(response)
    }

    async fn put(&mut self, key: &str, value: &[u8], ttl: u32) -> Result<(), Box<dyn Error + Send + Sync>> {
        let cmd_data = S::serialize_command(&Command::Put(Cow::Borrowed(key), value.to_vec(), ttl));
        self.stream.write_all(&cmd_data).await?;

        let mut buffer = [0; 1024];
        let n = self.stream.read(&mut buffer).await?;
        let response = S::deserialize_response(&buffer[..n])?;

        handle_put_response(response)
    }

    async fn delete(&mut self, key: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let cmd_data = S::serialize_command(&Command::Delete(Cow::Borrowed(key)));
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

    async fn stats(&mut self) -> Result<String, Box<dyn Error + Send + Sync>> {
        let cmd_data = S::serialize_command(&Command::Stats);
        self.stream.write_all(&cmd_data).await?;

        let mut buffer = [0; 4096];
        let n = self.stream.read(&mut buffer).await?;
        let response = S::deserialize_response(&buffer[..n])?;

        match response {
            Response::Ok(data) => {
                String::from_utf8(data)
                    .map_err(|e| format!("Invalid UTF-8 in stats response: {}", e).into())
            }
            Response::Error(msg) => Err(msg.into()),
            _ => Err("Invalid stats response".into()),
        }
    }

    async fn peer(&mut self) -> Result<String, Box<dyn Error + Send + Sync>> {
        let cmd_data = S::serialize_command(&Command::Peer);
        self.stream.write_all(&cmd_data).await?;

        let mut buffer = [0; 4096];
        let n = self.stream.read(&mut buffer).await?;
        let response = S::deserialize_response(&buffer[..n])?;

        handle_peer_response(response)
    }

    async fn clear(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let cmd_data = S::serialize_command(&Command::Clear);
        self.stream.write_all(&cmd_data).await?;

        let mut buffer = [0; 4096];
        let n = self.stream.read(&mut buffer).await?;
        let response = S::deserialize_response(&buffer[..n])?;

        match response {
            Response::Ok(_) => Ok(()),
            Response::Error(msg) => Err(msg.into()),
            _ => Err("Invalid clear response".into()),
        }
    }
}
