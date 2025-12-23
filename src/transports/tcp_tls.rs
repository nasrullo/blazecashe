//! # TCP Transport with TLS/Encryption Support
//!
//! This module provides TLS-encrypted TCP transport for BlazeCache, similar to how
//! UDP has QUIC-like features. TLS provides:
//!
//! - **Encryption**: All data in transit is encrypted
//! - **Authentication**: Server certificate verification
//! - **Integrity**: Protection against tampering
//! - **Forward Secrecy**: Perfect forward secrecy with ephemeral keys
//!
//! ## TLS Features
//!
//! ### 1. **Server-Side TLS**
//!    - Server presents certificate to clients
//!    - Supports custom certificates or auto-generated self-signed certs
//!    - Configurable TLS version (TLS 1.2+)
//!
//! ### 2. **Client-Side TLS**
//!    - Client verifies server certificate
//!    - Optional client certificate authentication (mutual TLS)
//!    - Configurable certificate validation
//!
//! ### 3. **Backward Compatibility**
//!    - Plain TCP still available for local/trusted networks
//!    - TLS is opt-in via configuration
//!    - Same protocol interface for both plain and TLS
//!
//! ## Usage
//!
//! ### Server with TLS
//!
//! ```rust,no_run
//! use blazecache::{Group, Getter, transports::{TlsTcpServer, ProtocolServer}, serializers::BinarySerializer};
//! use std::sync::Arc;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let group = Arc::new(Group::new("cache".to_string(), 100 * 1024 * 1024, getter, "127.0.0.1:8080".to_string()));
//! let server = TlsTcpServer::<BinarySerializer>::new(group)
//!     .with_certificate("cert.pem", "key.pem")?;
//!
//! server.start(8080).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Client with TLS
//!
//! ```rust,no_run
//! use blazecache::transports::{TlsTcpClient, ProtocolClient};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut client = TlsTcpClient::connect("localhost:8080").await?;
//! client.ping().await?;
//! # Ok(())
//! # }
//! ```

use crate::transports::common::{Command, ProtocolClient, ProtocolServer, Response};
use crate::transports::{
    handle_connection, handle_get_response, handle_ping_response, handle_put_response,
};
use crate::utils::persistence::PersistenceManager;
use crate::Group;
use async_trait::async_trait;
use std::borrow::Cow;
use std::error::Error;
use std::fs;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex as AsyncMutex;
use tokio_rustls::rustls::{pki_types::CertificateDer, pki_types::PrivateKeyDer, ServerConfig};
use tokio_rustls::rustls::{
    pki_types::ServerName,
    ClientConfig, RootCertStore,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_rustls::server::TlsStream as ServerTlsStream;
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tracing::{error, info, warn};

/// TLS-enabled TCP server for BlazeCache.
///
/// Provides the same interface as `TcpServer` but with TLS encryption.
/// All client connections are encrypted using TLS 1.2+.
pub struct TlsTcpServer<S> {
    /// Reference to the cache group that handles the actual cache operations.
    group: Arc<Group>,

    /// Phantom data to associate the serializer type with this server.
    serializer: std::marker::PhantomData<S>,

    /// Optional persistence manager for WAL logging.
    persistence: Option<Arc<AsyncMutex<PersistenceManager>>>,

    /// TLS server configuration.
    tls_config: Arc<ServerConfig>,
}

impl<S> TlsTcpServer<S> {
    /// Creates a new TLS TCP server instance.
    ///
    /// The server will handle incoming TLS-encrypted TCP connections.
    /// You must call `with_certificate()` or `with_auto_certificate()` before starting.
    pub fn new(group: Arc<Group>) -> Self {
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
        persistence: Option<Arc<AsyncMutex<PersistenceManager>>>,
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

        let certs: Vec<CertificateDer> = certs.into_iter().map(CertificateDer::from).collect();
        let key = PrivateKeyDer::Pkcs8(keys.remove(0).into());

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
impl<S: crate::transports::tcp::Serializer + 'static> ProtocolServer for TlsTcpServer<S> {
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

async fn handle_tls_tcp_connection<S: crate::transports::tcp::Serializer + 'static>(
    mut stream: ServerTlsStream<TcpStream>,
    server: Arc<TlsTcpServer<S>>,
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

/// TLS-enabled TCP client for BlazeCache.
///
/// Provides the same interface as `TcpClient` but with TLS encryption.
/// All communication with the server is encrypted using TLS 1.2+.
pub struct TlsTcpClient<S> {
    stream: ClientTlsStream<TcpStream>,
    serializer: std::marker::PhantomData<S>,
}

impl<S: crate::transports::tcp::Serializer + 'static> TlsTcpClient<S> {
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
impl<S: crate::transports::tcp::Serializer + 'static> ProtocolClient for TlsTcpClient<S> {
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
}

