//! # UDP Transport (QUIC)
//!
//! This module implements a QUIC-based transport using the Quinn library over UDP.
//! QUIC provides reliable, multiplexed, and secure transport.
//!
//! ## Features
//!
//! - **Reliability**: Built-in retransmission and congestion control
//! - **Multiplexing**: Multiple concurrent streams per connection
//! - **Security**: TLS 1.3 encryption built-in
//! - **Performance**: Optimized for low latency and high throughput
//! - **Automatic Fragmentation**: Large messages handled automatically

use crate::transports::common::{Command, ProtocolClient, ProtocolServer, Response};
use crate::transports::{
    handle_command, handle_get_response, handle_peer_response, handle_ping_response, handle_put_response, PersistenceManagerHandle, Serializer,
};
use crate::Group;
use async_trait::async_trait;
use quinn::{Endpoint, Connection, RecvStream, SendStream};
use quinn_proto::crypto::rustls::{QuicServerConfig, QuicClientConfig};
use rcgen::{CertificateParams, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{info, warn};

/// UDP Server using QUIC (via Quinn library)
pub struct UdpServer<S> {
    group: Arc<Group>,
    persistence: PersistenceManagerHandle,
    serializer: std::marker::PhantomData<S>,
}

impl<S: Serializer + 'static> UdpServer<S> {
    pub fn new(group: Arc<Group>) -> Self {
        Self {
            group,
            persistence: None,
            serializer: std::marker::PhantomData,
        }
    }

    pub fn with_persistence(
        group: Arc<Group>,
        persistence: PersistenceManagerHandle,
    ) -> Self {
        Self {
            group,
            persistence,
            serializer: std::marker::PhantomData,
        }
    }

    /// Generate a self-signed certificate for development/testing
    fn generate_certificate() -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), Box<dyn Error + Send + Sync>> {
        let key_pair = KeyPair::generate()?;
        let params = CertificateParams::new(vec!["localhost".to_string()])?;
        let cert = params.self_signed(&key_pair)?;
        
        let cert_bytes = cert.der().to_vec();
        let cert_der = CertificateDer::from(cert_bytes);
        let key_bytes = key_pair.serialize_der();
        let key_der = PrivateKeyDer::Pkcs8(key_bytes.into());
        
        Ok((vec![cert_der], key_der))
    }
}

#[async_trait]
impl<S: Serializer + 'static> ProtocolServer for UdpServer<S> {
    async fn start(&self, port: u16) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Install default crypto provider for rustls (must be called before using rustls)
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        
        // Generate self-signed certificate for development
        let (certs, key) = Self::generate_certificate()?;
        
        // Create server configuration using rustls
        let rustls_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)?;
        
        // Create Quinn server config from rustls config
        let quic_server_config = QuicServerConfig::try_from(Arc::new(rustls_config))?;
        let server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_server_config));
        
        // Create endpoint with server config
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        let endpoint = Endpoint::server(server_config, addr)?;
        
        info!(port = port, "Starting UDP (QUIC) server");
        
        // Accept incoming connections
        while let Some(conn) = endpoint.accept().await {
            let connection = conn.await?;
            let group = Arc::clone(&self.group);
            let persistence = self.persistence.clone();
            
            info!("New QUIC connection from {}", connection.remote_address());
            
            // Handle connection in a spawned task
            tokio::spawn(async move {
                if let Err(e) = handle_udp_connection::<S>(connection, group, persistence).await {
                    warn!("Error handling QUIC connection: {}", e);
                }
            });
        }
        
        Ok(())
    }
}

/// Handle a single QUIC connection
async fn handle_udp_connection<S: Serializer>(
    connection: Connection,
    group: Arc<Group>,
    persistence: PersistenceManagerHandle,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Accept bidirectional streams
    while let Ok((send_stream, recv_stream)) = connection.accept_bi().await {
        let group_clone = Arc::clone(&group);
        let persistence_clone = persistence.clone();
        
        tokio::spawn(async move {
            if let Err(e) = handle_udp_stream::<S>(send_stream, recv_stream, group_clone, persistence_clone).await {
                warn!("Error handling QUIC stream: {}", e);
            }
        });
    }
    
    Ok(())
}

/// Handle a single QUIC stream (request/response)
async fn handle_udp_stream<S: Serializer>(
    mut send_stream: SendStream,
    mut recv_stream: RecvStream,
    group: Arc<Group>,
    persistence: PersistenceManagerHandle,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Read request - use read_to_end with size limit (4GB max)
    const MAX_REQUEST_SIZE: usize = 4 << 30; // 4GB
    let request_data = recv_stream.read_to_end(MAX_REQUEST_SIZE).await?;
    
    // Deserialize command
    let cmd = S::deserialize_command(&request_data)?;
    
    // Handle command
    let response = handle_command(&group, cmd, persistence).await;
    
    // Serialize response
    let response_data = S::serialize_response(&response);
    
    // Send response
    send_stream.write_all(&response_data).await?;
    send_stream.finish()?; // finish() is not async
    
    Ok(())
}

/// UDP Client using QUIC (via Quinn library)
pub struct UdpClient<S> {
    connection: Connection,
    serializer: std::marker::PhantomData<S>,
}

#[async_trait]
impl<S: Serializer + 'static> ProtocolClient for UdpClient<S> {
    async fn connect(addr: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Parse address
        let addr: SocketAddr = addr.parse()?;
        
        // Install default crypto provider for rustls
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        
        // Create client configuration (accept any certificate for development)
        let mut rustls_config = rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        
        // Disable certificate verification for development (insecure)
        // In production, use proper certificate validation
        rustls_config.dangerous().set_certificate_verifier(Arc::new(AcceptAllVerifier));
        
        // Create Quinn client config from rustls config
        let quic_client_config = QuicClientConfig::try_from(Arc::new(rustls_config))?;
        let client_config = quinn::ClientConfig::new(Arc::new(quic_client_config));
        
        // Create endpoint with client config
        let mut endpoint = Endpoint::client(SocketAddr::from(([0, 0, 0, 0], 0)))?;
        endpoint.set_default_client_config(client_config);
        
        // Connect
        let connection = endpoint.connect(addr, "localhost")?.await?;
        
        info!("Connected to QUIC server at {}", addr);
        
        Ok(Self {
            connection,
            serializer: std::marker::PhantomData,
        })
    }

    async fn ping(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let cmd = Command::Ping;
        let cmd_data = S::serialize_command(&cmd);
        
        // Open bidirectional stream
        let (mut send, mut recv) = self.connection.open_bi().await?;
        
        // Send request
        send.write_all(&cmd_data).await?;
        send.finish()?; // finish() is not async
        
        // Read response - use read_to_end with size limit
        const MAX_RESPONSE_SIZE: usize = 4 << 30; // 4GB
        let response_data = recv.read_to_end(MAX_RESPONSE_SIZE).await?;
        
        let response = S::deserialize_response(&response_data)?;
        handle_ping_response(response)
    }

    async fn get(&mut self, key: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        use std::borrow::Cow;
        let cmd = Command::Get(Cow::Borrowed(key));
        let cmd_data = S::serialize_command(&cmd);
        
        // Open bidirectional stream
        let (mut send, mut recv) = self.connection.open_bi().await?;
        
        // Send request
        send.write_all(&cmd_data).await?;
        send.finish()?; // finish() is not async
        
        // Read response - use read_to_end with size limit
        const MAX_RESPONSE_SIZE: usize = 4 << 30; // 4GB
        let response_data = recv.read_to_end(MAX_RESPONSE_SIZE).await?;
        
        let response = S::deserialize_response(&response_data)?;
        handle_get_response(response)
    }

    async fn put(&mut self, key: &str, value: &[u8], ttl: u32) -> Result<(), Box<dyn Error + Send + Sync>> {
        use std::borrow::Cow;
        let cmd = Command::Put(Cow::Borrowed(key), value.to_vec(), ttl);
        let cmd_data = S::serialize_command(&cmd);
        
        // Open bidirectional stream
        let (mut send, mut recv) = self.connection.open_bi().await?;
        
        // Send request
        send.write_all(&cmd_data).await?;
        send.finish()?; // finish() is not async
        
        // Read response - use read_to_end with size limit
        const MAX_RESPONSE_SIZE: usize = 4 << 30; // 4GB
        let response_data = recv.read_to_end(MAX_RESPONSE_SIZE).await?;
        
        let response = S::deserialize_response(&response_data)?;
        handle_put_response(response)
    }

    async fn delete(&mut self, key: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
        use std::borrow::Cow;
        let cmd = Command::Delete(Cow::Borrowed(key));
        let cmd_data = S::serialize_command(&cmd);
        
        // Open bidirectional stream
        let (mut send, mut recv) = self.connection.open_bi().await?;
        
        // Send request
        send.write_all(&cmd_data).await?;
        send.finish()?; // finish() is not async
        
        // Read response - use read_to_end with size limit
        const MAX_RESPONSE_SIZE: usize = 4 << 30; // 4GB
        let response_data = recv.read_to_end(MAX_RESPONSE_SIZE).await?;
        
        let response = S::deserialize_response(&response_data)?;
        match response {
            Response::Ok(_) => Ok(true),
            Response::Error(msg) if msg == "Not found" => Ok(false),
            Response::Error(msg) => Err(msg.into()),
            _ => Err("Invalid delete response".into()),
        }
    }

    async fn stats(&mut self) -> Result<String, Box<dyn Error + Send + Sync>> {
        let cmd = Command::Stats;
        let cmd_data = S::serialize_command(&cmd);
        
        // Open bidirectional stream
        let (mut send, mut recv) = self.connection.open_bi().await?;
        
        // Send request
        send.write_all(&cmd_data).await?;
        send.finish()?; // finish() is not async
        
        // Read response - use read_to_end with size limit
        const MAX_RESPONSE_SIZE: usize = 4 << 30; // 4GB
        let response_data = recv.read_to_end(MAX_RESPONSE_SIZE).await?;
        
        let response = S::deserialize_response(&response_data)?;
        match response {
            Response::Ok(data) => Ok(String::from_utf8(data)?),
            Response::Error(msg) => Err(msg.into()),
            _ => Err("Invalid stats response".into()),
        }
    }

    async fn peer(&mut self) -> Result<String, Box<dyn Error + Send + Sync>> {
        let cmd = Command::Peer;
        let cmd_data = S::serialize_command(&cmd);
        
        // Open bidirectional stream
        let (mut send, mut recv) = self.connection.open_bi().await?;
        
        // Send request
        send.write_all(&cmd_data).await?;
        send.finish()?; // finish() is not async
        
        // Read response - use read_to_end with size limit
        const MAX_RESPONSE_SIZE: usize = 4 << 30; // 4GB
        let response_data = recv.read_to_end(MAX_RESPONSE_SIZE).await?;
        
        let response = S::deserialize_response(&response_data)?;
        handle_peer_response(response)
    }

    async fn clear(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let cmd = Command::Clear;
        let cmd_data = S::serialize_command(&cmd);
        
        // Open bidirectional stream
        let (mut send, mut recv) = self.connection.open_bi().await?;
        
        // Send request
        send.write_all(&cmd_data).await?;
        send.finish()?; // finish() is not async
        
        // Read response - use read_to_end with size limit
        const MAX_RESPONSE_SIZE: usize = 4 << 30; // 4GB
        let response_data = recv.read_to_end(MAX_RESPONSE_SIZE).await?;
        
        let response = S::deserialize_response(&response_data)?;
        match response {
            Response::Ok(_) => Ok(()),
            Response::Error(msg) => Err(msg.into()),
            _ => Err("Invalid clear response".into()),
        }
    }
}

/// Certificate verifier that accepts all certificates (for development only)
#[derive(Debug)]
struct AcceptAllVerifier;

impl rustls::client::danger::ServerCertVerifier for AcceptAllVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}
