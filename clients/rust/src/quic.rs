//! QUIC client implementation for BlazeCache
//!
//! This module provides a QUIC client that uses UDP transport with QUIC protocol.
//! The client is independent of the server implementation and can connect to
//! any BlazeCache server that supports QUIC/UDP.

use tokio::sync::Mutex;
use std::sync::Arc;
use std::io::Error as IOError;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::error::Error as STDError;

use blazecache::serializers::BinarySerializer;
use blazecache::transports::{UdpClient, ProtocolClient};
use crate::ClientError;

/// QUIC client for BlazeCache
///
/// This client uses QUIC protocol over UDP for communication with BlazeCache servers.
/// QUIC provides connection multiplexing, improved congestion control, and faster
/// connection establishment compared to TCP.
///
/// ## Example
///
/// ```rust,no_run
/// use blazecache_client::QuicClient;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let mut client = QuicClient::new("127.0.0.1:6793").await?;
///     
///     // Set a value
///     client.set("key", b"value".to_vec()).await?;
///     
///     // Get a value
///     let value = client.get("key").await?;
///     println!("Got: {:?}", value);
///     
///     Ok(())
/// }
/// ```
pub struct QuicClient {
    client: Arc<Mutex<UdpClient<BinarySerializer>>>,
}

impl QuicClient {
    /// Create a new QUIC client connected to the specified server
    ///
    /// # Arguments
    ///
    /// * `server_addr` - Server address in format "host:port" (e.g., "127.0.0.1:6793")
    ///
    /// # Errors
    ///
    /// Returns `ClientError` if connection fails
    pub async fn new(server_addr: &str) -> Result<Self, ClientError> {
        let client = UdpClient::<BinarySerializer>::connect(server_addr)
            .await
            .map_err(|e| ClientError::Io(IOError::new(std::io::ErrorKind::Other, e.to_string())))?;
        
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }

    /// Ping the server to verify connectivity
    ///
    /// # Errors
    ///
    /// Returns `ClientError` if ping fails
    pub async fn ping(&self) -> Result<(), ClientError> {
        let mut client = self.client.lock().await;
        client.ping().await
            .map_err(|e| ClientError::Io(IOError::new(std::io::ErrorKind::Other, e.to_string())))
    }

    /// Get a value from the cache
    ///
    /// # Arguments
    ///
    /// * `key` - The key to retrieve
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(value))` if the key exists, `Ok(None)` if not found,
    /// or `Err` if an error occurs
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ClientError> {
        let mut client = self.client.lock().await;
        match client.get(key).await {
            Ok(value) => Ok(Some(value)),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("Not found") || err_str.contains("not found") {
                    Ok(None)
                } else {
                    Err(ClientError::Io(IOError::new(std::io::ErrorKind::Other, err_str)))
                }
            }
        }
    }

    /// Set a value in the cache
    ///
    /// # Arguments
    ///
    /// * `key` - The key to set
    /// * `value` - The value to store
    ///
    /// # Errors
    ///
    /// Returns `ClientError` if the operation fails
    pub async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), ClientError> {
        self.set_with_ttl(key, value, 0).await
    }

    /// Set a value in the cache with TTL
    ///
    /// # Arguments
    ///
    /// * `key` - The key to set
    /// * `value` - The value to store
    /// * `ttl_secs` - Time to live in seconds (0 = no expiration)
    ///
    /// # Errors
    ///
    /// Returns `ClientError` if the operation fails
    pub async fn set_with_ttl(&self, key: &str, value: Vec<u8>, ttl_secs: u32) -> Result<(), ClientError> {
        let mut client = self.client.lock().await;
        client.put(key, &value, ttl_secs).await
            .map_err(|e| ClientError::Io(IOError::new(std::io::ErrorKind::Other, e.to_string())))
    }

    /// Delete a value from the cache
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` if the key was deleted, `Ok(false)` if not found,
    /// or `Err` if an error occurs
    pub async fn delete(&self, key: &str) -> Result<bool, ClientError> {
        let mut client = self.client.lock().await;
        client.delete(key).await
            .map_err(|e| ClientError::Io(IOError::new(std::io::ErrorKind::Other, e.to_string())))
    }
}


