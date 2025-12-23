//! # Common Transport Types
//!
//! This module defines the core types used by all transport implementations in BlazeCache.
//! It includes command and response enums, as well as the protocol traits that all
//! transport implementations must satisfy.
//!
//! ## Protocol Overview
//!
//! BlazeCache uses a simple request-response protocol where:
//! - Clients send `Command` messages to servers
//! - Servers respond with `Response` messages
//! - All transports (TCP, TLS, UDP) use the same command/response types
//!
//! ## Command Types
//!
//! - **GET**: Retrieve a value by key
//! - **PUT**: Store a key-value pair with optional TTL
//! - **DELETE**: Remove a key from the cache
//! - **PING**: Health check / connection test
//! - **STATS**: Get cache statistics
//! - **PEER**: List all peers in the cluster
//! - **CLEAR**: Clear all entries from main and hot caches
//!
//! ## Response Types
//!
//! - **Ok**: Successful operation (may include data)
//! - **Error**: Operation failed (includes error message)
//! - **Pong**: Response to PING command

use std::borrow::Cow;
use async_trait::async_trait;
use std::error::Error;

/// Protocol commands that clients can send to servers.
///
/// Commands use `Cow<'a, str>` for keys to allow both borrowed and owned strings,
/// which reduces allocations when keys are already available as string slices.
///
/// ## Variants
///
/// - `Get(key)`: Retrieve the value associated with a key
/// - `Put(key, value, ttl)`: Store a key-value pair with optional time-to-live in seconds
/// - `Delete(key)`: Remove a key from the cache
/// - `Peer`: Request list of all peers in the cluster
/// - `Ping`: Health check command (server responds with Pong)
/// - `Stats`: Request cache statistics (hits, misses, memory usage, etc.)
/// - `Clear`: Clear all entries from both main and hot caches
///
/// ## Example
///
/// ```rust
/// use blazecache::transports::common::Command;
/// use std::borrow::Cow;
///
/// // Create commands
/// let get_cmd = Command::Get(Cow::Borrowed("my_key"));
/// let put_cmd = Command::Put(Cow::Borrowed("my_key"), vec![1, 2, 3], 3600);
/// let ping_cmd = Command::Ping;
/// ```
#[derive(Debug, Clone)]
pub enum Command<'a> {
    /// Retrieve a value from the cache by key.
    ///
    /// If the key exists, the server responds with `Response::Ok(value)`.
    /// If the key doesn't exist, the server responds with `Response::Error("Not found")`.
    Get(Cow<'a, str>),

    /// Store a key-value pair in the cache.
    ///
    /// ## Parameters
    ///
    /// - `key`: The cache key (string)
    /// - `value`: The data to store (byte vector)
    /// - `ttl`: Time-to-live in seconds (0 = no expiration)
    ///
    /// The server responds with `Response::Ok(empty)` on success, or
    /// `Response::Error(message)` if the operation fails (e.g., item too large).
    Put(Cow<'a, str>, Vec<u8>, u32),

    /// Remove a key from the cache.
    ///
    /// The server responds with:
    /// - `Response::Ok(empty)` if the key was deleted
    /// - `Response::Error("Not found")` if the key didn't exist
    Delete(Cow<'a, str>),

    /// Request list of all peers in the cluster.
    ///
    /// Used for cluster discovery and health monitoring. The server responds
    /// with `Response::Ok(peer_list)` where peer_list is a comma-separated
    /// string of peer addresses (e.g., "127.0.0.1:6784,127.0.0.1:6785").
    Peer,

    /// Health check command.
    ///
    /// Used to test connectivity and server responsiveness. The server
    /// responds with `Response::Pong` if it's alive and responsive.
    Ping,

    /// Request cache statistics.
    ///
    /// The server responds with `Response::Ok(stats_json)` containing
    /// a JSON-like string with cache metrics including:
    /// - hits, misses, puts, deletes
    /// - evictions, hot_items, rejected_items
    /// - ttl_evictions, entry_count, memory_usage
    Stats,

    /// Clear all entries from both main and hot caches.
    ///
    /// The server responds with `Response::Ok(empty)` on success.
    /// This command removes all cached items from both the main cache
    /// and hot cache, effectively resetting the cache to an empty state.
    Clear,
}

/// Protocol responses that servers send to clients.
///
/// Responses indicate the result of a command execution. They can contain
/// data (for GET, STATS, PEER commands) or just status information.
///
/// ## Variants
///
/// - `Ok(data)`: Operation succeeded, optionally includes response data
/// - `Error(message)`: Operation failed, includes error description
/// - `Pong`: Response to PING command (indicates server is alive)
///
/// ## Example
///
/// ```rust
/// use blazecache::transports::common::Response;
///
/// // Successful GET response
/// let ok_response = Response::Ok(vec![1, 2, 3]);
///
/// // Error response
/// let error_response = Response::Error("Key not found".to_string());
///
/// // PING response
/// let pong_response = Response::Pong;
/// ```
#[derive(Debug, Clone)]
pub enum Response {
    /// Successful operation response.
    ///
    /// Contains the response data as a byte vector. For commands that don't
    /// return data (PUT, DELETE), this will be an empty vector.
    ///
    /// ## Data Contents by Command
    ///
    /// - **GET**: The cached value
    /// - **PUT/DELETE**: Empty vector (success indicator)
    /// - **STATS**: JSON-like string with statistics
    /// - **PEER**: Comma-separated list of peer addresses
    Ok(Vec<u8>),

    /// Error response indicating operation failure.
    ///
    /// Contains a human-readable error message describing what went wrong.
    ///
    /// ## Common Error Messages
    ///
    /// - `"Not found"`: Key doesn't exist (for GET, DELETE)
    /// - `"Item too large"`: Value exceeds cache size limit (for PUT)
    /// - `"Key empty"`: Key is empty or invalid (for all commands)
    /// - Custom messages for other error conditions
    Error(String),

    /// Response to PING command.
    ///
    /// Indicates the server is alive and responsive. This is used for
    /// health checks and connection testing.
    Pong,
}

/// Trait for protocol server implementations.
///
/// All transport implementations (TCP, TLS, UDP) must implement this trait
/// to provide a consistent server interface. The trait defines how servers
/// start listening for connections.
///
/// ## Implementation Requirements
///
/// - Servers must handle multiple concurrent connections
/// - Servers must parse incoming commands and route them to the cache
/// - Servers must serialize and send responses back to clients
/// - Servers should handle connection errors gracefully
///
/// ## Example
///
/// ```rust,no_run
/// use blazecache::transports::{ProtocolServer, TcpServer};
/// use blazecache::serializers::BinarySerializer;
///
/// let server = TcpServer::<BinarySerializer>::new(group);
/// server.start(8080).await?;
/// ```
#[async_trait]
pub trait ProtocolServer: Send + Sync {
    /// Starts the server listening on the specified port.
    ///
    /// This method should:
    /// 1. Bind to the specified port
    /// 2. Accept incoming connections
    /// 3. Handle each connection concurrently
    /// 4. Run indefinitely until a fatal error occurs
    ///
    /// ## Arguments
    ///
    /// * `port` - The port number to listen on
    ///
    /// ## Returns
    ///
    /// Returns `Ok(())` if the server starts successfully. The method will
    /// run indefinitely, only returning on fatal errors (e.g., port already in use).
    ///
    /// ## Errors
    ///
    /// - Port already in use
    /// - Permission denied (ports < 1024 require root on Unix)
    /// - Network interface unavailable
    /// - Other transport-specific errors
    async fn start(&self, port: u16) -> Result<(), Box<dyn Error + Send + Sync>>;
}

/// Trait for protocol client implementations.
///
/// All transport implementations (TCP, TLS, UDP) must implement this trait
/// to provide a consistent client interface. The trait defines how clients
/// connect to servers and execute cache operations.
///
/// ## Implementation Requirements
///
/// - Clients must establish connections to servers
/// - Clients must serialize commands and send them
/// - Clients must deserialize responses and return results
/// - Clients should handle network errors appropriately
///
/// ## Optional Commands
///
/// Some commands (`delete`, `stats`, `peer`) have default implementations
/// that return "not supported" errors. This allows older clients to work
/// with newer servers that support these commands, while maintaining
/// backward compatibility.
///
/// ## Example
///
/// ```rust,no_run
/// use blazecache::transports::{ProtocolClient, TcpClient};
/// use blazecache::serializers::BinarySerializer;
///
/// let mut client = TcpClient::<BinarySerializer>::connect("127.0.0.1:8080").await?;
/// client.ping().await?;
/// let value = client.get("my_key").await?;
/// ```
#[async_trait]
pub trait ProtocolClient: Send + Sync {
    /// Establishes a connection to the server at the given address.
    ///
    /// ## Arguments
    ///
    /// * `addr` - Server address in format "hostname:port" or "ip:port"
    ///
    /// ## Returns
    ///
    /// A connected client instance, or an error if connection fails.
    ///
    /// ## Errors
    ///
    /// - Connection refused (server not running)
    /// - Network unreachable
    /// - DNS resolution failure
    /// - Transport-specific errors (TLS handshake, etc.)
    async fn connect(addr: &str) -> Result<Self, Box<dyn Error + Send + Sync>>
    where
        Self: Sized;

    /// Sends a PING command to test connectivity.
    ///
    /// This is a lightweight operation used for health checks and connection
    /// testing. The server should respond with `Response::Pong`.
    ///
    /// ## Returns
    ///
    /// `Ok(())` if the server responds with Pong, or an error if the connection
    /// fails or the response is invalid.
    async fn ping(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Retrieves a value from the cache by key.
    ///
    /// ## Arguments
    ///
    /// * `key` - The cache key to retrieve
    ///
    /// ## Returns
    ///
    /// The cached value as a byte vector, or an error if:
    /// - The key doesn't exist (returns empty vector for compatibility)
    /// - Network error occurs
    /// - Response format is invalid
    async fn get(&mut self, key: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>>;

    /// Stores a key-value pair in the cache.
    ///
    /// ## Arguments
    ///
    /// * `key` - The cache key
    /// * `value` - The data to store
    /// * `ttl` - Time-to-live in seconds (0 = no expiration)
    ///
    /// ## Returns
    ///
    /// `Ok(())` if the value was stored successfully, or an error if:
    /// - The item is too large for the cache
    /// - Network error occurs
    /// - Response format is invalid
    async fn put(&mut self, key: &str, value: &[u8], ttl: u32) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Removes a key from the cache.
    ///
    /// This is an optional command with a default implementation that returns
    /// "not supported" for backward compatibility. Most modern clients
    /// should implement this method.
    ///
    /// ## Arguments
    ///
    /// * `key` - The cache key to delete
    ///
    /// ## Returns
    ///
    /// `Ok(true)` if the key was deleted, `Ok(false)` if the key didn't exist,
    /// or an error if the operation fails.
    async fn delete(&mut self, _key: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
        Err("delete not supported by this client".into())
    }

    /// Retrieves cache statistics.
    ///
    /// This is an optional command with a default implementation that returns
    /// "not supported" for backward compatibility. Most modern clients
    /// should implement this method.
    ///
    /// ## Returns
    ///
    /// A JSON-like string containing cache statistics, or an error if the
    /// operation fails. Statistics include hits, misses, puts, deletes,
    /// evictions, memory usage, etc.
    async fn stats(&mut self) -> Result<String, Box<dyn Error + Send + Sync>> {
        Err("stats not supported by this client".into())
    }

    /// Retrieves the list of all peers in the cluster.
    ///
    /// This is an optional command with a default implementation that returns
    /// "not supported" for backward compatibility. Most modern clients
    /// should implement this method.
    ///
    /// ## Returns
    ///
    /// A comma-separated string of peer addresses (e.g., "127.0.0.1:6784,127.0.0.1:6785"),
    /// or an error if the operation fails. Used for cluster discovery and
    /// client-side consistent hashing.
    async fn peer(&mut self) -> Result<String, Box<dyn Error + Send + Sync>> {
        Err("peer not supported by this client".into())
    }

    /// Clears all entries from both main and hot caches.
    ///
    /// This is an optional command with a default implementation that returns
    /// "not supported" for backward compatibility. Most modern clients
    /// should implement this method.
    ///
    /// ## Returns
    ///
    /// `Ok(())` if the cache was cleared successfully, or an error if the
    /// operation fails.
    async fn clear(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Err("clear not supported by this client".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_debug() {
        // Test that Command implements Debug trait
        let cmd = Command::Get(Cow::Borrowed("test"));
        let _ = format!("{:?}", cmd);
    }

    #[test]
    fn test_command_clone() {
        // Test that Command implements Clone trait
        let cmd1 = Command::Ping;
        let cmd2 = cmd1.clone();
        match (cmd1, cmd2) {
            (Command::Ping, Command::Ping) => {}
            _ => panic!("Clone failed"),
        }
    }

    #[test]
    fn test_response_debug() {
        // Test that Response implements Debug trait
        let resp = Response::Ok(vec![1, 2, 3]);
        let _ = format!("{:?}", resp);
    }

    #[test]
    fn test_response_clone() {
        // Test that Response implements Clone trait
        let resp1 = Response::Pong;
        let resp2 = resp1.clone();
        match (resp1, resp2) {
            (Response::Pong, Response::Pong) => {}
            _ => panic!("Clone failed"),
        }
    }

    #[test]
    fn test_command_variants() {
        // Test that all command variants can be created
        let _get = Command::Get(Cow::Borrowed("key"));
        let _put = Command::Put(Cow::Borrowed("key"), vec![1, 2, 3], 100);
        let _delete = Command::Delete(Cow::Borrowed("key"));
        let _peer = Command::Peer;
        let _ping = Command::Ping;
        let _stats = Command::Stats;
        let _clear = Command::Clear;
    }

    #[test]
    fn test_response_variants() {
        // Test that all response variants can be created
        let _ok = Response::Ok(vec![1, 2, 3]);
        let _error = Response::Error("test error".to_string());
        let _pong = Response::Pong;
    }
}
