//! # BlazeCache
//!
//! Ultra-high performance distributed cache with size-limited storage, binary protocol, and multi-language clients.
//!
//! ## Features
//!
//! - **Size-limited cache** - Configurable cache size with LRU eviction
//! - **Item size validation** - Individual items cannot exceed cache size
//! - **Binary protocol** - High-performance TCP/UDP protocol with 6 commands
//! - **Multi-language clients** - Rust, Java (JDK 25+), and Go clients
//! - **Hot item detection** - Automatic detection of frequently accessed data
//! - **Load balancing** - Round robin, weighted, and consistent hashing
//!
//! ## Quick Start
//!
//! ```rust
//! use blazecache::Cache;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create 1MB cache
//!     let cache = Cache::new(1024 * 1024);
//!     
//!     // Store data (validates size)
//!     cache.put("key".to_string(), b"value".to_vec(), 0).await?;
//!     
//!     // Retrieve data
//!     if let Some(value) = cache.get("key").await? {
//!         println!("Found: {:?}", String::from_utf8_lossy(&value));
//!     }
//!     
//!     // Large items are rejected
//!     let large_item = vec![0u8; 2 * 1024 * 1024]; // 2MB > 1MB cache
//!     match cache.put("large".to_string(), large_item, 0).await {
//!         Err(blazecache::BlazeCacheError::ItemTooLarge { .. }) => {
//!             println!("Item too large for cache!");
//!         }
//!         _ => {}
//!     }
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Protocol Support
//!
//! BlazeCache implements a binary protocol with these commands:
//! - **GET (0x01)** - Retrieve value by key
//! - **PUT (0x02)** - Store key-value pair  
//! - **DELETE (0x03)** - Remove key
//! - **PING (0x04)** - Health check
//! - **STATS (0x05)** - Server statistics
//! - **PEER (0x06)** - List cluster peers
//!
//! See [PROTOCOL.md](../PROTOCOL.md) for complete specification.

pub mod cache;
pub mod networking;
pub mod serializers;
pub mod transports;
pub mod utils;

// Re-export main types for convenience
pub use cache::Cache;
pub use cache::Value;
pub use cache::{Getter, Group, Setter};
pub use utils::error::{BlazeCacheError, Result};
pub use utils::persistence::PersistenceConfig;

// Type aliases for protocol combinations
pub use transports::{TcpClient, TcpServer, UdpClient, UdpServer};

// Hash map type alias for consistent hashing
use fnv::FnvBuildHasher;
use std::collections::HashMap;

/// Fast hash map using FNV hasher for better performance with string keys
///
/// This is used internally for consistent hashing and peer management
/// where hash speed is more important than cryptographic security.
///
/// ## Example
///
/// ```rust
/// use blazecache::FnvHashMap;
///
/// let mut map: FnvHashMap<String, i32> = FnvHashMap::default();
/// map.insert("key".to_string(), 42);
/// ```
pub type FnvHashMap<K, V> = HashMap<K, V, FnvBuildHasher>;
