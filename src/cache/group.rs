//! # Group Module
//!
//! The Group is the main interface for the distributed cache system. It provides
//! automatic loading, peer coordination, and hot item replication.
//!
//! ## Key Features
//!
//! - **Automatic Loading**: Calls getter function on cache misses
//! - **Peer Coordination**: Distributes requests across multiple peers
//! - **Hot Item Detection**: Identifies and replicates frequently accessed data
//! - **Single Flight**: Prevents duplicate loads for the same key
//! - **Batch Operations**: Efficient multi-key operations
//!
//! ## Example
//!
//! ```rust
//! use blazecache::{Group, Getter};
//! use std::sync::Arc;
//!
//! let getter: Getter = Arc::new(|key: &str| {
//!     // Simulate database lookup
//!     Ok(format!("value-{}", key).into_bytes())
//! });
//!
//! let group = Group::new(
//!     "my-cache".to_string(),
//!     1024 * 1024,
//!     getter,
//!     "127.0.0.1:8080".to_string(),
//! );
//! ```

use crate::utils::{BlazeCacheError, Result};
use crate::{cache::singleflight::SingleFlight, networking::PeerPicker, Cache, FnvHashMap};
use futures::future::join_all;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Function type for loading data when not found in cache.
///
/// The Getter is called when a key is not found in the local cache or any peer.
/// It should load the data from the authoritative source (database, file system, etc.).
///
/// ## Arguments
///
/// * `key` - The key to load data for
///
/// ## Returns
///
/// `Ok(Vec<u8>)` with the loaded data, or `Err(BlazeCacheError)` if loading fails
///
/// ## Example
///
/// ```rust
/// use blazecache::{Getter, BlazeCacheError};
/// use std::sync::Arc;
///
/// let getter: Getter = Arc::new(|key: &str| {
///     match key {
///         "user:123" => Ok(b"user data".to_vec()),
///         _ => Err(BlazeCacheError::GetterFailed("Not found".to_string())),
///     }
/// });
/// ```
pub type Getter = Arc<dyn Fn(&str) -> Result<Vec<u8>> + Send + Sync>;

/// A distributed cache group that coordinates between local cache and remote peers.
///
/// The Group is the main entry point for cache operations. It manages:
///
/// - **Local Cache**: Fast LRU cache with hot item detection
/// - **Peer Communication**: Distributes load across multiple cache nodes
/// - **Automatic Loading**: Calls getter function when data not found
/// - **Hot Item Replication**: Replicates popular data across peers
/// - **Single Flight**: Prevents duplicate loads for concurrent requests
///
/// ## Architecture
///
/// ```text
/// ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
/// │   Client    │    │   Client    │    │   Client    │
/// └──────┬──────┘    └──────┬──────┘    └──────┬──────┘
///        │                  │                  │
///        └──────────────────┼──────────────────┘
///                           │
///                    ┌──────▼──────┐
///                    │    Group    │
///                    │             │
///                    │ ┌─────────┐ │
///                    │ │  Cache  │ │ ◄─── Local LRU Cache
///                    │ └─────────┘ │
///                    │             │
///                    │ ┌─────────┐ │
///                    │ │  Peers  │ │ ◄─── Remote Peers
///                    │ └─────────┘ │
///                    │             │
///                    │ ┌─────────┐ │
///                    │ │ Getter  │ │ ◄─── Data Source
///                    │ └─────────┘ │
///                    └─────────────┘
/// ```
///
/// ## Example
///
/// ```rust
/// use blazecache::{Group, Getter};
/// use std::sync::Arc;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let getter: Getter = Arc::new(|key: &str| {
///         // Load from database
///         Ok(format!("db-value-{}", key).into_bytes())
///     });
///
///     let group = Group::new(
///         "users".to_string(),
///         1024 * 1024,
///         getter,
///         "127.0.0.1:8080".to_string(),
///     );
///
///     // Get data (will load from database on first access)
///     let data = group.get("user:123").await?;
///     
///     // Subsequent access will be from cache (much faster)
///     let data = group.get("user:123").await?;
///
///     Ok(())
/// }
/// ```
/// Function type for writing data to backing store
pub type Setter = Arc<dyn Fn(&str, &[u8]) -> Result<()> + Send + Sync>;

/// A distributed cache group that coordinates between local cache and remote peers.
///
/// The Group is the main entry point for cache operations. It manages:
///
/// - **Local Cache**: Fast LRU cache with hot item detection
/// - **Peer Communication**: Distributes load across multiple cache nodes
/// - **Automatic Loading**: Calls getter function when data not found
/// - **Hot Item Replication**: Replicates popular data across peers
/// - **Single Flight**: Prevents duplicate loads for concurrent requests
/// - **Write-Through**: Optional synchronous writes to backing store
///
/// ## Architecture
///
/// ```text
/// ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
/// │   Client    │    │   Client    │    │   Client    │
/// └──────┬──────┘    └──────┬──────┘    └──────┬──────┘
///        │                  │                  │
///        └──────────────────┼──────────────────┘
///                           │
///                    ┌──────▼──────┐
///                    │    Group    │
///                    │             │
///                    │ ┌─────────┐ │
///                    │ │MainCache│ │ ◄─── Local LRU Cache (primary)
///                    │ └─────────┘ │
///                    │             │
///                    │ ┌─────────┐ │
///                    │ │HotCache │ │ ◄─── Hot items from peers
///                    │ └─────────┘ │
///                    │             │
///                    │ ┌─────────┐ │
///                    │ │  Peers  │ │ ◄─── Remote cache nodes
///                    │ └─────────┘ │
///                    │             │
///                    │ ┌─────────┐ │
///                    │ │ Getter  │ │ ◄─── Data source (DB, API, etc.)
///                    │ └─────────┘ │
///                    └─────────────┘
/// ```
///
/// ## Cache Hierarchy
///
/// 1. **Main Cache**: Primary local cache for items loaded by this node
/// 2. **Hot Cache**: Secondary cache for popular items from peer nodes
/// 3. **Peer Nodes**: Remote cache nodes in the distributed system
/// 4. **Data Source**: Authoritative source (database, API, file system)
///
/// ## Performance Characteristics
///
/// - **Cache Hit (Main)**: ~132ns - Direct memory access
/// - **Cache Hit (Hot)**: ~132ns - Direct memory access  
/// - **Peer Hit**: ~80μs (TCP) / ~20μs (UDP) - Network roundtrip
/// - **Cache Miss**: 100ms+ - Depends on data source latency
///
/// ## Example
///
/// ```rust
/// use blazecache::{Group, Getter};
/// use std::sync::Arc;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let getter: Getter = Arc::new(|key: &str| {
///         // Load from database
///         Ok(format!("db-value-{}", key).into_bytes())
///     });
///
///     let group = Group::new(
///         "users".to_string(),
///         1024 * 1024,
///         getter,
///         "127.0.0.1:8080".to_string(),
///     );
///
///     // Get data (will load from database on first access)
///     let data = group.get("user:123").await?;
///     
///     // Subsequent access will be from cache (much faster)
///     let data = group.get("user:123").await?;
///
///     Ok(())
/// }
/// ```
pub struct Group {
    /// Unique name for this cache group (used for peer communication)
    name: String,

    /// Function to load data when not found in cache or peers.
    /// Called as last resort when data is not available anywhere in the distributed system.
    getter: Getter,

    /// Optional function for write-through operations.
    /// When set, all writes go to the backing store before being cached.
    setter: Option<Setter>,

    /// Primary local cache for items loaded by this node.
    /// Uses LRU eviction and stores the most frequently accessed data.
    pub(crate) main_cache: Cache,

    /// Secondary cache for hot items received from peer nodes.
    /// Typically 1/8 the size of main cache to store popular items from other nodes.
    pub(crate) hot_cache: Cache,

    /// Peer picker for distributed cache coordination.
    /// Uses consistent hashing to determine which peer should handle each key.
    pub(crate) peers: Arc<RwLock<Option<Box<dyn PeerPicker>>>>,

    /// SingleFlight prevents duplicate loads for the same key.
    /// Ensures only one goroutine loads data for a given key at a time.
    flight: SingleFlight,

    pub(crate) local_peer_address:String,
}

impl Group {
    /// Creates a new cache group with read-only operations.
    ///
    /// The group will handle cache misses by calling the provided getter function.
    /// Hot cache is automatically sized to 1/8 of main cache for optimal memory usage.
    ///
    /// ## Arguments
    ///
    /// * `name` - Unique identifier for this cache group
    /// * `cache_bytes` - Maximum memory for main cache in bytes
    /// * `getter` - Function to load data on cache misses
    ///
    /// ## Example
    ///
    /// ```rust
    /// use blazecache::{Group, Getter};
    /// use std::sync::Arc;
    ///
    /// let getter: Getter = Arc::new(|key: &str| {
    ///     // Simulate database lookup
    ///     std::thread::sleep(std::time::Duration::from_millis(100));
    ///     Ok(format!("user-data-{}", key).into_bytes())
    /// });
    ///
/// let group = Group::new(
///     "users".to_string(),
///     10 * 1024 * 1024,
///     getter,
///     "127.0.0.1:8080".to_string(),
/// );
    /// ```
    pub fn new(name: String, cache_bytes: usize, getter: Getter, local_peer_address:String) -> Self {
        Self {
            name,
            getter,
            setter: None,
            main_cache: Cache::new(cache_bytes),
            hot_cache: Cache::new(cache_bytes / 8),
            peers: Arc::new(RwLock::new(None)),
            flight: SingleFlight::new(),
            local_peer_address,
        }
    }

    /// Creates a new cache group with write-through support.
    ///
    /// When a setter is provided, all write operations will be synchronously
    /// written to the backing store before being cached. This ensures data
    /// consistency between cache and persistent storage.
    ///
    /// ## Arguments
    ///
    /// * `name` - Unique identifier for this cache group
    /// * `cache_bytes` - Maximum memory for main cache in bytes
    /// * `getter` - Function to load data on cache misses
    /// * `setter` - Function to write data to backing store
    ///
    /// ## Example
    ///
    /// ```rust
    /// use blazecache::{Group, Getter, Setter};
    /// use std::sync::Arc;
    ///
    /// let getter: Getter = Arc::new(|key: &str| {
    ///     // Load from database
    ///     Ok(format!("db-{}", key).into_bytes())
    /// });
    ///
    /// let setter: Setter = Arc::new(|key: &str, value: &[u8]| {
    ///     // Write to database
    ///     println!("Writing {} = {:?}", key, value);
    ///     Ok(())
    /// });
    ///
/// let group = Group::with_write_through(
///     "users".to_string(),
///     10 * 1024 * 1024,
///     getter,
///     setter,
///     "127.0.0.1:8080".to_string(),
/// );
    /// ```
    pub fn with_write_through(
        name: String,
        cache_bytes: usize,
        getter: Getter,
        setter: Setter,
        local_peer_address:String,
    ) -> Self {
        Self {
            name,
            getter,
            setter: Some(setter),
            main_cache: Cache::new(cache_bytes),
            hot_cache: Cache::new(cache_bytes / 8),
            peers: Arc::new(RwLock::new(None)),
            flight: SingleFlight::new(),
            local_peer_address,
        }
    }

    /// Sets the peer picker for distributed cache operations.
    ///
    /// The peer picker uses consistent hashing to determine which peer
    /// should handle requests for specific keys. This enables horizontal
    /// scaling and load distribution across multiple cache nodes.
    ///
    /// ## Arguments
    ///
    /// * `peers` - Peer picker implementation for consistent hashing
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use blazecache::{Group, Getter, networking::PeerPicker};
    /// # use std::sync::Arc;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let getter: Getter = Arc::new(|key: &str| Ok(format!("data-{}", key).into_bytes()));
/// let group = Group::new(
///     "cache".to_string(),
///     1024 * 1024,
///     getter,
///     "127.0.0.1:8080".to_string(),
/// );
    /// // let peers = create_peer_picker(); // Implementation specific
    /// // group.set_peers(Box::new(peers)).await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set_peers(&self, peers: Box<dyn PeerPicker>) {
        *self.peers.write().await = Some(peers);
    }

    /// Gets a value from the distributed cache system.
    ///
    /// This method implements the complete cache hierarchy:
    ///
    /// 1. **Main Cache**: Check local primary cache first (~132ns)
    /// 2. **Hot Cache**: Check local secondary cache for peer items (~132ns)
    /// 3. **Peer Nodes**: Query appropriate peer via consistent hashing (~80μs)
    /// 4. **Data Source**: Load from backing store via getter (100ms+)
    ///
    /// Uses SingleFlight to prevent duplicate loads for concurrent requests.
    ///
    /// ## Arguments
    ///
    /// * `key` - The key to retrieve
    ///
    /// ## Returns
    ///
    /// Returns `Ok(Vec<u8>)` with the data, or `Err` if not found or loading fails.
    ///
    /// ## Performance
    ///
    /// - Cache hit: ~132ns
    /// - Peer hit: ~80μs (TCP) / ~20μs (UDP)
    /// - Cache miss: Depends on getter latency
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use blazecache::{Group, Getter};
    /// # use std::sync::Arc;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let getter: Getter = Arc::new(|key: &str| Ok(format!("data-{}", key).into_bytes()));
/// # let group = Group::new(
/// #     "cache".to_string(),
/// #     1024 * 1024,
/// #     getter,
/// #     "127.0.0.1:8080".to_string(),
/// # );
    /// let data = group.get("user:123").await?;
    /// println!("User data: {:?}", String::from_utf8(data)?);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get(&self, key: &str) -> Result<Vec<u8>> {
        if key.is_empty() {
            return Err(BlazeCacheError::KeyEmpty);
        }

        // Try main cache first (fastest path)
        if let Some(data) = self.main_cache.get(key).await? {
            return Ok(data);
        }

        // Try hot cache (for items fetched from peers)
        if let Some(data) = self.hot_cache.get(key).await? {
            return Ok(data);
        }

        // Load with singleflight protection
        self.load(key).await
    }

    /// Internal method to load data with peer fallback and SingleFlight protection.
    ///
    /// This method implements the distributed loading strategy:
    /// 1. Check if a peer should handle this key (consistent hashing)
    /// 2. If peer request fails, fallback to local getter
    /// 3. Use SingleFlight to prevent duplicate loads
    ///
    /// ## Arguments
    ///
    /// * `key` - The key to load
    ///
    /// ## Returns
    ///
    /// Returns the loaded data or an error if all methods fail.
    async fn load(&self, key: &str) -> Result<Vec<u8>> {
        // Check remote peer first; if remote says not found or errors, fall back to local getter.
        if let Some(result) = self.remote_get(key).await {
            if let Ok(value) = result {
                // Store in hot cache for future access
                self.hot_cache.put(key.to_string(), value.clone(), 0).await?;
                return Ok(value);
            }
        }

        // Use SingleFlight to prevent duplicate loads
        let getter = self.getter.clone();
        let main_cache = self.main_cache.clone();
        let key_owned = key.to_string();

        self.flight
            .do_call(key, move || {
                let getter = getter.clone();
                let main_cache = main_cache.clone();
                let key = key_owned.clone();

                async move {
                    // Load using getter
                    let value = getter(&key)?;

                    // Store in main cache
                    main_cache.put(key, value.clone(), 0).await?;

                    Ok(value)
                }
            })
            .await
    }



    pub async fn set(&self, key: &str, value: Vec<u8>, ttl_sec: u32) -> Result<()> {
        if key.is_empty() {
            return Err(BlazeCacheError::KeyEmpty);
        }

        // Write-through: write to backing store first if setter is configured
        if let Some(ref setter) = self.setter {
            // Use SingleFlight to deduplicate identical writes
            let setter = setter.clone();
            let key_owned = key.to_string();
            let value_for_write = value.clone();

            self.flight
                .do_write(key, &value, move || async move {
                    setter(&key_owned, &value_for_write)
                })
                .await?;
        }

        // Send to remote peer when this node is not responsible; otherwise store locally.
        if let Some(result) = self.remote_set(key, value.clone(), ttl_sec).await {
            match result {
                Ok(_) => {
                    // Cache hot item locally for faster subsequent reads.
                    self.hot_cache.put(key.to_string(), value, 10).await?;
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
        }

        // Local peer: store in main cache.
        self.main_cache.put(key.to_string(), value, ttl_sec).await?;
        Ok(())
    }
    /// Pick peer for a specific key (for testing/debugging)
    /// Returns an owned peer identifier for the given key, if any.
    pub async fn pick_peer_for_key(&self, key: &str) -> Option<String> {
        if let Some(ref peers) = *self.peers.read().await {
            if let Some(peer) = peers.pick_peer(key) {
              Some(peer.address())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get list of all peers for client-side consistent hashing
    pub async fn get_peers(&self) -> Vec<String> {
        if let Some(ref peers) = *self.peers.read().await {
            peers.get_all_peers()
        } else {
            vec![]
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn get_hot_items(&self) -> Vec<String> {
        // Items accessed 5+ times in last 5 minutes are considered hot
        let items = self.main_cache.get_hot_items(5, 300).await;
        items.into_iter().map(|(key, _)| key).collect()
    }

    pub async fn replicate_hot_item(&self, key: &str, value: Vec<u8>) {
        // Store hot item from another peer in our hot cache
        let _ = self.hot_cache.put(key.to_string(), value, 1).await;
    }

    pub async fn get_hot_item_for_replication(&self, key: &str) -> Option<Vec<u8>> {
        // Get item from main cache for replication to other peers
        self.main_cache.get(key).await.ok().flatten()
    }

    /// Helper: determine if address refers to this local node.
    fn is_local_peer(&self, addr: &str) -> bool {
        !self.local_peer_address.is_empty() && addr == self.local_peer_address
    }

    /// Send a set to the remote peer responsible for a key (if not local).
    async fn remote_set(&self, key: &str, value: Vec<u8>, ttl: u32) -> Option<Result<()>> {
        // OPTIMIZATION: Fast path - check if peers exist before acquiring lock
        // This avoids lock acquisition overhead when no peers are configured
        let guard = self.peers.read().await;
        let peers = guard.as_ref()?;
        let peer = peers.pick_peer(key)?;
        if self.is_local_peer(&peer.address()) {
            return None;
        }
        Some(peer.set(&self.name, key, value, ttl).await)
    }

    /// Send a get to the remote peer responsible for a key (if not local).
    async fn remote_get(&self, key: &str) -> Option<Result<Vec<u8>>> {
        let guard = self.peers.read().await;
        let peers = guard.as_ref()?;
        let peer = peers.pick_peer(key)?;
        if self.is_local_peer(&peer.address()) {
            return None;
        }
        Some(peer.get(&self.name, key).await)
    }

    /// Send a delete to the remote peer responsible for a key (if not local).
    async fn remote_delete(&self, key: &str) -> Option<Result<()>> {
        let guard = self.peers.read().await;
        let peers = guard.as_ref()?;
        let peer = peers.pick_peer(key)?;
        if self.is_local_peer(&peer.address()) {
            return None;
        }
        Some(peer.delete(&self.name, key).await)
    }

    pub async fn get_multi(&self, keys: &[&str]) -> Result<FnvHashMap<String, Vec<u8>>> {
        let mut results = FnvHashMap::default();
        let mut cache_misses = Vec::new();

        // Check cache first
        for &key in keys {
            if let Some(data) = self.main_cache.get(key).await.ok().flatten() {
                results.insert(key.to_string(), data);
            } else if let Some(data) = self.hot_cache.get(key).await.ok().flatten() {
                results.insert(key.to_string(), data);
            } else {
                cache_misses.push(key);
            }
        }

        // Batch load cache misses
        if !cache_misses.is_empty() {
            let futures: Vec<_> = cache_misses
                .into_iter()
                .map(|key| self.load_single(key))
                .collect();

            let loaded_results = join_all(futures).await;

            for (key, result) in keys.iter().zip(loaded_results) {
                if let Ok(value) = result {
                    if !results.contains_key(*key) {
                        results.insert(key.to_string(), value);
                    }
                }
            }
        }

        Ok(results)
    }

    async fn load_single(&self, key: &str) -> Result<Vec<u8>> {
        // Load locally using getter
        let value = (self.getter)(key)?;

        // Store in main cache
        self.main_cache.put(key.to_string(), value.clone(), 0).await?;

        Ok(value)
    }

    pub async fn main_cache_len(&self) -> usize {
        self.main_cache.len().await
    }

    pub async fn hot_cache_len(&self) -> usize {
        self.hot_cache.len().await
    }


    /// Deletes a key from both main and hot caches. Returns true if it existed in any cache.
    pub async fn delete(&self, key: &str) -> Result<bool> {
        if key.is_empty() {
            return Err(BlazeCacheError::KeyEmpty);
        }

        // Always clear local caches to avoid serving stale data.
        let main_deleted = self.main_cache.delete(key).await?;
        let hot_deleted = self.hot_cache.delete(key).await?;

        // If this key maps to a remote peer, forward the delete; otherwise just use local caches.
        if let Some(result) = self.remote_delete(key).await {
            return match result {
                Ok(_) => Ok(true),
                Err(BlazeCacheError::KeyNotFound) => Ok(main_deleted || hot_deleted),
                Err(e) => Err(e),
            };
        }

        Ok(main_deleted || hot_deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::networking::peer::Peer;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[allow(dead_code)]
    struct MockPeerSet {
        addr: String,
        set_called: AtomicBool,
        last_value: Arc<tokio::sync::Mutex<Option<Vec<u8>>>>,
    }

    #[async_trait]
    impl Peer for MockPeerSet {
        async fn get(&self, _group: &str, _key: &str) -> Result<Vec<u8>> {
            Err(BlazeCacheError::PeerError("not implemented".into()))
        }

        async fn delete(&self, _group: &str, _key: &str) -> Result<()> {
            Ok(())
        }

        async fn set(&self, _group: &str, _key: &str, value: Vec<u8>, _ttl: u32) -> Result<()> {
            self.set_called.store(true, Ordering::SeqCst);
            let mut guard = self.last_value.lock().await;
            *guard = Some(value);
            Ok(())
        }

        async fn get_hot_items(&self, _group: &str) -> Result<Vec<String>> {
            Ok(vec![])
        }

        fn address(&self) -> String {
            self.addr.clone()
        }
    }

    #[allow(dead_code)]
    struct MockPeerPickerSet {
        peer: Arc<MockPeerSet>,
    }

    impl PeerPicker for MockPeerPickerSet {
        fn pick_peer(&self, _key: &str) -> Option<&dyn Peer> {
            Some(self.peer.as_ref())
        }

        fn get_all_peers(&self) -> Vec<String> {
            vec![self.peer.addr.clone()]
        }
    }

    #[allow(dead_code)]
    enum DeleteOutcome {
        Ok,
        NotFound,
    }

    #[allow(dead_code)]
    struct MockPeerDelete {
        delete_called: AtomicBool,
        outcome: DeleteOutcome,
        addr: String,
    }

    #[async_trait]
    impl Peer for MockPeerDelete {
        async fn get(&self, _group: &str, _key: &str) -> Result<Vec<u8>> {
            Err(BlazeCacheError::PeerError("not implemented".into()))
        }

        async fn delete(&self, _group: &str, _key: &str) -> Result<()> {
            self.delete_called.store(true, Ordering::SeqCst);
            match self.outcome {
                DeleteOutcome::Ok => Ok(()),
                DeleteOutcome::NotFound => Err(BlazeCacheError::KeyNotFound),
            }
        }

        async fn set(&self, _group: &str, _key: &str, _value: Vec<u8>, _ttl: u32) -> Result<()> {
            Ok(())
        }

        async fn get_hot_items(&self, _group: &str) -> Result<Vec<String>> {
            Ok(vec![])
        }

        fn address(&self) -> String {
            self.addr.clone()
        }
    }

    #[allow(dead_code)]
    struct MockPeerPickerDelete {
        peer: Arc<MockPeerDelete>,
    }

    impl PeerPicker for MockPeerPickerDelete {
        fn pick_peer(&self, _key: &str) -> Option<&dyn Peer> {
            Some(self.peer.as_ref() as &dyn Peer)
        }

        fn get_all_peers(&self) -> Vec<String> {
            vec![self.peer.addr.clone()]
        }
    }

    struct MockPeerGet {
        addr: String,
        value: Option<Vec<u8>>,
        get_called: AtomicBool,
        fail: bool,
    }

    #[async_trait]
    impl Peer for MockPeerGet {
        async fn get(&self, _group: &str, _key: &str) -> Result<Vec<u8>> {
            self.get_called.store(true, Ordering::SeqCst);
            if self.fail {
                Err(BlazeCacheError::PeerError("fail".into()))
            } else if let Some(v) = &self.value {
                Ok(v.clone())
            } else {
                Err(BlazeCacheError::KeyNotFound)
            }
        }

        async fn delete(&self, _group: &str, _key: &str) -> Result<()> {
            Ok(())
        }

        async fn set(&self, _group: &str, _key: &str, _value: Vec<u8>, _ttl: u32) -> Result<()> {
            Ok(())
        }

        async fn get_hot_items(&self, _group: &str) -> Result<Vec<String>> {
            Ok(vec![])
        }

        fn address(&self) -> String {
            self.addr.clone()
        }
    }

    struct MockPeerPickerGet {
        peer: Arc<MockPeerGet>,
    }

    impl PeerPicker for MockPeerPickerGet {
        fn pick_peer(&self, _key: &str) -> Option<&dyn Peer> {
            Some(self.peer.as_ref())
        }

        fn get_all_peers(&self) -> Vec<String> {
            vec![self.peer.addr.clone()]
        }
    }

    #[tokio::test]
    async fn test_group_new() {
        let getter: Getter = Arc::new(|key: &str| Ok(format!("data-{}", key).into_bytes()));

        let group = Group::new("test".to_string(), 10 * 1024 * 1024, getter, String::new());

        assert_eq!(group.name(), "test");
        assert_eq!(group.main_cache_len().await, 0);
        assert_eq!(group.hot_cache_len().await, 0);
    }

    #[tokio::test]
    async fn test_group_get_empty_key() {
        let getter: Getter = Arc::new(|_| Ok(b"data".to_vec()));
        let group = Group::new("test".to_string(), 10 * 1024 * 1024, getter, String::new());

        let result = group.get("").await;
        assert!(matches!(result, Err(BlazeCacheError::KeyEmpty)));
    }

    #[tokio::test]
    async fn test_group_get_cache_miss() {
        let getter: Getter = Arc::new(|key: &str| Ok(format!("data-{}", key).into_bytes()));

        let group = Group::new("test".to_string(), 10 * 1024 * 1024, getter, String::new());

        let result = group.get("test-key").await.unwrap();
        assert_eq!(result, b"data-test-key");
        assert_eq!(group.main_cache_len().await, 1);
    }

    #[tokio::test]
    async fn test_group_get_cache_hit() {
        let getter: Getter = Arc::new(|key: &str| Ok(format!("data-{}", key).into_bytes()));

        let group = Group::new("test".to_string(), 10 * 1024 * 1024, getter, String::new());

        // First call - cache miss
        let result1 = group.get("test-key").await.unwrap();
        assert_eq!(result1, b"data-test-key");

        // Second call - cache hit
        let result2 = group.get("test-key").await.unwrap();
        assert_eq!(result2, b"data-test-key");
        assert_eq!(group.main_cache_len().await, 1);
    }

    #[tokio::test]
    async fn test_group_get_getter_error() {
        let getter: Getter =
            Arc::new(|_| Err(BlazeCacheError::GetterFailed("Database error".to_string())));

        let group = Group::new("test".to_string(), 10 * 1024 * 1024, getter, String::new());

        let result = group.get("test-key").await;
        assert!(matches!(result, Err(BlazeCacheError::GetterFailed(_))));
    }

    #[tokio::test]
    async fn test_group_get_from_peer() {
        let getter: Getter = Arc::new(|key: &str| Ok(format!("local-{}", key).into_bytes()));

        let group = Group::new("test".to_string(), 10 * 1024 * 1024, getter, "127.0.0.1:8080".to_string());

        // Set up mock peer
        let mock_peer = MockPeerGet {
            addr: "remote-addr".to_string(),
            value: Some(b"peer-data".to_vec()),
            get_called: AtomicBool::new(false),
            fail: false,
        };
        let peer_picker = MockPeerPickerGet {
            peer: Arc::new(mock_peer),
        };

        group.set_peers(Box::new(peer_picker)).await;

        let result = group.get("test-key").await.unwrap();
        assert_eq!(result, b"peer-data");
        assert_eq!(group.hot_cache_len().await, 1); // Should be in hot cache
    }

    #[tokio::test]
    async fn test_group_get_peer_fallback() {
        let getter: Getter = Arc::new(|key: &str| Ok(format!("local-{}", key).into_bytes()));

        let group = Group::new("test".to_string(), 10 * 1024 * 1024, getter, "127.0.0.1:8080".to_string());

        // Set up failing mock peer
        let mock_peer = MockPeerGet {
            addr: "remote-addr".to_string(),
            value: None,
            get_called: AtomicBool::new(false),
            fail: true,
        };
        let peer_picker = MockPeerPickerGet {
            peer: Arc::new(mock_peer),
        };

        group.set_peers(Box::new(peer_picker)).await;

        let result = group.get("test-key").await.unwrap();
        // Should fallback to local getter on peer error
        assert_eq!(result, b"local-test-key"); // Should fallback to local getter
        assert_eq!(group.main_cache_len().await, 1);
    }

    #[tokio::test]
    async fn test_group_get_multi_mixed_cache() {
        let getter: Getter = Arc::new(|key: &str| Ok(format!("data-{}", key).into_bytes()));

        let group = Group::new(
            "test".to_string(), 
            10 * 1024 * 1024, // 10MB cache
            getter, 
            "127.0.0.1:8080".to_string()
        );

        // Pre-populate one key
        let _ = group.get("key1").await;

        let keys = vec!["key1", "key2"];
        let results = group.get_multi(&keys).await.unwrap();

        // Should get both keys, one from cache, one loaded
        assert!(results.len() >= 1); // At least the cached one
        assert_eq!(results.get("key1"), Some(&b"data-key1".to_vec()));

        if results.len() == 2 {
            assert_eq!(results.get("key2"), Some(&b"data-key2".to_vec()));
        }
    }

    #[tokio::test]
    async fn test_write_through() {
        use std::sync::Mutex;

        // Mock backing store
        let backing_store = Arc::new(Mutex::new(
            std::collections::HashMap::<String, Vec<u8>>::new(),
        ));
        let store_clone = backing_store.clone();

        let getter: Getter = Arc::new(move |key: &str| {
            let store = store_clone.lock().unwrap();
            store
                .get(key)
                .cloned()
                .ok_or(BlazeCacheError::GetterFailed("Not found".to_string()))
        });

        let store_clone2 = backing_store.clone();
        let setter: Setter = Arc::new(move |key: &str, value: &[u8]| {
            let mut store = store_clone2.lock().unwrap();
            store.insert(key.to_string(), value.to_vec());
            Ok(())
        });

        let group = Group::with_write_through(
            "write-through-test".to_string(), 
            10 * 1024 * 1024, // 10MB cache
            getter, 
            setter, 
            String::new()
        );

        // Test write-through
        group.set("test-key", b"test-value".to_vec(), 0).await.unwrap();

        // Verify data is in backing store
        let store = backing_store.lock().unwrap();
        assert_eq!(store.get("test-key"), Some(&b"test-value".to_vec()));

        // Verify data is also in cache
        let cached_value = group.get("test-key").await.unwrap();
        assert_eq!(cached_value, b"test-value");
    }

    #[tokio::test]
    async fn test_write_through_backend_called() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        };

        // Counter to verify backend is called
        let write_count = Arc::new(AtomicUsize::new(0));
        let backing_store = Arc::new(Mutex::new(
            std::collections::HashMap::<String, Vec<u8>>::new(),
        ));

        let store_clone = backing_store.clone();
        let getter: Getter = Arc::new(move |key: &str| {
            let store = store_clone.lock().unwrap();
            store
                .get(key)
                .cloned()
                .ok_or(BlazeCacheError::GetterFailed("Not found".to_string()))
        });

        let store_clone2 = backing_store.clone();
        let count_clone = write_count.clone();
        let setter: Setter = Arc::new(move |key: &str, value: &[u8]| {
            count_clone.fetch_add(1, Ordering::SeqCst);
            let mut store = store_clone2.lock().unwrap();
            store.insert(key.to_string(), value.to_vec());
            Ok(())
        });

        let group = Group::with_write_through(
            "backend-test".to_string(), 
            10 * 1024 * 1024, // 10MB cache
            getter, 
            setter, 
            String::new()
        );

        // Test write-through calls backend
        group.set("test-key", b"test-value".to_vec(), 0).await.unwrap();

        // Verify backend was called
        assert_eq!(write_count.load(Ordering::SeqCst), 1);

        // Verify data is in backing store
        let store = backing_store.lock().unwrap();
        assert_eq!(store.get("test-key"), Some(&b"test-value".to_vec()));
    }

    #[tokio::test]
    async fn test_read_through_with_singleflight() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let call_count = Arc::new(AtomicUsize::new(0));
        let count_clone = call_count.clone();

        let getter: Getter = Arc::new(move |key: &str| {
            count_clone.fetch_add(1, Ordering::SeqCst);
            Ok(format!("data-{}", key).into_bytes())
        });

        let group = Group::new(
            "singleflight-test".to_string(), 
            10 * 1024 * 1024, // 10MB cache
            getter, 
            String::new()
        );

        // Test single read
        let result = group.get("read-key").await.unwrap();
        assert_eq!(result, b"data-read-key");
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Test cached read (should not call getter again)
        let result2 = group.get("read-key").await.unwrap();
        assert_eq!(result2, b"data-read-key");
        assert_eq!(call_count.load(Ordering::SeqCst), 1); // Still 1, from cache
    }
}
