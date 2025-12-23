//! # Peer Discovery and Registry
//!
//! This module provides peer discovery and health monitoring functionality for
//! distributed cache clusters. It maintains a registry of known peers and tracks
//! their health status through periodic health checks.
//!
//! ## Features
//!
//! - **Peer Registry**: Maintains a set of known peers with their metadata
//! - **Health Monitoring**: Periodically checks peer health via HTTP health endpoints
//! - **Status Tracking**: Tracks peer status (Active, Inactive, Unreachable)
//! - **Thread-Safe**: Uses async locks for concurrent access
//!
//! ## Peer Lifecycle
//!
//! 1. **Discovery**: Peers are discovered via gossip protocol or manual configuration
//! 2. **Registration**: Peers are added to the registry with initial status
//! 3. **Health Checks**: Periodic health checks update peer status
//! 4. **Status Updates**: Status changes based on health check results
//! 5. **Removal**: Failed peers can be removed from the registry

use crate::utils::Result;
use crate::utils::time::current_timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval as tokio_interval;
use tracing::error;

/// Information about a peer node in the cluster.
///
/// This structure contains all metadata needed to identify and communicate with
/// a peer node. It's used by the gossip protocol and peer registry to track
/// cluster membership.
///
/// ## Fields
///
/// - `id`: Unique identifier for the peer (typically "address:port")
/// - `address`: IP address or hostname of the peer
/// - `port`: Port number where the peer listens
/// - `protocol`: Communication protocol ("tcp", "tls", "udp")
/// - `status`: Current health status of the peer
/// - `last_seen`: Timestamp of last successful communication
///
/// ## Equality and Hashing
///
/// Two `PeerInfo` instances are considered equal if they have the same `id`.
/// This allows using `PeerInfo` as a key in hash-based data structures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Unique identifier for this peer.
    ///
    /// Typically in the format "address:port" (e.g., "127.0.0.1:6784").
    /// This ID is used for equality comparisons and as a key in hash maps.
    pub id: String,

    /// IP address or hostname of the peer.
    ///
    /// Can be an IPv4 address (e.g., "192.168.1.100"), IPv6 address, or
    /// hostname (e.g., "cache-node-1.example.com").
    pub address: String,

    /// Port number where the peer listens for connections.
    ///
    /// This is the main cache port, not the gossip port.
    pub port: u16,

    /// Communication protocol used by this peer.
    ///
    /// Valid values: "tcp", "tls", "udp", or other protocol identifiers.
    pub protocol: String,

    /// Current health status of the peer.
    ///
    /// Updated by health checks and gossip protocol based on recent
    /// communication success or failure.
    pub status: PeerStatus,

    /// Unix timestamp of the last successful communication with this peer.
    ///
    /// Used to determine if a peer should be marked as inactive or unreachable.
    /// Updated whenever we successfully communicate with the peer.
    pub last_seen: u64,
}

/// Health status of a peer node.
///
/// The status indicates the current state of the peer based on recent
/// communication attempts and health checks.
///
/// ## Status Transitions
///
/// - **Active** → **Inactive**: Peer hasn't responded recently
/// - **Inactive** → **Unreachable**: Peer still not responding after timeout
/// - **Unreachable** → **Active**: Peer responds to health check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerStatus {
    /// Peer is healthy and responding to requests.
    ///
    /// This is the normal state for a functioning peer. The peer has
    /// responded successfully to recent health checks or requests.
    Active,

    /// Peer hasn't responded recently but may still be alive.
    ///
    /// This is an intermediate state indicating the peer may be experiencing
    /// issues but hasn't been confirmed as failed yet. Used during the
    /// suspicion period before marking as unreachable.
    Inactive,

    /// Peer is not responding and is considered failed.
    ///
    /// The peer has not responded to multiple health checks and is considered
    /// unreachable. It may be removed from the cluster or excluded from
    /// routing decisions.
    Unreachable,
}

/// Registry for tracking peer nodes in the cluster.
///
/// This structure maintains a thread-safe set of known peers and provides
/// methods for adding, removing, and querying peers. It also supports
/// automatic health monitoring through periodic health checks.
///
/// ## Thread Safety
///
/// Uses `Arc<RwLock<HashSet<PeerInfo>>>` for thread-safe concurrent access.
/// Multiple readers can access the registry simultaneously, while writers
/// get exclusive access.
///
/// ## Health Monitoring
///
/// The registry can perform periodic health checks on all registered peers
/// by sending HTTP requests to their health endpoints. This allows automatic
/// detection of failed or unreachable peers.
///
/// ## Example
///
/// ```rust,no_run
/// # use blazecache::networking::discovery::{PeerRegistry, PeerInfo, PeerStatus};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let registry = PeerRegistry::new();
///
/// let peer = PeerInfo {
///     id: "127.0.0.1:6784".to_string(),
///     address: "127.0.0.1".to_string(),
///     port: 6784,
///     protocol: "tcp".to_string(),
///     status: PeerStatus::Active,
///     last_seen: 0,
/// };
///
/// registry.add_peer(peer).await?;
/// # Ok(())
/// # }
/// ```
pub struct PeerRegistry {
    /// Thread-safe set of known peers.
    ///
    /// Uses `Arc` for shared ownership and `RwLock` for concurrent access.
    /// The `HashSet` ensures each peer ID appears only once.
    peers: Arc<RwLock<HashSet<PeerInfo>>>,

    /// Interval between health check rounds.
    ///
    /// Default: 30 seconds. Controls how frequently the registry checks
    /// the health of all registered peers.
    health_check_interval: std::time::Duration,
}

impl Default for PeerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerRegistry {
    /// Creates a new empty peer registry.
    ///
    /// The registry starts with no peers and uses default health check interval
    /// of 30 seconds.
    ///
    /// ## Returns
    ///
    /// A new `PeerRegistry` instance ready to track peers.
    pub fn new() -> Self {
        Self {
            // Create a new empty set of peers with thread-safe access
            peers: Arc::new(RwLock::new(HashSet::new())),
            // Default health check interval: 30 seconds
            // This balances responsiveness with network overhead
            health_check_interval: Duration::from_secs(30),
        }
    }

    /// Adds a peer to the registry.
    ///
    /// If a peer with the same ID already exists, it will be replaced with
    /// the new peer information. This allows updating peer metadata.
    ///
    /// ## Arguments
    ///
    /// * `peer` - The peer information to add or update
    ///
    /// ## Returns
    ///
    /// `Ok(())` if the peer was added successfully.
    ///
    /// ## Example
    ///
    /// ```rust
    /// # use blazecache::networking::discovery::{PeerRegistry, PeerInfo, PeerStatus};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let registry = PeerRegistry::new();
    /// let peer = PeerInfo {
    ///     id: "127.0.0.1:6784".to_string(),
    ///     address: "127.0.0.1".to_string(),
    ///     port: 6784,
    ///     protocol: "tcp".to_string(),
    ///     status: PeerStatus::Active,
    ///     last_seen: 0,
    /// };
    /// registry.add_peer(peer).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn add_peer(&self, peer: PeerInfo) -> Result<()> {
        // Acquire write lock to modify the peer set
        let mut peers = self.peers.write().await;
        // Insert or replace the peer (HashSet handles duplicates by ID)
        peers.insert(peer);
        Ok(())
    }

    /// Removes a peer from the registry by ID.
    ///
    /// ## Arguments
    ///
    /// * `peer_id` - The unique ID of the peer to remove
    ///
    /// ## Returns
    ///
    /// `Ok(true)` if the peer was found and removed, `Ok(false)` if the peer
    /// didn't exist in the registry.
    ///
    /// ## Example
    ///
    /// ```rust
    /// # use blazecache::networking::discovery::PeerRegistry;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let registry = PeerRegistry::new();
    /// // ... add peers ...
    /// let removed = registry.remove_peer("127.0.0.1:6784").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove_peer(&self, peer_id: &str) -> Result<bool> {
        // Acquire write lock to modify the peer set
        let mut peers = self.peers.write().await;
        // Remember initial size to detect if removal occurred
        let initial_len = peers.len();
        // Remove all peers matching the ID (should be at most one)
        peers.retain(|p| p.id != peer_id);
        // Return true if a peer was actually removed
        Ok(peers.len() < initial_len)
    }

    /// Lists all peers in the registry.
    ///
    /// ## Returns
    ///
    /// A vector containing all registered peers, regardless of their status.
    ///
    /// ## Example
    ///
    /// ```rust
    /// # use blazecache::networking::discovery::PeerRegistry;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let registry = PeerRegistry::new();
    /// // ... add peers ...
    /// let all_peers = registry.list_peers().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_peers(&self) -> Vec<PeerInfo> {
        // Acquire read lock (allows concurrent readers)
        let peers = self.peers.read().await;
        // Clone all peers into a vector
        peers.iter().cloned().collect()
    }

    /// Gets only the active (healthy) peers from the registry.
    ///
    /// This is useful for routing decisions, as you typically only want to
    /// send requests to peers that are known to be healthy and responsive.
    ///
    /// ## Returns
    ///
    /// A vector containing only peers with `PeerStatus::Active` status.
    ///
    /// ## Example
    ///
    /// ```rust
    /// # use blazecache::networking::discovery::PeerRegistry;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let registry = PeerRegistry::new();
    /// // ... add peers ...
    /// let active_peers = registry.get_active_peers().await;
    /// // Use active_peers for routing decisions
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_active_peers(&self) -> Vec<PeerInfo> {
        // Acquire read lock
        let peers = self.peers.read().await;
        // Filter to only active peers and collect into vector
        peers
            .iter()
            .filter(|p| matches!(p.status, PeerStatus::Active))
            .cloned()
            .collect()
    }

    /// Updates the status of a peer in the registry.
    ///
    /// This method updates both the status and the `last_seen` timestamp
    /// for the specified peer. It's used by health checks and gossip protocol
    /// to track peer health.
    ///
    /// ## Arguments
    ///
    /// * `peer_id` - The unique ID of the peer to update
    /// * `status` - The new status to assign to the peer
    ///
    /// ## Returns
    ///
    /// `Ok(())` if the update was successful (even if peer doesn't exist).
    ///
    /// ## Implementation Note
    ///
    /// This implementation drains the HashSet, updates the matching peer,
    /// and re-inserts all peers. This is necessary because `HashSet` doesn't
    /// support in-place mutation of elements.
    pub async fn update_peer_status(&self, peer_id: &str, status: PeerStatus) -> Result<()> {
        // Acquire write lock
        let mut peers = self.peers.write().await;
        // Drain all peers into a vector (HashSet doesn't support in-place mutation)
        let peers_vec: Vec<PeerInfo> = peers.drain().collect();

        // Update the matching peer and re-insert all peers
        for mut peer in peers_vec {
            if peer.id == peer_id {
                // Update status and last_seen timestamp
                peer.status = status.clone();
                peer.last_seen = current_timestamp();
            }
            // Re-insert the peer (updated or unchanged)
            peers.insert(peer);
        }
        Ok(())
    }

    /// Performs a health check on all registered peers.
    ///
    /// This method sends HTTP GET requests to each peer's health endpoint
    /// (`http://address:port/health`) and updates their status based on the
    /// response. Peers that respond successfully are marked as Active, while
    /// peers that don't respond are marked as Unreachable.
    ///
    /// ## Health Check Process
    ///
    /// 1. Iterate through all registered peers
    /// 2. Send HTTP GET request to `http://{address}:{port}/health`
    /// 3. If response is successful (2xx status), mark peer as Active
    /// 4. If request fails or times out, mark peer as Unreachable
    /// 5. Update `last_seen` timestamp for all peers checked
    ///
    /// ## Returns
    ///
    /// `Ok(())` if the health check completed (even if some peers failed).
    ///
    /// ## Errors
    ///
    /// Returns an error only if there's a critical failure in the health check
    /// process itself, not if individual peers fail their health checks.
    ///
    /// ## Note
    ///
    /// This method uses HTTP health checks. For peers that don't support HTTP,
    /// alternative health check mechanisms should be used (e.g., protocol-specific
    /// PING commands).
    pub async fn health_check(&self) -> Result<()> {
        // Get list of all peers to check
        let peers = self.list_peers().await;

        // Check each peer's health
        for peer in peers {
            // Construct health check URL
            // Note: This assumes peers have an HTTP health endpoint
            // For protocol-specific health checks, use the appropriate protocol
            let health_url = format!("http://{}:{}/health", peer.address, peer.port);

            // Send HTTP GET request to health endpoint
            match reqwest::get(&health_url).await {
                // Successful response (2xx status code)
                Ok(response) if response.status().is_success() => {
                    // Mark peer as active and update last_seen
                    self.update_peer_status(&peer.id, PeerStatus::Active)
                        .await?;
                }
                // Failed response or network error
                _ => {
                    // Mark peer as unreachable
                    self.update_peer_status(&peer.id, PeerStatus::Unreachable)
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Starts a background task that periodically performs health checks.
    ///
    /// This method spawns an async task that runs health checks at the configured
    /// interval. The task runs indefinitely until the registry is dropped.
    ///
    /// ## Health Check Interval
    ///
    /// Health checks run every `health_check_interval` seconds (default: 30 seconds).
    /// This can be configured when creating the registry.
    ///
    /// ## Background Task
    ///
    /// The health monitor runs in a separate tokio task and doesn't block the
    /// main execution. Errors during health checks are logged but don't stop
    /// the monitoring process.
    ///
    /// ## Example
    ///
    /// ```rust
    /// # use blazecache::networking::discovery::PeerRegistry;
    /// # #[tokio::main]
    /// # async fn main() {
    /// let registry = PeerRegistry::new();
    /// // ... add peers ...
    /// registry.start_health_monitor().await;
    /// // Health checks now run automatically in the background
    /// # }
    /// ```
    pub async fn start_health_monitor(&self) {
        // Clone the registry for the background task
        // Arc cloning is cheap, just increments reference count
        let registry = self.clone();
        
        // Spawn background task for health monitoring
        tokio::spawn(async move {
            // Create interval timer for periodic health checks
            let mut interval = tokio_interval(registry.health_check_interval);

            // Run health checks indefinitely
            loop {
                // Wait for next interval tick
                interval.tick().await;
                
                // Perform health check on all peers
                // Log errors but continue monitoring even if check fails
                if let Err(e) = registry.health_check().await {
                    error!(error = %e, "Health check failed");
                }
            }
        });
    }
}

impl Clone for PeerRegistry {
    /// Clones the peer registry.
    ///
    /// This creates a new `PeerRegistry` instance that shares the same
    /// underlying peer set. Changes made through one instance are visible
    /// to all cloned instances.
    ///
    /// ## Implementation
    ///
    /// Uses `Arc::clone()` to share the peer set, which is a cheap operation
    /// that just increments a reference count.
    fn clone(&self) -> Self {
        Self {
            // Clone the Arc (cheap, just increments reference count)
            peers: Arc::clone(&self.peers),
            // Copy the interval (small value, cheap to copy)
            health_check_interval: self.health_check_interval,
        }
    }
}

impl PartialEq for PeerInfo {
    /// Compares two `PeerInfo` instances for equality.
    ///
    /// Two peers are considered equal if they have the same `id`, regardless
    /// of other fields. This allows using `PeerInfo` as a key in hash-based
    /// collections.
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for PeerInfo {
    // Marker trait - no additional methods needed
    // Required because we implement Hash
}

impl std::hash::Hash for PeerInfo {
    /// Computes the hash of a `PeerInfo` instance.
    ///
    /// Uses only the `id` field for hashing, ensuring that peers with the
    /// same ID hash to the same value regardless of other field differences.
    /// This is consistent with the `PartialEq` implementation.
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_peer_registry() {
        // Test basic peer registry operations
        let registry = PeerRegistry::new();

        // Create a test peer
        let peer = PeerInfo {
            id: "peer1".to_string(),
            address: "127.0.0.1".to_string(),
            port: 6784,
            protocol: "http".to_string(),
            status: PeerStatus::Active,
            last_seen: 0,
        };

        // Add peer to registry
        registry.add_peer(peer.clone()).await.unwrap();

        // Verify peer was added
        let peers = registry.list_peers().await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, "peer1");

        // Remove peer from registry
        let removed = registry.remove_peer("peer1").await.unwrap();
        assert!(removed);

        // Verify peer was removed
        let peers = registry.list_peers().await;
        assert_eq!(peers.len(), 0);
    }
}
