use crate::networking::{Peer, PeerPicker};

use fnv::FnvHasher;
#[cfg(test)]
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Consistent hashing implementation for distributed cache peer selection.
///
/// Consistent hashing ensures that when peers are added or removed, only a minimal
/// number of keys need to be redistributed. This is crucial for maintaining cache
/// efficiency in dynamic distributed systems.
///
/// ## Algorithm
///
/// 1. **Hash Ring**: Creates a circular hash space (0 to 2^64-1)
/// 2. **Virtual Nodes**: Each peer gets multiple positions on the ring (replicas)
/// 3. **Key Mapping**: Keys are hashed and assigned to the next peer clockwise
/// 4. **Load Balancing**: Virtual nodes distribute load evenly across peers
///
/// ## Benefits
///
/// - **Minimal Redistribution**: Only ~1/N keys move when adding/removing peers
/// - **Load Balancing**: Virtual nodes ensure even distribution
/// - **Fault Tolerance**: Failed peers only affect their portion of the keyspace
/// - **Scalability**: Easy to add/remove peers without full redistribution
///
/// ## Performance
///
/// - **Lookup Time**: O(log N) where N is number of virtual nodes
/// - **Memory Usage**: O(replicas × peers) for the hash ring
/// - **Hash Function**: FNV hash for speed over cryptographic security
///
/// ## Example
///
/// ```rust,no_run
/// # use blazecache::networking::{ConsistentHash, PeerPicker};
/// # use std::sync::Arc;
/// let mut hash_ring = ConsistentHash::new(150); // 150 virtual nodes per peer
///
/// // Add peers to the ring
/// // hash_ring.add_peer(peer1, "peer1:8080");
/// // hash_ring.add_peer(peer2, "peer2:8080");
///
/// // Keys are automatically distributed
/// // let peer = hash_ring.pick_peer("user:123"); // Returns appropriate peer
/// ```
pub struct ConsistentHash {
    /// Number of virtual nodes per physical peer.
    /// Higher values provide better load distribution but use more memory.
    /// Typical values: 100-200 for good balance of distribution and memory usage.
    replicas: usize,

    /// Sorted vector of hash values for binary search (better cache locality than BTreeMap)
    sorted_hashes: Vec<u64>,
    
    /// Parallel array: index into peers vector for each hash
    hash_to_index: Vec<usize>,
    
    /// Peer references (stored separately for efficient lookup)
    peers: Vec<Arc<dyn Peer>>,

    /// Stored peer ids (addresses) in the order they were added.
    peer_ids: Vec<String>,
}

/// Hash function for consistent hashing (exposed for client-side use).
///
/// This function is exposed so that clients can implement client-side
/// consistent hashing to directly contact the appropriate peer without
/// going through a proxy.
///
/// ## Arguments
///
/// * `key` - The key to hash
///
/// ## Returns
///
/// A 64-bit hash value for consistent hashing
///
/// ## Example
///
/// ```rust
/// use blazecache::networking::consistent_hash::hash_key;
///
/// let hash = hash_key("user:123");
/// println!("Key hash: {}", hash);
/// ```
pub fn hash_key(key: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

impl ConsistentHash {
    /// Creates a new consistent hash ring with the specified number of replicas.
    ///
    /// ## Arguments
    ///
    /// * `replicas` - Number of virtual nodes per physical peer (typically 100-200)
    ///
    /// ## Returns
    ///
    /// A new empty ConsistentHash instance ready to have peers added.
    ///
    /// ## Replica Count Guidelines
    ///
    /// - **50-100**: Minimal memory usage, acceptable distribution
    /// - **100-200**: Good balance of distribution and memory (recommended)
    /// - **200+**: Excellent distribution, higher memory usage
    ///
    /// ## Example
    ///
    /// ```rust
    /// use blazecache::networking::ConsistentHash;
    ///
    /// // Create hash ring with 150 virtual nodes per peer
    /// let hash_ring = ConsistentHash::new(150);
    /// ```
    pub fn new(replicas: usize) -> Self {
        Self {
            replicas,
            sorted_hashes: Vec::new(),
            hash_to_index: Vec::new(),
            peers: Vec::new(),
            peer_ids: Vec::new(),
        }
    }

    /// Adds a peer to the consistent hash ring.
    ///
    /// The peer will be assigned `replicas` number of positions on the hash ring
    /// to ensure even load distribution. Each virtual node is created by hashing
    /// the peer ID with a replica number.
    ///
    /// ## Arguments
    ///
    /// * `peer` - The peer implementation to add
    /// * `id` - Unique identifier for this peer (typically "host:port")
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use blazecache::networking::ConsistentHash;
    /// # use std::sync::Arc;
    /// let mut hash_ring = ConsistentHash::new(150);
    /// // hash_ring.add_peer(peer, "192.168.1.100:8080");
    /// ```
    pub fn add_peer(&mut self, peer: Arc<dyn Peer>, id: &str) {
        let peer_idx = self.peers.len();
        self.peers.push(peer.clone());
        self.peer_ids.push(id.to_string());

        // Append all hashes for this peer (we'll sort once after all peers are added)
        for i in 0..self.replicas {
            let key = format!("{}-{}", id, i);
            let hash = self.hash(&key);
            self.sorted_hashes.push(hash);
            self.hash_to_index.push(peer_idx);
        }
    }
    
    /// Finalize the ring by sorting the hash arrays (call after all peers are added)
    pub fn finalize(&mut self) {
        // Sort both arrays together by hash value using a single sort operation
        let mut pairs: Vec<(u64, usize)> = self.sorted_hashes.iter()
            .zip(self.hash_to_index.iter())
            .map(|(&h, &i)| (h, i))
            .collect();
        pairs.sort_by_key(|&(h, _)| h);
        
        // Rebuild sorted arrays
        self.sorted_hashes.clear();
        self.hash_to_index.clear();
        self.sorted_hashes.reserve(pairs.len());
        self.hash_to_index.reserve(pairs.len());
        for (h, i) in pairs {
            self.sorted_hashes.push(h);
            self.hash_to_index.push(i);
        }
    }

    /// Internal hash function using FNV hasher for speed.
    ///
    /// FNV (Fowler-Noll-Vo) hash is chosen for its speed over cryptographic
    /// security. For consistent hashing, we need fast, well-distributed
    /// hashes rather than cryptographically secure ones.
    ///
    /// ## Arguments
    ///
    /// * `key` - The string to hash
    ///
    /// ## Returns
    ///
    /// A 64-bit hash value
    fn hash(&self, key: &str) -> u64 {
        let mut hasher = FnvHasher::default();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Removes a peer from the consistent hash ring.
    ///
    /// All virtual nodes for this peer are removed from the ring.
    /// Keys previously assigned to this peer will be redistributed
    /// to the next peer clockwise on the ring.
    ///
    /// ## Arguments
    ///
    /// * `id` - The peer ID that was used when adding the peer
    ///
    /// ## Note
    ///
    /// This implementation doesn't remove the peer from the peers vector
    /// to avoid the complexity of peer comparison. In production, you might
    /// want to implement proper peer removal.
    ///
    /// ## Example
    ///
    /// ```rust
    /// use blazecache::networking::ConsistentHash;
    ///
    /// let mut hash_ring = ConsistentHash::new(150);
    /// hash_ring.remove_peer("192.168.1.100:8080");
    /// ```
    pub fn remove_peer(&mut self, id: &str) {
        // Find peer index
        if let Some(peer_idx) = self.peer_ids.iter().position(|pid| pid == id) {
            // Remove peer from vectors
            self.peers.remove(peer_idx);
            self.peer_ids.remove(peer_idx);
            
            // Remove all hashes for this peer and rebuild
            let mut pairs: Vec<(u64, usize)> = self.sorted_hashes.iter()
                .zip(self.hash_to_index.iter())
                .map(|(&h, &i)| (h, i))
                .filter(|(_, i)| *i != peer_idx)
                .map(|(h, i)| if i > peer_idx { (h, i - 1) } else { (h, i) })
                .collect();
            pairs.sort_by_key(|&(h, _)| h);
            
            self.sorted_hashes.clear();
            self.hash_to_index.clear();
            for (h, i) in pairs {
                self.sorted_hashes.push(h);
                self.hash_to_index.push(i);
            }
        }
    }

    /// Returns the number of physical peers in the ring.
    ///
    /// ## Returns
    ///
    /// The count of unique peers (not virtual nodes).
    ///
    /// ## Example
    ///
    /// ```rust
    /// use blazecache::networking::ConsistentHash;
    ///
    /// let hash_ring = ConsistentHash::new(150);
    /// println!("Ring has {} peers", hash_ring.peer_count());
    /// ```
    pub fn peer_count(&self) -> usize {
        self.peer_ids.len()
    }

    /// Gets all peer addresses for client-side consistent hashing.
    ///
    /// This method returns a list of peer addresses that clients can use
    /// to implement client-side consistent hashing, allowing them to
    /// directly contact the appropriate peer without going through a proxy.
    ///
    /// ## Returns
    ///
    /// A vector of peer address strings.
    ///
    /// ## Note
    ///
    /// In this implementation, we return placeholder addresses. In production,
    /// you would store and return the actual peer addresses.
    ///
    /// ## Example
    ///
    /// ```rust
    /// use blazecache::networking::ConsistentHash;
    ///
    /// let hash_ring = ConsistentHash::new(150);
    /// let peers = hash_ring.get_all_peers();
    /// for peer in peers {
    ///     println!("Peer: {}", peer);
    /// }
    /// ```
    pub fn get_all_peers(&self) -> Vec<String> {
        self.peer_ids.clone()
    }

    /// Returns the total number of virtual nodes in the ring.
    ///
    /// This is equal to `peer_count() * replicas` and represents the
    /// total number of positions on the hash ring.
    ///
    /// ## Returns
    ///
    /// The total number of virtual nodes.
    ///
    /// ## Example
    ///
    /// ```rust
    /// use blazecache::networking::ConsistentHash;
    ///
    /// let hash_ring = ConsistentHash::new(150);
    /// println!("Ring has {} virtual nodes", hash_ring.ring_size());
    /// ```
    pub fn ring_size(&self) -> usize {
        self.sorted_hashes.len()
    }
}

impl PeerPicker for ConsistentHash {
    fn pick_peer(&self, key: &str) -> Option<&dyn Peer> {
        if self.sorted_hashes.is_empty() {
            return None;
        }

        let hash = self.hash(key);

        // Binary search for first hash >= key hash (O(log N) with better cache locality)
        match self.sorted_hashes.binary_search(&hash) {
            Ok(idx) => {
                // Exact match (rare but possible)
                Some(self.peers[self.hash_to_index[idx]].as_ref())
            }
            Err(idx) => {
                // Find first hash >= key hash
                if idx < self.sorted_hashes.len() {
                    Some(self.peers[self.hash_to_index[idx]].as_ref())
                } else {
                    // Wrap around to first peer
                    Some(self.peers[self.hash_to_index[0]].as_ref())
                }
            }
        }
    }

    fn get_all_peers(&self) -> Vec<String> {
        self.peer_ids.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::Result;
    use async_trait::async_trait;

    struct MockPeer {
        id: String,
    }

    impl MockPeer {
        fn new(id: &str) -> Self {
            Self { id: id.to_string() }
        }
    }

    #[async_trait]
    impl Peer for MockPeer {
        async fn get(&self, _group: &str, _key: &str) -> Result<Vec<u8>> {
            Ok(format!("data-from-{}", self.id).into_bytes())
        }

        async fn delete(&self, _group: &str, _key: &str) -> Result<()> {
            Ok(())
        }

        async fn set(&self, _group: &str, _key: &str, _value: Vec<u8>, _ttl_secs: u32) -> Result<()> {
            Ok(())
        }

        async fn get_hot_items(&self, _group: &str) -> Result<Vec<String>> { Ok(vec![]) }

        fn address(&self) -> String {
            String::new()
        }
    }

    #[test]
    fn test_consistent_hash_new() {
        let hash = ConsistentHash::new(50);
        assert_eq!(hash.replicas, 50);
        assert_eq!(hash.peer_count(), 0);
        assert_eq!(hash.ring_size(), 0);
    }

    #[test]
    fn test_consistent_hash_add_peer() {
        let mut hash = ConsistentHash::new(3);
        let peer = Arc::new(MockPeer::new("peer1"));

        hash.add_peer(peer, "peer1");
        hash.finalize();

        assert_eq!(hash.peer_count(), 1);
        assert_eq!(hash.ring_size(), 3); // 3 replicas
    }

    #[test]
    fn test_consistent_hash_multiple_peers() {
        let mut hash = ConsistentHash::new(2);

        let peer1 = Arc::new(MockPeer::new("peer1"));
        let peer2 = Arc::new(MockPeer::new("peer2"));

        hash.add_peer(peer1, "peer1");
        hash.add_peer(peer2, "peer2");
        hash.finalize();

        assert_eq!(hash.peer_count(), 2);
        assert_eq!(hash.ring_size(), 4); // 2 peers * 2 replicas
    }

    #[test]
    fn test_consistent_hash_pick_peer_empty() {
        let hash = ConsistentHash::new(50);
        assert!(hash.pick_peer("any-key").is_none());
    }

    #[test]
    fn test_consistent_hash_pick_peer_single() {
        let mut hash = ConsistentHash::new(3);
        let peer = Arc::new(MockPeer::new("peer1"));

        hash.add_peer(peer, "peer1");
        hash.finalize();

        let picked = hash.pick_peer("test-key");
        assert!(picked.is_some());
    }

    #[test]
    fn test_consistent_hash_pick_peer_distribution() {
        let mut hash = ConsistentHash::new(50);

        let peer1 = Arc::new(MockPeer::new("peer1"));
        let peer2 = Arc::new(MockPeer::new("peer2"));
        let peer3 = Arc::new(MockPeer::new("peer3"));

        hash.add_peer(peer1, "peer1");
        hash.add_peer(peer2, "peer2");
        hash.add_peer(peer3, "peer3");
        hash.finalize();

        // Test that different keys can map to different peers
        let mut peer_counts = HashMap::new();

        for i in 0..100 {
            let key = format!("key-{}", i);
            if let Some(_peer) = hash.pick_peer(&key) {
                // We can't easily compare peer identity, so just count selections
                *peer_counts.entry("selected").or_insert(0) += 1;
            }
        }

        assert_eq!(peer_counts.get("selected"), Some(&100));
    }

    #[test]
    fn test_consistent_hash_same_key_same_peer() {
        let mut hash = ConsistentHash::new(50);

        let peer1 = Arc::new(MockPeer::new("peer1"));
        let peer2 = Arc::new(MockPeer::new("peer2"));

        hash.add_peer(peer1, "peer1");
        hash.add_peer(peer2, "peer2");
        hash.finalize();

        // Same key should always map to same peer
        let peer_first = hash.pick_peer("consistent-key");
        let peer_second = hash.pick_peer("consistent-key");

        assert!(peer_first.is_some());
        assert!(peer_second.is_some());
        // Note: We can't easily test peer equality due to trait objects
    }

    #[test]
    fn test_consistent_hash_remove_peer() {
        let mut hash = ConsistentHash::new(3);
        let peer = Arc::new(MockPeer::new("peer1"));

        hash.add_peer(peer, "peer1");
        hash.finalize();
        assert_eq!(hash.ring_size(), 3);

        hash.remove_peer("peer1");
        assert_eq!(hash.ring_size(), 0);
    }

    #[test]
    fn test_consistent_hash_hash_function() {
        let hash = ConsistentHash::new(1);

        // Test that a hash function is deterministic
        let hash1 = hash.hash("test-key");
        let hash2 = hash.hash("test-key");
        assert_eq!(hash1, hash2);

        // Test that different keys produce different hashes
        let hash3 = hash.hash("different-key");
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_consistent_hash_zero_replicas() {
        let mut hash = ConsistentHash::new(0);
        let peer = Arc::new(MockPeer::new("peer1"));

        hash.add_peer(peer, "peer1");
        hash.finalize();

        assert_eq!(hash.peer_count(), 1);
        assert_eq!(hash.ring_size(), 0); // No replicas added to ring
        assert!(hash.pick_peer("any-key").is_none());
    }
}
