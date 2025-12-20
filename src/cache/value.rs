use crate::utils::error::{BlazeCacheError, Result};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::utils::time::current_timestamp;

/// A cache entry that includes data and metadata for hot item detection.
///
/// Each cache entry includes the actual data plus metadata used for:
/// - Hot item detection (access patterns)
/// - Automatic compression (memory optimization)
/// - TTL expiration (optional)
///
/// ## Structure
///
/// The Value contains:
/// - `data`: The actual cached value (may be compressed)
/// - `expire`: Optional expiration timestamp (None = no expiration)
/// - `access_count`: Number of times this item has been accessed
/// - `last_access`: Timestamp of last access (for hot item detection)
/// - `compressed`: Whether the data is compressed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Value {
    /// The cached data (may be compressed for large values)
    pub data: Vec<u8>,
    /// Optional expiration timestamp (None means no expiration)
    pub expire: u64,
    /// Timestamp of last access (used for hot item detection)
    pub last_access: u64,
    /// Number of times this item has been accessed (for hot item detection)
    pub access_count: u32,
    /// Whether the data is compressed (automatic for large values)
    pub compressed: bool,
}

impl Value {
    /// Creates a new cache value with the given data.
    ///
    /// Automatically compresses data if it's larger than 1KB to save memory.
    /// Initializes access tracking for hot item detection.
    ///
    /// ## Arguments
    ///
    /// * `data` - The data to cache
    ///
    /// ## Returns
    ///
    /// A new CacheValue with the data and initialized metadata
    ///
    /// ## Example
    ///
    /// ```rust
    /// use blazecache::cache::Value;
    ///
    /// let value = Value::new(b"hello world".to_vec(), 0);
    /// assert!(!value.compressed); // Small data isn't compressed
    /// ```
    pub fn new(data: Vec<u8>, ttl: u64) -> Self {
        // OPTIMIZATION: Lazy compression - store uncompressed initially
        // Compression will happen in background or on first access if needed
        // This avoids blocking PUT operations with compression overhead
        let compressed = false; // Always start uncompressed

        Self {
            data, // Store uncompressed initially
            expire: ttl,
            access_count: 1,
            last_access:current_timestamp(),
            compressed,
        }
    }
    
    /// Compress the value if it's large enough and not already compressed
    pub fn compress_if_needed(&mut self) -> Result<()> {
        const COMPRESSION_THRESHOLD: usize = 1024; // 1KB
        
        if !self.compressed && self.data.len() > COMPRESSION_THRESHOLD {
            let compressed_data = lz4_flex::compress_prepend_size(&self.data);
            // Only compress if it actually saves space
            if compressed_data.len() < self.data.len() {
                self.data = compressed_data;
                self.compressed = true;
            }
        }
        Ok(())
    }

    /// Gets the decompressed data from this cache value.
    ///
    /// This method handles automatic decompression for compressed values,
    /// providing transparent access to the original data regardless of
    /// whether it was compressed for storage.
    ///
    /// ## Returns
    ///
    /// The original uncompressed data, or an error if decompression fails
    ///
    /// ## Example
    ///
    /// ```rust
    /// use blazecache::cache::Value;
    ///
    /// let value = Value::new(b"test data".to_vec(), 0);
    /// let data = value.get_data().unwrap();
    /// assert_eq!(data, b"test data");
    /// ```
    pub fn is_expired(&self) -> bool {
        if self.expire > 0 {
            let now = current_timestamp();
            now > self.expire
        } else {
            false
        }
    }

    pub fn get_data(&self) -> Result<Vec<u8>> {
        if self.compressed {
            lz4_flex::decompress_size_prepended(&self.data)
                .map_err(|e| BlazeCacheError::CompressionError(e.to_string()))
        } else {
            Ok(self.data.clone())
        }
    }

    /// Records an access to this cache value for hot item detection.
    ///
    /// Updates the access count and timestamp, which are used to identify
    /// hot items that should be replicated across peers for better performance.
    ///
    /// ## Example
    ///
    /// ```rust
    /// use blazecache::cache::Value;
    ///
    /// let mut value = Value::new(b"data".to_vec(), 0);
    /// // Note: access_count starts at 1 after creation
    ///
    /// value.access();
    /// assert_eq!(value.access_count, 2);
    /// ```
    pub fn access(&mut self) {
        self.access_count += 1;
        self.last_access = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// Determines if this item is "hot" based on access patterns.
    ///
    /// An item is considered hot if it has been accessed frequently and recently.
    /// Hot items are candidates for replication across peers to improve performance.
    ///
    /// ## Arguments
    ///
    /// * `threshold_count` - Minimum access count to be considered hot
    /// * `threshold_age_secs` - Maximum age in seconds to be considered hot
    ///
    /// ## Returns
    ///
    /// `true` if the item meets both access count and recency thresholds
    ///
    /// ## Example
    ///
    /// ```rust
    /// use blazecache::cache::Value;
    ///
    /// let mut value = Value::new(b"data".to_vec(), 0);
    ///
    /// // Initially not hot (low access count)
    /// assert!(!value.is_hot(5, 60));
    ///
    /// // Access multiple times
    /// for _ in 0..10 {
    ///     value.access();
    /// }
    ///
    /// // Now it's hot (high access count and recent)
    /// assert!(value.is_hot(5, 60));
    /// ```
    pub fn is_hot(&self, threshold_count: u32, threshold_age_secs: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.access_count >= threshold_count && (now - self.last_access) <= threshold_age_secs
    }
}
