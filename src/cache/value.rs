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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_new() {
        let value = Value::new(b"test".to_vec(), 0);
        assert_eq!(value.data, b"test");
        assert_eq!(value.expire, 0);
        assert_eq!(value.access_count, 1);
        assert!(!value.compressed);
    }

    #[test]
    fn test_value_new_with_ttl() {
        let ttl = current_timestamp() + 100;
        let value = Value::new(b"test".to_vec(), ttl);
        assert_eq!(value.expire, ttl);
    }

    #[test]
    fn test_value_get_data_uncompressed() {
        let value = Value::new(b"test data".to_vec(), 0);
        let data = value.get_data().unwrap();
        assert_eq!(data, b"test data");
    }

    #[test]
    fn test_value_get_data_compressed() {
        let mut value = Value::new(vec![0u8; 2000], 0);
        value.compress_if_needed().unwrap();
        assert!(value.compressed);
        
        let data = value.get_data().unwrap();
        assert_eq!(data.len(), 2000);
    }

    #[test]
    fn test_value_is_expired_no_ttl() {
        let value = Value::new(b"test".to_vec(), 0);
        assert!(!value.is_expired());
    }

    #[test]
    fn test_value_is_expired_future() {
        let ttl = current_timestamp() + 1000;
        let value = Value::new(b"test".to_vec(), ttl);
        assert!(!value.is_expired());
    }

    #[test]
    fn test_value_is_expired_past() {
        let ttl = current_timestamp() - 100;
        let value = Value::new(b"test".to_vec(), ttl);
        assert!(value.is_expired());
    }

    #[test]
    fn test_value_access() {
        let mut value = Value::new(b"test".to_vec(), 0);
        let initial_count = value.access_count;
        value.access();
        assert_eq!(value.access_count, initial_count + 1);
    }

    #[test]
    fn test_value_is_hot() {
        let mut value = Value::new(b"test".to_vec(), 0);
        
        // Initially not hot
        assert!(!value.is_hot(5, 60));
        
        // Access multiple times
        for _ in 0..10 {
            value.access();
        }
        
        // Now it's hot
        assert!(value.is_hot(5, 60));
    }

    #[test]
    fn test_value_is_hot_low_count() {
        let value = Value::new(b"test".to_vec(), 0);
        // Access count is 1, threshold is 5
        assert!(!value.is_hot(5, 60));
    }

    #[test]
    fn test_value_is_hot_old() {
        let mut value = Value::new(b"test".to_vec(), 0);
        
        // Access multiple times
        for _ in 0..10 {
            value.access();
        }
        
        // Simulate old access by setting last_access to past
        value.last_access = current_timestamp() - 100;
        
        // Not hot because too old
        assert!(!value.is_hot(5, 60));
    }

    #[test]
    fn test_value_compress_if_needed_small() {
        let mut value = Value::new(b"small".to_vec(), 0);
        value.compress_if_needed().unwrap();
        assert!(!value.compressed);
    }

    #[test]
    fn test_value_compress_if_needed_large() {
        let mut value = Value::new(vec![0u8; 2000], 0);
        value.compress_if_needed().unwrap();
        assert!(value.compressed);
    }

    #[test]
    fn test_value_compress_if_needed_already_compressed() {
        let mut value = Value::new(vec![0u8; 2000], 0);
        value.compress_if_needed().unwrap();
        assert!(value.compressed);
        
        // Try to compress again
        let compressed_data = value.data.clone();
        value.compress_if_needed().unwrap();
        assert_eq!(value.data, compressed_data);
    }

    #[test]
    fn test_value_clone() {
        let value1 = Value::new(b"test".to_vec(), 100);
        let value2 = value1.clone();
        assert_eq!(value1.data, value2.data);
        assert_eq!(value1.expire, value2.expire);
        assert_eq!(value1.access_count, value2.access_count);
    }

    #[test]
    fn test_value_debug() {
        let value = Value::new(b"test".to_vec(), 0);
        let _ = format!("{:?}", value);
    }
}
