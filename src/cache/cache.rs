use crate::cache::value::Value;
use crate::utils::error::Result;
use lru::LruCache;
use parking_lot::RwLock;
use std::sync::Arc;
use crate::utils::time::current_timestamp;

/// High-performance LRU cache with size limits and item size validation
#[derive(Clone)]
pub struct Cache {
    data: Arc<RwLock<LruCache<String, CacheEntry>>>,
    max_size: usize,
    stats: Arc<RwLock<CacheStats>>,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    value: Value,
    size: usize,
}

#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub puts: u64,
    pub deletes: u64,
    pub evictions: u64,
    pub hot_items: u64,
    pub rejected_items: u64,
    pub entry_count: usize,
    pub memory_usage: usize,
}

impl Cache {
    pub fn new(max_size: usize) -> Self {
        Self {
            data: Arc::new(RwLock::new(LruCache::unbounded())),
            max_size,
            stats: Arc::new(RwLock::new(CacheStats::default())),
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // Use write lock for LRU order update, but minimize lock scope
        let mut data = self.data.write();

        if let Some(entry) = data.get_mut(key) {
            // Check per-entry expiration
            let expired = if entry.value.expire > 0 {
                let now_secs = current_timestamp();
                now_secs > entry.value.expire
            } else {
                false
            };
            
            if expired {
                data.pop(key);
                drop(data); // Release lock before stats update
                self.stats.write().misses += 1;
                return Ok(None);
            }
            
            // Extract data before releasing lock
            let result = entry.value.get_data()?;
            drop(data); // Release lock before stats update
            self.stats.write().hits += 1;
            return Ok(Some(result));
        }

        drop(data); // Release lock before stats update
        self.stats.write().misses += 1;
        Ok(None)
    }


    /// Put with optional per-item TTL seconds. 0 or None means no explicit TTL (falls back to default cache TTL).
    pub async fn put(&self, key: String, value: Vec<u8>, ttl_secs: u32) -> Result<()> {
        let item_size = key.len() + value.len() + 64;

        if item_size > self.max_size {
            self.stats.write().rejected_items += 1;
            return Err(crate::utils::error::BlazeCacheError::ItemTooLarge {
                item_size,
                max_size: self.max_size,
            });
        }

        let expire = if ttl_secs > 0 {
            current_timestamp() + ttl_secs as u64
        } else {
            0
        };

        let entry = CacheEntry {
            value:Value::new(value, expire),
            size: item_size,
        };

        let mut data = self.data.write();
        let is_new = !data.contains(&key);

        if is_new {
            let mut current_size = self.calculate_size(&data);
            while current_size + item_size > self.max_size {
                if let Some((_, evicted)) = data.pop_lru() {
                    current_size -= evicted.size;
                    self.stats.write().evictions += 1;
                } else {
                    break;
                }
            }

            data.put(key, entry);

            let mut stats = self.stats.write();
            stats.puts += 1;
            stats.entry_count = data.len();
            stats.memory_usage = self.calculate_size(&data);
        }

        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<bool> {
        let mut data = self.data.write();
        let existed = data.pop(key).is_some();

        if existed {
            let mut stats = self.stats.write();
            stats.deletes += 1;
            stats.entry_count = data.len();
            stats.memory_usage = self.calculate_size(&data);
        }

        Ok(existed)
    }

    fn calculate_size(&self, data: &LruCache<String, CacheEntry>) -> usize {
        data.iter().map(|(k, v)| k.len() + v.size).sum()
    }

    pub async fn stats(&self) -> CacheStats {
        self.stats.read().clone()
    }

    pub async fn len(&self) -> usize {
        self.data.read().len()
    }

    pub async fn is_empty(&self) -> bool {
        self.data.read().is_empty()
    }

    pub async fn get_hot_items(&self, _limit: usize, _threshold: u64) -> Vec<(String, Vec<u8>)> {
        Vec::new()
    }
}
