use crate::cache::value::Value;
use crate::utils::error::Result;
use lru::LruCache;
use parking_lot::RwLock;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::time;
use crate::utils::time::current_timestamp;
use std::collections::HashSet;

/// High-performance LRU cache with TTL support
pub struct Cache {
    data: Arc<RwLock<LruCache<String, CacheEntry>>>,
    max_size: usize,
    stats: Arc<RwLock<CacheStats>>, // Kept for backward compatibility
    atomic_stats: Arc<AtomicCacheStats>, // Lock-free stats
    cleanup_interval: Duration,
    running: Arc<AtomicBool>,
    // For deferred LRU updates: track recently accessed keys
    lru_update_queue: Arc<RwLock<HashSet<String>>>,
    lru_update_threshold: Arc<AtomicUsize>,
}

impl Clone for Cache {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
            max_size: self.max_size,
            stats: Arc::clone(&self.stats),
            atomic_stats: Arc::clone(&self.atomic_stats),
            cleanup_interval: self.cleanup_interval,
            running: Arc::clone(&self.running),
            lru_update_queue: Arc::clone(&self.lru_update_queue),
            lru_update_threshold: Arc::clone(&self.lru_update_threshold),
        }
    }
}

#[derive(Debug, Clone)]
struct CacheEntry {
    value: Arc<Value>, // Restore Arc<Value> to test the issue
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
    pub ttl_evictions: u64,
    pub entry_count: usize,
    pub memory_usage: usize,
}

/// Internal stats using atomics for lock-free updates
struct AtomicCacheStats {
    hits: AtomicUsize,
    misses: AtomicUsize,
    puts: AtomicUsize,
    deletes: AtomicUsize,
    evictions: AtomicUsize,
    hot_items: AtomicUsize,
    rejected_items: AtomicUsize,
    ttl_evictions: AtomicUsize,
    // These need locks as they're computed from cache state
    entry_count: Arc<RwLock<usize>>,
    memory_usage: Arc<RwLock<usize>>,
}

impl AtomicCacheStats {
    fn new() -> Self {
        Self {
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
            puts: AtomicUsize::new(0),
            deletes: AtomicUsize::new(0),
            evictions: AtomicUsize::new(0),
            hot_items: AtomicUsize::new(0),
            rejected_items: AtomicUsize::new(0),
            ttl_evictions: AtomicUsize::new(0),
            entry_count: Arc::new(RwLock::new(0)),
            memory_usage: Arc::new(RwLock::new(0)),
        }
    }
    
    fn to_cache_stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed) as u64,
            misses: self.misses.load(Ordering::Relaxed) as u64,
            puts: self.puts.load(Ordering::Relaxed) as u64,
            deletes: self.deletes.load(Ordering::Relaxed) as u64,
            evictions: self.evictions.load(Ordering::Relaxed) as u64,
            hot_items: self.hot_items.load(Ordering::Relaxed) as u64,
            rejected_items: self.rejected_items.load(Ordering::Relaxed) as u64,
            ttl_evictions: self.ttl_evictions.load(Ordering::Relaxed) as u64,
            entry_count: *self.entry_count.read(),
            memory_usage: *self.memory_usage.read(),
        }
    }
}

impl Cache {
    pub fn new(max_size: usize) -> Self {
        let cache = Self {
            data: Arc::new(RwLock::new(LruCache::unbounded())),
            max_size,
            stats: Arc::new(RwLock::new(CacheStats::default())), // Kept for compatibility
            atomic_stats: Arc::new(AtomicCacheStats::new()), // Lock-free stats
            cleanup_interval: Duration::from_secs(60), // Default cleanup every 60 seconds
            running: Arc::new(AtomicBool::new(false)),
            lru_update_queue: Arc::new(RwLock::new(HashSet::new())),
            lru_update_threshold: Arc::new(AtomicUsize::new(100)), // Update LRU after 100 reads
        };
        // Only start cleanup task if we're in a tokio runtime
        if tokio::runtime::Handle::try_current().is_ok() {
            cache.start_cleanup_task();
        }
        cache
    }

    /// Set the interval for the background cleanup task
    pub fn with_cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = interval;
        self
    }

    /// Start the background cleanup task
    fn start_cleanup_task(&self) {
        if self.running.swap(true, Ordering::SeqCst) {
            return; // Already running
        }

        let data = Arc::clone(&self.data);
        let stats = Arc::clone(&self.stats);
        let atomic_stats = Arc::clone(&self.atomic_stats);
        let interval = self.cleanup_interval;
        let running = Arc::clone(&self.running);

        tokio::spawn(async move {
            let mut interval = time::interval(interval);
            
            while running.load(Ordering::SeqCst) {
                interval.tick().await;
                
                let now = current_timestamp();
                let removed;
                
                {
                    let mut data = data.write();
                    let mut keys_to_remove = Vec::new();
                    
                    // Find expired keys
                    for (key, entry) in data.iter() {
                        if entry.value.expire > 0 && now > entry.value.expire {
                            keys_to_remove.push(key.clone());
                        }
                    }
                    
                    // Remove expired keys
                    removed = keys_to_remove.len();
                    for key in keys_to_remove {
                        data.pop(&key);
                    }
                }
                
                if removed > 0 {
                    // OPTIMIZATION: Use atomic increment
                    atomic_stats.ttl_evictions.fetch_add(removed, Ordering::Relaxed);
                    
                    let mut stats = stats.write();
                    stats.ttl_evictions += removed as u64;
                    let data_read = data.read();
                    stats.entry_count = data_read.len();
                    stats.memory_usage = data_read.iter().map(|(k, v)| k.len() + v.size).sum();
                }
            }
        });
    }

    /// Manually trigger cleanup of expired items
    pub async fn cleanup_expired(&self) -> usize {
        let now = current_timestamp();
        let mut removed = 0;
        
        {
            let mut data = self.data.write();
            let mut keys_to_remove = Vec::new();
            
            // Find expired keys
            for (key, entry) in data.iter() {
                if entry.value.expire > 0 && now > entry.value.expire {
                    keys_to_remove.push(key.clone());
                }
            }
            
            // Remove expired keys
            removed = keys_to_remove.len();
            for key in keys_to_remove {
                data.pop(&key);
            }
        }
        
        if removed > 0 {
            // OPTIMIZATION: Use atomic increment
            self.atomic_stats.ttl_evictions.fetch_add(removed, Ordering::Relaxed);
            
            let mut stats = self.stats.write();
            stats.ttl_evictions += removed as u64;
            let data_read = self.data.read();
            stats.entry_count = data_read.len();
            stats.memory_usage = self.calculate_size(&data_read);
            *self.atomic_stats.entry_count.write() = stats.entry_count;
            *self.atomic_stats.memory_usage.write() = stats.memory_usage;
        }
        
        removed
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // OPTIMIZATION: Use read lock first for better concurrency
        // Only upgrade to write lock if we need to remove expired entry or update LRU
        
        // Fast path: read lock with peek (doesn't update LRU order)
        let data = self.data.read();
        
        if let Some(entry) = data.peek(key) {
            // CRITICAL FIX: Clone Arc while holding read lock to ensure entry exists
            // Then check expiration on the cloned Arc (safe even after lock is dropped)
            let value_arc = Arc::clone(&entry.value);
            drop(data); // Release read lock early - Arc keeps value alive
            
            // Check expiration on cloned Arc (safe - Arc keeps value alive)
            let expired = if value_arc.expire > 0 {
                let now_secs = current_timestamp();
                now_secs > value_arc.expire
            } else {
                false
            };
            
            if expired {
                // Need write lock to remove expired entry
                let mut data = self.data.write();
                data.pop(key);
                drop(data);
                // OPTIMIZATION: Use atomic increment instead of write lock
                self.atomic_stats.misses.fetch_add(1, Ordering::Relaxed);
                // Also update legacy stats for compatibility
                self.stats.write().misses += 1;
                return Ok(None);
            }
            
            // Extract data from cloned Arc (safe - Arc keeps value alive)
            let result = value_arc.get_data()?;
            
            // Defer LRU update to reduce lock contention
            // Add to update queue, will be processed in batch
            {
                let mut queue = self.lru_update_queue.write();
                queue.insert(key.to_string());
                
                // If queue is large enough, trigger batch update
                if queue.len() >= self.lru_update_threshold.load(Ordering::Relaxed) {
                    let keys_to_update: Vec<String> = queue.drain().collect();
                    drop(queue);
                    
                    // Batch update LRU order with single write lock acquisition
                    let mut data = self.data.write();
                    for key in keys_to_update {
                        // Touch the key to update LRU order
                        if data.contains(&key) {
                            // Remove and re-insert to move to front
                            if let Some(entry) = data.pop(&key) {
                                data.put(key, entry);
                            }
                        }
                    }
                }
            }
            
            // OPTIMIZATION: Use atomic increment instead of write lock
            self.atomic_stats.hits.fetch_add(1, Ordering::Relaxed);
            // Also update legacy stats for compatibility
            self.stats.write().hits += 1;
            return Ok(Some(result));
        }

        drop(data);
        // OPTIMIZATION: Use atomic increment instead of write lock
        self.atomic_stats.misses.fetch_add(1, Ordering::Relaxed);
        // Also update legacy stats for compatibility
        self.stats.write().misses += 1;
        Ok(None)
    }
    


    /// Put with optional per-item TTL seconds. 0 or None means no explicit TTL (falls back to default cache TTL).
    /// 
    /// OPTIMIZATION: Takes key by value to avoid cloning when caller already owns it.
    pub async fn put(&self, key: String, value: Vec<u8>, ttl_secs: u32) -> Result<()> {
        let item_size = key.len() + value.len() + 64;

        if item_size > self.max_size {
            // OPTIMIZATION: Use atomic increment
            self.atomic_stats.rejected_items.fetch_add(1, Ordering::Relaxed);
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

        // OPTIMIZATION: Create value without compression (lazy compression)
        // Store uncompressed initially, compress later if needed
        let value = Value::new(value, expire);
        
        // Note: Compression can happen in background or on first access
        // For now, we store uncompressed to avoid blocking PUT operations
        
        let entry = CacheEntry {
            value: Arc::new(value), // Restore Arc<Value> to test the issue
            size: item_size,
        };

        let mut data = self.data.write();
        let is_new = !data.contains(&key);

        if is_new {
            let mut current_size = self.calculate_size(&data);
            while current_size + item_size > self.max_size {
                if let Some((_, evicted)) = data.pop_lru() {
                    current_size -= evicted.size;
                    // OPTIMIZATION: Use atomic increment
                    self.atomic_stats.evictions.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
            }

            data.put(key, entry);

            // OPTIMIZATION: Use atomic increment for puts
            self.atomic_stats.puts.fetch_add(1, Ordering::Relaxed);
            
            // Update computed stats (need lock for these)
            let mut stats = self.stats.write();
            stats.puts += 1;
            stats.entry_count = data.len();
            stats.memory_usage = self.calculate_size(&data);
            *self.atomic_stats.entry_count.write() = data.len();
            *self.atomic_stats.memory_usage.write() = stats.memory_usage;
        }

        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<bool> {
        let mut data = self.data.write();
        let existed = data.pop(key).is_some();

        if existed {
            // OPTIMIZATION: Use atomic increment
            self.atomic_stats.deletes.fetch_add(1, Ordering::Relaxed);
            
            let mut stats = self.stats.write();
            stats.deletes += 1;
            stats.entry_count = data.len();
            stats.memory_usage = self.calculate_size(&data);
            *self.atomic_stats.entry_count.write() = data.len();
            *self.atomic_stats.memory_usage.write() = stats.memory_usage;
        }

        Ok(existed)
    }

    fn calculate_size(&self, data: &LruCache<String, CacheEntry>) -> usize {
        data.iter().map(|(k, v)| k.len() + v.size).sum()
    }

    pub async fn stats(&self) -> CacheStats {
        // OPTIMIZATION: Use atomic stats, fallback to legacy for computed fields
        let mut legacy_stats = self.stats.read().clone();
        let atomic = &self.atomic_stats;
        
        // Update from atomics (more up-to-date)
        legacy_stats.hits = atomic.hits.load(Ordering::Relaxed) as u64;
        legacy_stats.misses = atomic.misses.load(Ordering::Relaxed) as u64;
        legacy_stats.puts = atomic.puts.load(Ordering::Relaxed) as u64;
        legacy_stats.deletes = atomic.deletes.load(Ordering::Relaxed) as u64;
        legacy_stats.evictions = atomic.evictions.load(Ordering::Relaxed) as u64;
        legacy_stats.rejected_items = atomic.rejected_items.load(Ordering::Relaxed) as u64;
        legacy_stats.ttl_evictions = atomic.ttl_evictions.load(Ordering::Relaxed) as u64;
        legacy_stats.entry_count = *atomic.entry_count.read();
        legacy_stats.memory_usage = *atomic.memory_usage.read();
        
        legacy_stats
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
