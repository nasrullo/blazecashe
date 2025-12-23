//! # Memory Pool for Buffer Reuse
//!
//! This module provides a memory pool for efficient buffer reuse, reducing
//! allocations and improving performance in high-throughput scenarios.
//!
//! ## Problem Solved
//!
//! Frequent allocation and deallocation of buffers can cause:
//! - Memory fragmentation
//! - Increased GC pressure (in GC'd languages)
//! - Allocation overhead
//! - Cache misses from memory churn
//!
//! ## Solution
//!
//! A memory pool maintains a set of pre-allocated buffers that can be reused.
//! When a buffer is needed, it's taken from the pool. When done, it's returned
//! to the pool for reuse. This reduces allocations and improves performance.
//!
//! ## Benefits
//!
//! - **Reduced Allocations**: Reuse buffers instead of allocating new ones
//! - **Better Cache Locality**: Reused buffers stay in cache
//! - **Predictable Performance**: Fewer allocation spikes
//! - **Memory Efficiency**: Buffers are cleared before reuse

use bytes::BytesMut;
use parking_lot::Mutex;
use std::sync::Arc;

/// A memory pool for efficient buffer reuse.
///
/// This pool maintains a set of pre-allocated `BytesMut` buffers that can be
/// reused across operations. When a buffer is needed, it's taken from the pool.
/// When done, it's returned to the pool (if it meets size requirements).
///
/// ## Pool Behavior
///
/// - **Get Buffer**: Returns a buffer from the pool, or allocates a new one if pool is empty
/// - **Return Buffer**: Returns a buffer to the pool if it's large enough and pool isn't full
/// - **Size Limit**: Pool size is limited to prevent unbounded memory growth
///
/// ## Thread Safety
///
/// Uses `parking_lot::Mutex` for fast, contention-free locking. Multiple threads
/// can safely get and return buffers concurrently.
///
/// ## Example
///
/// ```rust
/// use blazecache::cache::memory_pool::MemoryPool;
///
/// let pool = MemoryPool::new(8192, 10); // 8KB buffers, 10 initial buffers
///
/// // Get a buffer from the pool
/// let mut buffer = pool.get_buffer();
/// buffer.extend_from_slice(b"data");
///
/// // Return buffer to pool when done
/// pool.return_buffer(buffer);
/// ```
pub struct MemoryPool {
    /// Pool of available buffers ready for reuse.
    ///
    /// Buffers are stored in a vector and retrieved/returned via push/pop operations.
    /// Using `parking_lot::Mutex` for fast, low-contention locking.
    buffers: Arc<Mutex<Vec<BytesMut>>>,

    /// The size (capacity) of buffers in this pool.
    ///
    /// All buffers in the pool have at least this capacity. Buffers smaller
    /// than this are not returned to the pool.
    buffer_size: usize,
}

impl MemoryPool {
    /// Creates a new memory pool with the specified buffer size and initial count.
    ///
    /// ## Arguments
    ///
    /// * `buffer_size` - The capacity (in bytes) of buffers in the pool
    /// * `initial_count` - Number of buffers to pre-allocate
    ///
    /// ## Returns
    ///
    /// A new `MemoryPool` instance with pre-allocated buffers ready for use.
    ///
    /// ## Example
    ///
    /// ```rust
    /// use blazecache::cache::memory_pool::MemoryPool;
    ///
    /// // Create pool with 8KB buffers, pre-allocate 10 buffers
    /// let pool = MemoryPool::new(8192, 10);
    /// ```
    pub fn new(buffer_size: usize, initial_count: usize) -> Self {
        // Pre-allocate initial buffers for immediate use
        // This reduces allocation overhead during initial operations
        let mut buffers = Vec::with_capacity(initial_count);
        for _ in 0..initial_count {
            // Create buffers with the specified capacity
            buffers.push(BytesMut::with_capacity(buffer_size));
        }

        Self {
            // Wrap in Arc<Mutex<...>> for thread-safe shared access
            buffers: Arc::new(Mutex::new(buffers)),
            buffer_size,
        }
    }

    /// Gets a buffer from the pool.
    ///
    /// If the pool has available buffers, one is returned. If the pool is empty,
    /// a new buffer is allocated with the pool's buffer size.
    ///
    /// ## Returns
    ///
    /// A `BytesMut` buffer with at least `buffer_size` capacity. The buffer
    /// may contain old data, so it should be cleared or overwritten before use.
    ///
    /// ## Performance
    ///
    /// - **Pool Hit**: O(1) - Just pop from vector
    /// - **Pool Miss**: O(1) - Allocate new buffer
    ///
    /// ## Example
    ///
    /// ```rust
    /// # use blazecache::cache::memory_pool::MemoryPool;
    /// let pool = MemoryPool::new(8192, 10);
    /// let mut buffer = pool.get_buffer();
    /// // Buffer has at least 8192 bytes capacity
    /// assert!(buffer.capacity() >= 8192);
    /// ```
    pub fn get_buffer(&self) -> BytesMut {
        let mut buffers = self.buffers.lock();
        
        // Try to get a buffer from the pool
        buffers
            .pop()
            // If pool is empty, allocate a new buffer with the pool's size
            .unwrap_or_else(|| BytesMut::with_capacity(self.buffer_size))
    }

    /// Returns a buffer to the pool for reuse.
    ///
    /// The buffer is cleared (length set to 0) before being returned to the pool.
    /// Only buffers with sufficient capacity are returned; smaller buffers are
    /// dropped to prevent pool pollution.
    ///
    /// ## Arguments
    ///
    /// * `buffer` - The buffer to return to the pool
    ///
    /// ## Pool Size Limit
    ///
    /// The pool has a maximum size (currently 100 buffers) to prevent unbounded
    /// memory growth. If the pool is full, the buffer is dropped instead of
    /// being added to the pool.
    ///
    /// ## Buffer Requirements
    ///
    /// Only buffers with capacity >= `buffer_size` are returned to the pool.
    /// Smaller buffers are dropped to maintain pool quality.
    ///
    /// ## Example
    ///
    /// ```rust
    /// # use blazecache::cache::memory_pool::MemoryPool;
    /// let pool = MemoryPool::new(8192, 10);
    /// let mut buffer = pool.get_buffer();
    /// buffer.extend_from_slice(b"some data");
    /// // Return buffer to pool when done
    /// pool.return_buffer(buffer);
    /// ```
    pub fn return_buffer(&self, mut buffer: BytesMut) {
        // Only return buffers that meet the minimum size requirement
        // This prevents pool pollution with undersized buffers
        if buffer.capacity() >= self.buffer_size {
            // Clear the buffer before returning (set length to 0, keep capacity)
            // This ensures the next user gets a clean buffer
            buffer.clear();
            
            let mut buffers = self.buffers.lock();
            
            // Limit pool size to prevent unbounded memory growth
            // If pool is full, drop the buffer instead of adding it
            if buffers.len() < 100 {
                buffers.push(buffer);
            }
            // If pool is full, buffer is dropped here (freed)
        }
        // If buffer is too small, it's dropped here (not returned to pool)
    }
}

// Global memory pool instance for shared use across the application.
//
// This is a singleton pool with 8KB buffers and 50 pre-allocated buffers.
// It's available throughout the application for efficient buffer reuse.
//
// ## Configuration
//
// - **Buffer Size**: 8192 bytes (8KB) - suitable for most cache operations
// - **Initial Count**: 50 buffers - provides good initial capacity
// - **Max Pool Size**: 100 buffers (enforced in `return_buffer`)
//
// ## Usage
//
// ```rust
// use blazecache::cache::memory_pool::GLOBAL_POOL;
//
// let buffer = GLOBAL_POOL.get_buffer();
// // Use buffer...
// GLOBAL_POOL.return_buffer(buffer);
// ```
//
// ## Thread Safety
//
// The global pool is thread-safe and can be used concurrently from multiple
// threads without additional synchronization.
lazy_static::lazy_static! {
    #[allow(dead_code)]
    pub static ref GLOBAL_POOL: MemoryPool = MemoryPool::new(8192, 50);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_buffer_has_requested_capacity() {
        // Test that buffers from pool have the requested capacity
        let pool = MemoryPool::new(1024, 2);
        let buf = pool.get_buffer();
        
        // Buffer should have at least the requested capacity
        assert!(buf.capacity() >= 1024);
    }

    #[test]
    fn return_buffer_clears_before_reuse() {
        // Test that returned buffers are cleared before reuse
        let pool = MemoryPool::new(256, 0);

        // Get a buffer and write data to it
        let mut b = pool.get_buffer();
        b.extend_from_slice(&[1, 2, 3, 4, 5]);
        assert_eq!(b.len(), 5);

        // Return buffer to pool
        pool.return_buffer(b);

        // Next buffer should be cleared (length 0)
        // In this single-threaded test, we'll likely get the same buffer back
        let b2 = pool.get_buffer();
        assert_eq!(b2.len(), 0); // Buffer was cleared
        assert!(b2.capacity() >= 256); // Capacity is preserved
    }
}
