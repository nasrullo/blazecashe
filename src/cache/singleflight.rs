//! # SingleFlight Pattern Implementation
//!
//! This module implements the SingleFlight pattern to prevent duplicate concurrent
//! requests for the same resource. When multiple requests arrive for the same key
//! simultaneously, only one actual operation is performed and all requests share
//! the result.
//!
//! ## Problem Solved
//!
//! In a distributed cache, multiple clients might request the same missing key
//! simultaneously. Without SingleFlight, this could cause:
//! - Multiple expensive database queries for the same key
//! - Duplicate network requests to peers
//! - Wasted resources and increased latency
//!
//! ## Solution
//!
//! SingleFlight deduplicates concurrent requests by:
//! 1. Tracking in-flight operations by key
//! 2. If a key is already being processed, new requests wait for the existing operation
//! 3. All requests share the same result when the operation completes
//!
//! ## Example
//!
//! ```rust,no_run
//! use blazecache::cache::singleflight::SingleFlight;
//! # async fn example() {
//! let flight = SingleFlight::new();
//!
//! // Multiple concurrent requests for "key1"
//! let result1 = flight.do_call("key1", || async { Ok(b"data".to_vec()) }).await;
//! let result2 = flight.do_call("key1", || async { Ok(b"data".to_vec()) }).await;
//! // Only one operation actually runs
//! # }
//! ```

use crate::utils::Result;
use crate::FnvHashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use tokio::time::{sleep, Duration as TokioDuration};

/// Type alias for a shared result of a read operation.
///
/// Used in SingleFlight to share results between concurrent callers.
/// The result is wrapped in Arc to allow cloning.
type SharedReadResult = Arc<Result<Vec<u8>>>;

/// Type alias for a shared result of a write operation.
///
/// Used in SingleFlight to share results between concurrent callers.
/// The result is wrapped in Arc to allow cloning.
type SharedWriteResult = Arc<Result<()>>;

/// Type alias for a watch receiver that broadcasts read operation results.
///
/// The receiver is used to notify all waiting callers when a read operation completes.
type ReadResultReceiver = watch::Receiver<Option<SharedReadResult>>;

/// Type alias for a watch receiver that broadcasts write operation results.
///
/// The receiver is used to notify all waiting callers when a write operation completes.
type WriteResultReceiver = watch::Receiver<Option<SharedWriteResult>>;

/// Type alias for the map tracking in-flight read operations.
///
/// Maps keys to watch receivers that will broadcast the result when the operation completes.
type InFlightReads = Arc<Mutex<FnvHashMap<String, ReadResultReceiver>>>;

/// Type alias for the map tracking in-flight write operations.
///
/// Maps keys to watch receivers that will broadcast the result when the operation completes.
type InFlightWrites = Arc<Mutex<FnvHashMap<String, WriteResultReceiver>>>;

/// SingleFlight implementation for deduplicating concurrent operations.
///
/// This structure prevents duplicate concurrent operations for the same key by
/// tracking in-flight operations and allowing multiple callers to share results.
///
/// ## How It Works
///
/// 1. When `do_call()` is invoked for a key, it checks if that key is already
///    being processed
/// 2. If the key is in-flight, the new request awaits the existing shared future
/// 3. If the key is not in-flight, it creates a new watch channel, stores the receiver,
///    spawns a task to execute the operation, and broadcasts the result to all waiters
/// 4. After completion, the key is removed from the in-flight set
///
/// ## Thread Safety
///
/// Uses `Arc<Mutex<...>>` for thread-safe concurrent access. Multiple async tasks
/// can safely call methods on the same `SingleFlight` instance.
///
/// ## Future Sharing
///
/// This implementation properly shares futures between concurrent callers using
/// `tokio::sync::watch` channels. All concurrent requests for the same key will
/// await the same operation and receive the same result.
pub struct SingleFlight {
    /// Map of in-flight read operations by key.
    ///
    /// When a key is being read, a watch channel receiver is added to this map.
    /// Multiple concurrent callers can clone the receiver and await the same result.
    /// Results are wrapped in Arc to allow cloning.
    calls: InFlightReads,

    /// Map of in-flight write operations by key.
    ///
    /// When a key is being written, a watch channel receiver is added to this map.
    /// Multiple concurrent callers can clone the receiver and await the same result.
    /// Results are wrapped in Arc to allow cloning.
    writes: InFlightWrites,
}

impl Default for SingleFlight {
    fn default() -> Self {
        Self::new()
    }
}

impl SingleFlight {
    /// Creates a new SingleFlight instance.
    ///
    /// The instance starts with empty in-flight maps and is ready to deduplicate
    /// concurrent operations.
    ///
    /// ## Returns
    ///
    /// A new `SingleFlight` instance.
    pub fn new() -> Self {
        Self {
            // Create empty maps for tracking in-flight operations
            // Using FnvHashMap for faster hashing with string keys
            calls: Arc::new(Mutex::new(FnvHashMap::default())),
            writes: Arc::new(Mutex::new(FnvHashMap::default())),
        }
    }

    /// Executes a read operation with SingleFlight deduplication.
    ///
    /// If multiple concurrent calls are made for the same key, this method
    /// ensures that only one actual operation executes, and all callers share
    /// the same result.
    ///
    /// ## Arguments
    ///
    /// * `key` - The cache key being read
    /// * `f` - A closure that returns a future performing the actual read operation
    ///
    /// ## Type Parameters
    ///
    /// * `F` - Closure type that takes no arguments and returns a future
    /// * `Fut` - The future type returned by the closure
    ///
    /// ## Returns
    ///
    /// The result of the read operation (a byte vector).
    ///
    /// ## Implementation Details
    ///
    /// 1. Check if the key is already being processed
    /// 2. If yes, clone the watch receiver and await the shared result
    /// 3. If no, create a watch channel, store the receiver, spawn a task to execute
    ///    the operation, and broadcast the result to all waiters
    /// 4. Remove key from in-flight set after operation completes
    pub async fn do_call<F, Fut>(&self, key: &str, f: F) -> Result<Vec<u8>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<Vec<u8>>> + Send + 'static,
    {
        let key = key.to_string();

        // Check if this key is already being processed
        let receiver = {
            let mut calls = self.calls.lock().await;
            if let Some(existing_receiver) = calls.get(&key) {
                // Key is already being processed - clone receiver to await shared result
                existing_receiver.clone()
            } else {
                // Key is not in-flight - create new watch channel and store receiver
                let (tx, rx) = watch::channel(None);
                calls.insert(key.clone(), rx.clone());
                drop(calls); // Release lock before spawning task

                // Spawn task to execute the operation and broadcast result
                let key_for_cleanup = key.clone();
                let calls_for_cleanup = Arc::clone(&self.calls);
                tokio::spawn(async move {
                    // Execute the actual read operation
                    let result = f().await;
                    
                    // Broadcast result to all waiting callers (wrap in Arc for cloning)
                    let _ = tx.send(Some(Arc::new(result)));
                    
                    // Remove key from in-flight set after operation completes
                    calls_for_cleanup.lock().await.remove(&key_for_cleanup);
                });

                rx
            }
        };

        // Wait for the result to be available
        let mut receiver = receiver;
        loop {
            receiver.changed().await.expect("sender should not be dropped");
            if let Some(result_arc) = receiver.borrow().as_ref() {
                // Clone the result from Arc (Result now implements Clone via BlazeCacheError::Clone)
                return (**result_arc).clone();
            }
        }
    }

    /// Executes a write operation with SingleFlight deduplication.
    ///
    /// Similar to `do_call()`, but for write operations. Prevents duplicate
    /// concurrent writes for the same key. All concurrent write requests for the
    /// same key will share the same result.
    ///
    /// ## Arguments
    ///
    /// * `key` - The cache key being written
    /// * `_value` - The value being written (currently unused, reserved for future use)
    /// * `f` - A closure that returns a future performing the actual write operation
    ///
    /// ## Returns
    ///
    /// The result of the write operation (typically `Ok(())` on success).
    ///
    /// ## Implementation
    ///
    /// Same pattern as `do_call()` but for write operations. Tracks writes
    /// separately from reads to allow concurrent read and write operations.
    /// Uses watch channels to share results between concurrent callers.
    pub async fn do_write<F, Fut>(&self, key: &str, _value: &[u8], f: F) -> Result<()>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let key = key.to_string();

        // Check if this key is already being written
        let receiver = {
            let mut writes = self.writes.lock().await;
            if let Some(existing_receiver) = writes.get(&key) {
                // Key is already being written - clone receiver to await shared result
                existing_receiver.clone()
            } else {
                // Key is not in-flight - create new watch channel and store receiver
                let (tx, rx) = watch::channel(None);
                writes.insert(key.clone(), rx.clone());
                drop(writes); // Release lock before spawning task

                // Spawn task to execute the operation and broadcast result
                let key_for_cleanup = key.clone();
                let writes_for_cleanup = Arc::clone(&self.writes);
                tokio::spawn(async move {
                    // Execute the actual write operation
                    let result = f().await;
                    
                    // Broadcast result to all waiting callers (wrap in Arc for cloning)
                    let _ = tx.send(Some(Arc::new(result)));
                    
                    // Remove key from in-flight set after operation completes
                    writes_for_cleanup.lock().await.remove(&key_for_cleanup);
                });

                rx
            }
        };

        // Wait for the result to be available
        let mut receiver = receiver;
        loop {
            receiver.changed().await.expect("sender should not be dropped");
            if let Some(result_arc) = receiver.borrow().as_ref() {
                // Clone the result from Arc (Result now implements Clone via BlazeCacheError::Clone)
                return (**result_arc).clone();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_singleflight_read_basic() {
        // Test basic read operation through SingleFlight
        let flight = SingleFlight::new();
        
        // Execute a read operation
        let result = flight
            .do_call("test", || async { Ok(b"data".to_vec()) })
            .await;
        
        // Verify operation succeeded and returned correct data
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"data");
    }

    #[tokio::test]
    async fn test_singleflight_write_basic() {
        // Test basic write operation through SingleFlight
        let flight = SingleFlight::new();
        
        // Execute a write operation
        let result = flight.do_write("test", b"value", || async { Ok(()) }).await;
        
        // Verify operation succeeded
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_singleflight_concurrent_reads() {
        // Test that concurrent reads for the same key share the result
        let flight = Arc::new(SingleFlight::new());
        let call_count = Arc::new(AtomicUsize::new(0));
        
        // Spawn multiple concurrent requests for the same key
        let mut handles = Vec::new();
        for _ in 0..10 {
            let flight_clone = Arc::clone(&flight);
            let call_count_clone = Arc::clone(&call_count);
            let handle = tokio::spawn(async move {
                flight_clone
                    .do_call("shared_key", move || {
                        let call_count = Arc::clone(&call_count_clone);
                        async move {
                            // Increment counter to verify operation only runs once
                            call_count.fetch_add(1, Ordering::SeqCst);
                            sleep(TokioDuration::from_millis(10)).await;
                            Ok(b"shared_result".to_vec())
                        }
                    })
                    .await
            });
            handles.push(handle);
        }
        
        // Wait for all requests to complete
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }
        
        // Verify all results are the same (shared result)
        assert_eq!(results.len(), 10);
        for result in &results {
            assert!(result.is_ok());
            assert_eq!(result.as_ref().unwrap(), b"shared_result");
        }
        
        // Verify the operation only executed once (true SingleFlight behavior)
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_singleflight_concurrent_writes() {
        // Test that concurrent writes for the same key share the result
        let flight = Arc::new(SingleFlight::new());
        let call_count = Arc::new(AtomicUsize::new(0));
        
        // Spawn multiple concurrent write requests for the same key
        let mut handles = Vec::new();
        for _ in 0..10 {
            let flight_clone = Arc::clone(&flight);
            let call_count_clone = Arc::clone(&call_count);
            let handle = tokio::spawn(async move {
                flight_clone
                    .do_write("shared_key", b"value", move || {
                        let call_count = Arc::clone(&call_count_clone);
                        async move {
                            // Increment counter to verify operation only runs once
                            call_count.fetch_add(1, Ordering::SeqCst);
                            sleep(TokioDuration::from_millis(10)).await;
                            Ok(())
                        }
                    })
                    .await
            });
            handles.push(handle);
        }
        
        // Wait for all requests to complete
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }
        
        // Verify all results are successful
        assert_eq!(results.len(), 10);
        for result in &results {
            assert!(result.is_ok());
        }
        
        // Verify the operation only executed once (true SingleFlight behavior)
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }
}
