//! # Threading and Runtime Utilities
//!
//! This module provides utilities for optimizing thread and CPU core usage in BlazeCache.
//! It includes functionality for CPU affinity (pinning threads to specific cores) and
//! optimized runtime configuration for better performance.
//!
//! ## CPU Affinity
//!
//! CPU affinity allows you to pin threads to specific CPU cores, which can improve
//! performance by:
//! - Reducing cache misses (thread stays on same core)
//! - Improving NUMA locality
//! - Reducing context switching overhead
//!
//! ## Runtime Configuration
//!
//! The `OptimizedRuntime` provides a pre-configured Tokio runtime with:
//! - Custom thread names for easier debugging
//! - Larger stack sizes for complex operations
//! - CPU affinity support for worker threads

use crate::utils::{BlazeCacheError, Result};
use core_affinity::CoreId;
use std::future::Future;
use tokio::runtime::{Builder, Runtime};

/// An optimized Tokio runtime with CPU affinity support.
///
/// This runtime is configured with performance optimizations including:
/// - Custom thread naming for easier debugging
/// - Larger stack sizes (2MB) for complex async operations
/// - Support for pinning tasks to specific CPU cores
///
/// ## Performance Benefits
///
/// - **Reduced Cache Misses**: Pinning threads to cores improves L1/L2 cache hit rates
/// - **NUMA Awareness**: Better memory locality on NUMA systems
/// - **Predictable Performance**: Consistent core assignment reduces scheduling variance
///
/// ## Example
///
/// ```rust,no_run
/// use blazecache::utils::threading::OptimizedRuntime;
///
/// // Create runtime with 4 worker threads
/// let runtime = OptimizedRuntime::new(4)?;
///
/// // Spawn a task pinned to CPU core 0
/// let core_ids = core_affinity::get_core_ids().unwrap();
/// let handle = runtime.spawn_pinned(core_ids[0], async {
///     // This task will run on CPU core 0
///     42
/// });
/// ```
pub struct OptimizedRuntime {
    /// The underlying Tokio runtime instance.
    ///
    /// This is a multi-threaded runtime configured for optimal cache performance.
    runtime: Runtime,
}

impl OptimizedRuntime {
    /// Creates a new optimized runtime with the specified number of worker threads.
    ///
    /// ## Arguments
    ///
    /// * `worker_threads` - Number of worker threads to spawn. Should typically match
    ///   the number of CPU cores available, minus 1 for system overhead.
    ///
    /// ## Returns
    ///
    /// A new `OptimizedRuntime` instance, or an error if runtime creation fails.
    ///
    /// ## Configuration Details
    ///
    /// The runtime is configured with:
    /// - **Thread Name**: "blazecache-worker" for easier debugging
    /// - **Stack Size**: 2MB (larger than default for complex async operations)
    /// - **All Features**: I/O, time, and signal drivers enabled
    ///
    /// ## Errors
    ///
    /// Returns `BlazeCacheError::GetterFailed` if the runtime cannot be created.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use blazecache::utils::threading::OptimizedRuntime;
    ///
    /// // Create runtime with optimal worker count
    /// let worker_count = blazecache::utils::threading::get_optimal_worker_count();
    /// let runtime = OptimizedRuntime::new(worker_count)?;
    /// ```
    pub fn new(worker_threads: usize) -> Result<Self> {
        // Build a multi-threaded runtime with custom configuration
        // Multi-threaded runtime allows parallel execution of async tasks
        let runtime = Builder::new_multi_thread()
            .worker_threads(worker_threads) // Set number of worker threads
            .thread_name("blazecache-worker") // Custom name for easier debugging in profilers
            .thread_stack_size(2 * 1024 * 1024) // 2MB stack for complex async operations
            .enable_all() // Enable I/O, time, and signal drivers
            .build()
            .map_err(|e| {
                // Convert runtime creation errors to BlazeCacheError
                BlazeCacheError::GetterFailed(format!("Runtime creation failed: {}", e))
            })?;

        Ok(Self { runtime })
    }

    /// Spawns a future on the runtime, pinned to a specific CPU core.
    ///
    /// This method spawns the future on the runtime and immediately pins the executing
    /// thread to the specified CPU core. This improves performance by:
    /// - Keeping the thread on the same core (better cache locality)
    /// - Reducing context switching overhead
    /// - Improving NUMA memory access patterns
    ///
    /// ## Arguments
    ///
    /// * `core_id` - The CPU core ID to pin the thread to
    /// * `future` - The async future to execute
    ///
    /// ## Returns
    ///
    /// A `JoinHandle` that can be used to await the result of the future.
    ///
    /// ## Type Constraints
    ///
    /// - `F`: Must be a `Future` that is `Send` and `'static`
    /// - `F::Output`: Must be `Send` (required for cross-thread communication)
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use blazecache::utils::threading::OptimizedRuntime;
    /// use core_affinity::get_core_ids;
    ///
    /// let runtime = OptimizedRuntime::new(4)?;
    /// let core_ids = get_core_ids().unwrap();
    ///
    /// // Pin task to first CPU core
    /// let handle = runtime.spawn_pinned(core_ids[0], async {
    ///     // Heavy computation that benefits from CPU affinity
    ///     compute_expensive_operation().await
    /// });
    ///
    /// let result = handle.await?;
    /// ```
    pub fn spawn_pinned<F>(&self, core_id: CoreId, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        // Spawn the future on the runtime
        self.runtime.spawn(async move {
            // Pin the current thread to the specified CPU core
            // This must be done inside the spawned task, not before spawning
            core_affinity::set_for_current(core_id);
            // Execute the future
            future.await
        })
    }

    /// Blocks the current thread until the future completes.
    ///
    /// This is useful for running async code from synchronous contexts, or for
    /// testing async code in unit tests.
    ///
    /// ## Arguments
    ///
    /// * `future` - The async future to execute
    ///
    /// ## Returns
    ///
    /// The output of the future.
    ///
    /// ## Warning
    ///
    /// This method blocks the current thread. Use sparingly in async contexts,
    /// as it can cause deadlocks if called from within an async runtime.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use blazecache::utils::threading::OptimizedRuntime;
    ///
    /// let runtime = OptimizedRuntime::new(2)?;
    ///
    /// // Block until async operation completes
    /// let result = runtime.block_on(async {
    ///     some_async_operation().await
    /// });
    /// ```
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        // Block the current thread until the future completes
        self.runtime.block_on(future)
    }
}

/// Calculates the optimal number of worker threads for the cache runtime.
///
/// This function determines the best number of worker threads by:
/// 1. Getting the total number of CPU cores
/// 2. Reserving 1 core for system overhead (OS, other processes)
/// 3. Using the remaining cores for cache workers
///
/// ## Returns
///
/// The number of worker threads to use. Always returns at least 1, even on single-core systems.
///
/// ## Algorithm
///
/// ```
/// worker_count = max(1, cpu_count - 1)
/// ```
///
/// This ensures:
/// - At least 1 worker thread (for single-core systems)
/// - System has at least 1 core for OS and other processes
/// - Maximum utilization of available CPU cores
///
/// ## Example
///
/// ```rust
/// use blazecache::utils::threading::get_optimal_worker_count;
///
/// // On a 4-core system, returns 3
/// // On a 8-core system, returns 7
/// // On a 1-core system, returns 1
/// let workers = get_optimal_worker_count();
/// ```
pub fn get_optimal_worker_count() -> usize {
    // Get the number of logical CPU cores available
    let cpu_count = num_cpus::get();
    
    // Reserve 1 CPU for system overhead (OS, other processes, etc.)
    // Use the rest for cache workers, but ensure at least 1 worker
    // This prevents over-subscription and ensures system responsiveness
    (cpu_count - 1).max(1)
}

/// Pins the current thread to a specific CPU core.
///
/// This function attempts to set CPU affinity for the current thread to the
/// specified core. This can improve performance by:
/// - Reducing cache misses (thread stays on same core)
/// - Improving memory locality
/// - Reducing context switching
///
/// ## Arguments
///
/// * `core_id` - The index of the CPU core to pin to (0-based)
///
/// ## Returns
///
/// `Ok(())` if successful, or an error if core affinity cannot be set.
///
/// ## Notes
///
/// - This function is a no-op if `core_affinity` is not available on the platform
/// - The core_id is an index into the available core IDs, not a physical core number
/// - On systems where core affinity is not supported, this will silently succeed
///
/// ## Example
///
/// ```rust,no_run
/// use blazecache::utils::threading::pin_current_thread_to_core;
///
/// // Pin current thread to first CPU core
/// pin_current_thread_to_core(0).unwrap();
/// ```
pub fn pin_current_thread_to_core(core_id: usize) -> Result<()> {
    // Get list of available CPU core IDs
    // If core_affinity is not available, this returns an empty list
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    
    // If the requested core ID exists, pin the thread to it
    if let Some(&core) = core_ids.get(core_id) {
        // Set CPU affinity for the current thread
        // This is a best-effort operation - it may fail silently on some platforms
        core_affinity::set_for_current(core);
    }
    
    // Always return Ok - this function is best-effort and should not fail
    // if core affinity is not available
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_optimal_worker_count() {
        let count = get_optimal_worker_count();
        // Should always return at least 1 worker
        assert!(count >= 1);
        
        let cpu_count = num_cpus::get();
        // Should never exceed the number of CPU cores
        assert!(count <= cpu_count);
        
        // On multi-core systems, should be cpu_count - 1
        if cpu_count > 1 {
            assert_eq!(count, cpu_count - 1);
        }
    }

    #[test]
    fn test_optimized_runtime_new() {
        // Test that we can create a runtime with 2 worker threads
        let runtime = OptimizedRuntime::new(2);
        assert!(runtime.is_ok());
    }

    #[test]
    fn test_optimized_runtime_block_on() {
        // Test that block_on works correctly
        let runtime = OptimizedRuntime::new(2).unwrap();
        let result = runtime.block_on(async { 42 });
        assert_eq!(result, 42);
    }

    #[test]
    fn test_pin_current_thread_to_core() {
        // This test may fail if core_affinity is not available, so we just check it doesn't panic
        // Core affinity is platform-dependent and may not be available in all environments
        let _ = pin_current_thread_to_core(0);
    }
}
