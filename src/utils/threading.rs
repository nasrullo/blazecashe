use crate::utils::{BlazeCacheError, Result};
use core_affinity::CoreId;
use tokio::runtime::{Builder, Runtime};

pub struct OptimizedRuntime {
    runtime: Runtime,
}

impl OptimizedRuntime {
    pub fn new(worker_threads: usize) -> Result<Self> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name("blazecache-worker")
            .thread_stack_size(2 * 1024 * 1024) // 2MB stack
            .enable_all()
            .build()
            .map_err(|e| {
                BlazeCacheError::GetterFailed(format!("Runtime creation failed: {}", e))
            })?;

        Ok(Self { runtime })
    }

    pub fn spawn_pinned<F>(&self, core_id: CoreId, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime.spawn(async move {
            // Pin to specific CPU core
            core_affinity::set_for_current(core_id);
            future.await
        })
    }

    pub fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }
}

pub fn get_optimal_worker_count() -> usize {
    let cpu_count = num_cpus::get();
    // Reserve 1 CPU for system, use rest for cache workers
    (cpu_count - 1).max(1)
}

pub fn pin_current_thread_to_core(core_id: usize) -> Result<()> {
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    if let Some(&core) = core_ids.get(core_id) {
        core_affinity::set_for_current(core);
    }
    Ok(())
}
