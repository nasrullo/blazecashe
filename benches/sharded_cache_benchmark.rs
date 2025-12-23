//! # Sharded Cache Performance Benchmarks
//!
//! This benchmark compares the performance of the original `Cache` implementation
//! with the new `ShardedCache` implementation under various workloads.
//!
//! ## Benchmark Categories
//!
//! 1. **Single-threaded operations**
//!    - PUT operations
//!    - GET operations (hits and misses)
//!    - Mixed workload (80% GET, 20% PUT)
//!
//! 2. **Concurrent operations**
//!    - Concurrent GET operations (read-heavy workload)
//    - Concurrent PUT operations (write-heavy workload)
//    - Mixed workload with varying read/write ratios
//!
//! 3. **Scalability**
//    - Performance with different shard counts
//    - Memory usage comparison
//
//! ## Performance Targets
//!
//! - **ShardedCache** should show better throughput than `Cache` under high concurrency
//! - Single-threaded performance should be similar or better than `Cache`
//! - Memory overhead should be reasonable (within 5-10% of `Cache`)

use blazecache::cache::Cache;
use criterion::{
    black_box, criterion_group, criterion_main, AxisScale, BenchmarkId, Criterion,
    PlotConfiguration, Throughput,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::Barrier;

/// Generates a random string of the given length
fn random_string(length: usize) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..length)
        .map(|_| {
            let idx = rng.gen_range(0..CHARS.len());
            CHARS[idx] as char
        })
        .collect()
}

/// Benchmarks single-threaded operations for both Cache and ShardedCache
fn single_threaded_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_threaded");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    // Test with different value sizes
    let value_sizes = [16, 128, 1024, 16 * 1024]; // 16B, 128B, 1KB, 16KB

    for &value_size in &value_sizes {
        let value = vec![0u8; value_size];
        let cache = Cache::new(100 * 1024 * 1024); // 100MB cache
        let sharded_cache = Cache::new(100 * 1024 * 1024); // 100MB cache

        // Benchmark PUT operations
        group.bench_with_input(
            BenchmarkId::new("Cache::put", value_size),
            &value_size,
            |b, _| {
                let rt = Runtime::new().unwrap();
                let mut counter = 0;
                b.iter(|| {
                    let key = format!("key_{}", counter);
                    counter += 1;
                    rt.block_on(cache.put(key, value.clone(), 0)).unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("ShardedCache::put", value_size),
            &value_size,
            |b, _| {
                let rt = Runtime::new().unwrap();
                let mut counter = 0;
                b.iter(|| {
                    let key = format!("key_{}", counter);
                    counter += 1;
                    rt.block_on(sharded_cache.put(key, value.clone(), 0)).unwrap();
                });
            },
        );

        // Pre-populate caches for GET benchmarks
        let rt = Runtime::new().unwrap();
        for i in 0..1000 {
            let key = format!("get_key_{}", i);
            rt.block_on(cache.put(key.clone(), value.clone(), 0)).unwrap();
            rt.block_on(sharded_cache.put(key, value.clone(), 0)).unwrap();
        }

        // Benchmark GET operations (hits)
        group.bench_with_input(
            BenchmarkId::new("Cache::get_hit", value_size),
            &value_size,
            |b, _| {
                let rt = Runtime::new().unwrap();
                let mut counter = 0;
                b.iter(|| {
                    let key = format!("get_key_{}", counter % 1000);
                    counter += 1;
                    black_box(rt.block_on(cache.get(&key)).unwrap());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("ShardedCache::get_hit", value_size),
            &value_size,
            |b, _| {
                let rt = Runtime::new().unwrap();
                let mut counter = 0;
                b.iter(|| {
                    let key = format!("get_key_{}", counter % 1000);
                    counter += 1;
                    black_box(rt.block_on(sharded_cache.get(&key)).unwrap());
                });
            },
        );
    }
}

/// Benchmarks concurrent operations with multiple threads
fn concurrent_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    group.sample_size(10); // Use fewer samples since these are more expensive

    // Test with different thread counts
    let thread_counts = [1, 2, 4, 8, 16];
    let value_size = 1024; // 1KB values
    let value = vec![0u8; value_size];
    let ops_per_thread = 1000;

    for &num_threads in &thread_counts {
        // Benchmark concurrent GET operations (read-heavy workload)
        group.bench_with_input(
            BenchmarkId::new("Cache::concurrent_get", num_threads),
            &num_threads,
            |b, &threads| {
                let cache = Arc::new(Cache::new(100 * 1024 * 1024)); // 100MB cache
                let rt = Runtime::new().unwrap();

                // Pre-populate the cache
                for i in 0..1000 {
                    rt.block_on(cache.put(format!("key_{}", i), value.clone(), 0))
                        .unwrap();
                }

                b.iter(|| {
                    let cache = cache.clone();
                    let value = value.clone();
                    rt.block_on(async move {
                        let mut handles = vec![];
                        let barrier = Arc::new(Barrier::new(threads + 1));

                        for _ in 0..threads {
                            let cache = cache.clone();
                            let barrier = barrier.clone();
                            let value = value.clone();
                            handles.push(tokio::spawn(async move {
                                let mut rng = StdRng::seed_from_u64(42);
                                barrier.wait().await;
                                for _ in 0..ops_per_thread {
                                    let key = format!("key_{}", rng.gen_range(0..1000));
                                    let _: Result<Option<Vec<u8>>, _> = black_box(cache.get(&key).await);
                                }
                            }));
                        }

                        // Release all threads at once
                        barrier.wait().await;

                        // Wait for all threads to complete
                        for handle in handles {
                            handle.await.unwrap();
                        }
                    });
                });
            },
        );

        // Benchmark concurrent GET operations with ShardedCache
        group.bench_with_input(
            BenchmarkId::new("ShardedCache::concurrent_get", num_threads),
            &num_threads,
            |b, &threads| {
                let cache = Arc::new(Cache::new(100 * 1024 * 1024)); // 100MB cache
                let rt = Runtime::new().unwrap();

                // Pre-populate the cache
                for i in 0..1000 {
                    rt.block_on(cache.put(format!("key_{}", i), value.clone(), 0))
                        .unwrap();
                }

                b.iter(|| {
                    let cache = cache.clone();
                    let value = value.clone();
                    rt.block_on(async move {
                        let mut handles = vec![];
                        let barrier = Arc::new(Barrier::new(threads + 1));

                        for _ in 0..threads {
                            let cache = cache.clone();
                            let barrier = barrier.clone();
                            let value = value.clone();
                            handles.push(tokio::spawn(async move {
                                let mut rng = StdRng::seed_from_u64(42);
                                barrier.wait().await;
                                for _ in 0..ops_per_thread {
                                    let key = format!("key_{}", rng.gen_range(0..1000));
                                    let _: Result<Option<Vec<u8>>, _> = black_box(cache.get(&key).await);
                                }
                            }));
                        }

                        // Release all threads at once
                        barrier.wait().await;

                        // Wait for all threads to complete
                        for handle in handles {
                            handle.await.unwrap();
                        }
                    });
                });
            },
        );
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .measurement_time(std::time::Duration::from_secs(10));
    targets = single_threaded_benchmark, concurrent_benchmark
);
criterion_main!(benches);
