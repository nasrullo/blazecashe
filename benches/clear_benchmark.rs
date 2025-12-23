//! # Clear Operation Performance Benchmark
//!
//! This benchmark measures the performance of the CLEAR operation
//! under various cache sizes and configurations.

use blazecache::cache::Cache;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Benchmark clear operation with different cache sizes
fn bench_clear_operation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("clear_operation");
    group.sample_size(10); // Clear is expensive, use fewer samples

    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &size| {
                let cache = Arc::new(Cache::new(100 * 1024 * 1024)); // Large enough cache
                let cache_clone = cache.clone();

                // Pre-populate cache
                rt.block_on(async {
                    for i in 0..size {
                        let key = format!("key_{}", i);
                        let value = vec![0u8; 100]; // 100 bytes per value
                        let _ = cache_clone.put(key, value, 0).await;
                    }
                });

                b.to_async(&rt).iter(|| async {
                    cache_clone.clear().await;
                });
            },
        );
    }
    group.finish();
}

/// Benchmark clear operation on empty cache
fn bench_clear_empty(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("clear_empty_cache", |b| {
        let cache = Arc::new(Cache::new(1024 * 1024));
        let cache_clone = cache.clone();

        b.to_async(&rt).iter(|| async {
            black_box(cache_clone.clear().await);
        });
    });
}

/// Benchmark clear operation after many operations
fn bench_clear_after_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("clear_after_many_operations", |b| {
        let cache = Arc::new(Cache::new(10 * 1024 * 1024));
        let cache_clone = cache.clone();

        // Pre-populate with many operations
        rt.block_on(async {
            for i in 0..1000 {
                let key = format!("key_{}", i);
                let value = vec![0u8; 1000];
                let _ = cache_clone.put(key.clone(), value.clone(), 0).await;
                let _ = cache_clone.get(&key).await;
                if i % 2 == 0 {
                    let _ = cache_clone.delete(&key).await;
                }
            }
        });

        b.to_async(&rt).iter(|| async {
            black_box(cache_clone.clear().await);
        });
    });
}

criterion_group!(benches, bench_clear_operation, bench_clear_empty, bench_clear_after_operations);
criterion_main!(benches);

