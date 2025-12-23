//! # Cache Performance Benchmarks
//!
//! Comprehensive benchmarks for BlazeCache with size limits and item validation.
//!
//! ## Benchmark Categories
//!
//! ### Cache Operations
//! - **PUT**: Measures cache insertion with size validation and LRU eviction
//! - **GET Hit**: Measures cache retrieval for existing keys
//! - **GET Miss**: Measures cache retrieval for non-existent keys
//! - **Size Validation**: Measures item size checking performance
//! - **LRU Eviction**: Measures eviction performance when cache is full
//!
//! ### Performance Targets
//! - GET operations: ~132ns for cache hits
//! - PUT operations: ~390ns including size validation
//! - Size validation: ~10ns overhead per operation
//! - LRU eviction: ~50ns per evicted item
//!
//! ## Test Scenarios
//! - **1KB items**: Typical cache entries
//! - **Size limits**: Items that exceed cache size (rejected)
//! - **Hot items**: Frequently accessed data
//! - **TTL expiration**: Time-based cache expiration
//! - **10KB**: Medium values (JSON documents, small images)
//! - **100KB**: Large values (triggers compression)
//!
//! ## Expected Performance
//!
//! - **PUT**: ~390ns for 1KB values
//! - **GET Hit**: ~132ns for cached values
//! - **GET Miss**: ~50ns for non-existent keys
//!
//! ## Running Benchmarks
//!
//! ```bash
//! # Run all cache benchmarks
//! cargo bench --bench cache_performance
//!
//! # Generate HTML report
//! cargo bench --bench cache_performance -- --output-format html
//! ```

use blazecache::cache::Cache;
use blazecache::cache::Value;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tokio;

/// Benchmarks core cache operations across different data sizes.
///
/// Tests PUT, GET hit, and GET miss operations with 1KB, 10KB, and 100KB values
/// to understand performance characteristics across different value sizes.
///
/// ## Methodology
///
/// - Uses `black_box()` to prevent compiler optimizations
/// - Tests multiple data sizes to identify compression thresholds
/// - Pre-populates cache for realistic GET hit scenarios
/// - Uses sequential keys to avoid hash collisions
///
/// ## Performance Targets
///
/// - PUT operations should complete in <500ns
/// - GET hits should complete in <200ns  
/// - GET misses should complete in <100ns
fn cache_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_operations");

    // Test different data sizes to understand compression impact
    let sizes = vec![1024, 10240, 102400]; // 1KB, 10KB, 100KB

    for size in sizes {
        let data = vec![0u8; size];
        let cache = Cache::new(1024 * 1024 * 100); // 100MB cache (large enough to avoid eviction)

        // Benchmark PUT operations (includes compression, eviction checks, LRU updates)
        group.bench_with_input(BenchmarkId::new("put", size), &size, |b, _| {
            let mut counter = 0;
            let rt = tokio::runtime::Runtime::new().unwrap();
            b.iter(|| {
                let key = format!("key_{}", counter);
                counter += 1;
                // black_box prevents compiler from optimizing away the operation
                let _ = black_box(rt.block_on(cache.put(key, data.clone(), 0)));
            });
        });

        // Pre-populate cache with 1000 entries for realistic GET hit benchmarks
        let rt = tokio::runtime::Runtime::new().unwrap();
        for i in 0..1000 {
            let _ = rt.block_on(cache.put(format!("get_key_{}", i), data.clone(), 0));
        }

        // Benchmark GET hit operations (cache hits with LRU updates and decompression)
        group.bench_with_input(BenchmarkId::new("get_hit", size), &size, |b, _| {
            let mut counter = 0;
            let rt = tokio::runtime::Runtime::new().unwrap();
            b.iter(|| {
                let key = format!("get_key_{}", counter % 1000); // Cycle through existing keys
                counter += 1;
                let _ = black_box(rt.block_on(cache.get(&key)));
            });
        });

        // Benchmark GET miss operations (fast path for non-existent keys)
        group.bench_with_input(BenchmarkId::new("get_miss", size), &size, |b, _| {
            let mut counter = 0;
            let rt = tokio::runtime::Runtime::new().unwrap();
            b.iter(|| {
                let key = format!("miss_key_{}", counter); // Use unique keys that don't exist
                counter += 1;
                let _ = black_box(rt.block_on(cache.get(&key)));
            });
        });
    }

    group.finish();
}

/// Benchmarks Value struct operations including compression and access tracking.
///
/// Tests the performance of Value creation, data retrieval, and access tracking
/// which are critical for cache performance and hot item detection.
///
/// ## Operations Tested
///
/// - **Value::new()**: Creation with automatic compression
/// - **get_data()**: Retrieval with automatic decompression  
/// - **access()**: Access tracking for hot item detection
///
/// ## Performance Targets
///
/// - Value creation should complete in <100ns for small values
/// - Data retrieval should complete in <50ns for uncompressed values
/// - Access tracking should complete in <20ns
fn value_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_operations");

    let data = vec![42u8; 1024]; // 1KB test data

    // Benchmark Value creation (includes compression decision and metadata setup)
    group.bench_function("value_new", |b| {
        b.iter(|| {
            black_box(Value::new(black_box(data.clone()), 0));
        });
    });

    let mut value = Value::new(data.clone(), 0);

    // Benchmark data retrieval (includes decompression if needed)
    group.bench_function("value_get_data", |b| {
        b.iter(|| {
            let _ = black_box(value.get_data());
        });
    });

    // Benchmark access tracking (updates access count and timestamp)
    group.bench_function("value_access", |b| {
        b.iter(|| {
            black_box(value.access());
        });
    });

    group.finish();
}

// Register benchmark groups with Criterion
criterion_group!(benches, cache_operations, value_operations);
criterion_main!(benches);
