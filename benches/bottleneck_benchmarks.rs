//! # Performance Bottleneck Benchmarks
//!
//! This benchmark suite identifies and measures specific performance bottlenecks
//! in BlazeCache to guide optimization efforts.

use blazecache::cache::Cache;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Benchmark 1: Lock Contention in Cache Operations
/// 
/// Measures the overhead of acquiring write locks for LRU updates.
/// GET operations use write locks even for read-only access to update LRU order.
fn benchmark_lock_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("lock_contention");
    
    let cache = Cache::new(100 * 1024 * 1024);
    let rt = Runtime::new().unwrap();
    
    // Pre-populate cache
    for i in 0..1000 {
        let _ = rt.block_on(cache.put(format!("key_{}", i), vec![0u8; 1024], 0));
    }
    
    // Benchmark GET with write lock (current implementation)
    group.bench_function("get_with_write_lock", |b| {
        b.iter(|| {
            let key = format!("key_{}", black_box(500));
            let _ = rt.block_on(cache.get(&key));
        });
    });
    
    // Benchmark concurrent GET operations (simulating contention)
    group.bench_function("concurrent_gets_10_threads", |b| {
        b.iter(|| {
            use std::sync::atomic::{AtomicUsize, Ordering};
            use std::thread;
            
            let counter = Arc::new(AtomicUsize::new(0));
            let mut handles = vec![];
            
            for _ in 0..10 {
                let cache = cache.clone();
                let counter = counter.clone();
                let rt = Runtime::new().unwrap();
                
                handles.push(thread::spawn(move || {
                    for i in 0..100 {
                        let key = format!("key_{}", i % 1000);
                        let _ = rt.block_on(cache.get(&key));
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }));
            }
            
            for handle in handles {
                handle.join().unwrap();
            }
        });
    });
    
    group.finish();
}

/// Benchmark 2: Memory Allocations
///
/// Measures the cost of cloning data in cache operations.
fn benchmark_memory_allocations(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_allocations");
    
    let cache = Cache::new(100 * 1024 * 1024);
    let rt = Runtime::new().unwrap();
    
    // Benchmark PUT with different value sizes
    for size in [1024, 10240, 102400] {
        let data = vec![0u8; size];
        
        group.bench_with_input(BenchmarkId::new("put_clone_value", size), &size, |b, _| {
            b.iter(|| {
                let key = format!("key_{}", black_box(0));
                let value = data.clone(); // Simulates current behavior
                let _ = rt.block_on(cache.put(key, value, 0));
            });
        });
    }
    
    // Benchmark GET that clones data
    let _ = rt.block_on(cache.put("test_key".to_string(), vec![0u8; 10240], 0));
    
    group.bench_function("get_clone_data", |b| {
        b.iter(|| {
            let _ = rt.block_on(cache.get("test_key"));
        });
    });
    
    // Benchmark String allocations in deserialization
    group.bench_function("string_from_utf8", |b| {
        let key_bytes = b"test_key_12345".to_vec();
        b.iter(|| {
            let _ = String::from_utf8(black_box(key_bytes.clone())).unwrap();
        });
    });
    
    group.finish();
}

/// Benchmark 3: Compression Overhead
///
/// Measures the cost of compressing/decompressing data.
fn benchmark_compression_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression_overhead");
    
    // Test different data sizes
    for size in [1024, 1025, 10240, 102400] {
        let data = vec![0u8; size];
        
        // Benchmark compression
        group.bench_with_input(BenchmarkId::new("compress", size), &size, |b, _| {
            b.iter(|| {
                let _ = lz4_flex::compress_prepend_size(black_box(&data));
            });
        });
        
        // Benchmark decompression
        let compressed = lz4_flex::compress_prepend_size(&data);
        group.bench_with_input(BenchmarkId::new("decompress", size), &size, |b, _| {
            b.iter(|| {
                let _ = lz4_flex::decompress_size_prepended(black_box(&compressed));
            });
        });
    }
    
    group.finish();
}

/// Benchmark 4: Stats Updates
///
/// Measures the overhead of updating statistics on every operation.
fn benchmark_stats_updates(c: &mut Criterion) {
    let mut group = c.benchmark_group("stats_updates");
    
    let cache = Cache::new(100 * 1024 * 1024);
    let rt = Runtime::new().unwrap();
    
    // Benchmark GET with stats update
    let _ = rt.block_on(cache.put("test_key".to_string(), vec![0u8; 1024], 0));
    
    group.bench_function("get_with_stats", |b| {
        b.iter(|| {
            let _ = rt.block_on(cache.get("test_key"));
        });
    });
    
    // Benchmark PUT with stats update
    group.bench_function("put_with_stats", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("key_{}", counter);
            counter += 1;
            let _ = rt.block_on(cache.put(key, vec![0u8; 1024], 0));
        });
    });
    
    group.finish();
}

/// Benchmark 5: TTL Cleanup Iteration
///
/// Measures the cost of iterating through all keys to find expired ones.
fn benchmark_ttl_cleanup(c: &mut Criterion) {
    let mut group = c.benchmark_group("ttl_cleanup");
    
    let cache = Cache::new(100 * 1024 * 1024);
    let rt = Runtime::new().unwrap();
    
    // Pre-populate with expired items
    for i in 0..10000 {
        let _ = rt.block_on(cache.put(format!("key_{}", i), vec![0u8; 100], 1)); // 1 second TTL
    }
    
    // Wait for expiration
    std::thread::sleep(std::time::Duration::from_secs(2));
    
    // Benchmark cleanup iteration
    group.bench_function("cleanup_10000_expired", |b| {
        b.iter(|| {
            let _ = rt.block_on(cache.cleanup_expired());
        });
    });
    
    // Benchmark with different cache sizes
    for cache_size in [1000, 5000, 10000] {
        let cache = Cache::new(100 * 1024 * 1024);
        let rt = Runtime::new().unwrap();
        
        for i in 0..cache_size {
            let _ = rt.block_on(cache.put(format!("key_{}", i), vec![0u8; 100], 1));
        }
        
        std::thread::sleep(std::time::Duration::from_secs(2));
        
        group.bench_with_input(BenchmarkId::new("cleanup_iteration", cache_size), &cache_size, |b, _| {
            b.iter(|| {
                let _ = rt.block_on(cache.cleanup_expired());
            });
        });
    }
    
    group.finish();
}

/// Benchmark 6: TCP Buffer Handling
///
/// Measures the overhead of fixed-size buffers and multiple read/write calls.
fn benchmark_tcp_buffer_handling(c: &mut Criterion) {
    let mut group = c.benchmark_group("tcp_buffer_handling");
    
    // Simulate current 8KB buffer
    let mut buffer = [0u8; 8192];
    let data = vec![0u8; 10000]; // Larger than buffer
    
    // Benchmark buffer copying
    group.bench_function("buffer_copy_8kb", |b| {
        b.iter(|| {
            let src = black_box(&data[..8192]);
            buffer.copy_from_slice(src);
        });
    });
    
    // Benchmark multiple small reads (simulating partial reads)
    group.bench_function("multiple_small_reads", |b| {
        let mut pos = 0;
        b.iter(|| {
            let chunk_size = 1024;
            if pos + chunk_size <= data.len() {
                buffer[..chunk_size].copy_from_slice(&data[pos..pos + chunk_size]);
                pos = (pos + chunk_size) % data.len();
            }
        });
    });
    
    group.finish();
}

/// Benchmark 7: String Allocations in Deserialization
///
/// Measures the cost of creating String objects from byte slices.
fn benchmark_string_allocations(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_allocations");
    
    // Simulate key deserialization
    let key_bytes = b"test_key_12345".to_vec();
    
    // Current approach: String::from_utf8 with clone
    group.bench_function("string_from_utf8_clone", |b| {
        b.iter(|| {
            let _ = String::from_utf8(black_box(key_bytes.clone())).unwrap();
        });
    });
    
    // Alternative: String::from_utf8_lossy (no allocation for valid UTF-8)
    group.bench_function("string_from_utf8_lossy", |b| {
        b.iter(|| {
            let _ = String::from_utf8_lossy(black_box(&key_bytes));
        });
    });
    
    // Benchmark key cloning in cache operations
    let cache = Cache::new(100 * 1024 * 1024);
    let rt = Runtime::new().unwrap();
    
    group.bench_function("key_clone_in_put", |b| {
        let key = "test_key".to_string();
        b.iter(|| {
            let key_clone = key.clone(); // Current behavior
            let _ = rt.block_on(cache.put(key_clone, vec![0u8; 1024], 0));
        });
    });
    
    group.finish();
}

/// Benchmark 8: LRU Cache Operations
///
/// Measures the overhead of LRU cache operations (get_mut, pop_lru).
fn benchmark_lru_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("lru_operations");
    
    let cache = Cache::new(100 * 1024 * 1024);
    let rt = Runtime::new().unwrap();
    
    // Pre-populate cache
    for i in 0..1000 {
        let _ = rt.block_on(cache.put(format!("key_{}", i), vec![0u8; 1024], 0));
    }
    
    // Benchmark get_mut (triggers LRU update)
    group.bench_function("lru_get_mut", |b| {
        b.iter(|| {
            let key = format!("key_{}", black_box(500));
            let _ = rt.block_on(cache.get(&key));
        });
    });
    
    // Benchmark eviction (pop_lru)
    let small_cache = Cache::new(100 * 1024); // Small cache to force evictions
    let rt2 = Runtime::new().unwrap();
    
    group.bench_function("lru_eviction", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("key_{}", counter);
            counter += 1;
            let _ = rt2.block_on(small_cache.put(key, vec![0u8; 1024], 0));
        });
    });
    
    group.finish();
}

/// Benchmark 9: Concurrent Operations
///
/// Measures performance degradation under concurrent load.
fn benchmark_concurrent_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_operations");
    
    let cache = Arc::new(Cache::new(100 * 1024 * 1024));
    let rt = Runtime::new().unwrap();
    
    // Pre-populate
    for i in 0..1000 {
        let _ = rt.block_on(cache.put(format!("key_{}", i), vec![0u8; 1024], 0));
    }
    
    // Benchmark single-threaded baseline
    group.bench_function("single_threaded_get", |b| {
        b.iter(|| {
            for i in 0..100 {
                let key = format!("key_{}", i % 1000);
                let _ = rt.block_on(cache.get(&key));
            }
        });
    });
    
    // Benchmark with different concurrency levels
    for num_threads in [2, 4, 8, 16] {
        group.bench_with_input(BenchmarkId::new("concurrent_get", num_threads), &num_threads, |b, &n| {
            use std::sync::atomic::{AtomicUsize, Ordering};
            use std::thread;
            
            b.iter(|| {
                let counter = Arc::new(AtomicUsize::new(0));
                let mut handles = vec![];
                
                for _ in 0..n {
                    let cache = cache.clone();
                    let counter = counter.clone();
                    let rt = Runtime::new().unwrap();
                    
                    handles.push(thread::spawn(move || {
                        for i in 0..(100 / n) {
                            let key = format!("key_{}", i % 1000);
                            let _ = rt.block_on(cache.get(&key));
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }));
                }
                
                for handle in handles {
                    handle.join().unwrap();
                }
            });
        });
    }
    
    group.finish();
}

/// Benchmark 10: Value Decompression on Every GET
///
/// Measures the cost of checking and decompressing on every cache hit.
fn benchmark_value_decompression(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_decompression");
    
    let cache = Cache::new(100 * 1024 * 1024);
    let rt = Runtime::new().unwrap();
    
    // Store compressed value (simulating >1KB data)
    let large_data = vec![0u8; 2048];
    let _ = rt.block_on(cache.put("compressed_key".to_string(), large_data, 0));
    
    // Benchmark GET that decompresses
    group.bench_function("get_with_decompression", |b| {
        b.iter(|| {
            let _ = rt.block_on(cache.get("compressed_key"));
        });
    });
    
    // Benchmark decompression directly
    let compressed = lz4_flex::compress_prepend_size(&vec![0u8; 2048]);
    group.bench_function("decompress_only", |b| {
        b.iter(|| {
            let _ = lz4_flex::decompress_size_prepended(black_box(&compressed));
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    benchmark_lock_contention,
    benchmark_memory_allocations,
    benchmark_compression_overhead,
    benchmark_stats_updates,
    benchmark_ttl_cleanup,
    benchmark_tcp_buffer_handling,
    benchmark_string_allocations,
    benchmark_lru_operations,
    benchmark_concurrent_operations,
    benchmark_value_decompression
);
criterion_main!(benches);
