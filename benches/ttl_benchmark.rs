use blazecache::cache::Cache;
use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;
use tokio::runtime::Runtime;

fn ttl_eviction_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let cache = Cache::new(100 * 1024 * 1024) // 100MB cache
        .with_cleanup_interval(Duration::from_secs(60));

    // Pre-populate with items that will expire
    rt.block_on(async {
        for i in 0..1000 {
            cache.put(
                format!("key_{}", i),
                vec![0u8; 1024], // 1KB value
                1, // 1 second TTL
            ).await.unwrap();
        }
    });

    // Wait for items to expire
    std::thread::sleep(Duration::from_secs(2));

    c.bench_function("ttl_cleanup_1000_items", |b| {
        b.iter(|| {
            rt.block_on(cache.cleanup_expired());
        })
    });
}

criterion_group!(benches, ttl_eviction_benchmark);
criterion_main!(benches);
