use blazecache::Cache;
use std::time::Instant;

#[tokio::test]
async fn test_cache_get_performance() {
    let cache = Cache::new(1024 * 1024);
    cache
        .put("perf_key".to_string(), b"perf_value".to_vec(), 0)
        .await
        .unwrap();

    let start = Instant::now();
    for _ in 0..1000 {
        let _ = cache.get("perf_key").await.unwrap();
    }
    let duration = start.elapsed();

    let avg_ns = duration.as_nanos() / 1000;
    println!("Average GET time: {}ns", avg_ns);

    // Realistic expectation: under 10 microseconds
    assert!(avg_ns < 10000, "Cache GET too slow: {}ns > 10000ns", avg_ns);
}

#[tokio::test]
async fn test_cache_put_performance() {
    let cache = Cache::new(1024 * 1024);

    let start = Instant::now();
    for i in 0..1000 {
        let _ = cache
            .put(format!("key_{}", i), b"value".to_vec(), 0)
            .await
            .unwrap();
    }
    let duration = start.elapsed();

    let avg_ns = duration.as_nanos() / 1000;
    println!("Average PUT time: {}ns", avg_ns);

    // Realistic expectation: under 50 microseconds (includes size validation, LRU updates)
    assert!(avg_ns < 50000, "Cache PUT too slow: {}ns > 50000ns", avg_ns);
}
