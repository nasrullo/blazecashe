use blazecache::Cache;
use tokio;

#[tokio::test]
async fn test_cache_basic_operations() {
    let cache = Cache::new(1024 * 1024);

    // Test put and get
    cache
        .put("key1".to_string(), b"value1".to_vec(), 0)
        .await
        .unwrap();
    let result = cache.get("key1").await.unwrap();
    assert_eq!(result, Some(b"value1".to_vec()));

    // Test non-existent key
    let result = cache.get("nonexistent").await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_cache_size_limits() {
    let cache = Cache::new(100); // 100 bytes

    // Small item should work
    cache
        .put("small".to_string(), b"data".to_vec(), 0)
        .await
        .unwrap();

    // Large item should fail
    let large_data = vec![0u8; 200];
    let result = cache.put("large".to_string(), large_data, 0).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_cache_eviction() {
    let cache = Cache::new(500); // 500 bytes

    // Fill cache
    for i in 0..10 {
        cache
            .put(format!("key_{}", i), vec![0u8; 40], 0)
            .await
            .unwrap();
    }

    let len = cache.len().await;
    assert!(len < 10); // Some items should be evicted
}

#[tokio::test]
async fn test_cache_stats() {
    let cache = Cache::new(1024);

    cache
        .put("key1".to_string(), b"value1".to_vec(), 0)
        .await
        .unwrap();
    cache.get("key1").await.unwrap();
    cache.get("nonexistent").await.unwrap();

    let stats = cache.stats().await;
    assert_eq!(stats.puts, 1);
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 1);
}

#[tokio::test]
async fn test_cache_delete() {
    let cache = Cache::new(1024);

    cache
        .put("key1".to_string(), b"value1".to_vec(), 0)
        .await
        .unwrap();
    let deleted = cache.delete("key1").await.unwrap();
    assert!(deleted);

    let result = cache.get("key1").await.unwrap();
    assert_eq!(result, None);

    let deleted = cache.delete("nonexistent").await.unwrap();
    assert!(!deleted);
}
