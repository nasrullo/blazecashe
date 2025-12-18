use blazecache::Cache;
use std::time::Duration;
use tokio;

#[tokio::test]
async fn test_cache_size_validation() {
    let cache = Cache::new(1000); // 1KB cache

    // Small item should work
    cache
        .put("small".to_string(), vec![0u8; 100], 0)
        .await
        .unwrap();

    // Item larger than cache should fail
    let result = cache.put("large".to_string(), vec![0u8; 2000], 0).await;
    assert!(result.is_err());

    // Verify small item is still there
    let value = cache.get("small").await.unwrap();
    assert!(value.is_some());

    // Test delete
    let deleted = cache.delete("small").await.unwrap();
    assert!(deleted);

    let value = cache.get("small").await.unwrap();
    assert!(value.is_none());
}

#[tokio::test]
async fn test_lru_eviction() {
    let cache = Cache::new(500); // Small cache

    // Fill cache with items
    for i in 0..10 {
        cache
            .put(format!("key_{}", i), vec![0u8; 40], 0)
            .await
            .unwrap();
    }

    // Cache should have evicted some items
    let len = cache.len().await;
    assert!(len < 10);

    // Most recent items should still be there
    let recent = cache.get("key_9").await.unwrap();
    assert!(recent.is_some());
}

#[tokio::test]
async fn test_ttl_expiration() {
    let cache = Cache::new(1024);

    // Set explicit 1-second TTL; cache uses second granularity.
    cache
        .put("ttl_key".to_string(), b"ttl_value".to_vec(), 1)
        .await
        .unwrap();

    // Should be available immediately
    let value = cache.get("ttl_key").await.unwrap();
    assert_eq!(value, Some(b"ttl_value".to_vec()));

    // Wait beyond TTL (1s)
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Should be expired
    let value = cache.get("ttl_key").await.unwrap();
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_cache_stats() {
    let cache = Cache::new(1024);

    // Initial stats
    let stats = cache.stats().await;
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, 0);
    assert_eq!(stats.puts, 0);

    // Put and get
    cache
        .put("stats_key".to_string(), b"stats_value".to_vec(), 0)
        .await
        .unwrap();
    cache.get("stats_key").await.unwrap(); // Hit
    cache.get("missing_key").await.unwrap(); // Miss

    let stats = cache.stats().await;
    assert_eq!(stats.puts, 1);
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 1);
}

#[tokio::test]
async fn test_empty_cache() {
    let cache = Cache::new(1024);

    assert!(cache.is_empty().await);
    assert_eq!(cache.len().await, 0);

    cache
        .put("key".to_string(), b"value".to_vec(), 0)
        .await
        .unwrap();

    assert!(!cache.is_empty().await);
    assert_eq!(cache.len().await, 1);
}
