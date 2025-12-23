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

#[tokio::test]
async fn test_cache_clear() {
    let cache = Cache::new(1024 * 1024);

    // Add multiple items
    for i in 0..10 {
        cache
            .put(format!("key_{}", i), format!("value_{}", i).into_bytes(), 0)
            .await
            .unwrap();
    }

    // Verify items exist
    assert_eq!(cache.len().await, 10);
    assert!(cache.get("key_0").await.unwrap().is_some());
    assert!(cache.get("key_5").await.unwrap().is_some());
    assert!(cache.get("key_9").await.unwrap().is_some());

    // Get stats before clear
    let stats_before = cache.stats().await;
    let entry_count_before = stats_before.entry_count;
    assert!(entry_count_before > 0); // Should have some entries
    assert!(stats_before.memory_usage > 0);

    // Clear cache
    cache.clear().await;

    // Verify all items are gone
    assert_eq!(cache.len().await, 0);
    assert!(cache.get("key_0").await.unwrap().is_none());
    assert!(cache.get("key_5").await.unwrap().is_none());
    assert!(cache.get("key_9").await.unwrap().is_none());

    // Verify stats are reset
    let stats_after = cache.stats().await;
    assert_eq!(stats_after.entry_count, 0);
    assert_eq!(stats_after.memory_usage, 0);
    
    // Verify other stats are preserved (hits, misses, puts should remain)
    // Note: puts might be less than 10 if some items were evicted due to size
    assert!(stats_after.hits >= stats_before.hits);
    assert!(stats_after.misses >= stats_before.misses);
    assert!(stats_after.puts >= stats_before.puts);
}

#[tokio::test]
async fn test_cache_clear_empty() {
    let cache = Cache::new(1024 * 1024);

    // Clear empty cache should not panic
    cache.clear().await;

    // Verify it's still empty
    assert_eq!(cache.len().await, 0);
    assert_eq!(cache.stats().await.entry_count, 0);
}

#[tokio::test]
async fn test_group_clear() {
    use blazecache::{Group, Getter};
    use std::sync::Arc;

    let getter: Getter = Arc::new(|_key: &str| Err(blazecache::BlazeCacheError::KeyNotFound));
    let group = Group::new("test-cache".to_string(), 1024 * 1024, getter, String::new());

    // Add items to main and hot cache
    group.set("key1", b"value1".to_vec(), 0).await.unwrap();
    group.set("key2", b"value2".to_vec(), 0).await.unwrap();

    // Verify items exist
    assert_eq!(group.main_cache_len().await, 2);
    assert!(group.get("key1").await.is_ok());

    // Clear group
    group.clear().await;

    // Verify both caches are cleared
    assert_eq!(group.main_cache_len().await, 0);
    assert_eq!(group.hot_cache_len().await, 0);
    assert!(group.get("key1").await.is_err());
}
