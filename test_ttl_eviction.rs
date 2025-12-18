use blazecache::cache::cache::Cache;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    println!("=== Testing TTL Eviction ===");
    
    // Create cache with short cleanup interval for testing
    let cache = Cache::new(10 * 1024 * 1024) // 10MB cache
        .with_cleanup_interval(Duration::from_secs(2)); // Cleanup every 2 seconds
    
    println!("1. Adding items with TTL...");
    
    // Add items with short TTL (3 seconds)
    for i in 0..10 {
        let key = format!("ttl-key-{}", i);
        let value = format!("value-{}", i).into_bytes();
        cache.put(key.clone(), value, 3).await.unwrap();
        println!("   Added: {}", key);
    }
    
    let stats = cache.stats().await;
    println!("   Cache size: {} items", cache.len().await);
    println!("   Stats - puts: {}, ttl_evictions: {}", stats.puts, stats.ttl_evictions);
    
    println!("\n2. Verifying items are accessible...");
    for i in 0..10 {
        let key = format!("ttl-key-{}", i);
        match cache.get(&key).await {
            Ok(Some(v)) => println!("   ✓ {} = {}", key, String::from_utf8_lossy(&v)),
            Ok(None) => println!("   ✗ {} not found", key),
            Err(e) => println!("   ✗ {} error: {}", key, e),
        }
    }
    
    println!("\n3. Waiting for TTL expiration (5 seconds)...");
    sleep(Duration::from_secs(5)).await;
    
    println!("\n4. Checking stats after expiration...");
    let stats = cache.stats().await;
    println!("   Cache size: {} items", cache.len().await);
    println!("   Stats - ttl_evictions: {}", stats.ttl_evictions);
    
    println!("\n5. Verifying expired items are gone...");
    let mut found = 0;
    let mut not_found = 0;
    for i in 0..10 {
        let key = format!("ttl-key-{}", i);
        match cache.get(&key).await {
            Ok(Some(_)) => {
                println!("   ⚠ {} still exists (should be expired)", key);
                found += 1;
            }
            Ok(None) => {
                println!("   ✓ {} correctly expired", key);
                not_found += 1;
            }
            Err(e) => println!("   ✗ {} error: {}", key, e),
        }
    }
    
    println!("\n6. Manual cleanup test...");
    // Add a new item with very short TTL
    cache.put("manual-test".to_string(), b"test".to_vec(), 1).await.unwrap();
    println!("   Added item with 1 second TTL");
    sleep(Duration::from_secs(2)).await;
    
    let removed = cache.cleanup_expired().await;
    println!("   Manual cleanup removed {} expired items", removed);
    
    let stats = cache.stats().await;
    println!("\n=== Final Stats ===");
    println!("   Total puts: {}", stats.puts);
    println!("   TTL evictions: {}", stats.ttl_evictions);
    println!("   Cache size: {} items", cache.len().await);
    println!("   Found after expiration: {}", found);
    println!("   Not found (expired): {}", not_found);
    
    if not_found == 10 && stats.ttl_evictions > 0 {
        println!("\n✅ TTL eviction is working correctly!");
    } else {
        println!("\n⚠ TTL eviction may need adjustment");
    }
}
