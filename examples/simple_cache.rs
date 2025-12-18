use blazecache::Cache;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = Cache::new(1024 * 1024);

    cache
        .put("key1".to_string(), b"Hello, World!".to_vec(), 0)
        .await?;
    cache
        .put("key2".to_string(), b"BlazeCache is fast!".to_vec(), 0)
        .await?;

    if let Some(value) = cache.get("key1").await? {
        println!("key1: {}", String::from_utf8_lossy(&value));
    }

    if let Some(value) = cache.get("key2").await? {
        println!("key2: {}", String::from_utf8_lossy(&value));
    }

    let stats = cache.stats().await;
    println!("Cache stats: {:?}", stats);
    println!("Cache has {} items", cache.len().await);

    Ok(())
}
