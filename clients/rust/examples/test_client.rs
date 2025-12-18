use blazecache_client::{TcpClient, SelectionStrategy};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing BlazeCache Rust client...");
    
    // Create client with round robin strategy
    let servers = vec!["127.0.0.1:8080".to_string()];
    let client = TcpClient::new(servers);
    
    // Test ping
    match client.ping().await {
        Ok(_) => println!("✓ Ping successful"),
        Err(e) => println!("✗ Ping failed: {}", e),
    }
    
    // Test set
    match client.set("rust-key", b"rust-value".to_vec()).await {
        Ok(_) => println!("✓ Set successful"),
        Err(e) => println!("✗ Set failed: {}", e),
    }
    
    // Test get
    match client.get("rust-key").await {
        Ok(Some(value)) => println!("✓ Get successful: {}", String::from_utf8_lossy(&value)),
        Ok(None) => println!("✗ Key not found"),
        Err(e) => println!("✗ Get failed: {}", e),
    }
    
    // Test get non-existent key
    match client.get("nonexistent").await {
        Ok(Some(_)) => println!("✗ Should not have found key"),
        Ok(None) => println!("✓ Correctly returned None for missing key"),
        Err(e) => println!("✗ Get failed: {}", e),
    }
    
    // Test delete
    match client.delete("rust-key").await {
        Ok(true) => println!("✓ Delete successful"),
        Ok(false) => println!("✗ Key not found for delete"),
        Err(e) => println!("✗ Delete failed: {}", e),
    }
    
    // Test multi-get
    let _ = client.set("key1", b"value1".to_vec()).await;
    let _ = client.set("key2", b"value2".to_vec()).await;
    
    match client.get_multi(&["key1", "key2", "key3"]).await {
        Ok(results) => {
            println!("✓ Multi-get successful: {} keys found", results.len());
            for (key, value) in results {
                println!("  {}: {}", key, String::from_utf8_lossy(&value));
            }
        },
        Err(e) => println!("✗ Multi-get failed: {}", e),
    }
    
    println!("\n✅ Rust client test completed!");
    
    Ok(())
}
