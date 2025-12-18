use blazecache_client::TcpClient;
use tokio;

#[tokio::main]
async fn main() {
    let client = TcpClient::new(vec!["127.0.0.1:6784".to_string()]);
    
    println!("Testing set...");
    match client.set("test_key", b"test_value".to_vec()).await {
        Ok(_) => println!("✓ SET successful"),
        Err(e) => println!("✗ SET failed: {}", e),
    }
    
    println!("Testing get...");
    match client.get("test_key").await {
        Ok(Some(v)) => println!("✓ GET successful: {:?}", String::from_utf8_lossy(&v)),
        Ok(None) => println!("✓ GET returned None (key not found)"),
        Err(e) => println!("✗ GET failed: {}", e),
    }
}
