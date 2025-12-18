use blazecache_client::TcpClient;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::main]
async fn main() {
    let client = TcpClient::new(vec!["127.0.0.1:6784".to_string(), "127.0.0.1:6786".to_string(), "127.0.0.1:6788".to_string()]);
    
    // Wait a bit
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    println!("Testing ping...");
    match timeout(Duration::from_secs(2), client.ping()).await {
        Ok(Ok(_)) => println!("✓ PING successful"),
        Ok(Err(e)) => {
            println!("✗ PING failed: {}", e);
            eprintln!("Error type: {:?}", e);
        },
        Err(_) => println!("✗ PING timed out"),
    }
}
