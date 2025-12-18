use blazecache_client::TcpClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = TcpClient::new(vec!["127.0.0.1:6784".to_string()]);
    
    println!("Testing PING...");
    match client.ping().await {
        Ok(_) => println!("PING successful!"),
        Err(e) => println!("PING failed: {}", e),
    }
    
    Ok(())
}

