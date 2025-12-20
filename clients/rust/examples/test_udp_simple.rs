use blazecache::transports::{UdpClient, ProtocolClient};
use blazecache::serializers::BinarySerializer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server_addr = "127.0.0.1:6793";
    
    println!("Connecting to UDP server at {}...", server_addr);
    let mut client: UdpClient<BinarySerializer> = ProtocolClient::connect(server_addr).await?;
    
    println!("Testing PING...");
    match client.ping().await {
        Ok(_) => println!("✓ PING successful"),
        Err(e) => {
            eprintln!("✗ PING failed: {}", e);
            return Err(e);
        }
    }
    
    println!("\nTesting PUT...");
    let key = "test-key";
    let value = b"test-value";
    match client.put(key, value, 3600).await {
        Ok(_) => println!("✓ PUT successful"),
        Err(e) => {
            eprintln!("✗ PUT failed: {}", e);
            return Err(e);
        }
    }
    
    println!("\nTesting GET...");
    match client.get(key).await {
        Ok(v) => {
            if v == value {
                println!("✓ GET successful, value matches");
            } else {
                eprintln!("✗ GET value mismatch: expected {:?}, got {:?}", value, v);
            }
        }
        Err(e) => {
            eprintln!("✗ GET failed: {}", e);
            return Err(e);
        }
    }
    
    println!("\nAll tests passed!");
    Ok(())
}

