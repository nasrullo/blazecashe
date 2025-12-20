use blazecache_client::transports::{ProtocolClient, UdpClient};
use blazecache_client::transports::BinarySerializer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server_addr = "127.0.0.1:6793";
    
    println!("Testing simple UDP operations...");
    
    // Connect
    let mut client: UdpClient<BinarySerializer> = ProtocolClient::connect(server_addr).await?;
    println!("✓ Connected");
    
    // Ping
    match client.ping().await {
        Ok(_) => println!("✓ Ping successful"),
        Err(e) => {
            eprintln!("✗ Ping failed: {}", e);
            return Err(e);
        }
    }
    
    // PUT
    println!("Testing PUT...");
    match client.put("test_key", b"test_value", 3600).await {
        Ok(_) => println!("✓ PUT successful"),
        Err(e) => {
            eprintln!("✗ PUT failed: {}", e);
            return Err(e);
        }
    }
    
    // GET
    println!("Testing GET...");
    match client.get("test_key").await {
        Ok(value) => {
            if value == b"test_value" {
                println!("✓ GET successful: {:?}", String::from_utf8_lossy(&value));
            } else {
                eprintln!("✗ GET returned wrong value: {:?}", value);
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

