use blazecache::transports::{UdpClient, ProtocolClient};
use blazecache::serializers::BinarySerializer;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server_addr = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:6793".to_string());
    
    println!("=== Testing UDP PUT (Simple) ===");
    println!("Server: {}\n", server_addr);
    
    // Connect
    println!("1. Connecting to server...");
    let mut client: UdpClient<BinarySerializer> = match timeout(
        Duration::from_secs(5),
        ProtocolClient::connect(&server_addr)
    ).await {
        Ok(Ok(c)) => {
            println!("   ✓ Connected\n");
            c
        }
        Ok(Err(e)) => {
            println!("   ✗ Connection failed: {}", e);
            return Err(e);
        }
        Err(_) => {
            println!("   ✗ Connection timeout");
            return Err("Connection timeout".into());
        }
    };
    
    // Test PUT with same parameters as perf test
    println!("2. Testing PUT (perf-key-1, 100 bytes)...");
    let key = "perf-key-1";
    let value = vec![0x42u8; 100];
    match timeout(
        Duration::from_secs(10),
        client.put(key, &value, 3600)
    ).await {
        Ok(Ok(_)) => println!("   ✓ PUT successful\n"),
        Ok(Err(e)) => {
            println!("   ✗ PUT failed: {}\n", e);
            return Err(e);
        }
        Err(_) => {
            println!("   ✗ PUT timeout\n");
            return Err("PUT timeout".into());
        }
    }
    
    // Test GET
    println!("3. Testing GET...");
    match timeout(Duration::from_secs(10), client.get(key)).await {
        Ok(Ok(received)) => {
            if received == value {
                println!("   ✓ GET successful (value matches, len={})\n", received.len());
            } else {
                println!("   ✗ GET value mismatch: expected len={}, got len={}\n", value.len(), received.len());
                return Err("Value mismatch".into());
            }
        }
        Ok(Err(e)) => {
            println!("   ✗ GET failed: {}\n", e);
            return Err(e);
        }
        Err(_) => {
            println!("   ✗ GET timeout\n");
            return Err("GET timeout".into());
        }
    }
    
    println!("=== UDP PUT test successful! ===");
    Ok(())
}


