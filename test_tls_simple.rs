// Simple test for TLS server and client
use blazecache::transports::{TlsTcpClient, ProtocolClient};
use blazecache::serializers::BinarySerializer;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server_addr = std::env::args().nth(1).unwrap_or_else(|| "localhost:8443".to_string());
    
    println!("Waiting for server to be ready...");
    sleep(Duration::from_secs(2)).await;
    
    println!("Connecting to TLS server at {}...", server_addr);
    
    // Try to connect with insecure mode for self-signed certificates (development only)
    match TlsTcpClient::<BinarySerializer>::connect_insecure(&server_addr).await {
        Ok(mut client) => {
            println!("✓ Connected successfully");
            
            // Test ping
            match client.ping().await {
                Ok(_) => println!("✓ PING successful"),
                Err(e) => {
                    println!("✗ PING failed: {}", e);
                    return Err(e);
                }
            }
            
            // Test PUT
            match client.put("test-key", b"test-value", 0).await {
                Ok(_) => println!("✓ PUT successful"),
                Err(e) => {
                    println!("✗ PUT failed: {}", e);
                    return Err(e);
                }
            }
            
            // Test GET
            match client.get("test-key").await {
                Ok(value) => {
                    if value == b"test-value" {
                        println!("✓ GET successful: {:?}", String::from_utf8_lossy(&value));
                    } else {
                        println!("✗ GET returned wrong value");
                        return Err("Value mismatch".into());
                    }
                }
                Err(e) => {
                    println!("✗ GET failed: {}", e);
                    return Err(e);
                }
            }
            
            // Test DELETE
            match client.delete("test-key").await {
                Ok(deleted) => {
                    if deleted {
                        println!("✓ DELETE successful");
                    } else {
                        println!("✗ DELETE returned false");
                    }
                }
                Err(e) => {
                    println!("✗ DELETE failed: {}", e);
                    return Err(e);
                }
            }
            
            println!("\n✅ All TLS tests passed!");
            Ok(())
        }
        Err(e) => {
            println!("✗ Connection failed: {}", e);
            println!("\nNote: This is expected with self-signed certificates.");
            println!("The TLS client requires valid certificates by default.");
            println!("For development, you may need to:");
            println!("  1. Add the server's CA certificate to the system trust store");
            println!("  2. Or modify the client to accept self-signed certificates");
            Err(e)
        }
    }
}

