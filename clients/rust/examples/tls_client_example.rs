//! Example of using TLS-enabled BlazeCache client
//!
//! This example shows how to use the TLS TCP client from the server library
//! to connect to a TLS-enabled BlazeCache server.

use blazecache::transports::{TlsTcpClient, ProtocolClient};
use blazecache::serializers::BinarySerializer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server_addr = std::env::args().nth(1).unwrap_or_else(|| "localhost:8443".to_string());
    
    println!("Connecting to TLS server at {}...", server_addr);
    
    // Connect to TLS-enabled server
    // Use insecure mode for self-signed certificates (development only)
    let mut client = TlsTcpClient::<BinarySerializer>::connect_insecure(&server_addr).await?;
    
    // Test ping
    client.ping().await?;
    println!("✓ Ping successful");
    
    // Test PUT
    client.put("test-key", b"test-value", 0).await?;
    println!("✓ PUT successful");
    
    // Test GET
    let value = client.get("test-key").await?;
    println!("✓ GET successful: {:?}", value);
    
    // Test DELETE
    let deleted = client.delete("test-key").await?;
    println!("✓ DELETE successful: {}", deleted);
    
    Ok(())
}

