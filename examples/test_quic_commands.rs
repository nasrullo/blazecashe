use blazecache::transports::{UdpClient, ProtocolClient};
use blazecache::serializers::BinarySerializer;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server_addr = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:6793".to_string());
    
    println!("=== Testing QUIC Commands ===");
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
    
    // Test Ping
    println!("2. Testing PING...");
    match timeout(Duration::from_secs(30), client.ping()).await {
        Ok(Ok(_)) => println!("   ✓ PING successful\n"),
        Ok(Err(e)) => {
            println!("   ✗ PING failed: {}\n", e);
            return Err(e);
        }
        Err(_) => {
            println!("   ✗ PING timeout\n");
            return Err("PING timeout".into());
        }
    }
    
    // Test SET
    println!("3. Testing SET...");
    let test_key = "test-quic-key";
    let test_value = b"test-quic-value";
    match timeout(
        Duration::from_secs(5),
        client.put(test_key, test_value, 3600)
    ).await {
        Ok(Ok(_)) => println!("   ✓ SET successful\n"),
        Ok(Err(e)) => {
            println!("   ✗ SET failed: {}\n", e);
            return Err(e);
        }
        Err(_) => {
            println!("   ✗ SET timeout\n");
            return Err("SET timeout".into());
        }
    }
    
    // Test GET
    println!("4. Testing GET...");
    match timeout(Duration::from_secs(5), client.get(test_key)).await {
        Ok(Ok(value)) => {
            if value == test_value {
                println!("   ✓ GET successful (value matches)\n");
            } else {
                println!("   ✗ GET value mismatch: expected {:?}, got {:?}\n", test_value, value);
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
    
    // Test DELETE
    println!("5. Testing DELETE...");
    match timeout(Duration::from_secs(5), client.delete(test_key)).await {
        Ok(Ok(true)) => println!("   ✓ DELETE successful\n"),
        Ok(Ok(false)) => {
            println!("   ✗ DELETE returned false (key not found)\n");
            return Err("Key not found for delete".into());
        }
        Ok(Err(e)) => {
            println!("   ✗ DELETE failed: {}\n", e);
            return Err(e);
        }
        Err(_) => {
            println!("   ✗ DELETE timeout\n");
            return Err("DELETE timeout".into());
        }
    }
    
    // Verify DELETE worked
    println!("6. Verifying DELETE (GET should fail)...");
    match timeout(Duration::from_secs(5), client.get(test_key)).await {
        Ok(Ok(_)) => {
            println!("   ✗ GET succeeded after DELETE (unexpected)\n");
            return Err("Key still exists after delete".into());
        }
        Ok(Err(e)) => {
            if e.to_string().contains("Not found") || e.to_string().contains("not found") {
                println!("   ✓ GET correctly returned error after DELETE\n");
            } else {
                println!("   ? GET returned error: {} (expected 'Not found')\n", e);
            }
        }
        Err(_) => {
            println!("   ✗ GET timeout\n");
            return Err("GET timeout".into());
        }
    }
    
    println!("=== All QUIC commands tested successfully! ===");
    Ok(())
}

