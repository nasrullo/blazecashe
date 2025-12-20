use blazecache_client::TcpClient;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server_addr = "127.0.0.1:6792";
    let client = TcpClient::new(vec![server_addr.to_string()]);
    
    println!("Testing connection reuse...");
    
    // First operation
    let start = Instant::now();
    let _ = client.set("test1", b"value1".to_vec()).await;
    let first = start.elapsed();
    println!("First SET: {:?}", first);
    
    // Second operation (should reuse connection)
    let start = Instant::now();
    let _ = client.set("test2", b"value2".to_vec()).await;
    let second = start.elapsed();
    println!("Second SET: {:?}", second);
    
    // Third operation
    let start = Instant::now();
    let _ = client.get("test1").await;
    let third = start.elapsed();
    println!("First GET: {:?}", third);
    
    println!("\nIf connection pooling works, second/third should be faster than first");
    println!("First: {:?}, Second: {:?}, Third: {:?}", first, second, third);
    
    Ok(())
}

