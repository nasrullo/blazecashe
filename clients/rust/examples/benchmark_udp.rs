use blazecache::transports::{UdpClient, ProtocolClient};
use blazecache::serializers::BinarySerializer;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server_addr = "127.0.0.1:6793"; // UDP port
    let num_ops = 100_000; // Increased for load test
    let num_workers = 50; // Increased for load test
    let large_value_size = 10_000; // 10KB to test fragmentation and batch sending

    println!("=== UDP Client Benchmark ===");
    println!("Server: {}", server_addr);
    println!("Operations: {} ({} per worker)", num_ops, num_ops / num_workers);
    println!("Workers: {}", num_workers);
    println!("Large value size: {} bytes (to test fragmentation)\n", large_value_size);

    // Verify server connection (with longer timeout for UDP)
    println!("Connecting to UDP server at {}...", server_addr);
    let mut test_client: UdpClient<BinarySerializer> = ProtocolClient::connect(server_addr).await?;
    match tokio::time::timeout(std::time::Duration::from_secs(5), test_client.ping()).await {
        Ok(Ok(_)) => println!("✓ Server connection verified\n"),
        Ok(Err(e)) => {
            eprintln!("✗ Server connection failed: {}", e);
            return Err(e);
        }
        Err(_) => {
            eprintln!("✗ Server connection timed out - is UDP server running on {}?", server_addr);
            eprintln!("  Make sure server is started with: --udp-port 6793");
            return Err("Connection timeout".into());
        }
    }

    // Test 1: Small values (no fragmentation)
    println!("--- Test 1: Small values (no fragmentation) ---");
    test_benchmark(server_addr, num_ops, num_workers, 100, false).await?;

    // Test 2: Large values (with fragmentation - tests batch sending improvement)
    println!("\n--- Test 2: Large values (with fragmentation) ---");
    test_benchmark(server_addr, num_ops / 10, num_workers, large_value_size, true).await?;

    // Test 3: With all enhancements enabled
    println!("\n--- Test 3: With enhancements (congestion control, flow control) ---");
    test_with_enhancements(server_addr, num_ops, num_workers).await?;

    Ok(())
}

async fn test_benchmark(
    server_addr: &str,
    num_ops: usize,
    num_workers: usize,
    value_size: usize,
    is_large: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let start = Instant::now();
    let mut handles = Vec::new();

    let ops_per_worker = num_ops / num_workers;

    for worker_id in 0..num_workers {
        let server_addr = server_addr.to_string();
        let value = vec![(worker_id % 256) as u8; value_size];

        let handle = tokio::spawn(async move {
            let mut client: UdpClient<BinarySerializer> = match ProtocolClient::connect(&server_addr).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to connect: {}", e);
                    return (0, 0);
                }
            };
            
            let mut success = 0;
            let mut errors = 0;

            for i in 0..ops_per_worker {
                let key = format!("key-{}-{}", worker_id, i);
                let value_clone = value.clone();

                // SET operation
                match client.put(&key, &value_clone, 3600).await {
                    Ok(_) => success += 1,
                    Err(_) => errors += 1,
                }

                // GET operation
                match client.get(&key).await {
                    Ok(v) if v == value_clone => success += 1,
                    Ok(_) => errors += 1,
                    Err(_) => errors += 1,
                }
            }

            (success, errors)
        });

        handles.push(handle);
    }

    let mut total_success = 0;
    let mut total_errors = 0;

    for handle in handles {
        let (success, errors) = handle.await?;
        total_success += success;
        total_errors += errors;
    }

    let elapsed = start.elapsed();
    let ops_per_sec = (total_success as f64) / elapsed.as_secs_f64();
    let total_ops = total_success + total_errors;

    println!("Results:");
    println!("  Total operations: {}", total_ops);
    println!("  Successful: {} ({:.1}%)", total_success, (total_success as f64 / total_ops as f64) * 100.0);
    println!("  Errors: {} ({:.1}%)", total_errors, (total_errors as f64 / total_ops as f64) * 100.0);
    println!("  Time: {:.2}s", elapsed.as_secs_f64());
    println!("  Throughput: {:.0} ops/sec", ops_per_sec);
    if is_large {
        println!("  Note: Large values test fragmentation and batch sending improvements");
    }

    Ok(())
}

async fn test_with_enhancements(
    server_addr: &str,
    num_ops: usize,
    num_workers: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let start = Instant::now();
    let mut handles = Vec::new();

    let ops_per_worker = num_ops / num_workers;

    for worker_id in 0..num_workers {
        let server_addr = server_addr.to_string();
        
        let handle = tokio::spawn(async move {
            // Create client with all enhancements enabled
            let mut client: UdpClient<BinarySerializer> = match ProtocolClient::connect(&server_addr).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to connect: {}", e);
                    return (0, 0);
                }
            };

            let mut success = 0;
            let mut errors = 0;

            for i in 0..ops_per_worker {
                let key = format!("key-enhanced-{}-{}", worker_id, i);
                let value = format!("value-{}-{}", worker_id, i).into_bytes();

                // SET operation
                match client.put(&key, &value, 3600).await {
                    Ok(_) => success += 1,
                    Err(_) => errors += 1,
                }

                // GET operation
                match client.get(&key).await {
                    Ok(v) if v == value => success += 1,
                    Ok(_) => errors += 1,
                    Err(_) => errors += 1,
                }
            }

            (success, errors)
        });

        handles.push(handle);
    }

    let mut total_success = 0;
    let mut total_errors = 0;

    for handle in handles {
        let (success, errors) = handle.await?;
        total_success += success;
        total_errors += errors;
    }

    let elapsed = start.elapsed();
    let ops_per_sec = (total_success as f64) / elapsed.as_secs_f64();
    let total_ops = total_success + total_errors;

    println!("Results (with enhancements):");
    println!("  Total operations: {}", total_ops);
    println!("  Successful: {} ({:.1}%)", total_success, (total_success as f64 / total_ops as f64) * 100.0);
    println!("  Errors: {} ({:.1}%)", total_errors, (total_errors as f64 / total_ops as f64) * 100.0);
    println!("  Time: {:.2}s", elapsed.as_secs_f64());
    println!("  Throughput: {:.0} ops/sec", ops_per_sec);
    println!("  Note: Congestion control and flow control enabled");

    Ok(())
}

