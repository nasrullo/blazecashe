use blazecache::transports::{UdpClient, ProtocolClient};
use blazecache::serializers::BinarySerializer;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server_addr = std::env::var("SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:6790".to_string());
    let num_ops = std::env::var("NUM_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);
    let num_workers = std::env::var("NUM_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    println!("=== UDP Client Load Test: {} operations with {} workers ===", num_ops, num_workers);
    println!("Server: {}", server_addr);

    // Verify server connection
    let mut test_client: UdpClient<BinarySerializer> = ProtocolClient::connect(&server_addr).await?;
    match tokio::time::timeout(std::time::Duration::from_secs(5), test_client.ping()).await {
        Ok(Ok(_)) => println!("✓ Server connection verified\n"),
        Ok(Err(e)) => {
            eprintln!("✗ Server connection failed: {}", e);
            return Err(e);
        }
        Err(_) => {
            eprintln!("✗ Server connection timed out");
            return Err("Connection timeout".into());
        }
    }

    let start = Instant::now();
    let mut handles = Vec::new();

    let ops_per_worker = num_ops / num_workers;
    let remainder = num_ops % num_workers;

    for worker_id in 0..num_workers {
        let ops = if worker_id < remainder {
            ops_per_worker + 1
        } else {
            ops_per_worker
        };

        let server_addr = server_addr.clone();

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

            for i in 0..ops {
                let key = format!("key-{}-{}", worker_id, i);
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
        match handle.await {
            Ok((success, errors)) => {
                total_success += success;
                total_errors += errors;
            }
            Err(e) => {
                eprintln!("Task error: {}", e);
                total_errors += 1;
            }
        }
    }

    let elapsed = start.elapsed();
    let total_ops = total_success + total_errors;
    let throughput = if elapsed.as_secs_f64() > 0.0 {
        (total_success as f64) / elapsed.as_secs_f64()
    } else {
        0.0
    };
    let avg_latency = if total_success > 0 {
        elapsed.as_secs_f64() / (total_success as f64) * 1_000_000.0 // microseconds
    } else {
        0.0
    };

    println!("=== Results ===");
    println!("Total operations: {} ({} SET + {} GET)", total_ops, total_ops / 2, total_ops / 2);
    println!("SET operations: {} successful, {} errors", total_success / 2, total_errors / 2);
    println!("GET operations: {} successful, {} errors", total_success / 2, total_errors / 2);
    println!("Overall: {} successful ({:.2}%), {} errors ({:.2}%)", 
             total_success, 
             (total_success as f64 / total_ops as f64) * 100.0,
             total_errors,
             (total_errors as f64 / total_ops as f64) * 100.0);
    println!("Time elapsed: {:?}", elapsed);
    println!("Throughput: {:.2} ops/sec", throughput);
    println!("Avg latency: {:.2} µs/op", avg_latency);

    Ok(())
}

