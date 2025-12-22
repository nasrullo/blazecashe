use blazecache_client::{TcpClient, SelectionStrategy};
use std::time::Instant;
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server_addr_str = std::env::var("SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:6784".to_string());
    // Parse comma-separated server addresses
    let servers: Vec<String> = server_addr_str.split(',').map(|s| s.trim().to_string()).collect();
    let num_ops = std::env::var("NUM_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);
    let num_workers = std::env::var("NUM_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    println!("=== Rust Client Load Test: {} operations with {} workers ===", num_ops, num_workers);
    println!("Servers: {:?}", servers);

    // Verify server connection
    let test_client = TcpClient::new(servers.clone());
    match test_client.ping().await {
        Ok(_) => println!("✓ Server connection verified\n"),
        Err(e) => {
            eprintln!("✗ Server connection failed: {}", e);
            return Err(e.into());
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

        let client = TcpClient::with_strategy(
            servers.clone(),
            SelectionStrategy::RoundRobin,
        );

        let handle = tokio::spawn(async move {
            let mut success = 0;
            let mut errors = 0;

            for i in 0..ops {
                let key = format!("key-{}-{}", worker_id, i);
                let value = format!("value-{}-{}", worker_id, i).into_bytes();

                // SET operation
                match client.set_with_ttl(&key, value.clone(), 3600).await {
                    Ok(_) => success += 1,
                    Err(_) => errors += 1,
                }

                // GET operation
                match client.get(&key).await {
                    Ok(Some(v)) if v == value => success += 1,
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
    let throughput = (total_success as f64) / elapsed.as_secs_f64();
    let avg_latency = elapsed.as_secs_f64() / (total_success as f64) * 1_000_000.0; // microseconds

    println!("=== Results ===");
    println!("Total operations: {}", total_success + total_errors);
    println!("Successful: {} ({:.2}%)", total_success, (total_success as f64 / (total_success + total_errors) as f64) * 100.0);
    println!("Errors: {} ({:.2}%)", total_errors, (total_errors as f64 / (total_success + total_errors) as f64) * 100.0);
    println!("Time elapsed: {:?}", elapsed);
    println!("Throughput: {:.2} ops/sec", throughput);
    println!("Avg latency: {:.2} µs/op", avg_latency);

    Ok(())
}
