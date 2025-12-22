use blazecache::transports::{UdpClient, ProtocolClient};
use blazecache::serializers::BinarySerializer;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Get server address from environment or use default
    // Parse comma-separated server addresses
    let server_addrs_str = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:6793".to_string());
    let server_addrs: Vec<String> = server_addrs_str.split(',').map(|s| s.trim().to_string()).collect();
    
    // Get number of operations from environment or use default
    let num_ops = std::env::var("NUM_OPS")
        .unwrap_or_else(|_| "100000".to_string())
        .parse::<usize>()
        .unwrap_or(100_000);
    
    // Get number of workers from environment or use default
    let num_workers = std::env::var("NUM_WORKERS")
        .unwrap_or_else(|_| "100".to_string())
        .parse::<usize>()
        .unwrap_or(100);

    println!("=== UDP (QUIC) Client Load Test: {} operations with {} workers ===", num_ops, num_workers);
    println!("Servers: {:?}", server_addrs);
    println!();

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

        // Distribute workers across available servers
        let worker_server_addr = server_addrs[worker_id % server_addrs.len()].clone();

        let handle = tokio::spawn(async move {
            let mut client: UdpClient<BinarySerializer> = match ProtocolClient::connect(&worker_server_addr).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Worker {}: Failed to connect: {}", worker_id, e);
                    return (0, 0, 0, 0);
                }
            };
            
            let mut set_success = 0;
            let mut set_errors = 0;
            let mut get_success = 0;
            let mut get_errors = 0;

            for i in 0..ops {
                let key = format!("key-{}-{}", worker_id, i);
                let value = format!("value-{}-{}", worker_id, i).into_bytes();

                // SET operation
                match client.put(&key, &value, 3600).await {
                    Ok(_) => set_success += 1,
                    Err(_) => set_errors += 1,
                }

                // GET operation
                match client.get(&key).await {
                    Ok(v) if v == value => get_success += 1,
                    Ok(_) => get_errors += 1,
                    Err(_) => get_errors += 1,
                }
            }

            (set_success, set_errors, get_success, get_errors)
        });

        handles.push(handle);
    }

    let mut total_set_success = 0;
    let mut total_set_errors = 0;
    let mut total_get_success = 0;
    let mut total_get_errors = 0;

    for handle in handles {
        let (set_success, set_errors, get_success, get_errors) = handle.await?;
        total_set_success += set_success;
        total_set_errors += set_errors;
        total_get_success += get_success;
        total_get_errors += get_errors;
    }

    let elapsed = start.elapsed();
    let total_ops = total_set_success + total_set_errors + total_get_success + total_get_errors;
    let total_success = total_set_success + total_get_success;
    let total_errors = total_set_errors + total_get_errors;
    let throughput = (total_success as f64) / elapsed.as_secs_f64();
    let avg_latency = elapsed.as_secs_f64() / (total_success as f64) * 1_000_000.0; // microseconds

    println!("=== Results ===");
    println!("Total operations: {} ({} SET + {} GET)", total_ops, total_set_success + total_set_errors, total_get_success + total_get_errors);
    println!("SET operations: {} successful, {} errors", total_set_success, total_set_errors);
    println!("GET operations: {} successful, {} errors", total_get_success, total_get_errors);
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

