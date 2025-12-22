use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Result;
use clap::Parser;
use tracing::{error, info};
use blazecache::transports::{UdpClient, ProtocolClient};
use blazecache::serializers::BinarySerializer;

use crate::stats::OpenOperationStats;

/// Rust UDP client performance test
#[derive(Parser)]
#[clap(name = "rust-client")]
pub struct Opt {
    /// Server address to connect to
    #[clap(default_value = "127.0.0.1:6793", value_name = "SERVER")]
    pub server: String,
    /// Number of concurrent operations
    #[clap(long, default_value = "100")]
    pub concurrency: u64,
    /// Value size for PUT operations (can use SI suffixes: k, M, G)
    #[clap(long, default_value = "1k", value_parser = crate::parse_byte_size)]
    pub value_size: u64,
    /// The time to run in seconds
    #[clap(long, default_value = "60")]
    pub duration: u64,
    /// The interval in seconds at which stats are reported
    #[clap(long, default_value = "1")]
    pub interval: u64,
}

pub async fn run(opt: Opt, operation_stats: OpenOperationStats) -> Result<()> {
    info!("Connecting to server at {}", opt.server);
    
    // Wait for server to be ready with retries
    info!("Waiting for server to be ready...");
    let mut server_ready = false;
    for attempt in 1..=10 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        match <UdpClient<BinarySerializer> as ProtocolClient>::connect(&opt.server).await {
            Ok(mut test_client) => {
                match tokio::time::timeout(Duration::from_secs(2), test_client.ping()).await {
                    Ok(Ok(_)) => {
                        info!("Server is ready (attempt {})", attempt);
                        server_ready = true;
                        break;
                    }
                    Ok(Err(e)) => {
                        info!("Ping failed on attempt {}: {} (retrying...)", attempt, e);
                    }
                    Err(_) => {
                        info!("Ping timeout on attempt {} (retrying...)", attempt);
                    }
                }
            }
            Err(e) => {
                info!("Connection failed on attempt {}: {} (retrying...)", attempt, e);
            }

        
        
        }
    }
    
    if !server_ready {
        return Err(anyhow::anyhow!("Server not ready after 10 attempts. Please ensure the server is running on {}", opt.server));
    }

    let operation_stats = Arc::new(operation_stats);
    let value_size = opt.value_size;
    let server_addr = opt.server.clone();
    
    let start_time = Instant::now();
    let duration = Duration::from_secs(opt.duration);
    let mut handles = Vec::new();
    let request_id_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Spawn worker tasks (one client per worker, like Go client)
    for worker_id in 0..opt.concurrency {
        let operation_stats = operation_stats.clone();
        let server_addr = server_addr.clone();
        let value_size = value_size;
        let request_id_counter = request_id_counter.clone();
        let start_time = start_time;
        let duration = duration;
        
        let handle = tokio::spawn(async move {
            // Create ONE client per worker (reused for all operations)
            let mut client = match ProtocolClient::connect(&server_addr).await {
                Ok(c) => c,
                Err(e) => {
                    error!("Worker {}: Failed to create client: {}", worker_id, e);
                    return;
                }
            };
            
            // Small delay to ensure socket is ready
            tokio::time::sleep(Duration::from_millis(10)).await;
            
            // Run operations in a loop until duration expires
            loop {
                // Check if duration has elapsed
                if start_time.elapsed() >= duration {
                    break;
                }
                
                let request_id = request_id_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                
                // Run operation with the reused client
                let op_result = run_operation_with_client(
                    &mut client,
                    request_id,
                    value_size,
                    operation_stats.clone(),
                ).await;
                
                if let Err(e) = op_result {
                    error!("Worker {}: operation failed for request_id={}: {:#}", worker_id, request_id, e);
                }
            }
        });
        
        handles.push(handle);
    }

    // Wait for duration or signal
    tokio::select! {
        _ = tokio::time::sleep(duration) => {
            info!("Test duration completed, waiting for workers to finish...");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Interrupted, shutting down...");
        }
    };
    
    // Wait for all worker tasks to complete
    info!("Waiting for {} workers to complete...", handles.len());
    for handle in handles {
        let _ = handle.await;
    }
    info!("All workers completed");

    Ok(())
}

async fn run_operation_with_client(
    client: &mut UdpClient<BinarySerializer>,
    request_id: u64,
    value_size: u64,
    operation_stats: Arc<OpenOperationStats>,
) -> Result<()> {
    let key = format!("perf-key-{}", request_id);
    let value = vec![0x42u8; value_size as usize];

    // PUT operation
    let put_start = Instant::now();
    let put_stats = operation_stats.new_put(value_size);
    
    match client.put(&key, &value, 3600).await {
        Ok(_) => {
            put_stats.finish(put_start.elapsed());
        }
        Err(e) => {
            return Err(anyhow::anyhow!("PUT failed: {}", e));
        }
    }

    // GET operation
    let get_start = Instant::now();
    let get_stats = operation_stats.new_get(value_size);
    
    let received = client.get(&key).await
        .map_err(|e| anyhow::anyhow!("GET failed: {}", e))?;
    
    get_stats.on_first_byte(get_start.elapsed());
    get_stats.on_bytes(received.len());
    get_stats.finish(get_start.elapsed());

    Ok(())
}

