use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Result;
use clap::Parser;
use tracing::{error, info};
use blazecache::transports::{UdpClient, TcpClient, ProtocolClient};
use blazecache::serializers::BinarySerializer;

use crate::stats::OpenOperationStats;

/// Rust client performance test
#[derive(Parser)]
#[clap(name = "rust-client")]
pub struct Opt {
    /// Server address to connect to
    #[clap(default_value = "127.0.0.1:6793", value_name = "SERVER")]
    pub server: String,
    /// Transport type: tcp or udp
    #[clap(long, default_value = "udp")]
    pub transport: String,
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
    info!("Starting performance test");
    info!("Server: {}", opt.server);
    info!("Transport: {}", opt.transport);
    info!("Concurrency: {}", opt.concurrency);
    info!("Value size: {} bytes", opt.value_size);
    info!("Duration: {} seconds", opt.duration);
    
    // Simple connection test - try once, fail fast if server not available
    info!("Testing connection to server...");
    match opt.transport.as_str() {
        "tcp" => {
            match <TcpClient<BinarySerializer> as ProtocolClient>::connect(&opt.server).await {
                Ok(mut client) => {
                    match client.ping().await {
                        Ok(_) => info!("✓ Server is ready (TCP)"),
                        Err(e) => {
                            return Err(anyhow::anyhow!(
                                "Failed to ping server at {}: {}\nPlease ensure the server is running.",
                                opt.server, e
                            ));
                        }
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Failed to connect to server at {}: {}\nPlease ensure the server is running.",
                        opt.server, e
                    ));
                }
            }
        }
        "udp" => {
            match <UdpClient<BinarySerializer> as ProtocolClient>::connect(&opt.server).await {
                Ok(mut client) => {
                    match client.ping().await {
                        Ok(_) => info!("✓ Server is ready (UDP)"),
                        Err(e) => {
                            return Err(anyhow::anyhow!(
                                "Failed to ping server at {}: {}\nPlease ensure the server is running.",
                                opt.server, e
                            ));
                        }
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Failed to connect to server at {}: {}\nPlease ensure the server is running.",
                        opt.server, e
                    ));
                }
            }
        }
        _ => {
            return Err(anyhow::anyhow!("Invalid transport type: {}. Use 'tcp' or 'udp'", opt.transport));
        }
    }

    let operation_stats = Arc::new(operation_stats);
    let value_size = opt.value_size;
    let server_addr = opt.server.clone();
    let transport_type = opt.transport.clone();
    
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
        let transport_type = transport_type.clone();
        
        let handle = tokio::spawn(async move {
            // Create ONE client per worker (reused for all operations)
            // Use match to handle different transport types
            match transport_type.as_str() {
                "tcp" => {
                    let mut client = match <TcpClient<BinarySerializer> as ProtocolClient>::connect(&server_addr).await {
                        Ok(c) => c,
                        Err(e) => {
                            error!("Worker {}: Failed to create TCP client: {}", worker_id, e);
                            return;
                        }
                    };
                    
                    // Small delay to ensure socket is ready
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    
                    // Run operations in a loop until duration expires
                    loop {
                        if start_time.elapsed() >= duration {
                            break;
                        }
                        
                        let request_id = request_id_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        
                        if let Err(e) = run_operation_with_tcp_client(
                            &mut client,
                            request_id,
                            value_size,
                            operation_stats.clone(),
                        ).await {
                            error!("Worker {}: operation failed for request_id={}: {:#}", worker_id, request_id, e);
                        }
                    }
                }
                "udp" => {
                    let mut client = match <UdpClient<BinarySerializer> as ProtocolClient>::connect(&server_addr).await {
                        Ok(c) => c,
                        Err(e) => {
                            error!("Worker {}: Failed to create UDP client: {}", worker_id, e);
                            return;
                        }
                    };
                    
                    // Small delay to ensure socket is ready
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    
                    // Run operations in a loop until duration expires
                    loop {
                        if start_time.elapsed() >= duration {
                            break;
                        }
                        
                        let request_id = request_id_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        
                        if let Err(e) = run_operation_with_udp_client(
                            &mut client,
                            request_id,
                            value_size,
                            operation_stats.clone(),
                        ).await {
                            error!("Worker {}: operation failed for request_id={}: {:#}", worker_id, request_id, e);
                        }
                    }
                }
                _ => {
                    error!("Worker {}: Invalid transport type: {}", worker_id, transport_type);
                    return;
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

// Helper function for TCP client operations
async fn run_operation_with_tcp_client(
    client: &mut TcpClient<BinarySerializer>,
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

// Helper function for UDP client operations
async fn run_operation_with_udp_client(
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

