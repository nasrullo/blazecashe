use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Result;
use clap::Parser;
use tokio::sync::Semaphore;
use tracing::{error, info};
use blazecache::transports::{UdpClient, ProtocolClient};
use blazecache::serializers::BinarySerializer;

use crate::stats::OpenOperationStats;

/// Rust UDP client performance test
#[derive(Parser)]
#[clap(name = "rust-client")]
pub struct Opt {
    /// Server address to connect to
    #[clap(default_value = "127.0.0.1:6793")]
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
    
    let mut client = <UdpClient<BinarySerializer> as ProtocolClient>::connect(&opt.server)
        .await
        .map_err(|e| anyhow::anyhow!("connecting to server: {}", e))?;

    // Verify connection
    client.ping().await.map_err(|e| anyhow::anyhow!("ping failed: {}", e))?;
    info!("Connected and verified");

    let sem = Arc::new(Semaphore::new(opt.concurrency as usize));
    let operation_stats = Arc::new(operation_stats);
    let server = opt.server.clone();
    let value_size = opt.value_size;

    let drive_fut = async {
        let mut request_id = 0u64;
        loop {
            let permit = sem.clone().acquire_owned().await.unwrap();
            let operation_stats = operation_stats.clone();
            let server = server.clone();
            request_id += 1;

            tokio::spawn(async move {
                if let Err(e) = run_operation(
                    &server,
                    request_id,
                    value_size,
                    operation_stats.clone(),
                ).await {
                    error!("operation failed: {:#}", e);
                }
                drop(permit);
            });
        }
    };

    tokio::select! {
        _ = drive_fut => {}
        _ = tokio::signal::ctrl_c() => {
            info!("shutting down");
        }
        _ = tokio::time::sleep(Duration::from_secs(opt.duration)) => {
            info!("test duration completed");
        }
    };

    Ok(())
}

async fn run_operation(
    server: &str,
    request_id: u64,
    value_size: u64,
    operation_stats: Arc<OpenOperationStats>,
) -> Result<()> {
    // Connect client for this operation
    let mut client = <UdpClient<BinarySerializer> as ProtocolClient>::connect(server)
        .await
        .map_err(|e| anyhow::anyhow!("connecting: {}", e))?;

    let key = format!("perf-key-{}", request_id);
    let value = vec![0x42u8; value_size as usize];

    // PUT operation
    let put_start = Instant::now();
    let put_stats = operation_stats.new_put(value_size);
    
    client.put(&key, &value, 3600).await
        .map_err(|e| anyhow::anyhow!("PUT failed: {}", e))?;
    
    put_stats.finish(put_start.elapsed());

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

