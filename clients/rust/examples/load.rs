use blazecache_client::{TcpClient, SelectionStrategy};
use std::env;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let seed = if args.len() > 1 {
        args[1].clone()
    } else {
        env::var("SEED").unwrap_or_else(|_| "127.0.0.1:6784".to_string())
    };
    let ops: usize = if args.len() > 2 {
        args[2].parse().unwrap_or(500)
    } else {
        env::var("OPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(500)
    };
    let mode = env::var("MODE").unwrap_or_else(|_| "rr".to_string());
    let refresh_secs: u64 = env::var("REFRESH_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    let startup_wait_secs: u64 = env::var("STARTUP_WAIT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    let client = if mode == "ch" {
        // Discovery + consistent hash ring
        TcpClient::with_discovery(seed.clone(), refresh_secs)
    } else {
        // Simple round robin with static seed list (comma-separated)
        let servers: Vec<String> = seed
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        TcpClient::with_strategy(servers, SelectionStrategy::RoundRobin)
    };

    // Give gossip/discovery time to populate peers
    sleep(Duration::from_secs(startup_wait_secs.max(1))).await;

    let mut ok = 0usize;
    let mut errs = 0usize;

    for i in 0..ops {
        let key = format!("load-key-{i}");
        let val = format!("value-{i}").into_bytes();

        if let Err(e) = client.set(&key, val.clone()).await {
            eprintln!("SET error on {key}: {e}");
            errs += 1;
            continue;
        }

        match client.get(&key).await {
            Ok(Some(v)) if v == val => ok += 1,
            Ok(Some(v)) => {
                eprintln!("Data mismatch on {key}: {:?}", String::from_utf8_lossy(&v));
                errs += 1;
            }
            Ok(None) => {
                eprintln!("Missing key after set: {key}");
                errs += 1;
            }
            Err(e) => {
                eprintln!("GET error on {key}: {e}");
                errs += 1;
            }
        }
    }

    println!(
        "Load test complete. total_ops={} success={} errors={}",
        ops, ok, errs
    );

    Ok(())
}
