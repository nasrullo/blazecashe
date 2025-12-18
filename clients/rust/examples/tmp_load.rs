use blazecache_client::{TcpClient, SelectionStrategy};
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::task;
use tokio::time::Duration;

#[tokio::main]
async fn main() {
    let total_puts: usize = env::args().nth(1).and_then(|v| v.parse().ok()).unwrap_or(1000);
    let concurrency: usize = env::args().nth(2).and_then(|v| v.parse().ok()).unwrap_or(16);
    let progress_step: usize = env::args().nth(3).and_then(|v| v.parse().ok()).unwrap_or(10_000);

    let strategy = env::args()
        .nth(4)
        .map(|s| s.to_lowercase())
        .map(|s| if s == "hash" { SelectionStrategy::ConsistentHashing } else { SelectionStrategy::RoundRobin })
        .unwrap_or(SelectionStrategy::RoundRobin);

    let servers = vec![
        "127.0.0.1:6784".to_string(),
        "127.0.0.1:6786".to_string(),
        "127.0.0.1:6788".to_string(),
    ];

    let client = Arc::new(match strategy {
        SelectionStrategy::RoundRobin => TcpClient::new(servers),
        SelectionStrategy::ConsistentHashing => TcpClient::with_strategy(servers, SelectionStrategy::ConsistentHashing),
    });

    let start = Instant::now();
    let ok = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicUsize::new(0));

    use tokio::sync::Semaphore;
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut handles = Vec::with_capacity(concurrency);

    // Process in batches to avoid overwhelming the runtime
    for chunk_start in (0..total_puts).step_by(concurrency) {
        handles.clear();
        for i in chunk_start..(chunk_start + concurrency).min(total_puts) {
            let client = Arc::clone(&client);
            let ok = Arc::clone(&ok);
            let fail = Arc::clone(&fail);
            let sem = Arc::clone(&semaphore);
            handles.push(task::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let key = format!("load_put_{}", i);
                let result = client.set(&key, b"value".to_vec()).await;
                drop(_permit);
                
                match result {
                    Ok(_) => { ok.fetch_add(1, Ordering::Relaxed); }
                    Err(e) => {
                        let fail_count = fail.fetch_add(1, Ordering::Relaxed);
                        if fail_count < 5 {
                            eprintln!("Error for key {}: {:?}", key, e);
                        }
                    }
                }
            }));
        }

        // Wait for current batch to complete
        for h in handles.drain(..) {
            let _ = h.await;
        }
        
        // Small delay between batches to avoid overwhelming connections
        if chunk_start + concurrency < total_puts {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Progress reporting
        if (chunk_start + concurrency) % progress_step == 0 || (chunk_start + concurrency) >= total_puts {
            let done = (chunk_start + concurrency).min(total_puts);
            println!("progress: {} / {}", done, total_puts);
        }
    }

    let dur = start.elapsed();
    let ok = ok.load(Ordering::Relaxed);
    let fail = fail.load(Ordering::Relaxed);
    let ops_per_sec = ok as f64 / dur.as_secs_f64().max(0.0001);
    println!(
        "puts ok: {} fail: {} total: {} in {:?} ({:.1} ops/sec)",
        ok,
        fail,
        ok + fail,
        dur,
        ops_per_sec
    );
}
