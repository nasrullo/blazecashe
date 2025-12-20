use blazecache_client::Client;
use std::time::{Duration, Instant};
use std::collections::VecDeque;

fn format_duration(d: Duration) -> String {
    let nanos = d.as_nanos() as f64;
    if nanos < 1000.0 {
        format!("{:.2}ns", nanos)
    } else if nanos < 1_000_000.0 {
        format!("{:.2}µs", nanos / 1000.0)
    } else {
        format!("{:.2}ms", nanos / 1_000_000.0)
    }
}

fn print_stats(name: &str, latencies: &[Duration]) {
    let mut sorted = latencies.to_vec();
    sorted.sort();
    
    let min = sorted[0];
    let max = sorted[sorted.len() - 1];
    let median = sorted[sorted.len() / 2];
    let p95_idx = (sorted.len() as f64 * 0.95) as usize;
    let p95 = sorted[p95_idx.min(sorted.len() - 1)];
    let p99_idx = (sorted.len() as f64 * 0.99) as usize;
    let p99 = sorted[p99_idx.min(sorted.len() - 1)];
    
    let sum: Duration = latencies.iter().sum();
    let avg = sum / latencies.len() as u32;
    
    println!("  Min:    {}", format_duration(min));
    println!("  Max:    {}", format_duration(max));
    println!("  Avg:    {}", format_duration(avg));
    println!("  Median: {}", format_duration(median));
    println!("  P95:    {}", format_duration(p95));
    println!("  P99:    {}", format_duration(p99));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server_addr = "127.0.0.1:6792";
    
    println!("=== Network Latency Measurement ===");
    println!("Server: {}\n", server_addr);
    
    let client = Client::new(vec![server_addr.to_string()]).await?;
    
    // Test ping first
    client.ping().await?;
    println!("✓ Server connection verified\n");
    
    // Measure PING latency
    println!("=== PING Latency ===");
    let mut ping_latencies = Vec::new();
    for i in 0..100 {
        let start = Instant::now();
        client.ping().await?;
        ping_latencies.push(start.elapsed());
    }
    print_stats("PING", &ping_latencies);
    
    // Measure SET latency
    println!("\n=== SET Latency ===");
    let mut set_latencies = Vec::new();
    for i in 0..100 {
        let key = format!("latency-test-key-{}", i);
        let value = format!("value-{}", i).into_bytes();
        
        let start = Instant::now();
        client.set(&key, &value, 3600).await?;
        set_latencies.push(start.elapsed());
    }
    print_stats("SET", &set_latencies);
    
    // Measure GET latency (warm cache)
    println!("\n=== GET Latency (cache hit) ===");
    let mut get_latencies = Vec::new();
    for i in 0..100 {
        let key = format!("latency-test-key-{}", i);
        
        let start = Instant::now();
        let _result = client.get(&key).await?;
        get_latencies.push(start.elapsed());
    }
    print_stats("GET (hit)", &get_latencies);
    
    // Measure SET+GET combined latency
    println!("\n=== SET+GET Combined Latency ===");
    let mut combined_latencies = Vec::new();
    for i in 0..100 {
        let key = format!("latency-test-combined-{}", i);
        let value = format!("value-{}", i).into_bytes();
        
        let start = Instant::now();
        client.set(&key, &value, 3600).await?;
        let _result = client.get(&key).await?;
        combined_latencies.push(start.elapsed());
    }
    print_stats("SET+GET", &combined_latencies);
    
    println!("\n=== Summary ===");
    println!("PING RTT:     {}", format_duration(ping_latencies[ping_latencies.len() / 2]));
    println!("SET RTT:      {}", format_duration(set_latencies[set_latencies.len() / 2]));
    println!("GET (hit) RTT: {}", format_duration(get_latencies[get_latencies.len() / 2]));
    println!("SET+GET RTT:  {}", format_duration(combined_latencies[combined_latencies.len() / 2]));
    
    Ok(())
}

