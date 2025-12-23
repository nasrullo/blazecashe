use clap::{Parser, Subcommand};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt, Registry};

use blazecache_perf::{client_rust, client_go, stats};

#[tokio::main]
async fn main() {
    let opt = Cli::parse();

    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    Registry::default()
        .with(fmt::layer())
        .with(filter)
        .init();

    let r = match opt.command {
        Commands::Rust(opt) => {
            let test_duration = opt.duration;
            let operation_stats = stats::OpenOperationStats::default();
            let operation_stats_clone = operation_stats.clone();
            let operation_stats_for_stats = operation_stats.clone();
            let interval = opt.interval;
            
            // Stats reporting task
            let stats_handle = {
                let operation_stats_for_stats = operation_stats_for_stats.clone();
                tokio::spawn(async move {
                    let interval_duration = std::time::Duration::from_secs(interval);
                    let mut stats = stats::Stats::default();

                    loop {
                        let start = std::time::Instant::now();
                        tokio::time::sleep(interval_duration).await;
                        {
                            stats.on_interval(start, &operation_stats_for_stats);
                            stats.print();
                        }
                    }
                })
            };

            let test_start = std::time::Instant::now();
            let opt_for_test = opt;
            let test_result = tokio::select! {
                result = client_rust::run(opt_for_test, operation_stats_clone) => result,
                _ = tokio::signal::ctrl_c() => {
                    info!("shutting down");
                    Ok(())
                }
            };
            
            // Cancel stats task
            stats_handle.abort();
            
            // Print final stats after test completes
            if test_result.is_ok() {
                // Give a moment for any remaining operations to finish
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                
                // Create final stats with the actual test start time
                let mut final_stats = stats::Stats::default();
                final_stats.set_start_time(test_start);
                
                // Process all operation stats
                // on_interval: interval_start = start - start_instant, interval_end = start_instant.elapsed()
                // We want: interval_start = 0, interval_end = test_duration
                // So: start = start_instant (which is test_start), and start_instant.elapsed() should be test_duration
                // But start_instant.elapsed() is from when we set it (just now), not from test_start
                // So we need to call on_interval with test_start + test_duration
                let interval_end = test_start + std::time::Duration::from_secs(test_duration);
                final_stats.on_interval(interval_end, &operation_stats);
                println!("\n=== Final Statistics ===");
                final_stats.print();
            }
            
            test_result
        }
        Commands::Go(opt) => client_go::run(opt).await,
    };
    
    if let Err(e) = r {
        error!("{:#}", e);
        std::process::exit(1);
    }
}

#[derive(Parser)]
#[clap(name = "blazecache-perf", about = "Blazecache performance testing tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run Rust UDP client performance test
    Rust(client_rust::Opt),
    /// Run Go UDP client performance test
    Go(client_go::Opt),
}

