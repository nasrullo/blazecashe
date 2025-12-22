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
            let operation_stats = stats::OpenOperationStats::default();
            let operation_stats_clone = operation_stats.clone();
            let interval = opt.interval;
            
            let stats_fut = async move {
                let interval_duration = std::time::Duration::from_secs(interval);
                let mut stats = stats::Stats::default();

                loop {
                    let start = std::time::Instant::now();
                    tokio::time::sleep(interval_duration).await;
                    {
                        stats.on_interval(start, &operation_stats);
                        stats.print();
                    }
                }
            };

            tokio::select! {
                result = client_rust::run(opt, operation_stats_clone) => result,
                _ = stats_fut => Ok(()),
                _ = tokio::signal::ctrl_c() => {
                    info!("shutting down");
                    Ok(())
                }
            }
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

