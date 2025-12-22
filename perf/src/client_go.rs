use std::process::{Command, Stdio};
use anyhow::{Context, Result};
use clap::Parser;
use tracing::info;

/// Go UDP client performance test runner
#[derive(Parser)]
#[clap(name = "go-client")]
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

pub async fn run(opt: Opt) -> Result<()> {
    info!("Running Go client performance test");
    
    // Build Go perf client if it doesn't exist
    let project_root = std::env::current_dir()
        .context("failed to get current directory")?
        .parent()
        .ok_or_else(|| anyhow::anyhow!("no parent directory"))?
        .to_path_buf();
    
    let go_perf_bin = project_root.join("target/go-perf-client");
    let go_perf_source = project_root.join("clients/go/examples/perf_client.go");
    
    if !go_perf_bin.exists() {
        info!("Building Go performance client...");
        let status = Command::new("go")
            .current_dir(&project_root)
            .args(&["build", "-o", go_perf_bin.to_str().unwrap(), go_perf_source.to_str().unwrap()])
            .status()
            .context("failed to build Go client")?;
        
        if !status.success() {
            anyhow::bail!("Go client build failed");
        }
    }

    // Run Go client
    let output = Command::new(go_perf_bin.to_str().unwrap())
        .current_dir(&project_root)
        .args(&[
            "-server", &opt.server,
            "-concurrency", &opt.concurrency.to_string(),
            "-value-size", &opt.value_size.to_string(),
            "-duration", &opt.duration.to_string(),
            "-interval", &opt.interval.to_string(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("failed to run Go client")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("Go client failed: {}", stderr);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("{}", stdout);

    Ok(())
}

