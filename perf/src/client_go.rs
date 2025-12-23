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
    
    // Check if Go client repository exists in common locations
    let possible_go_client_dirs = vec![
        project_root.join("client-repos/blazecache-client-go"),
        project_root.join("../blazecache-client-go"),
        project_root.join("../../blazecache-client-go"),
    ];
    
    let go_client_dir = possible_go_client_dirs
        .iter()
        .find(|dir| dir.exists())
        .ok_or_else(|| anyhow::anyhow!(
            "Go client repository not found. Please clone blazecache-client-go to one of:\n  - {}\n  - {}\n  - {}",
            possible_go_client_dirs[0].display(),
            possible_go_client_dirs[1].display(),
            possible_go_client_dirs[2].display()
        ))?;
    
    let go_perf_source = go_client_dir.join("examples/perf_client.go");
    
    // Ensure target directory exists
    if let Some(parent) = go_perf_bin.parent() {
        std::fs::create_dir_all(parent)
            .context("failed to create target directory")?;
    }
    
    // Always rebuild to ensure it's up to date
    info!("Building Go performance client...");
    let go_bin = std::env::var("GO_BIN").unwrap_or_else(|_| "go".to_string());
    let status = Command::new(&go_bin)
        .current_dir(&go_client_dir)
        .args(&["build", "-o", go_perf_bin.to_str().unwrap(), go_perf_source.to_str().unwrap()])
        .status()
        .context(format!("failed to build Go client (tried '{}')", go_bin))?;
    
    if !status.success() {
        anyhow::bail!("Go client build failed");
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

