use blazecache::networking::{GossipConfig, GossipProtocol, PeerInfo, PeerRegistry, PeerStatus};
use blazecache::networking::consistent_hash::ConsistentHash;
use blazecache::networking::remote_peer::RemotePeer;
use blazecache::serializers::BinarySerializer;
use blazecache::transports::ProtocolServer;
use blazecache::utils::persistence::{PersistenceConfig, PersistenceManager};
use blazecache::utils::config::AppConfig;
use blazecache::{BlazeCacheError, Getter, Group, TcpServer, UdpServer};
use std::env;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::interval;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use blazecache::utils::time::current_timestamp;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = env::args().collect();
    let config = AppConfig::load(&args);

    if args.iter().any(|a| a == "-h" || a == "--help") {
        print_help();
        return Ok(());
    }
    if args.iter().any(|a| a == "-v" || a == "--version") {
        println!("blazecache 0.1.0");
        return Ok(());
    }

    // Honor RUST_LOG first, otherwise fall back to config log level or default info.
    let log_level_str = env::var("RUST_LOG")
        .or_else(|_| config.log_level.clone().ok_or_else(|| env::VarError::NotPresent))
        .unwrap_or_else(|_| "info".to_string());

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&log_level_str))
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    // Apply hot cache TTL to env for cache constructors (Group reads it).
    if let Some(ttl) = config.hot_cache_ttl_secs {
        env::set_var("HOT_CACHE_TTL_SECS", ttl.to_string());
    }

    info!(port = config.port, memory_mb = config.memory_mb, "Starting blazecache server");
    
    if config.daemon {
        info!("Running in daemon mode");
    } else {
        info!("Use Ctrl+C to stop");
    }
    let local_addr = get_local_address();
    let local_peer_id = format!("{}:{}", local_addr, config.port);
    // For the standalone server binary we use an in-memory group with no backing store.
    // The getter returns KeyNotFound for cache misses so only explicitly PUT values exist.
    let getter: Getter = Arc::new(|_key: &str| Err(BlazeCacheError::KeyNotFound));
    let group = Arc::new(Group::new(
        "default".to_string(),
        config.memory_mb * 1024 * 1024,
        getter,
        local_peer_id.clone()
    ));
   
    // Initialize peer ring with self to allow ownership checks before gossip fills peers.
    initialize_peer_ring(&group, &local_peer_id).await;

    // Initialize optional persistence manager and recover data if enabled.
    let persistence_manager = initialize_persistence(&config, &group).await;

    // Initialize gossip protocol if enabled
    let _registry = if config.gossip.enabled {
        initialize_gossip(&config, &group, &local_peer_id, &local_addr).await
    } else {
        None
    };

    // Start TCP server using the shared transport implementation with optional persistence.
    let tcp_server = TcpServer::<BinarySerializer>::with_persistence(Arc::clone(&group), persistence_manager.clone());
    let tcp_port = config.port;
    tokio::spawn(async move {
        if let Err(e) = tcp_server.start(tcp_port).await {
            error!(error = %e, "TCP server error");
        }
    });

    // Start UDP server if UDP port is configured
    if let Some(udp_port) = config.udp_port {
        let udp_server = UdpServer::<BinarySerializer>::with_persistence(Arc::clone(&group), persistence_manager);
        let udp_port_clone = udp_port;
        tokio::spawn(async move {
            info!(port = udp_port_clone, "Starting UDP server...");
            match udp_server.start(udp_port_clone).await {
                Ok(_) => {
                    error!("UDP server exited unexpectedly");
                }
                Err(e) => {
                    error!(error = %e, "UDP server error");
                }
            }
        });
        info!(port = udp_port, "UDP server enabled");
    }

    // Keep main thread alive
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Gets local IP address (simplified - returns 127.0.0.1 for now)
/// Derives the address we should advertise to peers.
/// Priority: container hostname IP -> fallback loopback.
fn get_local_address() -> String {
    // Prefer docker/bridge friendly: hostname -i (first IPv4)
    if let Ok(output) = Command::new("hostname").arg("-i").output() {
        if output.status.success() {
            if let Ok(s) = String::from_utf8(output.stdout) {
                if let Some(ip) = s
                    .split_whitespace()
                    .find(|ip| !ip.starts_with("127.") && ip.contains('.'))
                {
                    return ip.to_string();
                }
            }
        }
    }

    // Fallback to loopback if detection fails
    "127.0.0.1".to_string()
}

/// Initialize the peer ring with the local node
async fn initialize_peer_ring(group: &Arc<Group>, local_peer_id: &str) {
    let mut initial_ring = ConsistentHash::new(150);
    initial_ring.add_peer(Arc::new(RemotePeer::new(local_peer_id.to_string())), local_peer_id);
    initial_ring.finalize(); // Sort once after all peers are added
    group.set_peers(Box::new(initial_ring)).await;
}

/// Initialize persistence manager and recover data if enabled
async fn initialize_persistence(
    config: &AppConfig,
    group: &Arc<Group>,
) -> Option<Arc<AsyncMutex<PersistenceManager>>> {
    if !config.persistence.enabled {
        return None;
    }

    let mut cfg = PersistenceConfig::default();
    cfg.enabled = true;
    cfg.data_dir = config
        .persistence
        .data_dir
        .clone()
        .unwrap_or_else(|| "./cache_data".to_string());
    
    if let Some(interval) = config.persistence.snapshot_interval {
        cfg.snapshot_interval_secs = interval;
    }
    if let Some(wal) = config.persistence.wal_disabled {
        cfg.wal_enabled = wal;
    }
    if let Some(max_mb) = config.persistence.wal_max_mb {
        cfg.max_wal_size_bytes = max_mb * 1024 * 1024;
    }
    if let Some(compress) = config.persistence.no_compress_snapshots {
        cfg.compress_snapshots = compress;
    }

    match PersistenceManager::new(cfg) {
        Ok(manager) => {
            // Recover any existing data and load into the group
            if let Ok(Some(entries)) = manager.recover() {
                for (k, v) in entries {
                    if let Ok(bytes) = v.get_data() {
                        // Ignore per-key failures to continue recovery
                        let _ = group.set(&k, bytes, 0).await;
                    }
                }
            }
            Some(Arc::new(AsyncMutex::new(manager)))
        }
        Err(e) => {
            error!(error = %e, "Failed to initialize persistence");
            None
        }
    }
}

/// Initialize gossip protocol for peer discovery
async fn initialize_gossip(
    config: &AppConfig,
    group: &Arc<Group>,
    local_peer_id: &str,
    local_addr: &str,
) -> Option<Arc<PeerRegistry>> {
    let registry = Arc::new(PeerRegistry::new());
    
    // Create local peer info
    let local_peer = PeerInfo {
        id: local_peer_id.to_string(),
        address: local_addr.to_string(),
        port: config.port,
        protocol: "tcp".to_string(),
        status: PeerStatus::Active,
        last_seen: current_timestamp(),
    };

    // Configure gossip from config
    let mut gossip_config = GossipConfig::default();
    gossip_config.gossip_port = config.gossip.port.unwrap_or(config.port + 1);
    
    if let Some(interval) = config.gossip.interval {
        gossip_config.gossip_interval = Duration::from_secs(interval);
    }
    if let Some(fanout) = config.gossip.fanout {
        gossip_config.fanout = fanout;
    }
    if let Some(timeout) = config.gossip.suspicion_timeout {
        gossip_config.suspicion_timeout = Duration::from_secs(timeout);
    }
    if let Some(timeout) = config.gossip.failure_timeout {
        gossip_config.failure_timeout = Duration::from_secs(timeout);
    }
    if let Some(interval) = config.gossip.failure_check_interval {
        gossip_config.failure_check_interval = Duration::from_secs(interval);
    }

    let gossip_port = gossip_config.gossip_port;

    // Start gossip protocol
    match GossipProtocol::new(local_peer, Arc::clone(&registry), gossip_config).await {
        Ok(gossip) => {
            gossip.start();
            info!(gossip_port = gossip_port, "Gossip protocol enabled");
            
            // Start background task to update Group's peer picker from discovered peers
            start_peer_discovery_task(group, &registry, local_peer_id);
        }
        Err(e) => {
            warn!(error = %e, "Failed to start gossip protocol");
            return None;
        }
    }
    
    Some(registry)
}

/// Start background task to update peer picker from gossip discoveries
fn start_peer_discovery_task(
    group: &Arc<Group>,
    registry: &Arc<PeerRegistry>,
    local_peer_id: &str,
) {
    let group_clone = Arc::clone(group);
    let registry_clone = Arc::clone(registry);
    let local_peer_id = local_peer_id.to_string();
    
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            
            let discovered_peers = registry_clone.get_active_peers().await;
            
            if !discovered_peers.is_empty() {
                // Build consistent hash ring from active peers
                let mut ring = ConsistentHash::new(150);
                // Include local node
                ring.add_peer(
                    Arc::new(RemotePeer::new(local_peer_id.clone())),
                    &local_peer_id,
                );
                // Add discovered peers
                for peer in &discovered_peers {
                    let addr = format!("{}:{}", peer.address, peer.port);
                    ring.add_peer(Arc::new(RemotePeer::new(addr.clone())), &addr);
                }
                // Finalize ring before using it
                ring.finalize();
                // Update group's peer picker
                group_clone.set_peers(Box::new(ring)).await;
                debug!(peer_count = discovered_peers.len(), "Updated peer picker from gossip");
            }
        }
    });
}

fn print_help() {
    println!("blazecache - High-performance cache server");
    println!();
    println!("USAGE:");
    println!("    blazecache [OPTIONS]");
    println!();
    println!("OPTIONS:");
    println!("    -p, --port <PORT>              TCP port to listen on [default: 6784]");
    println!("        --udp-port <PORT>          UDP port to listen on [optional]");
    println!("    -m, --memory <MB>              Memory limit in MB [default: 64]");
    println!("    -d, --daemon                   Run as daemon");
    println!("    -w, --wal                      Enable persistence (WAL + recovery)");
    println!("        --data-dir <DIR>          Persistence data directory [default: ./cache_data]");
    println!("        --snapshot-interval <S>   Snapshot interval in seconds [default: 300]");
    println!("        --wal-max-mb <MB>         Max WAL size before rotation [default: 100]");
    println!("        --wal-disabled            Disable WAL (snapshots only)");
    println!("        --no-compress-snapshots   Disable snapshot compression");
    println!("        --gossip                  Enable gossip protocol for peer discovery");
    println!("        --gossip-port <PORT>      Gossip UDP port [default: cache_port + 1]");
    println!("        --gossip-interval <S>     Gossip interval in seconds [default: 1]");
        println!("        --gossip-fanout <N>       Number of peers to contact per round [default: 3]");
        println!("        --gossip-suspicion-timeout <S> Time before marking peer inactive [default: 15]");
        println!("        --gossip-failure-timeout <S> Time before marking peer failed [default: 30]");
        println!("        --gossip-failure-check-interval <S> Failure check interval [default: 5]");
        println!("        --log-level <LEVEL>        Set log level (trace, debug, info, warn, error) [default: info]");
        println!("    -h, --help             Print help information");
    println!("    -v, --version          Print version information");
    println!();
    println!("EXAMPLES:");
    println!("    blazecache                    # Start on port 6784 with 64MB");
    println!("    blazecache -p 11211 -m 128   # Start on port 11211 with 128MB");
    println!("    blazecache -d                 # Run as daemon");
}
