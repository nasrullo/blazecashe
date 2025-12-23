//! # Configuration Management
//!
//! This module handles loading and parsing configuration from multiple sources with
//! a well-defined precedence order. Configuration can be specified via:
//!
//! 1. **Configuration File** (`blazecache.toml`) - Base configuration
//! 2. **Environment Variables** - Override file settings
//! 3. **Command-Line Arguments** - Highest precedence, override everything
//!
//! ## Configuration Precedence
//!
//! ```
//! Defaults < File < Environment < Command-Line
//! ```
//!
//! Later sources override earlier ones, allowing flexible configuration management
//! for different deployment scenarios (development, staging, production).
//!
//! ## Example Configuration File
//!
//! ```toml
//! [server]
//! port = 6784
//! memory = 1024
//!
//! [persistence]
//! enabled = true
//! data_dir = "/var/lib/blazecache"
//!
//! [gossip]
//! enabled = true
//! port = 6785
//! ```
//!
//! ## Environment Variables
//!
//! All settings can be overridden via environment variables:
//! - `BLAZECACHE_PORT` - TCP port
//! - `BLAZECACHE_MEMORY_MB` - Memory limit in MB
//! - `PERSISTENCE_ENABLED` - Enable persistence
//! - `GOSSIP_ENABLED` - Enable gossip protocol
//! - And many more...

use serde::Deserialize;
use std::fs;
use std::path::Path;

/// Main application configuration structure.
///
/// This structure holds all configuration settings for the BlazeCache server.
/// It combines settings from file, environment, and command-line sources.
///
/// ## Configuration Sources
///
/// Settings are loaded in this order (later sources override earlier):
/// 1. Default values (defined in `Default` implementation)
/// 2. Configuration file (`blazecache.toml`)
/// 3. Environment variables
/// 4. Command-line arguments
///
/// ## Fields
///
/// All fields are public and can be accessed directly after loading.
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// TCP port number for the main cache server.
    ///
    /// Default: 6784
    ///
    /// This is the port where clients connect to perform cache operations.
    pub port: u16,

    /// Optional TLS-encrypted TCP port.
    ///
    /// If `None`, TLS is disabled. If `Some(port)`, a TLS server will listen
    /// on that port in addition to the plain TCP server.
    pub tls_port: Option<u16>,

    /// Optional UDP (QUIC) port.
    ///
    /// If `None`, UDP is disabled. If `Some(port)`, a UDP server will listen
    /// on that port for QUIC-encrypted connections.
    pub udp_port: Option<u16>,

    /// Memory limit for the cache in megabytes.
    ///
    /// Default: 64 MB
    ///
    /// This determines the maximum amount of memory the cache can use.
    /// When the limit is reached, LRU eviction occurs.
    pub memory_mb: usize,

    /// Whether to run as a daemon (background process).
    ///
    /// Default: false
    ///
    /// When `true`, the server detaches from the terminal and runs in the
    /// background. Useful for production deployments.
    pub daemon: bool,

    /// Optional log level override.
    ///
    /// If `None`, uses default or `RUST_LOG` environment variable.
    /// Valid values: "trace", "debug", "info", "warn", "error"
    pub log_level: Option<String>,

    /// Persistence configuration section.
    ///
    /// Controls whether and how cache data is persisted to disk for crash recovery.
    pub persistence: PersistenceSection,

    /// Gossip protocol configuration section.
    ///
    /// Controls automatic peer discovery and cluster membership management.
    pub gossip: GossipSection,

    /// Time-to-live for hot cache entries in seconds.
    ///
    /// Default: 1 second
    ///
    /// Hot cache is a secondary cache for frequently accessed items from peer nodes.
    /// This TTL determines how long items stay in the hot cache before expiring.
    pub hot_cache_ttl_secs: Option<u64>,
}

/// Persistence configuration settings.
///
/// Controls whether cache data is persisted to disk and how persistence works.
/// Persistence enables crash recovery by saving cache state to disk.
///
/// ## Features
///
/// - **Write-Ahead Log (WAL)**: Logs all writes before applying them
/// - **Snapshots**: Periodic full cache state dumps
/// - **Compression**: Optional compression of snapshots to save disk space
///
/// ## Example
///
/// ```rust
/// use blazecache::utils::config::PersistenceSection;
///
/// let mut persistence = PersistenceSection::default();
/// persistence.enabled = true;
/// persistence.data_dir = Some("/var/lib/blazecache".to_string());
/// persistence.snapshot_interval = Some(300); // 5 minutes
/// ```
#[derive(Debug, Clone, Default)]
pub struct PersistenceSection {
    /// Whether persistence is enabled.
    ///
    /// When `false`, all persistence features are disabled and cache is
    /// purely in-memory. When `true`, WAL and snapshots are active.
    pub enabled: bool,

    /// Directory where persistence data is stored.
    ///
    /// If `None`, defaults to `./cache_data` in the current working directory.
    /// Should be set to a persistent location (e.g., `/var/lib/blazecache`) in production.
    pub data_dir: Option<String>,

    /// Interval between snapshots in seconds.
    ///
    /// If `None`, uses default (typically 300 seconds / 5 minutes).
    /// Snapshots are full cache state dumps used for recovery.
    pub snapshot_interval: Option<u64>,

    /// Whether to disable the Write-Ahead Log (WAL).
    ///
    /// If `Some(true)`, WAL is disabled and only snapshots are used.
    /// If `Some(false)` or `None`, WAL is enabled for better durability.
    pub wal_disabled: Option<bool>,

    /// Maximum WAL size in megabytes before rotation.
    ///
    /// If `None`, uses default (typically 100 MB).
    /// When WAL reaches this size, it's rotated and a new WAL file is created.
    pub wal_max_mb: Option<usize>,

    /// Whether to disable snapshot compression.
    ///
    /// If `Some(true)`, snapshots are stored uncompressed (faster, larger).
    /// If `Some(false)` or `None`, snapshots are compressed (slower, smaller).
    pub no_compress_snapshots: Option<bool>,
}

/// Gossip protocol configuration settings.
///
/// Controls automatic peer discovery and cluster membership management using
/// a gossip-based protocol. Gossip allows nodes to discover each other without
/// manual configuration.
///
/// ## How It Works
///
/// 1. Nodes periodically exchange membership information with random peers
/// 2. Information propagates through the cluster via gossip rounds
/// 3. Eventually, all nodes learn about all other nodes
/// 4. Nodes track peer health and remove failed nodes
///
/// ## Example
///
/// ```rust
/// use blazecache::utils::config::GossipSection;
///
/// let mut gossip = GossipSection::default();
/// gossip.enabled = true;
/// gossip.port = Some(6785);
/// gossip.interval = Some(1); // Gossip every 1 second
/// gossip.fanout = Some(3); // Contact 3 random peers per round
/// ```
#[derive(Debug, Clone, Default)]
pub struct GossipSection {
    /// Whether gossip protocol is enabled.
    ///
    /// When `false`, peer discovery is disabled and peers must be configured manually.
    /// When `true`, automatic peer discovery via gossip is active.
    pub enabled: bool,

    /// UDP port for gossip protocol communication.
    ///
    /// If `None`, defaults to `cache_port + 1` (e.g., if cache is on 6784,
    /// gossip uses 6785).
    pub port: Option<u16>,

    /// Interval between gossip rounds in seconds.
    ///
    /// If `None`, uses default (typically 1 second).
    /// Lower values mean faster peer discovery but more network traffic.
    pub interval: Option<u64>,

    /// Number of random peers to contact per gossip round.
    ///
    /// If `None`, uses default (typically 3).
    /// Higher values mean faster propagation but more network traffic.
    pub fanout: Option<usize>,

    /// Time in seconds before marking a peer as inactive (suspicious).
    ///
    /// If `None`, uses default (typically 15 seconds).
    /// Peers that haven't been seen for this duration are marked as suspicious.
    pub suspicion_timeout: Option<u64>,

    /// Time in seconds before marking a peer as failed.
    ///
    /// If `None`, uses default (typically 30 seconds).
    /// Peers that haven't been seen for this duration are removed from the cluster.
    pub failure_timeout: Option<u64>,

    /// Interval in seconds between failure checks.
    ///
    /// If `None`, uses default (typically 5 seconds).
    /// Controls how often the system checks for failed peers.
    pub failure_check_interval: Option<u64>,
}

/// Internal structure for deserializing TOML configuration file.
///
/// This structure matches the expected format of `blazecache.toml` and is used
/// only for file parsing. It's not part of the public API.
#[derive(Debug, Deserialize, Default)]
struct FileConfig {
    /// Server configuration section from file.
    server: Option<ServerFile>,

    /// Persistence configuration section from file.
    persistence: Option<PersistenceFile>,

    /// Gossip configuration section from file.
    gossip: Option<GossipFile>,

    /// Logging configuration section from file.
    logging: Option<LoggingFile>,

    /// Hot cache configuration section from file.
    hot_cache: Option<HotCacheFile>,
}

/// Server configuration from TOML file.
///
/// Maps to the `[server]` section in `blazecache.toml`.
#[derive(Debug, Deserialize, Default)]
struct ServerFile {
    /// TCP port number.
    port: Option<u16>,

    /// Memory limit in megabytes.
    memory: Option<usize>,

    /// Whether to run as daemon.
    daemon: Option<bool>,
}

/// Persistence configuration from TOML file.
///
/// Maps to the `[persistence]` section in `blazecache.toml`.
#[derive(Debug, Deserialize, Default)]
struct PersistenceFile {
    enabled: Option<bool>,
    data_dir: Option<String>,
    snapshot_interval: Option<u64>,
    wal_disabled: Option<bool>,
    wal_max_mb: Option<usize>,
    no_compress_snapshots: Option<bool>,
}

/// Gossip configuration from TOML file.
///
/// Maps to the `[gossip]` section in `blazecache.toml`.
#[derive(Debug, Deserialize, Default)]
struct GossipFile {
    enabled: Option<bool>,
    port: Option<u16>,
    interval: Option<u64>,
    fanout: Option<usize>,
    suspicion_timeout: Option<u64>,
    failure_timeout: Option<u64>,
    failure_check_interval: Option<u64>,
}

/// Logging configuration from TOML file.
///
/// Maps to the `[logging]` section in `blazecache.toml`.
#[derive(Debug, Deserialize, Default)]
struct LoggingFile {
    /// Log level: "trace", "debug", "info", "warn", or "error".
    level: Option<String>,
}

/// Hot cache configuration from TOML file.
///
/// Maps to the `[hot_cache]` section in `blazecache.toml`.
#[derive(Debug, Deserialize, Default)]
struct HotCacheFile {
    /// Time-to-live for hot cache entries in seconds.
    ttl_secs: Option<u64>,
}

impl Default for AppConfig {
    /// Creates default configuration with sensible defaults.
    ///
    /// These defaults are suitable for development and testing. Production
    /// deployments should override these via configuration file or environment variables.
    ///
    /// ## Default Values
    ///
    /// - Port: 6784
    /// - Memory: 64 MB
    /// - Daemon: false
    /// - Persistence: disabled
    /// - Gossip: disabled
    /// - Hot cache TTL: 1 second
    fn default() -> Self {
        Self {
            port: 6784,
            tls_port: None,
            udp_port: None,
            memory_mb: 64,
            daemon: false,
            log_level: None,
            persistence: PersistenceSection::default(),
            gossip: GossipSection::default(),
            hot_cache_ttl_secs: Some(1),
        }
    }
}

impl AppConfig {
    /// Loads configuration from all sources in precedence order.
    ///
    /// Configuration is loaded and merged from multiple sources with the following
    /// precedence (later sources override earlier ones):
    ///
    /// 1. **Default values** - Base configuration
    /// 2. **Configuration file** (`blazecache.toml`) - File-based settings
    /// 3. **Environment variables** - Environment-based overrides
    /// 4. **Command-line arguments** - Highest precedence, runtime overrides
    ///
    /// ## Arguments
    ///
    /// * `args` - Command-line arguments (typically from `std::env::args()`)
    ///
    /// ## Returns
    ///
    /// A fully configured `AppConfig` instance with all settings merged.
    ///
    /// ## Example
    ///
    /// ```rust
    /// use blazecache::utils::config::AppConfig;
    ///
    /// let args: Vec<String> = std::env::args().collect();
    /// let config = AppConfig::load(&args);
    /// ```
    pub fn load(args: &[String]) -> Self {
        // Start with default configuration
        let mut cfg = Self::default();

        // 1) Load from configuration file (if it exists)
        // File provides base configuration that can be overridden
        if let Ok(file_cfg) = load_file_config("blazecache.toml") {
            apply_file(&mut cfg, &file_cfg);
        }

        // 2) Apply environment variable overrides
        // Environment variables override file settings
        apply_env(&mut cfg);

        // 3) Apply command-line argument overrides
        // CLI arguments have highest precedence and override everything
        apply_cli(&mut cfg, args);

        cfg
    }
}

/// Loads configuration from a TOML file.
///
/// This function attempts to read and parse a TOML configuration file. If the
/// file doesn't exist, it returns a default (empty) configuration rather than
/// an error, allowing the application to run with defaults or other config sources.
///
/// ## Arguments
///
/// * `path` - Path to the configuration file (typically "blazecache.toml")
///
/// ## Returns
///
/// A `FileConfig` structure with parsed settings, or a default empty config
/// if the file doesn't exist or cannot be read.
///
/// ## Errors
///
/// Returns an error only if the file exists but cannot be read or parsed.
/// Missing files are handled gracefully by returning default configuration.
fn load_file_config(path: &str) -> Result<FileConfig, Box<dyn std::error::Error>> {
    // Check if the configuration file exists
    // If it doesn't exist, return default config (not an error)
    if !Path::new(path).exists() {
        return Ok(FileConfig::default());
    }
    
    // Read the file content
    let content = fs::read_to_string(path)?;
    
    // Parse TOML format into FileConfig structure
    let cfg: FileConfig = toml::from_str(&content)?;
    Ok(cfg)
}

/// Applies configuration from a TOML file to the main config.
///
/// This function merges file-based configuration into the main `AppConfig`.
/// Only fields that are present in the file are applied; missing fields
/// leave the existing config unchanged.
///
/// ## Arguments
///
/// * `cfg` - The main configuration to update (modified in place)
/// * `file` - The file configuration to apply
fn apply_file(cfg: &mut AppConfig, file: &FileConfig) {
    // Apply server configuration section
    if let Some(server) = &file.server {
        if let Some(port) = server.port {
            cfg.port = port;
        }
        if let Some(memory) = server.memory {
            cfg.memory_mb = memory;
        }
        if let Some(daemon) = server.daemon {
            cfg.daemon = daemon;
        }
    }
    
    // Apply persistence configuration section
    if let Some(p) = &file.persistence {
        if let Some(enabled) = p.enabled {
            cfg.persistence.enabled = enabled;
        }
        // Use file value if present, otherwise keep existing value
        cfg.persistence.data_dir = p.data_dir.clone().or(cfg.persistence.data_dir.clone());
        cfg.persistence.snapshot_interval = p.snapshot_interval.or(cfg.persistence.snapshot_interval);
        cfg.persistence.wal_disabled = p.wal_disabled.or(cfg.persistence.wal_disabled);
        cfg.persistence.wal_max_mb = p.wal_max_mb.or(cfg.persistence.wal_max_mb);
        cfg.persistence.no_compress_snapshots =
            p.no_compress_snapshots.or(cfg.persistence.no_compress_snapshots);
    }
    
    // Apply gossip configuration section
    if let Some(g) = &file.gossip {
        if let Some(enabled) = g.enabled {
            cfg.gossip.enabled = enabled;
        }
        // Use file value if present, otherwise keep existing value
        cfg.gossip.port = g.port.or(cfg.gossip.port);
        cfg.gossip.interval = g.interval.or(cfg.gossip.interval);
        cfg.gossip.fanout = g.fanout.or(cfg.gossip.fanout);
        cfg.gossip.suspicion_timeout = g.suspicion_timeout.or(cfg.gossip.suspicion_timeout);
        cfg.gossip.failure_timeout = g.failure_timeout.or(cfg.gossip.failure_timeout);
        cfg.gossip.failure_check_interval =
            g.failure_check_interval.or(cfg.gossip.failure_check_interval);
    }
    
    // Apply logging configuration section
    if let Some(l) = &file.logging {
        if let Some(level) = &l.level {
            cfg.log_level = Some(level.clone());
        }
    }
    
    // Apply hot cache configuration section
    if let Some(h) = &file.hot_cache {
        cfg.hot_cache_ttl_secs = h.ttl_secs.or(cfg.hot_cache_ttl_secs);
    }
}

/// Applies configuration from environment variables.
///
/// This function reads environment variables and overrides the configuration.
/// Environment variables follow the naming convention:
/// - `BLAZECACHE_*` for server settings
/// - `PERSISTENCE_*` for persistence settings
/// - `GOSSIP_*` for gossip settings
///
/// ## Environment Variable Names
///
/// - `BLAZECACHE_PORT` - TCP port
/// - `BLAZECACHE_UDP_PORT` - UDP port
/// - `BLAZECACHE_MEMORY_MB` - Memory limit
/// - `BLAZECACHE_DAEMON` - Run as daemon
/// - `BLAZECACHE_LOG_LEVEL` - Log level
/// - `PERSISTENCE_ENABLED` - Enable persistence
/// - `PERSISTENCE_DATA_DIR` - Data directory
/// - `GOSSIP_ENABLED` - Enable gossip
/// - And many more...
///
/// ## Arguments
///
/// * `cfg` - The main configuration to update (modified in place)
fn apply_env(cfg: &mut AppConfig) {
    // Server configuration from environment
    if let Ok(port) = std::env::var("BLAZECACHE_PORT") {
        if let Ok(p) = port.parse() {
            cfg.port = p;
        }
    }
    if let Ok(port) = std::env::var("BLAZECACHE_UDP_PORT") {
        if let Ok(p) = port.parse() {
            cfg.udp_port = Some(p);
        }
    }
    if let Ok(mem) = std::env::var("BLAZECACHE_MEMORY_MB") {
        if let Ok(m) = mem.parse() {
            cfg.memory_mb = m;
        }
    }
    if let Ok(daemon) = std::env::var("BLAZECACHE_DAEMON") {
        if let Ok(d) = daemon.parse() {
            cfg.daemon = d;
        }
    }
    if let Ok(level) = std::env::var("BLAZECACHE_LOG_LEVEL") {
        if !level.trim().is_empty() {
            cfg.log_level = Some(level);
        }
    }
    
    // Persistence configuration from environment
    if let Ok(enabled) = std::env::var("PERSISTENCE_ENABLED") {
        if let Ok(val) = enabled.parse() {
            cfg.persistence.enabled = val;
        }
    }
    if let Ok(dir) = std::env::var("PERSISTENCE_DATA_DIR") {
        if !dir.trim().is_empty() {
            cfg.persistence.data_dir = Some(dir);
        }
    }
    if let Ok(snap) = std::env::var("PERSISTENCE_SNAPSHOT_INTERVAL") {
        if let Ok(val) = snap.parse() {
            cfg.persistence.snapshot_interval = Some(val);
        }
    }
    if let Ok(wal_disabled) = std::env::var("PERSISTENCE_WAL_DISABLED") {
        if let Ok(val) = wal_disabled.parse() {
            cfg.persistence.wal_disabled = Some(val);
        }
    }
    if let Ok(wal_max) = std::env::var("PERSISTENCE_WAL_MAX_MB") {
        if let Ok(val) = wal_max.parse() {
            cfg.persistence.wal_max_mb = Some(val);
        }
    }
    if let Ok(no_compress) = std::env::var("PERSISTENCE_NO_COMPRESS_SNAPSHOTS") {
        if let Ok(val) = no_compress.parse() {
            cfg.persistence.no_compress_snapshots = Some(val);
        }
    }
    
    // Gossip configuration from environment
    if let Ok(enabled) = std::env::var("GOSSIP_ENABLED") {
        if let Ok(val) = enabled.parse() {
            cfg.gossip.enabled = val;
        }
    }
    if let Ok(port) = std::env::var("GOSSIP_PORT") {
        if let Ok(val) = port.parse() {
            cfg.gossip.port = Some(val);
        }
    }
    if let Ok(interval) = std::env::var("GOSSIP_INTERVAL") {
        if let Ok(val) = interval.parse() {
            cfg.gossip.interval = Some(val);
        }
    }
    if let Ok(fanout) = std::env::var("GOSSIP_FANOUT") {
        if let Ok(val) = fanout.parse() {
            cfg.gossip.fanout = Some(val);
        }
    }
    if let Ok(val) = std::env::var("GOSSIP_SUSPICION_TIMEOUT") {
        if let Ok(num) = val.parse() {
            cfg.gossip.suspicion_timeout = Some(num);
        }
    }
    if let Ok(val) = std::env::var("GOSSIP_FAILURE_TIMEOUT") {
        if let Ok(num) = val.parse() {
            cfg.gossip.failure_timeout = Some(num);
        }
    }
    if let Ok(val) = std::env::var("GOSSIP_FAILURE_CHECK_INTERVAL") {
        if let Ok(num) = val.parse() {
            cfg.gossip.failure_check_interval = Some(num);
        }
    }
    
    // Hot cache configuration from environment
    if let Ok(ttl) = std::env::var("HOT_CACHE_TTL_SECS") {
        if let Ok(val) = ttl.parse() {
            cfg.hot_cache_ttl_secs = Some(val);
        }
    }
}

/// Applies configuration from command-line arguments.
///
/// This function parses command-line arguments and overrides the configuration.
/// Command-line arguments have the highest precedence and override all other sources.
///
/// ## Supported Arguments
///
/// - `-p, --port <PORT>` - TCP port
/// - `--tls-port <PORT>` - TLS port
/// - `--udp-port <PORT>` - UDP port
/// - `-m, --memory <MB>` - Memory limit
/// - `-d, --daemon` - Run as daemon
/// - `--gossip` - Enable gossip
/// - `--gossip-port <PORT>` - Gossip port
/// - `--data-dir <DIR>` - Persistence data directory
/// - And many more...
///
/// ## Arguments
///
/// * `cfg` - The main configuration to update (modified in place)
/// * `args` - Command-line arguments (typically from `std::env::args()`)
///
/// ## Parsing Logic
///
/// The function iterates through arguments and matches them against known flags.
/// Flags that take values (e.g., `--port 8080`) consume the next argument.
/// Flags without values (e.g., `--daemon`) are boolean flags.
fn apply_cli(cfg: &mut AppConfig, args: &[String]) {
    // Start at index 1 to skip the program name (args[0])
    let mut i = 1;
    
    // Iterate through all command-line arguments
    while i < args.len() {
        match args[i].as_str() {
            // TCP port: -p or --port
            "-p" | "--port" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.port = val;
                    }
                    i += 1; // Skip the value argument
                }
            }
            // TLS port: --tls-port
            "--tls-port" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.tls_port = Some(val);
                    }
                    i += 1;
                }
            }
            // UDP port: --udp-port
            "--udp-port" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.udp_port = Some(val);
                    }
                    i += 1;
                }
            }
            // Memory limit: -m or --memory
            "-m" | "--memory" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.memory_mb = val;
                    }
                    i += 1;
                }
            }
            // Daemon mode: -d or --daemon (boolean flag, no value)
            "-d" | "--daemon" => {
                cfg.daemon = true;
            }
            // Enable gossip: --gossip (boolean flag, no value)
            "--gossip" => {
                cfg.gossip.enabled = true;
            }
            // Gossip port: --gossip-port
            "--gossip-port" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.gossip.port = Some(val);
                    }
                    i += 1;
                }
            }
            // Gossip interval: --gossip-interval
            "--gossip-interval" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.gossip.interval = Some(val);
                    }
                    i += 1;
                }
            }
            // Gossip fanout: --gossip-fanout
            "--gossip-fanout" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.gossip.fanout = Some(val);
                    }
                    i += 1;
                }
            }
            // Gossip suspicion timeout: --gossip-suspicion-timeout
            "--gossip-suspicion-timeout" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.gossip.suspicion_timeout = Some(val);
                    }
                    i += 1;
                }
            }
            // Gossip failure timeout: --gossip-failure-timeout
            "--gossip-failure-timeout" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.gossip.failure_timeout = Some(val);
                    }
                    i += 1;
                }
            }
            // Gossip failure check interval: --gossip-failure-check-interval
            "--gossip-failure-check-interval" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.gossip.failure_check_interval = Some(val);
                    }
                    i += 1;
                }
            }
            // Persistence data directory: --data-dir
            "--data-dir" => {
                if i + 1 < args.len() {
                    cfg.persistence.data_dir = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            // Snapshot interval: --snapshot-interval
            "--snapshot-interval" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.persistence.snapshot_interval = Some(val);
                    }
                    i += 1;
                }
            }
            // Disable WAL: --wal-disabled (boolean flag, no value)
            "--wal-disabled" => {
                cfg.persistence.wal_disabled = Some(true);
            }
            // WAL max size: --wal-max-mb
            "--wal-max-mb" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.persistence.wal_max_mb = Some(val);
                    }
                    i += 1;
                }
            }
            // Disable snapshot compression: --no-compress-snapshots (boolean flag)
            "--no-compress-snapshots" => {
                cfg.persistence.no_compress_snapshots = Some(true);
            }
            // Hot cache TTL: --hot-cache-ttl
            "--hot-cache-ttl" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.hot_cache_ttl_secs = Some(val);
                    }
                    i += 1;
                }
            }
            // Unknown argument - ignore it (allows for future extensions)
            _ => {}
        }
        // Move to next argument
        i += 1;
    }
}
