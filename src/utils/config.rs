use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub port: u16,
    pub memory_mb: usize,
    pub daemon: bool,
    pub log_level: Option<String>,
    pub persistence: PersistenceSection,
    pub gossip: GossipSection,
    pub hot_cache_ttl_secs: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct PersistenceSection {
    pub enabled: bool,
    pub data_dir: Option<String>,
    pub snapshot_interval: Option<u64>,
    pub wal_disabled: Option<bool>,
    pub wal_max_mb: Option<usize>,
    pub no_compress_snapshots: Option<bool>,
}

#[derive(Debug, Clone, Default)]
pub struct GossipSection {
    pub enabled: bool,
    pub port: Option<u16>,
    pub interval: Option<u64>,
    pub fanout: Option<usize>,
    pub suspicion_timeout: Option<u64>,
    pub failure_timeout: Option<u64>,
    pub failure_check_interval: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
struct FileConfig {
    server: Option<ServerFile>,
    persistence: Option<PersistenceFile>,
    gossip: Option<GossipFile>,
    logging: Option<LoggingFile>,
    hot_cache: Option<HotCacheFile>,
}

#[derive(Debug, Deserialize, Default)]
struct ServerFile {
    port: Option<u16>,
    memory: Option<usize>,
    daemon: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
struct PersistenceFile {
    enabled: Option<bool>,
    data_dir: Option<String>,
    snapshot_interval: Option<u64>,
    wal_disabled: Option<bool>,
    wal_max_mb: Option<usize>,
    no_compress_snapshots: Option<bool>,
}

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

#[derive(Debug, Deserialize, Default)]
struct LoggingFile {
    level: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct HotCacheFile {
    ttl_secs: Option<u64>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            port: 6784,
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
    pub fn load(args: &[String]) -> Self {
        let mut cfg = Self::default();

        // 1) file
        if let Ok(file_cfg) = load_file_config("blazecache.toml") {
            apply_file(&mut cfg, &file_cfg);
        }

        // 2) env
        apply_env(&mut cfg);

        // 3) cli
        apply_cli(&mut cfg, args);

        cfg
    }
}

fn load_file_config(path: &str) -> Result<FileConfig, Box<dyn std::error::Error>> {
    if !Path::new(path).exists() {
        return Ok(FileConfig::default());
    }
    let content = fs::read_to_string(path)?;
    let cfg: FileConfig = toml::from_str(&content)?;
    Ok(cfg)
}

fn apply_file(cfg: &mut AppConfig, file: &FileConfig) {
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
    if let Some(p) = &file.persistence {
        if let Some(enabled) = p.enabled {
            cfg.persistence.enabled = enabled;
        }
        cfg.persistence.data_dir = p.data_dir.clone().or(cfg.persistence.data_dir.clone());
        cfg.persistence.snapshot_interval = p.snapshot_interval.or(cfg.persistence.snapshot_interval);
        cfg.persistence.wal_disabled = p.wal_disabled.or(cfg.persistence.wal_disabled);
        cfg.persistence.wal_max_mb = p.wal_max_mb.or(cfg.persistence.wal_max_mb);
        cfg.persistence.no_compress_snapshots =
            p.no_compress_snapshots.or(cfg.persistence.no_compress_snapshots);
    }
    if let Some(g) = &file.gossip {
        if let Some(enabled) = g.enabled {
            cfg.gossip.enabled = enabled;
        }
        cfg.gossip.port = g.port.or(cfg.gossip.port);
        cfg.gossip.interval = g.interval.or(cfg.gossip.interval);
        cfg.gossip.fanout = g.fanout.or(cfg.gossip.fanout);
        cfg.gossip.suspicion_timeout = g.suspicion_timeout.or(cfg.gossip.suspicion_timeout);
        cfg.gossip.failure_timeout = g.failure_timeout.or(cfg.gossip.failure_timeout);
        cfg.gossip.failure_check_interval =
            g.failure_check_interval.or(cfg.gossip.failure_check_interval);
    }
    if let Some(l) = &file.logging {
        if let Some(level) = &l.level {
            cfg.log_level = Some(level.clone());
        }
    }
    if let Some(h) = &file.hot_cache {
        cfg.hot_cache_ttl_secs = h.ttl_secs.or(cfg.hot_cache_ttl_secs);
    }
}

fn apply_env(cfg: &mut AppConfig) {
    if let Ok(port) = std::env::var("BLAZECACHE_PORT") {
        if let Ok(p) = port.parse() {
            cfg.port = p;
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
    if let Ok(ttl) = std::env::var("HOT_CACHE_TTL_SECS") {
        if let Ok(val) = ttl.parse() {
            cfg.hot_cache_ttl_secs = Some(val);
        }
    }
}

fn apply_cli(cfg: &mut AppConfig, args: &[String]) {
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-p" | "--port" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.port = val;
                    }
                    i += 1;
                }
            }
            "-m" | "--memory" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.memory_mb = val;
                    }
                    i += 1;
                }
            }
            "-d" | "--daemon" => {
                cfg.daemon = true;
            }
            "--gossip" => {
                cfg.gossip.enabled = true;
            }
            "--gossip-port" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.gossip.port = Some(val);
                    }
                    i += 1;
                }
            }
            "--gossip-interval" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.gossip.interval = Some(val);
                    }
                    i += 1;
                }
            }
            "--gossip-fanout" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.gossip.fanout = Some(val);
                    }
                    i += 1;
                }
            }
            "--gossip-suspicion-timeout" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.gossip.suspicion_timeout = Some(val);
                    }
                    i += 1;
                }
            }
            "--gossip-failure-timeout" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.gossip.failure_timeout = Some(val);
                    }
                    i += 1;
                }
            }
            "--gossip-failure-check-interval" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.gossip.failure_check_interval = Some(val);
                    }
                    i += 1;
                }
            }
            "--data-dir" => {
                if i + 1 < args.len() {
                    cfg.persistence.data_dir = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "--snapshot-interval" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.persistence.snapshot_interval = Some(val);
                    }
                    i += 1;
                }
            }
            "--wal-disabled" => {
                cfg.persistence.wal_disabled = Some(true);
            }
            "--wal-max-mb" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.persistence.wal_max_mb = Some(val);
                    }
                    i += 1;
                }
            }
            "--no-compress-snapshots" => {
                cfg.persistence.no_compress_snapshots = Some(true);
            }
            "--hot-cache-ttl" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse() {
                        cfg.hot_cache_ttl_secs = Some(val);
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
}

