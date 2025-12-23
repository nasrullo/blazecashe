//! # Persistence Module
//!
//! Provides optional durability for cache data similar to memcached persistence.
//! Supports both periodic snapshots and write-ahead logging for different durability guarantees.
//!
//! ## Features
//!
//! - **Snapshots**: Periodic full cache dumps to disk
//! - **Write-Ahead Log (WAL)**: Log all writes for crash recovery
//! - **Configurable**: Enable/disable persistence per cache
//! - **Efficient**: Binary serialization with compression
//! - **Recovery**: Automatic recovery on startup
//!
//! ## Example
//!
//! ```rust,no_run
//! use blazecache::PersistenceConfig;
//!
//! let config = PersistenceConfig {
//!     enabled: true,
//!     snapshot_interval_secs: 300, // 5 minutes
//!     wal_enabled: true,
//!     data_dir: "/tmp/blazecache".to_string(),
//!     max_wal_size_bytes: 100 * 1024 * 1024, // 100MB
//!     compress_snapshots: true,
//! };
//!
//! // Persistence config can be used with cache implementations
//! ```

use crate::utils::{BlazeCacheError, Result};
use crate::{cache::Value, FnvHashMap};
use crate::utils::time::current_timestamp;
use ciborium::de::from_reader as cbor_deserialize;
use ciborium::ser::into_writer as cbor_serialize;
use lru::LruCache;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::PathBuf;
use std::sync::Weak;
use tokio::time::{interval, Duration};
use tracing::error;

/// Type alias for weak reference to cache data for snapshot tasks.
type CacheWeakRef = Weak<RwLock<LruCache<String, Value>>>;

/// Configuration for cache persistence
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    /// Data directory for persistence files (default: "./data")
    pub data_dir: String,
    /// Maximum WAL file size before rotation (default: 100MB)
    pub max_wal_size_bytes: usize,
    /// Snapshot interval in seconds (default: 300 = 5 minutes)
    pub snapshot_interval_secs: u64,
    /// Enable persistence (default: false)
    pub enabled: bool,
    /// Enable write-ahead logging (default: true)
    pub wal_enabled: bool,
    /// Compress snapshots (default: true)
    pub compress_snapshots: bool,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            snapshot_interval_secs: 300, // 5 minutes
            wal_enabled: true,
            data_dir: "./data".to_string(),
            max_wal_size_bytes: 100 * 1024 * 1024, // 100MB
            compress_snapshots: true,
        }
    }
}

/// Snapshot of cache data for persistence
#[derive(Serialize, Deserialize)]
struct CacheSnapshot {
    timestamp: u64,
    version: u32,
    entries: FnvHashMap<String, Value>,
}

/// Write-ahead log entry
#[derive(Serialize, Deserialize, Debug)]
pub enum WalEntry {
    Put { key: String, value: Value },
    Delete { key: String },
    Clear,
}

/// Persistence manager for cache durability
pub struct PersistenceManager {
    config: PersistenceConfig,
    data_dir: PathBuf,
    wal_file: Option<BufWriter<File>>,
    wal_size: usize,
}

impl PersistenceManager {
    /// Creates a new persistence manager
    pub fn new(config: PersistenceConfig) -> Result<Self> {
        if !config.enabled {
            return Ok(Self {
                config,
                data_dir: PathBuf::new(),
                wal_file: None,
                wal_size: 0,
            });
        }

        let data_dir = PathBuf::from(&config.data_dir);
        fs::create_dir_all(&data_dir).map_err(|e| {
            BlazeCacheError::NetworkError(format!("Failed to create data dir: {}", e))
        })?;

        let mut manager = Self {
            config: config.clone(),
            data_dir,
            wal_file: None,
            wal_size: 0,
        };

        if config.wal_enabled {
            manager.open_wal()?;
        }

        Ok(manager)
    }

    /// Opens or creates WAL file
    fn open_wal(&mut self) -> Result<()> {
        let wal_path = self.data_dir.join("cache.wal");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .map_err(|e| BlazeCacheError::NetworkError(format!("Failed to open WAL: {}", e)))?;

        self.wal_size = file
            .metadata()
            .map_err(|e| BlazeCacheError::NetworkError(format!("Failed to get WAL size: {}", e)))?
            .len() as usize;

        self.wal_file = Some(BufWriter::new(file));
        Ok(())
    }

    /// Logs a WAL entry
    pub fn log_entry(&mut self, entry: WalEntry) -> Result<()> {
        if !self.config.enabled || !self.config.wal_enabled {
            return Ok(());
        }

        if let Some(ref mut wal_file) = self.wal_file {
            let mut serialized = Vec::new();
            cbor_serialize(&entry, &mut serialized).map_err(|e| {
                BlazeCacheError::SerializationError(format!("WAL serialize failed: {}", e))
            })?;

            // Write length prefix + data
            let len = serialized.len() as u32;
            wal_file
                .write_all(&len.to_le_bytes())
                .map_err(|e| BlazeCacheError::NetworkError(format!("WAL write failed: {}", e)))?;
            wal_file
                .write_all(&serialized)
                .map_err(|e| BlazeCacheError::NetworkError(format!("WAL write failed: {}", e)))?;
            wal_file
                .flush()
                .map_err(|e| BlazeCacheError::NetworkError(format!("WAL flush failed: {}", e)))?;

            self.wal_size += 4 + serialized.len();

            // Rotate WAL if too large
            if self.wal_size > self.config.max_wal_size_bytes {
                self.rotate_wal()?;
            }
        }

        Ok(())
    }

    /// Rotates WAL file
    fn rotate_wal(&mut self) -> Result<()> {
        if let Some(ref mut wal_file) = self.wal_file {
            wal_file
                .flush()
                .map_err(|e| BlazeCacheError::NetworkError(format!("WAL flush failed: {}", e)))?;
        }

        // Move current WAL to backup
        let wal_path = self.data_dir.join("cache.wal");
        let backup_path = self.data_dir.join(format!(
            "cache.wal.{}",
            current_timestamp()
        ));

        if wal_path.exists() {
            fs::rename(&wal_path, &backup_path).map_err(|e| {
                BlazeCacheError::NetworkError(format!("WAL rotation failed: {}", e))
            })?;
        }

        // Open new WAL
        self.wal_file = None;
        self.wal_size = 0;
        self.open_wal()?;

        Ok(())
    }

    /// Creates a snapshot of cache data
    pub fn create_snapshot(&self, entries: FnvHashMap<String, Value>) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let snapshot = CacheSnapshot {
            timestamp: current_timestamp(),
            version: 1,
            entries,
        };

        let snapshot_path = self.data_dir.join("cache.snapshot");
        let temp_path = self.data_dir.join("cache.snapshot.tmp");

        let file = File::create(&temp_path).map_err(|e| {
            BlazeCacheError::NetworkError(format!("Failed to create snapshot: {}", e))
        })?;
        let mut writer = BufWriter::new(file);

        if self.config.compress_snapshots {
            let compressed = crate::cache::compression::compress(
                &{
                    let mut data = Vec::new();
                    cbor_serialize(&snapshot, &mut data).map_err(|e| {
                        BlazeCacheError::SerializationError(format!("Snapshot serialize failed: {}", e))
                    })?;
                    data
                },
            )
            .map_err(|e| {
                BlazeCacheError::CompressionError(format!("Snapshot compression failed: {}", e))
            })?;
            writer.write_all(&compressed).map_err(|e| {
                BlazeCacheError::NetworkError(format!("Snapshot write failed: {}", e))
            })?;
        } else {
            cbor_serialize(&snapshot, &mut writer).map_err(|e| {
                BlazeCacheError::SerializationError(format!("Snapshot serialize failed: {}", e))
            })?;
        }

        writer
            .flush()
            .map_err(|e| BlazeCacheError::NetworkError(format!("Snapshot flush failed: {}", e)))?;

        // Atomic rename
        fs::rename(&temp_path, &snapshot_path)
            .map_err(|e| BlazeCacheError::NetworkError(format!("Snapshot rename failed: {}", e)))?;

        Ok(())
    }

    /// Recovers cache data from persistence
    pub fn recover(&self) -> Result<Option<FnvHashMap<String, Value>>> {
        if !self.config.enabled {
            return Ok(None);
        }

        let mut entries = self.load_snapshot()?;

        // Apply WAL entries
        if self.config.wal_enabled {
            self.apply_wal(&mut entries)?;
        }

        Ok(Some(entries))
    }

    /// Loads snapshot from disk
    pub fn load_snapshot(&self) -> Result<FnvHashMap<String, Value>> {
        let snapshot_path = self.data_dir.join("cache.snapshot");

        if !snapshot_path.exists() {
            return Ok(FnvHashMap::default());
        }

        let file = File::open(&snapshot_path).map_err(|e| {
            BlazeCacheError::NetworkError(format!("Failed to open snapshot: {}", e))
        })?;
        let mut reader = BufReader::new(file);

        let snapshot: CacheSnapshot = if self.config.compress_snapshots {
            let mut compressed_data = Vec::new();
            Read::read_to_end(&mut reader, &mut compressed_data).map_err(|e| {
                BlazeCacheError::NetworkError(format!("Snapshot read failed: {}", e))
            })?;

            let decompressed =
                crate::cache::compression::decompress(&compressed_data).map_err(|e| {
                    BlazeCacheError::CompressionError(format!(
                        "Snapshot decompression failed: {}",
                        e
                    ))
                })?;

            cbor_deserialize(&decompressed[..]).map_err(|e| {
                BlazeCacheError::SerializationError(format!("Snapshot deserialize failed: {}", e))
            })?
        } else {
            cbor_deserialize(&mut reader).map_err(|e| {
                BlazeCacheError::SerializationError(format!("Snapshot deserialize failed: {}", e))
            })?
        };

        Ok(snapshot.entries)
    }

    /// Applies WAL entries to recovered data
    fn apply_wal(&self, entries: &mut FnvHashMap<String, Value>) -> Result<()> {
        let wal_path = self.data_dir.join("cache.wal");

        if !wal_path.exists() {
            return Ok(());
        }

        let file = File::open(&wal_path)
            .map_err(|e| BlazeCacheError::NetworkError(format!("Failed to open WAL: {}", e)))?;
        let mut reader = BufReader::new(file);

        loop {
            // Read length prefix
            let mut len_bytes = [0u8; 4];
            match Read::read_exact(&mut reader, &mut len_bytes) {
                Ok(_) => {}
                Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    return Err(BlazeCacheError::NetworkError(format!(
                        "WAL read failed: {}",
                        e
                    )))
                }
            }

            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut entry_bytes = vec![0u8; len];
            Read::read_exact(&mut reader, &mut entry_bytes)
                .map_err(|e| BlazeCacheError::NetworkError(format!("WAL read failed: {}", e)))?;

            let entry: WalEntry = cbor_deserialize(&entry_bytes[..]).map_err(|e| {
                BlazeCacheError::SerializationError(format!("WAL deserialize failed: {}", e))
            })?;

            // Apply entry
            match entry {
                WalEntry::Put { key, value } => {
                    entries.insert(key, value);
                }
                WalEntry::Delete { key } => {
                    entries.remove(&key);
                }
                WalEntry::Clear => {
                    entries.clear();
                }
            }
        }

        Ok(())
    }

    /// Clears all persistence data (WAL and snapshots)
    ///
    /// This method removes all WAL files and snapshot files from the data directory.
    /// It also closes and reopens the WAL file to start fresh.
    pub fn clear_all(&mut self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // Close current WAL file
        if let Some(ref mut wal_file) = self.wal_file {
            wal_file
                .flush()
                .map_err(|e| BlazeCacheError::NetworkError(format!("WAL flush failed: {}", e)))?;
        }
        self.wal_file = None;
        self.wal_size = 0;

        // Delete WAL file
        let wal_path = self.data_dir.join("cache.wal");
        if wal_path.exists() {
            fs::remove_file(&wal_path).map_err(|e| {
                BlazeCacheError::NetworkError(format!("Failed to delete WAL: {}", e))
            })?;
        }

        // Delete all WAL backup files
        let entries = fs::read_dir(&self.data_dir).map_err(|e| {
            BlazeCacheError::NetworkError(format!("Failed to read data directory: {}", e))
        })?;
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if let Some(file_name) = path.file_name() {
                    if let Some(name_str) = file_name.to_str() {
                        if name_str.starts_with("cache.wal.") {
                            fs::remove_file(&path).map_err(|e| {
                                BlazeCacheError::NetworkError(format!(
                                    "Failed to delete WAL backup {}: {}",
                                    name_str, e
                                ))
                            })?;
                        }
                    }
                }
            }
        }

        // Delete snapshot file
        let snapshot_path = self.data_dir.join("cache.snapshot");
        if snapshot_path.exists() {
            fs::remove_file(&snapshot_path).map_err(|e| {
                BlazeCacheError::NetworkError(format!("Failed to delete snapshot: {}", e))
            })?;
        }

        // Delete temporary snapshot file
        let temp_snapshot_path = self.data_dir.join("cache.snapshot.tmp");
        if temp_snapshot_path.exists() {
            let _ = fs::remove_file(&temp_snapshot_path);
        }

        // Reopen WAL if enabled
        if self.config.wal_enabled {
            self.open_wal()?;
        }

        Ok(())
    }

    /// Starts periodic snapshot task
    pub fn start_snapshot_task(&self, cache: CacheWeakRef) {
        if !self.config.enabled || self.config.snapshot_interval_secs == 0 {
            return;
        }

        let config = self.config.clone();
        let data_dir = self.data_dir.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(config.snapshot_interval_secs));

            loop {
                interval.tick().await;

                if let Some(cache_ref) = cache.upgrade() {
                    let entries = {
                        let cache_guard = cache_ref.read();
                        let mut entries = FnvHashMap::default();

                        // Iterate over LruCache entries
                        for (key, value) in cache_guard.iter() {
                            entries.insert(key.clone(), value.clone());
                        }

                        entries
                    };

                    let manager = PersistenceManager {
                        config: config.clone(),
                        data_dir: data_dir.clone(),
                        wal_file: None,
                        wal_size: 0,
                    };

                    if let Err(e) = manager.create_snapshot(entries) {
                        error!(error = %e, "Snapshot creation failed");
                    }
                } else {
                    // Cache has been dropped, exit task
                    break;
                }
            }
        });
    }
}
