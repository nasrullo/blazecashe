use blazecache::utils::persistence::{PersistenceConfig, PersistenceManager};
use tempfile::TempDir;

#[test]
fn test_persistence_clear_all() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    let mut config = PersistenceConfig::default();
    config.enabled = true;
    config.data_dir = data_dir.to_str().unwrap().to_string();
    config.wal_enabled = true;

    let mut manager = PersistenceManager::new(config).unwrap();

    // Create some WAL entries
    use blazecache::utils::persistence::WalEntry;
    use blazecache::cache::Value;
    manager
        .log_entry(WalEntry::Put {
            key: "key1".to_string(),
            value: Value::new(b"value1".to_vec(), 0),
        })
        .unwrap();
    manager
        .log_entry(WalEntry::Put {
            key: "key2".to_string(),
            value: Value::new(b"value2".to_vec(), 0),
        })
        .unwrap();

    // Verify WAL file exists
    let wal_path = data_dir.join("cache.wal");
    assert!(wal_path.exists());

    // Get initial WAL size
    let initial_size = wal_path.metadata().unwrap().len();

    // Clear all persistence data
    manager.clear_all().unwrap();

    // Verify WAL file is recreated but empty (since we clear and reopen)
    assert!(wal_path.exists()); // Should be recreated after clear
    let new_size = wal_path.metadata().unwrap().len();
    assert_eq!(new_size, 0); // New WAL should be empty
}

#[test]
fn test_persistence_clear_all_with_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    let mut config = PersistenceConfig::default();
    config.enabled = true;
    config.data_dir = data_dir.to_str().unwrap().to_string();
    config.wal_enabled = true;

    let mut manager = PersistenceManager::new(config).unwrap();

    // Create a snapshot
    use blazecache::{FnvHashMap, cache::Value};
    let mut entries = FnvHashMap::default();
    entries.insert("key1".to_string(), Value::new(b"value1".to_vec(), 0));
    entries.insert("key2".to_string(), Value::new(b"value2".to_vec(), 0));
    manager.create_snapshot(entries).unwrap();

    // Verify snapshot exists
    let snapshot_path = data_dir.join("cache.snapshot");
    assert!(snapshot_path.exists());

    // Clear all persistence data
    manager.clear_all().unwrap();

    // Verify snapshot is deleted
    assert!(!snapshot_path.exists());
}

