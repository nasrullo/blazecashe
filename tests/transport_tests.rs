use blazecache::serializers::BinarySerializer;
use blazecache::transports::{ProtocolClient, ProtocolServer};
use blazecache::{Getter, Group, TcpClient, TcpServer};
use blazecache::networking::ConsistentHash;
use blazecache::networking::remote_peer::RemotePeer;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_tcp_binary_transport() {
    let getter: Getter = Arc::new(|key: &str| Ok(format!("test-{}", key).into_bytes()));

    let group = Arc::new(Group::new("test-cache".to_string(), 1024 * 1024, getter, String::new()));
    let _registry = Arc::new(blazecache::networking::PeerRegistry::new());
    let server = TcpServer::<BinarySerializer>::new(group);

    // Start server in background
    tokio::spawn(async move {
        let _ = server.start(19999).await;
    });

    sleep(Duration::from_millis(100)).await;

    // Test client
    let mut client = TcpClient::<BinarySerializer>::connect("127.0.0.1:19999")
        .await
        .unwrap();

    // Test ping
    client.ping().await.unwrap();

    // Test put/get
    client.put("test_key", b"test_value", 0).await.unwrap();
    let value = client.get("test_key").await.unwrap();
    assert_eq!(value, b"test_value");
}

#[tokio::test]
async fn test_tcp_delete() {
    let getter: Getter = Arc::new(|_key: &str| Err(blazecache::BlazeCacheError::KeyNotFound));

    let group = Arc::new(Group::new("test-cache".to_string(), 1024 * 1024, getter, String::new()));
    let server = TcpServer::<BinarySerializer>::new(group);

    tokio::spawn(async move {
        let _ = server.start(19998).await;
    });

    sleep(Duration::from_millis(100)).await;

    let mut client = TcpClient::<BinarySerializer>::connect("127.0.0.1:19998")
        .await
        .unwrap();

    // Put a value
    client.put("delete_key", b"delete_value", 0).await.unwrap();

    // Delete it
    let deleted = client.delete("delete_key").await.unwrap();
    assert!(deleted);

    // Try to delete non-existent key
    let deleted = client.delete("nonexistent").await.unwrap();
    assert!(!deleted);
}

#[tokio::test]
async fn test_tcp_stats() {
    let getter: Getter = Arc::new(|_key: &str| Err(blazecache::BlazeCacheError::KeyNotFound));

    let group = Arc::new(Group::new("test-cache".to_string(), 1024 * 1024, getter, String::new()));
    let server = TcpServer::<BinarySerializer>::new(group);

    tokio::spawn(async move {
        let _ = server.start(19997).await;
    });

    sleep(Duration::from_millis(100)).await;

    let mut client = TcpClient::<BinarySerializer>::connect("127.0.0.1:19997")
        .await
        .unwrap();

    // Get stats
    let stats = client.stats().await.unwrap();
    assert!(stats.contains("hits"));
    assert!(stats.contains("misses"));
}

#[tokio::test]
async fn test_tcp_peer() {
    let getter: Getter = Arc::new(|_key: &str| Err(blazecache::BlazeCacheError::KeyNotFound));

    let group = Arc::new(Group::new("test-cache".to_string(), 1024 * 1024, getter, "127.0.0.1:19996".to_string()));
    
    // Set up peers
    let mut ring = ConsistentHash::new(150);
    ring.add_peer(Arc::new(RemotePeer::new("127.0.0.1:19996".to_string())), "127.0.0.1:19996");
    ring.add_peer(Arc::new(RemotePeer::new("127.0.0.1:19995".to_string())), "127.0.0.1:19995");
    ring.finalize();
    group.set_peers(Box::new(ring)).await;

    let server = TcpServer::<BinarySerializer>::new(group);

    tokio::spawn(async move {
        let _ = server.start(19996).await;
    });

    sleep(Duration::from_millis(100)).await;

    let mut client = TcpClient::<BinarySerializer>::connect("127.0.0.1:19996")
        .await
        .unwrap();

    // Get peers
    let peers = client.peer().await.unwrap();
    assert!(peers.contains("127.0.0.1:19996"));
    assert!(peers.contains("127.0.0.1:19995"));
}

#[tokio::test]
async fn test_tcp_peer_empty() {
    let getter: Getter = Arc::new(|_key: &str| Err(blazecache::BlazeCacheError::KeyNotFound));

    let group = Arc::new(Group::new("test-cache".to_string(), 1024 * 1024, getter, String::new()));
    let server = TcpServer::<BinarySerializer>::new(group);

    tokio::spawn(async move {
        let _ = server.start(19995).await;
    });

    sleep(Duration::from_millis(100)).await;

    let mut client = TcpClient::<BinarySerializer>::connect("127.0.0.1:19995")
        .await
        .unwrap();

    // Get peers when no peers are configured
    let peers = client.peer().await.unwrap();
    assert_eq!(peers, "");
}

#[tokio::test]
async fn test_tcp_clear() {
    let getter: Getter = Arc::new(|_key: &str| Err(blazecache::BlazeCacheError::KeyNotFound));

    let group = Arc::new(Group::new("test-cache".to_string(), 1024 * 1024, getter, String::new()));
    let server = TcpServer::<BinarySerializer>::new(group);

    tokio::spawn(async move {
        let _ = server.start(19994).await;
    });

    sleep(Duration::from_millis(100)).await;

    let mut client = TcpClient::<BinarySerializer>::connect("127.0.0.1:19994")
        .await
        .unwrap();

    // Add multiple items
    client.put("key1", b"value1", 0).await.unwrap();
    client.put("key2", b"value2", 0).await.unwrap();
    client.put("key3", b"value3", 0).await.unwrap();

    // Verify items exist
    assert_eq!(client.get("key1").await.unwrap(), b"value1");
    assert_eq!(client.get("key2").await.unwrap(), b"value2");
    assert_eq!(client.get("key3").await.unwrap(), b"value3");

    // Get stats before clear
    let stats_before = client.stats().await.unwrap();
    assert!(stats_before.contains("\"entry_count\":3") || stats_before.contains("\"entry_count\": 3"));

    // Clear cache
    client.clear().await.unwrap();

    // Verify all items are gone
    assert_eq!(client.get("key1").await.unwrap(), b"");
    assert_eq!(client.get("key2").await.unwrap(), b"");
    assert_eq!(client.get("key3").await.unwrap(), b"");

    // Verify stats are reset
    let stats_after = client.stats().await.unwrap();
    assert!(stats_after.contains("\"entry_count\":0") || stats_after.contains("\"entry_count\": 0"));
    assert!(stats_after.contains("\"memory_usage\":0") || stats_after.contains("\"memory_usage\": 0"));
}

#[tokio::test]
async fn test_tcp_clear_empty() {
    let getter: Getter = Arc::new(|_key: &str| Err(blazecache::BlazeCacheError::KeyNotFound));

    let group = Arc::new(Group::new("test-cache".to_string(), 1024 * 1024, getter, String::new()));
    let server = TcpServer::<BinarySerializer>::new(group);

    tokio::spawn(async move {
        let _ = server.start(19993).await;
    });

    sleep(Duration::from_millis(100)).await;

    let mut client = TcpClient::<BinarySerializer>::connect("127.0.0.1:19993")
        .await
        .unwrap();

    // Clear empty cache should not error
    client.clear().await.unwrap();

    // Verify it's still empty
    let stats = client.stats().await.unwrap();
    assert!(stats.contains("\"entry_count\":0") || stats.contains("\"entry_count\": 0"));
}
