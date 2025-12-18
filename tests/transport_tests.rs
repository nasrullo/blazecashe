use blazecache::serializers::BinarySerializer;
use blazecache::transports::{ProtocolClient, ProtocolServer};
use blazecache::{Getter, Group, TcpClient, TcpServer};
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
