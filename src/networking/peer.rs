use crate::utils::Result;
use async_trait::async_trait;

#[async_trait]
pub trait Peer: Send + Sync {
    async fn get(&self, group: &str, key: &str) -> Result<Vec<u8>>;
    async fn delete(&self, group: &str, key: &str) -> Result<()>;
    async fn set(&self, _group: &str, key: &str, value:Vec<u8>, ttl: u32) -> Result<()>;
    async fn get_hot_items(&self, group: &str) -> Result<Vec<String>>;
    async fn clear(&self, group: &str) -> Result<()>;
    fn address(&self)-> String;
}

pub trait PeerPicker: Send + Sync {
    fn pick_peer(&self, key: &str) -> Option<&dyn Peer>;
    fn get_all_peers(&self) -> Vec<String>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Arc;

    struct MockPeer {
        addr: String,
    }

    impl MockPeer {
        fn new(addr: &str) -> Self {
            Self {
                addr: addr.to_string(),
            }
        }
    }

    #[async_trait]
    impl Peer for MockPeer {
        async fn get(&self, _group: &str, _key: &str) -> Result<Vec<u8>> {
            Ok(b"test".to_vec())
        }

        async fn delete(&self, _group: &str, _key: &str) -> Result<()> {
            Ok(())
        }

        async fn set(&self, _group: &str, _key: &str, _value: Vec<u8>, _ttl: u32) -> Result<()> {
            Ok(())
        }

        async fn get_hot_items(&self, _group: &str) -> Result<Vec<String>> {
            Ok(vec!["item1".to_string(), "item2".to_string()])
        }

        async fn clear(&self, _group: &str) -> Result<()> {
            Ok(())
        }

        fn address(&self) -> String {
            self.addr.clone()
        }
    }

    struct MockPeerPicker {
        peers: Vec<Arc<MockPeer>>,
    }

    impl MockPeerPicker {
        fn new() -> Self {
            Self {
                peers: vec![
                    Arc::new(MockPeer::new("peer1")),
                    Arc::new(MockPeer::new("peer2")),
                ],
            }
        }
    }

    impl PeerPicker for MockPeerPicker {
        fn pick_peer(&self, _key: &str) -> Option<&dyn Peer> {
            self.peers.first().map(|p| p.as_ref() as &dyn Peer)
        }

        fn get_all_peers(&self) -> Vec<String> {
            self.peers.iter().map(|p| p.address()).collect()
        }
    }

    #[tokio::test]
    async fn test_mock_peer_get() {
        let peer = MockPeer::new("test");
        let result = peer.get("group", "key").await.unwrap();
        assert_eq!(result, b"test");
    }

    #[tokio::test]
    async fn test_mock_peer_delete() {
        let peer = MockPeer::new("test");
        peer.delete("group", "key").await.unwrap();
    }

    #[tokio::test]
    async fn test_mock_peer_set() {
        let peer = MockPeer::new("test");
        peer.set("group", "key", vec![1, 2, 3], 100).await.unwrap();
    }

    #[tokio::test]
    async fn test_mock_peer_get_hot_items() {
        let peer = MockPeer::new("test");
        let items = peer.get_hot_items("group").await.unwrap();
        assert_eq!(items, vec!["item1".to_string(), "item2".to_string()]);
    }

    #[test]
    fn test_mock_peer_address() {
        let peer = MockPeer::new("test");
        assert_eq!(peer.address(), "test");
    }

    #[test]
    fn test_mock_peer_picker_pick_peer() {
        let picker = MockPeerPicker::new();
        let peer = picker.pick_peer("key");
        assert!(peer.is_some());
        assert_eq!(peer.unwrap().address(), "peer1");
    }

    #[test]
    fn test_mock_peer_picker_get_all_peers() {
        let picker = MockPeerPicker::new();
        let peers = picker.get_all_peers();
        assert_eq!(peers.len(), 2);
        assert!(peers.contains(&"peer1".to_string()));
        assert!(peers.contains(&"peer2".to_string()));
    }
}
