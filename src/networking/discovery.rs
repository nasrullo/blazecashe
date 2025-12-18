use crate::utils::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::error;
use crate::utils::time::current_timestamp;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub protocol: String,
    pub status: PeerStatus,
    pub last_seen: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerStatus {
    Active,
    Inactive,
    Unreachable,
}

pub struct PeerRegistry {
    peers: Arc<RwLock<HashSet<PeerInfo>>>,
    health_check_interval: std::time::Duration,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashSet::new())),
            health_check_interval: std::time::Duration::from_secs(30),
        }
    }

    pub async fn add_peer(&self, peer: PeerInfo) -> Result<()> {
        let mut peers = self.peers.write().await;
        peers.insert(peer);
        Ok(())
    }

    pub async fn remove_peer(&self, peer_id: &str) -> Result<bool> {
        let mut peers = self.peers.write().await;
        let initial_len = peers.len();
        peers.retain(|p| p.id != peer_id);
        Ok(peers.len() < initial_len)
    }

    pub async fn list_peers(&self) -> Vec<PeerInfo> {
        let peers = self.peers.read().await;
        peers.iter().cloned().collect()
    }

    pub async fn get_active_peers(&self) -> Vec<PeerInfo> {
        let peers = self.peers.read().await;
        peers
            .iter()
            .filter(|p| matches!(p.status, PeerStatus::Active))
            .cloned()
            .collect()
    }

    pub async fn update_peer_status(&self, peer_id: &str, status: PeerStatus) -> Result<()> {
        let mut peers = self.peers.write().await;
        let peers_vec: Vec<PeerInfo> = peers.drain().collect();

        for mut peer in peers_vec {
            if peer.id == peer_id {
                peer.status = status.clone();
                peer.last_seen = current_timestamp();
            }
            peers.insert(peer);
        }
        Ok(())
    }

    pub async fn health_check(&self) -> Result<()> {
        let peers = self.list_peers().await;

        for peer in peers {
            let health_url = format!("http://{}:{}/health", peer.address, peer.port);

            match reqwest::get(&health_url).await {
                Ok(response) if response.status().is_success() => {
                    self.update_peer_status(&peer.id, PeerStatus::Active)
                        .await?;
                }
                _ => {
                    self.update_peer_status(&peer.id, PeerStatus::Unreachable)
                        .await?;
                }
            }
        }

        Ok(())
    }

    pub async fn start_health_monitor(&self) {
        let registry = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(registry.health_check_interval);

            loop {
                interval.tick().await;
                if let Err(e) = registry.health_check().await {
                    error!(error = %e, "Health check failed");
                }
            }
        });
    }
}

impl Clone for PeerRegistry {
    fn clone(&self) -> Self {
        Self {
            peers: Arc::clone(&self.peers),
            health_check_interval: self.health_check_interval,
        }
    }
}

impl PartialEq for PeerInfo {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for PeerInfo {}

impl std::hash::Hash for PeerInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_peer_registry() {
        let registry = PeerRegistry::new();

        let peer = PeerInfo {
            id: "peer1".to_string(),
            address: "127.0.0.1".to_string(),
            port: 6784,
            protocol: "http".to_string(),
            status: PeerStatus::Active,
            last_seen: 0,
        };

        registry.add_peer(peer.clone()).await.unwrap();

        let peers = registry.list_peers().await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, "peer1");

        let removed = registry.remove_peer("peer1").await.unwrap();
        assert!(removed);

        let peers = registry.list_peers().await;
        assert_eq!(peers.len(), 0);
    }
}
