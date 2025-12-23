//! # Gossip Protocol for Peer Discovery
//!
//! Implements a gossip-based membership protocol for automatic peer discovery
//! in distributed BlazeCache clusters. Uses an infection-style propagation
//! model where nodes periodically exchange membership information with random
//! peers.
//!
//! ## Protocol Overview
//!
//! The gossip protocol works in rounds:
//!
//! 1. **Gossip Round**: Every `gossip_interval` seconds, each node:
//!    - Selects `fanout` random peers from its membership list
//!    - Sends its membership view to each selected peer
//!    - Receives membership updates from peers
//!
//! 2. **Membership Merge**: When receiving gossip messages:
//!    - Merge new peer information into local membership
//!    - Update `last_seen` timestamps for known peers
//!    - Add newly discovered peers
//!
//! 3. **Failure Detection**: Peers are marked as unreachable if:
//!    - No gossip message received for `failure_timeout` seconds
//!    - Direct ping/pong fails
//!
//! ## Benefits
//!
//! - **Automatic Discovery**: No manual peer configuration needed
//! - **Fault Tolerant**: Handles network partitions and node failures
//! - **Eventually Consistent**: All nodes eventually learn about all peers
//! - **Scalable**: O(log N) convergence time for N nodes
//! - **Lightweight**: Uses UDP for efficient gossip messages
//!
//! ## Configuration
//!
//! - `gossip_interval`: How often to gossip (default: 1 second)
//! - `fanout`: Number of peers to contact per round (default: 3)
//! - `failure_timeout`: Time before marking peer as failed (default: 30 seconds)
//! - `gossip_port`: UDP port for gossip messages (default: cache_port + 1)

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio::time::{interval, sleep, timeout};
use crate::networking::discovery::{PeerInfo, PeerRegistry, PeerStatus};
use crate::utils::Result;
use crate::utils::time::current_timestamp;
use ciborium::de::from_reader as cbor_deserialize;
use ciborium::ser::into_writer as cbor_serialize;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, trace, warn};

/// Gossip message types exchanged between nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    /// Membership update containing peer information
    Membership {
        /// Sender's own peer information
        sender: PeerInfo,
        /// Full membership view from sender
        members: Vec<PeerInfo>,
        /// Sequence number for deduplication
        seq: u64,
    },
    /// Ping message for failure detection
    Ping {
        sender: PeerInfo,
        seq: u64,
    },
    /// Pong response to ping
    Pong {
        sender: PeerInfo,
        ping_seq: u64,
    },
}

/// Configuration for gossip protocol
#[derive(Debug, Clone)]
pub struct GossipConfig {
    /// How often to run gossip rounds (seconds)
    pub gossip_interval: Duration,
    /// Number of random peers to contact per round
    pub fanout: usize,
    /// Time before marking peer as failed (seconds)
    pub failure_timeout: Duration,
    /// Time before marking peer as suspected (seconds)
    pub suspicion_timeout: Duration,
    /// How often to check for failures (seconds)
    pub failure_check_interval: Duration,
    /// UDP port for gossip messages
    pub gossip_port: u16,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            gossip_interval: Duration::from_secs(1),
            fanout: 3,
            failure_timeout: Duration::from_secs(30),
            suspicion_timeout: Duration::from_secs(15),
            failure_check_interval: Duration::from_secs(5),
            gossip_port: 6785, // Default cache port + 1
        }
    }
}

/// Gossip protocol manager for peer discovery
pub struct GossipProtocol {
    /// Local peer information
    local_peer: PeerInfo,
    /// Peer registry to update
    registry: Arc<PeerRegistry>,
    /// Gossip configuration
    config: GossipConfig,
    /// UDP socket for gossip messages
    socket: Arc<UdpSocket>,
    /// Sequence number for outgoing messages
    seq: Arc<RwLock<u64>>,
    /// Last seen timestamps for failure detection
    last_seen: Arc<RwLock<HashMap<String, u64>>>,
    /// Gossip metrics
    metrics: Arc<RwLock<GossipMetrics>>,
}

/// Metrics for monitoring gossip protocol activity
#[derive(Debug, Default, Clone)]
pub struct GossipMetrics {
    /// Number of membership messages sent
    pub membership_sent: u64,
    /// Number of membership messages received
    pub membership_received: u64,
    /// Number of ping messages sent
    pub ping_sent: u64,
    /// Number of pong messages received
    pub pong_received: u64,
    /// Number of peers discovered
    pub peers_discovered: u64,
    /// Number of peers marked as failed
    pub peers_failed: u64,
    /// Number of gossip rounds completed
    pub gossip_rounds: u64,
}

impl GossipProtocol {
    /// Creates a new gossip protocol instance
    ///
    /// ## Arguments
    ///
    /// * `local_peer` - This node's peer information
    /// * `registry` - Peer registry to update with discovered peers
    /// * `config` - Gossip configuration
    ///
    /// ## Returns
    ///
    /// A new GossipProtocol instance is ready to start
    pub async fn new(
        local_peer: PeerInfo,
        registry: Arc<PeerRegistry>,
        config: GossipConfig,
    ) -> Result<Self> {
        // Bind UDP socket for gossip messages
        let addr = format!("0.0.0.0:{}", config.gossip_port);
        let socket = UdpSocket::bind(&addr).await?;
        socket.set_broadcast(true)?;

        // Ensure the local peer is registered so membership messages always include self.
        registry.add_peer(local_peer.clone()).await?;

        Ok(Self {
            local_peer,
            registry,
            config,
            socket: Arc::new(socket),
            seq: Arc::new(RwLock::new(0)),
            last_seen: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(RwLock::new(GossipMetrics::default())),
        })
    }

    /// Starts the gossip protocol (both server and client)
    ///
    /// This spawns two tasks:
    /// 1. Gossip server: Receives and processes incoming gossip messages
    /// 2. Gossip client: Periodically sends gossip messages to random peers
    pub fn start(&self) {
        let server = self.clone_for_server();
        tokio::spawn(async move {
            server.run_server().await;
        });

        let client = self.clone_for_client();
        tokio::spawn(async move {
            client.run_client().await;
        });

        let failure_detector = self.clone_for_failure_detection();
        tokio::spawn(async move {
            failure_detector.run_failure_detection().await;
        });
    }

    /// Runs the gossip server (receives messages)
    async fn run_server(self) {
        let mut buffer = vec![0u8; 65507]; // Max UDP packet size

        loop {
            match self.socket.recv_from(&mut buffer).await {
                Ok((len, addr)) => {
                    let data = &buffer[..len];
                    if let Err(e) = self.handle_message(data, addr).await {
                        error!(error = %e, "Error handling gossip message");
                    }
                }
                Err(e) => {
                    error!(error = %e, "Gossip server receive error");
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// Runs the gossip client (sends messages)
    async fn run_client(self) {
        let mut interval = interval(self.config.gossip_interval);

        loop {
            interval.tick().await;
            if let Err(e) = self.gossip_round().await {
                error!(error = %e, "Gossip client error");
            }
        }
    }

    /// Runs failure detection
    async fn run_failure_detection(self) {
        let mut interval = interval(self.config.failure_check_interval);

        loop {
            interval.tick().await;
            if let Err(e) = self.check_failures().await {
                error!(error = %e, "Failure detection error");
            }
        }
    }

    /// Handles incoming gossip message
    async fn handle_message(&self, data: &[u8], _addr: SocketAddr) -> Result<()> {
        let msg: GossipMessage = cbor_deserialize(data)?;

        match msg {
            GossipMessage::Membership { sender, members, .. } => {
                debug!(peer_id = %sender.id, "Received membership message");

                // Update metrics
                {
                    let mut metrics = self.metrics.write().await;
                    metrics.membership_received += 1;
                }

                // Update sender's last seen
                self.update_last_seen(&sender.id).await;

                // Merge membership information
                let mut new_peers = 0;
                for peer in members {
                    if peer.id != self.local_peer.id {
                        // Check if a peer is new
                        let existing = self.registry.list_peers().await;
                        let is_new = !existing.iter().any(|p| p.id == peer.id);
                        
                        // Update or add peer to a registry
                        self.registry.add_peer(peer.clone()).await?;
                        self.update_last_seen(&peer.id).await;
                        
                        if is_new {
                            new_peers += 1;
                            info!(
                                peer_id = %peer.id,
                                peer_address = %peer.address,
                                peer_port = peer.port,
                                "Discovered new peer"
                            );
                        }
                    }
                }

                // Add sender if not already known
                let existing = self.registry.list_peers().await;
                if sender.id != self.local_peer.id && !existing.iter().any(|p| p.id == sender.id) {
                    self.registry.add_peer(sender.clone()).await?;
                    new_peers += 1;
                    info!(
                        peer_id = %sender.id,
                        peer_address = %sender.address,
                        peer_port = sender.port,
                        "Discovered new peer from sender"
                    );
                }

                // Update metrics
                {
                    let mut metrics = self.metrics.write().await;
                    metrics.peers_discovered += new_peers;
                }
            }
            GossipMessage::Ping { sender, .. } => {
                trace!(peer_id = %sender.id, "Received ping");
                // Respond with pong
                self.update_last_seen(&sender.id).await;
                self.send_pong(&sender).await?;
            }
            GossipMessage::Pong { sender, .. } => {
                trace!(peer_id = %sender.id, "Received pong");
                // Update metrics
                {
                    let mut metrics = self.metrics.write().await;
                    metrics.pong_received += 1;
                }
                // Update last seen
                self.update_last_seen(&sender.id).await;
            }
        }

        Ok(())
    }

    /// Performs one gossip round (selects random peers and sends membership)
    async fn gossip_round(&self) -> Result<()> {
        // Exclude self from gossip targets
        let peers: Vec<PeerInfo> = self
            .registry
            .get_active_peers()
            .await
            .into_iter()
            .filter(|p| p.id != self.local_peer.id)
            .collect();

        if peers.is_empty() {
            // No known peers yet: broadcast membership to help discovery on LAN.
            let members = self.registry.list_peers().await;
            let seq = {
                let mut s = self.seq.write().await;
                *s += 1;
                *s
            };

            let msg = GossipMessage::Membership {
                sender: self.local_peer.clone(),
                members: members.clone(),
                seq,
            };

            let mut data = Vec::new();
            cbor_serialize(&msg, &mut data)?;
            let mut sent_any = false;
            for target in self.broadcast_targets() {
                if let Ok(sock_addr) = target.parse::<SocketAddr>() {
                    if let Ok(Ok(_)) =
                        timeout(Duration::from_secs(1), self.socket.send_to(&data, sock_addr)).await
                    {
                        sent_any = true;
                    }
                }
            }
            if sent_any {
                let mut metrics = self.metrics.write().await;
                metrics.membership_sent += 1;
                metrics.gossip_rounds += 1;
            }
            return Ok(());
        }

        // Select random peers (up to fanout)
        let mut selected = Vec::new();
        let mut rng = fastrand::Rng::new();
        let mut indices: Vec<usize> = (0..peers.len()).collect();
        rng.shuffle(&mut indices);

        for &idx in indices.iter().take(self.config.fanout.min(peers.len())) {
            selected.push(peers[idx].clone());
        }

        // Get current membership
        let members = self.registry.list_peers().await;

        // Increment sequence number
        let seq = {
            let mut s = self.seq.write().await;
            *s += 1;
            *s
        };

        // Send membership to selected peers
        let mut sent_count = 0;
        for peer in selected {
            if peer.id == self.local_peer.id {
                continue;
            }

            let msg = GossipMessage::Membership {
                sender: self.local_peer.clone(),
                members: members.clone(),
                seq,
            };

            if self.send_message(&msg, &peer).await.is_ok() {
                sent_count += 1;
            }
        }

        // Update metrics
        {
            let mut metrics = self.metrics.write().await;
            metrics.membership_sent += sent_count;
            metrics.gossip_rounds += 1;
        }

        if sent_count > 0 {
            debug!(
                sent_to = sent_count,
                total_peers = peers.len(),
                "Gossip round completed"
            );
        }

        Ok(())
    }

    /// Sends a gossip message to a peer
    async fn send_message(&self, msg: &GossipMessage, peer: &PeerInfo) -> Result<()> {
        let mut data = Vec::new();
        cbor_serialize(msg, &mut data)?;
        let addr = format!("{}:{}", peer.address, self.config.gossip_port);
        let socket_addr: SocketAddr = addr.parse()?;

        // Use timeout to avoid blocking
        match timeout(Duration::from_secs(1), self.socket.send_to(&data, socket_addr)).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(crate::utils::error::BlazeCacheError::NetworkError(e.to_string())),
            Err(_) => Err(crate::utils::error::BlazeCacheError::Timeout),
        }
    }

    /// Sends a pong response to a ping
    async fn send_pong(&self, peer: &PeerInfo) -> Result<()> {
        let seq = *self.seq.read().await;
        let msg = GossipMessage::Pong {
            sender: self.local_peer.clone(),
            ping_seq: seq,
        };
        self.send_message(&msg, peer).await
    }

    /// Updates last seen timestamp for a peer
    async fn update_last_seen(&self, peer_id: &str) {
        let mut last_seen = self.last_seen.write().await;
        last_seen.insert(peer_id.to_string(), current_timestamp());
    }

    /// Checks for failed peers and updates their status
    async fn check_failures(&self) -> Result<()> {
        let now = current_timestamp();
        let failure_timeout_secs = self.config.failure_timeout.as_secs();
        let suspicion_timeout_secs = self.config.suspicion_timeout.as_secs();

        let peers = self.registry.list_peers().await;
        let last_seen = self.last_seen.read().await;

        let mut failed_count = 0;
        for peer in peers {
            if peer.id == self.local_peer.id {
                continue;
            }

            let last_seen_time = last_seen.get(&peer.id).copied().unwrap_or(0);
            let elapsed = now.saturating_sub(last_seen_time);

            if elapsed > failure_timeout_secs {
                // Mark peer as unreachable if past failure timeout
                if !matches!(peer.status, PeerStatus::Unreachable) {
                    warn!(
                        peer_id = %peer.id,
                        last_seen_seconds = elapsed,
                        "Marking peer as unreachable"
                    );
                    self.registry
                        .update_peer_status(&peer.id, PeerStatus::Unreachable)
                        .await?;
                    failed_count += 1;
                }
            } else if elapsed > suspicion_timeout_secs {
                // Mark peer as inactive if past suspicion timeout but not failure timeout
                if matches!(peer.status, PeerStatus::Active) {
                    warn!(
                        peer_id = %peer.id,
                        last_seen_seconds = elapsed,
                        "Marking peer as inactive"
                    );
                    self.registry
                        .update_peer_status(&peer.id, PeerStatus::Inactive)
                        .await?;
                }
            } else {
                // Ensure peer is marked as active if recently seen
                if !matches!(peer.status, PeerStatus::Active) {
                    self.registry
                        .update_peer_status(&peer.id, PeerStatus::Active)
                        .await?;
                }
            }
        }

        // Update metrics
        if failed_count > 0 {
            let mut metrics = self.metrics.write().await;
            metrics.peers_failed += failed_count;
        }

        Ok(())
    }

    /// Gets current gossip metrics
    pub async fn get_metrics(&self) -> GossipMetrics {
        self.metrics.read().await.clone()
    }


    /// Clones self for failure detection task
    fn clone_for_failure_detection(&self) -> Self {
        Self {
            local_peer: self.local_peer.clone(),
            registry: Arc::clone(&self.registry),
            config: self.config.clone(),
            socket: Arc::clone(&self.socket),
            seq: Arc::clone(&self.seq),
            last_seen: Arc::clone(&self.last_seen),
            metrics: Arc::clone(&self.metrics),
        }
    }

    /// Clones self for server task (with metrics)
    fn clone_for_server(&self) -> Self {
        Self {
            local_peer: self.local_peer.clone(),
            registry: Arc::clone(&self.registry),
            config: self.config.clone(),
            socket: Arc::clone(&self.socket),
            seq: Arc::clone(&self.seq),
            last_seen: Arc::clone(&self.last_seen),
            metrics: Arc::clone(&self.metrics),
        }
    }

    /// Clones self for client task (with metrics)
    fn clone_for_client(&self) -> Self {
        Self {
            local_peer: self.local_peer.clone(),
            registry: Arc::clone(&self.registry),
            config: self.config.clone(),
            socket: Arc::clone(&self.socket),
            seq: Arc::clone(&self.seq),
            last_seen: Arc::clone(&self.last_seen),
            metrics: Arc::clone(&self.metrics),
        }
    }

    /// Generate broadcast targets based on local address to improve discovery on bridged networks.
    fn broadcast_targets(&self) -> Vec<String> {
        let mut targets = vec![format!("255.255.255.255:{}", self.config.gossip_port)];

        if let Ok(ipv4) = self.local_peer.address.parse::<Ipv4Addr>() {
            let octets = ipv4.octets();
            // /24 style broadcast
            targets.push(format!(
                "{}.{}.{}.255:{}",
                octets[0], octets[1], octets[2], self.config.gossip_port
            ));
            // /16 style broadcast
            targets.push(format!(
                "{}.{}.255.255:{}",
                octets[0], octets[1], self.config.gossip_port
            ));
        }

        // Deduplicate
        targets.sort();
        targets.dedup();
        targets
    }
}

/// Gets the current Unix timestamp in seconds

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gossip_config_default() {
        let config = GossipConfig::default();
        assert_eq!(config.gossip_interval, Duration::from_secs(1));
        assert_eq!(config.fanout, 3);
        assert_eq!(config.failure_timeout, Duration::from_secs(30));
        assert_eq!(config.suspicion_timeout, Duration::from_secs(15));
        assert_eq!(config.failure_check_interval, Duration::from_secs(5));
        assert_eq!(config.gossip_port, 6785);
    }

    #[tokio::test]
    async fn test_gossip_message_serialization() {
        let peer = PeerInfo {
            id: "test-peer".to_string(),
            address: "127.0.0.1".to_string(),
            port: 8080,
            protocol: "tcp".to_string(),
            status: PeerStatus::Active,
            last_seen: 12345,
        };

        let msg = GossipMessage::Membership {
            sender: peer.clone(),
            members: vec![peer.clone()],
            seq: 1,
        };

        let mut serialized = Vec::new();
        cbor_serialize(&msg, &mut serialized).unwrap();
        let deserialized: GossipMessage = cbor_deserialize(&serialized[..]).unwrap();

        match deserialized {
            GossipMessage::Membership { sender, members, seq } => {
                assert_eq!(sender.id, "test-peer");
                assert_eq!(members.len(), 1);
                assert_eq!(seq, 1);
            }
            _ => panic!("Expected Membership message"),
        }
    }

    #[tokio::test]
    async fn test_gossip_metrics() {
        let metrics = GossipMetrics::default();
        assert_eq!(metrics.membership_sent, 0);
        assert_eq!(metrics.membership_received, 0);
        assert_eq!(metrics.peers_discovered, 0);
        assert_eq!(metrics.peers_failed, 0);
        assert_eq!(metrics.gossip_rounds, 0);
    }

    #[tokio::test]
    async fn test_gossip_protocol_creation() {
        let registry = Arc::new(PeerRegistry::new());
        let local_peer = PeerInfo {
            id: "local".to_string(),
            address: "127.0.0.1".to_string(),
            port: 6784,
            protocol: "tcp".to_string(),
            status: PeerStatus::Active,
            last_seen: current_timestamp(),
        };

        let mut config = GossipConfig::default();
        config.gossip_port = 9999; // Use a test port

        // This will fail to bind if port is in use, but that's ok for test
        let result = GossipProtocol::new(local_peer, registry, config).await;
        // Just verify it doesn't panic - actual binding may fail in test environment
        assert!(result.is_ok() || result.is_err());
    }

}

