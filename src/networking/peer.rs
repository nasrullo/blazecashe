use crate::utils::Result;
use async_trait::async_trait;

#[async_trait]
pub trait Peer: Send + Sync {
    async fn get(&self, group: &str, key: &str) -> Result<Vec<u8>>;
    async fn delete(&self, group: &str, key: &str) -> Result<()>;
    async fn set(&self, _group: &str, key: &str, value:Vec<u8>, ttl: u32) -> Result<()>;
    async fn get_hot_items(&self, group: &str) -> Result<Vec<String>>;
    fn address(&self)-> String;
}

pub trait PeerPicker: Send + Sync {
    fn pick_peer(&self, key: &str) -> Option<&dyn Peer>;
    fn get_all_peers(&self) -> Vec<String>;
}
