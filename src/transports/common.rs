use std::borrow::Cow;

#[derive(Debug, Clone)]
pub enum Command<'a> {
    Get(Cow<'a, str>),
    Put(Cow<'a, str>, Vec<u8>, u32),
    Delete(Cow<'a, str>),
    Peer,
    Ping,
}

#[derive(Debug, Clone)]
pub enum Response {
    Ok(Vec<u8>),
    Error(String),
    Pong,
}

use async_trait::async_trait;
use std::error::Error;

#[async_trait]
pub trait ProtocolServer: Send + Sync {
    async fn start(&self, port: u16) -> Result<(), Box<dyn Error + Send + Sync>>;
}

#[async_trait]
pub trait ProtocolClient: Send + Sync {
    async fn connect(addr: &str) -> Result<Self, Box<dyn Error + Send + Sync>>
    where
        Self: Sized;
    async fn ping(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn get(&mut self, key: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>>;
    async fn put(&mut self, key: &str, value: &[u8], ttl: u32) -> Result<(), Box<dyn Error + Send + Sync>>;

    // Optional command; default returns unsupported to preserve compatibility
    async fn delete(&mut self, _key: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
        Err("delete not supported by this client".into())
    }
}
