use crate::networking::peer::Peer;
use crate::serializers::binary::BinarySerializer;
use crate::transports::common::{Command, Response};
use crate::transports::Serializer;
use crate::utils::{Result, error::BlazeCacheError};
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

pub struct RemotePeer {
    pub addr: String,
    conn: Mutex<Option<TcpStream>>,
}

impl RemotePeer {
    pub fn new(addr: String) -> Self {
        Self {
            addr,
            conn: Mutex::new(None),
        }
    }

    async fn send_command(&self, cmd: Command) -> Result<Response> {
        let payload = BinarySerializer::serialize_command(&cmd);
        let mut guard = self.conn.lock().await;

        for attempt in 0..2 {
            // Ensure connection
            if guard.is_none() {
                *guard = Some(TcpStream::connect(&self.addr).await?);
            }

            if let Some(stream) = guard.as_mut() {
                if let Err(e) = stream.write_all(&payload).await {
                    // drop and retry
                    *guard = None;
                    if attempt == 1 {
                        return Err(BlazeCacheError::IoError(e));
                    }
                    continue;
                }

                let mut buf = vec![0u8; 8192];
                match stream.read(&mut buf).await {
                    Ok(0) => {
                        *guard = None;
                        if attempt == 1 {
                            return Err(BlazeCacheError::PeerError("connection closed".into()));
                        }
                        continue;
                    }
                    Ok(n) => {
                        let resp = BinarySerializer::deserialize_response(&buf[..n])
                            .map_err(|e| BlazeCacheError::PeerError(e.to_string()))?;
                        return Ok(resp);
                    }
                    Err(e) => {
                        *guard = None;
                        if attempt == 1 {
                            return Err(BlazeCacheError::IoError(e));
                        }
                    }
                }
            }
        }

        Err(BlazeCacheError::PeerError("remote send failed".into()))
    }
}

#[async_trait]
impl Peer for RemotePeer {
    async fn get(&self, _group: &str, key: &str) -> Result<Vec<u8>> {
        match self.send_command(Command::Get(key.to_string())).await? {
            Response::Ok(data) => Ok(data),
            Response::Error(msg) => Err(BlazeCacheError::PeerError(msg).into()),
            _ => Err(BlazeCacheError::PeerError("Invalid get response".into()).into()),
        }
    }

    async fn delete(&self, _group: &str, key: &str) -> Result<()> {
        match self.send_command(Command::Delete(key.to_string())).await? {
            Response::Ok(_) => Ok(()),
            Response::Error(msg) => {
                if msg == "Not found" {
                    Err(BlazeCacheError::KeyNotFound)
                } else {
                    Err(BlazeCacheError::PeerError(msg).into())
                }
            }
            _ => Err(BlazeCacheError::PeerError("Invalid get response".into()).into()),
        }
    }

    async fn set(&self, _group: &str, key: &str, value:Vec<u8>, ttl: u32) -> Result<()> {
        match self.send_command(Command::Put(key.to_string(), value, ttl)).await? {
            Response::Ok(_) => Ok(()),
            Response::Error(msg) => Err(BlazeCacheError::PeerError(msg).into()),
            _ => Err(BlazeCacheError::PeerError("Invalid get response".into()).into()),
        }
    }

    async fn get_hot_items(&self, _group: &str) -> Result<Vec<String>> {
        Ok(vec![])
    }

    fn address(&self) -> String {
        self.addr.clone()
    }
}

