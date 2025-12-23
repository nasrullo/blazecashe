use crate::networking::peer::Peer;
use crate::serializers::binary::BinarySerializer;
use crate::transports::common::{Command, Response};
use crate::transports::Serializer;
use crate::utils::{Result, error::BlazeCacheError};
use async_trait::async_trait;
use std::borrow::Cow;
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

    async fn send_command<'a>(&self, cmd: Command<'a>) -> Result<Response> {
        let payload = BinarySerializer::serialize_command(&cmd);

        for attempt in 0..2 {
            // Ensure connection exists (release lock before async connect)
            {
                let guard = self.conn.lock().await;
                if guard.is_none() {
                    drop(guard); // Release lock before async operation
                    let stream = TcpStream::connect(&self.addr).await?;
                    let mut guard = self.conn.lock().await;
                    *guard = Some(stream);
                }
            } // Lock released here

            // Perform I/O operations with minimal lock scope
            // Note: We need to hold the lock during I/O to prevent concurrent access to the stream
            // This is acceptable because I/O operations are typically fast and the lock is per-peer
            let result = {
                let mut guard = self.conn.lock().await;
                if let Some(stream) = guard.as_mut() {
                    // Write operation (typically fast)
                    match stream.write_all(&payload).await {
                        Ok(_) => {
                            // Read operation (typically fast)
                            let mut buf = vec![0u8; 8192];
                            match stream.read(&mut buf).await {
                                Ok(0) => {
                                    *guard = None;
                                    Err(BlazeCacheError::PeerError("connection closed".into()))
                                }
                                Ok(n) => {
                                    drop(guard); // Release lock before deserialization
                                    let resp = BinarySerializer::deserialize_response(&buf[..n])
                                        .map_err(|e| BlazeCacheError::PeerError(e.to_string()))?;
                                    return Ok(resp);
                                }
                                Err(e) => {
                                    *guard = None;
                                    Err(BlazeCacheError::IoError(e))
                                }
                            }
                        }
                        Err(e) => {
                            *guard = None;
                            Err(BlazeCacheError::IoError(e))
                        }
                    }
                } else {
                    Err(BlazeCacheError::PeerError("connection lost".into()))
                }
            };

            // Handle retry logic
            match result {
                Err(BlazeCacheError::IoError(_)) | Err(BlazeCacheError::PeerError(_)) => {
                    if attempt == 1 {
                        return result;
                    }
                    // Retry on next iteration
                }
                _ => return result,
            }
        }

        Err(BlazeCacheError::PeerError("remote send failed".into()))
    }
}

#[async_trait]
impl Peer for RemotePeer {
    async fn get(&self, _group: &str, key: &str) -> Result<Vec<u8>> {
        match self.send_command(Command::Get(Cow::Borrowed(key))).await? {
            Response::Ok(data) => Ok(data),
            Response::Error(msg) => Err(BlazeCacheError::PeerError(msg)),
            _ => Err(BlazeCacheError::PeerError("Invalid get response".into())),
        }
    }

    async fn delete(&self, _group: &str, key: &str) -> Result<()> {
        match self.send_command(Command::Delete(Cow::Borrowed(key))).await? {
            Response::Ok(_) => Ok(()),
            Response::Error(msg) => {
                if msg == "Not found" {
                    Err(BlazeCacheError::KeyNotFound)
                } else {
                    Err(BlazeCacheError::PeerError(msg))
                }
            }
            _ => Err(BlazeCacheError::PeerError("Invalid get response".into())),
        }
    }

    async fn set(&self, _group: &str, key: &str, value:Vec<u8>, ttl: u32) -> Result<()> {
        match self.send_command(Command::Put(Cow::Borrowed(key), value, ttl)).await? {
            Response::Ok(_) => Ok(()),
            Response::Error(msg) => Err(BlazeCacheError::PeerError(msg)),
            _ => Err(BlazeCacheError::PeerError("Invalid get response".into())),
        }
    }

    async fn get_hot_items(&self, _group: &str) -> Result<Vec<String>> {
        Ok(vec![])
    }

    async fn clear(&self, _group: &str) -> Result<()> {
        match self.send_command(Command::Clear).await? {
            Response::Ok(_) => Ok(()),
            Response::Error(msg) => Err(BlazeCacheError::PeerError(msg)),
            _ => Err(BlazeCacheError::PeerError("Invalid clear response".into())),
        }
    }

    fn address(&self) -> String {
        self.addr.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remote_peer_new() {
        let peer = RemotePeer::new("127.0.0.1:8080".to_string());
        assert_eq!(peer.addr, "127.0.0.1:8080");
    }

    #[test]
    fn test_remote_peer_address() {
        let peer = RemotePeer::new("127.0.0.1:8080".to_string());
        assert_eq!(peer.address(), "127.0.0.1:8080");
    }
}
