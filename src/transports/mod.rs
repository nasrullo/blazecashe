pub mod common;
pub mod tcp;
pub mod udp;

use crate::cache::{Group, Value};
use crate::utils::persistence::{PersistenceManager, WalEntry};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

pub use common::{Command, ProtocolClient, ProtocolServer, Response};
pub use tcp::{Serializer, TcpClient, TcpServer};
pub use udp::{UdpClient, UdpServer};

// Common connection handler to avoid TCP/UDP duplication
pub async fn handle_connection<S: Serializer>(
    data: &[u8],
    group: &Arc<Group>,
    persistence: Option<Arc<AsyncMutex<PersistenceManager>>>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let cmd = S::deserialize_command(data)?;
    // Removed verbose logging from hot path for performance
    let response = handle_command(group, cmd, persistence).await;
    Ok(S::serialize_response(&response))
}

// Common command handler to avoid duplication
pub async fn handle_command<'a>(
    group: &Arc<Group>,
    cmd: Command<'a>,
    persistence: Option<Arc<AsyncMutex<PersistenceManager>>>,
) -> Response {
    match cmd {
        Command::Get(key) => match group.get(key.as_ref()).await {
            Ok(value) => Response::Ok(value),
            Err(e) => {
                // Optimize common error cases to avoid string allocation
                let err_msg = match &e {
                    crate::utils::BlazeCacheError::KeyNotFound => "Not found",
                    crate::utils::BlazeCacheError::KeyEmpty => "Key empty",
                    _ => {
                        // Only allocate for complex errors
                        return Response::Error(e.to_string());
                    }
                };
                Response::Error(err_msg.to_string())
            }
        },
        Command::Put(key, value, ttl_sec) => {
            // Optimize: Only clone value for WAL if persistence is actually enabled
            // This avoids unnecessary clone when persistence is disabled
            let value_for_wal = persistence.as_ref().map(|_| value.clone());
            
            let result = group.set(key.as_ref(), value, ttl_sec).await;

            // Log to WAL if persistence is enabled and operation succeeded (non-blocking)
            if result.is_ok() {
                if let (Some(pm), Some(value_for_wal)) = (&persistence, value_for_wal) {
                    // Use Cow::to_string() which is efficient for both Borrowed and Owned
                    let key_for_wal = key.to_string();
                    let pm_clone = pm.clone();
                    // Always spawn async to avoid blocking the request path
                    tokio::spawn(async move {
                        let mut mgr = pm_clone.lock().await;
                        let _ = mgr.log_entry(WalEntry::Put {
                            key: key_for_wal,
                            value: Value::new(value_for_wal, ttl_sec as u64),
                        });
                    });
                }
            }

            match result {
                Ok(_) => Response::Ok(vec![]),
                Err(e) => {
                    // Use static string where possible to avoid allocation
                    let err_msg = match &e {
                        crate::utils::BlazeCacheError::ItemTooLarge { .. } => "Item too large",
                        crate::utils::BlazeCacheError::KeyNotFound => "Not found",
                        _ => {
                            // Only allocate string for complex errors
                            return Response::Error(e.to_string());
                        }
                    };
                    Response::Error(err_msg.to_string())
                }
            }
        }
        Command::Delete(key) => {
            let result = group.delete(key.as_ref()).await;

            // Log to WAL if persistence is enabled and operation succeeded (non-blocking)
            if result.is_ok() {
                if let Some(pm) = &persistence {
                    let key_for_wal = key.to_string();
                    let pm_clone = pm.clone();
                    // Always spawn async to avoid blocking the request path
                    tokio::spawn(async move {
                        let mut mgr = pm_clone.lock().await;
                        let _ = mgr.log_entry(WalEntry::Delete { key: key_for_wal });
                    });
                }
            }

            match result {
                Ok(true) => Response::Ok(vec![]),
                Ok(false) => Response::Error("Not found".to_string()),
                Err(e) => Response::Error(e.to_string()),
            }
        }
        Command::Peer => {
            let peers = group.get_peers().await;
            Response::Ok(peers.join(",").into_bytes())
        }
        Command::Ping => Response::Pong,
    }
}

// Common response handlers to avoid duplication
pub fn handle_ping_response(
    response: Response,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match response {
        Response::Pong => Ok(()),
        _ => Err("Invalid ping response".into()),
    }
}

pub fn handle_get_response(
    response: Response,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    match response {
        Response::Ok(data) => Ok(data),
        Response::Error(msg) => Err(msg.into()),
        _ => Err("Invalid get response".into()),
    }
}

pub fn handle_put_response(
    response: Response,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match response {
        Response::Ok(_) => Ok(()),
        Response::Error(msg) => Err(msg.into()),
        _ => Err("Invalid put response".into()),
    }
}

