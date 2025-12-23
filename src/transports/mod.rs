pub mod common;
pub mod udp;
pub mod tcp;

use crate::cache::{Group, Value};
use crate::utils::persistence::{PersistenceManager, WalEntry};
use crate::utils::BlazeCacheError;
use std::error::Error as StdError;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

pub use common::{Command, ProtocolClient, ProtocolServer, Response};
pub use udp::{UdpClient, UdpServer};
pub use tcp::{Serializer, TcpClient, TcpServer, TlsTcpClient, TlsTcpServer};

/// Type alias for a boxed error that can be sent across threads.
///
/// Used in transport layer functions that need to return errors that implement
/// both `Error`, `Send`, and `Sync` traits for thread-safe error handling.
pub type TransportError = Box<dyn StdError + Send + Sync>;

/// Type alias for an optional persistence manager wrapped in async mutex.
///
/// Used throughout the transport layer to pass persistence managers that may
/// or may not be enabled, wrapped in Arc and Mutex for thread-safe access.
pub type PersistenceManagerHandle = Option<Arc<AsyncMutex<PersistenceManager>>>;

// Common connection handler to avoid TCP/UDP duplication
pub async fn handle_connection<S: Serializer>(
    data: &[u8],
    group: &Arc<Group>,
    persistence: PersistenceManagerHandle,
) -> Result<Vec<u8>, TransportError> {
    let cmd = S::deserialize_command(data)?;
    // Removed verbose logging from hot path for performance
    let response = handle_command(group, cmd, persistence).await;
    Ok(S::serialize_response(&response))
}

// Common command handler to avoid duplication
pub async fn handle_command<'a>(
    group: &Arc<Group>,
    cmd: Command<'a>,
    persistence: PersistenceManagerHandle,
) -> Response {
    match cmd {
        Command::Get(key) => match group.get(key.as_ref()).await {
            Ok(value) => Response::Ok(value),
            Err(e) => {
                // Optimize common error cases to avoid string allocation
                let err_msg = match &e {
                    BlazeCacheError::KeyNotFound => "Not found",
                    BlazeCacheError::KeyEmpty => "Key empty",
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
                        BlazeCacheError::ItemTooLarge { .. } => "Item too large",
                        BlazeCacheError::KeyNotFound => "Not found",
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
        Command::Stats => {
            let stats = group.stats().await;
            // Format stats as JSON-like string for easy parsing
            let stats_json = format!(
                r#"{{"hits":{},"misses":{},"puts":{},"deletes":{},"evictions":{},"hot_items":{},"rejected_items":{},"ttl_evictions":{},"entry_count":{},"memory_usage":{}}}"#,
                stats.hits,
                stats.misses,
                stats.puts,
                stats.deletes,
                stats.evictions,
                stats.hot_items,
                stats.rejected_items,
                stats.ttl_evictions,
                stats.entry_count,
                stats.memory_usage
            );
            Response::Ok(stats_json.into_bytes())
        }
        Command::Clear => {
            // Clear local caches
            group.clear().await;
            
            // Clear persistence (WAL and snapshots) if enabled
            if let Some(pm) = &persistence {
                let pm_clone = pm.clone();
                tokio::spawn(async move {
                    let mut mgr = pm_clone.lock().await;
                    // Log Clear entry to WAL first
                    let _ = mgr.log_entry(WalEntry::Clear);
                    // Then clear all persistence files
                    let _ = mgr.clear_all();
                });
            }
            
            Response::Ok(vec![])
        }
    }
}

// Common response handlers to avoid duplication
pub fn handle_ping_response(
    response: Response,
) -> Result<(), TransportError> {
    match response {
        Response::Pong => Ok(()),
        _ => Err("Invalid ping response".into()),
    }
}

pub fn handle_get_response(
    response: Response,
) -> Result<Vec<u8>, TransportError> {
    match response {
        Response::Ok(data) => Ok(data),
        Response::Error(msg) if msg == "Not found" => Ok(vec![]), // Not found is valid, return empty
        Response::Error(msg) => Err(msg.into()),
        _ => Err("Invalid get response".into()),
    }
}

pub fn handle_put_response(
    response: Response,
) -> Result<(), TransportError> {
    match response {
        Response::Ok(_) => Ok(()),
        Response::Error(msg) => Err(msg.into()),
        _ => Err("Invalid put response".into()),
    }
}

pub fn handle_peer_response(
    response: Response,
) -> Result<String, TransportError> {
    match response {
        Response::Ok(data) => {
            String::from_utf8(data)
                .map_err(|e| format!("Invalid UTF-8 in peer response: {}", e).into())
        }
        Response::Error(msg) => Err(msg.into()),
        _ => Err("Invalid peer response".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_ping_response() {
        let resp = Response::Pong;
        assert!(handle_ping_response(resp).is_ok());
    }

    #[test]
    fn test_handle_ping_response_invalid() {
        let resp = Response::Ok(vec![]);
        assert!(handle_ping_response(resp).is_err());
    }

    #[test]
    fn test_handle_get_response_ok() {
        let resp = Response::Ok(vec![1u8, 2, 3]);
        let result = handle_get_response(resp).unwrap();
        assert_eq!(result, vec![1u8, 2, 3]);
    }

    #[test]
    fn test_handle_get_response_not_found() {
        let resp = Response::Error("Not found".to_string());
        let result: Vec<u8> = handle_get_response(resp).unwrap();
        assert_eq!(result, Vec::<u8>::new());
    }

    #[test]
    fn test_handle_get_response_error() {
        let resp = Response::Error("Other error".to_string());
        assert!(handle_get_response(resp).is_err());
    }

    #[test]
    fn test_handle_put_response_ok() {
        let resp = Response::Ok(vec![]);
        assert!(handle_put_response(resp).is_ok());
    }

    #[test]
    fn test_handle_put_response_error() {
        let resp = Response::Error("Error".to_string());
        assert!(handle_put_response(resp).is_err());
    }

    #[test]
    fn test_handle_peer_response_ok() {
        let resp = Response::Ok(b"peer1,peer2".to_vec());
        let result = handle_peer_response(resp).unwrap();
        assert_eq!(result, "peer1,peer2");
    }

    #[test]
    fn test_handle_peer_response_error() {
        let resp = Response::Error("Error".to_string());
        assert!(handle_peer_response(resp).is_err());
    }

    #[test]
    fn test_handle_peer_response_invalid() {
        let resp = Response::Pong;
        assert!(handle_peer_response(resp).is_err());
    }

    #[test]
    fn test_handle_peer_response_invalid_utf8() {
        let resp = Response::Ok(vec![0xFF, 0xFE, 0xFD]);
        assert!(handle_peer_response(resp).is_err());
    }
}

