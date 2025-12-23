use std::error::Error as StdError;
use std::fmt;
use std::io::Error as IoError;
use std::net::AddrParseError;
use ciborium::ser::Error as CborSerError;
use ciborium::de::Error as CborDeError;

#[cfg(test)]
use std::net::IpAddr;

/// Result type alias for BlazeCache operations
pub type Result<T> = std::result::Result<T, BlazeCacheError>;

/// Error types for BlazeCache operations
#[derive(Debug)]
pub enum BlazeCacheError {
    GetterFailed(String),
    NetworkError(String),
    SerializationError(String),
    CompressionError(String),
    ItemTooLarge { item_size: usize, max_size: usize },
    KeyEmpty,
    KeyNotFound,
    CacheFull,
    Timeout,
    InvalidConfig,
    PeerError(String),
    IoError(IoError),
}

impl fmt::Display for BlazeCacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlazeCacheError::GetterFailed(msg) => write!(f, "Getter failed: {}", msg),
            BlazeCacheError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            BlazeCacheError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            BlazeCacheError::CompressionError(msg) => write!(f, "Compression error: {}", msg),
            BlazeCacheError::ItemTooLarge {
                item_size,
                max_size,
            } => {
                write!(
                    f,
                    "Item too large: {} bytes exceeds cache limit of {} bytes",
                    item_size, max_size
                )
            }
            BlazeCacheError::KeyEmpty => write!(f, "Key is empty"),
            BlazeCacheError::KeyNotFound => write!(f, "Key not found"),
            BlazeCacheError::CacheFull => write!(f, "Cache is full"),
            BlazeCacheError::Timeout => write!(f, "Operation timed out"),
            BlazeCacheError::InvalidConfig => write!(f, "Invalid configuration"),
            BlazeCacheError::PeerError(msg) => write!(f, "Peer error: {}", msg),
            BlazeCacheError::IoError(err) => write!(f, "I/O error: {}", err),
        }
    }
}

impl StdError for BlazeCacheError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            BlazeCacheError::IoError(err) => Some(err),
            _ => None,
        }
    }
}

impl From<IoError> for BlazeCacheError {
    fn from(err: IoError) -> Self {
        BlazeCacheError::IoError(err)
    }
}

impl From<serde_json::Error> for BlazeCacheError {
    fn from(err: serde_json::Error) -> Self {
        BlazeCacheError::SerializationError(err.to_string())
    }
}

impl From<CborSerError<IoError>> for BlazeCacheError {
    fn from(err: CborSerError<IoError>) -> Self {
        BlazeCacheError::SerializationError(err.to_string())
    }
}

impl From<CborDeError<IoError>> for BlazeCacheError {
    fn from(err: CborDeError<IoError>) -> Self {
        BlazeCacheError::SerializationError(err.to_string())
    }
}

impl From<AddrParseError> for BlazeCacheError {
    fn from(err: AddrParseError) -> Self {
        BlazeCacheError::NetworkError(format!("Address parse error: {}", err))
    }
}

impl Clone for BlazeCacheError {
    fn clone(&self) -> Self {
        match self {
            BlazeCacheError::GetterFailed(msg) => BlazeCacheError::GetterFailed(msg.clone()),
            BlazeCacheError::NetworkError(msg) => BlazeCacheError::NetworkError(msg.clone()),
            BlazeCacheError::SerializationError(msg) => BlazeCacheError::SerializationError(msg.clone()),
            BlazeCacheError::CompressionError(msg) => BlazeCacheError::CompressionError(msg.clone()),
            BlazeCacheError::ItemTooLarge { item_size, max_size } => {
                BlazeCacheError::ItemTooLarge {
                    item_size: *item_size,
                    max_size: *max_size,
                }
            }
            BlazeCacheError::KeyEmpty => BlazeCacheError::KeyEmpty,
            BlazeCacheError::KeyNotFound => BlazeCacheError::KeyNotFound,
            BlazeCacheError::CacheFull => BlazeCacheError::CacheFull,
            BlazeCacheError::Timeout => BlazeCacheError::Timeout,
            BlazeCacheError::InvalidConfig => BlazeCacheError::InvalidConfig,
            BlazeCacheError::PeerError(msg) => BlazeCacheError::PeerError(msg.clone()),
            // Convert IoError to string to make it cloneable
            BlazeCacheError::IoError(err) => BlazeCacheError::IoError(
                IoError::new(err.kind(), err.to_string())
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let errors = vec![
            BlazeCacheError::GetterFailed("test".to_string()),
            BlazeCacheError::NetworkError("test".to_string()),
            BlazeCacheError::SerializationError("test".to_string()),
            BlazeCacheError::CompressionError("test".to_string()),
            BlazeCacheError::ItemTooLarge {
                item_size: 100,
                max_size: 50,
            },
            BlazeCacheError::KeyEmpty,
            BlazeCacheError::KeyNotFound,
            BlazeCacheError::CacheFull,
            BlazeCacheError::Timeout,
            BlazeCacheError::InvalidConfig,
            BlazeCacheError::PeerError("test".to_string()),
            BlazeCacheError::IoError(IoError::other("test")),
        ];

        for err in errors {
            let _ = format!("{}", err);
        }
    }

    #[test]
    fn test_error_debug() {
        let err = BlazeCacheError::KeyNotFound;
        let _ = format!("{:?}", err);
    }

    #[test]
    fn test_error_source() {
        let io_err = IoError::other("test");
        let blaze_err = BlazeCacheError::IoError(io_err);
        assert!(blaze_err.source().is_some());

        let other_err = BlazeCacheError::KeyNotFound;
        assert!(other_err.source().is_none());
    }

    #[test]
    fn test_from_io_error() {
        let io_err = IoError::other("test");
        let blaze_err: BlazeCacheError = io_err.into();
        match blaze_err {
            BlazeCacheError::IoError(_) => {}
            _ => panic!("Expected IoError"),
        }
    }

    #[test]
    fn test_from_serde_json_error() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let blaze_err: BlazeCacheError = json_err.into();
        match blaze_err {
            BlazeCacheError::SerializationError(_) => {}
            _ => panic!("Expected SerializationError"),
        }
    }

    #[test]
    fn test_from_cbor_error() {
        use ciborium::de::from_reader as cbor_deserialize;
        // Create a CBOR error by trying to deserialize invalid data
        let data = vec![0xFF; 100];
        let result: std::result::Result<u32, _> = cbor_deserialize(&data[..]);
        if let Err(cbor_err) = result {
            let blaze_err: BlazeCacheError = cbor_err.into();
            match blaze_err {
                BlazeCacheError::SerializationError(_) => {}
                _ => panic!("Expected SerializationError"),
            }
        }
    }

    #[test]
    fn test_from_addr_parse_error() {
        let parse_err = "invalid".parse::<IpAddr>().unwrap_err();
        let blaze_err: BlazeCacheError = parse_err.into();
        match blaze_err {
            BlazeCacheError::NetworkError(_) => {}
            _ => panic!("Expected NetworkError"),
        }
    }

    #[test]
    fn test_item_too_large_display() {
        let err = BlazeCacheError::ItemTooLarge {
            item_size: 1000,
            max_size: 500,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("1000"));
        assert!(msg.contains("500"));
    }
}
