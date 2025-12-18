use std::fmt;

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
    IoError(std::io::Error),
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

impl std::error::Error for BlazeCacheError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BlazeCacheError::IoError(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for BlazeCacheError {
    fn from(err: std::io::Error) -> Self {
        BlazeCacheError::IoError(err)
    }
}

impl From<serde_json::Error> for BlazeCacheError {
    fn from(err: serde_json::Error) -> Self {
        BlazeCacheError::SerializationError(err.to_string())
    }
}

impl From<bincode::Error> for BlazeCacheError {
    fn from(err: bincode::Error) -> Self {
        BlazeCacheError::SerializationError(err.to_string())
    }
}

impl From<std::net::AddrParseError> for BlazeCacheError {
    fn from(err: std::net::AddrParseError) -> Self {
        BlazeCacheError::NetworkError(format!("Address parse error: {}", err))
    }
}
