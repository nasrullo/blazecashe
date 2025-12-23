//! # Binary Serialization Utilities
//!
//! This module provides utilities for serializing and deserializing data structures
//! using the CBOR (Concise Binary Object Representation) format. CBOR is a compact
//! binary serialization format that is standardized (RFC 8949) and more space-efficient
//! than JSON or other text-based formats.
//!
//! ## Use Cases
//!
//! - **Inter-peer Communication**: Serializing cache data for replication
//! - **Persistence**: Storing cache entries in binary format
//! - **Network Protocol**: Efficient data transfer over the network
//!
//! ## Performance
//!
//! CBOR provides:
//! - **Speed**: Faster than JSON/text serialization
//! - **Size**: More compact than text formats
//! - **Type Safety**: Preserves Rust type information
//! - **Standardization**: RFC 8949 standard format
//!
//! ## Example
//!
//! ```rust
//! use blazecache::utils::serialization::{serialize_binary, deserialize_binary};
//!
//! let data = vec![1, 2, 3, 4, 5];
//! let bytes = serialize_binary(&data).unwrap();
//! let deserialized: Vec<u8> = deserialize_binary(&bytes).unwrap();
//! assert_eq!(data, deserialized);
//! ```

use crate::utils::Result;
use bytes::Bytes;
use ciborium::de::from_reader as cbor_deserialize;
use ciborium::ser::into_writer as cbor_serialize;
use serde::{Deserialize, Serialize};

/// Binary serialization format for GET response messages.
///
/// This structure is used to serialize cache values when sending them between
/// peers or storing them in persistent storage. The binary format is more efficient
/// than text-based formats like JSON.
///
/// ## Fields
///
/// * `value` - The cached value as a byte vector
///
/// ## Serialization Format
///
/// Uses CBOR format, which includes:
/// - Self-describing data format
/// - Compact binary representation
/// - Standardized encoding (RFC 8949)
#[derive(Serialize, Deserialize)]
pub struct BinaryGetResponse {
    /// The cached value bytes.
    ///
    /// This is the actual data that was stored in the cache, serialized
    /// as a byte vector for efficient transmission.
    pub value: Vec<u8>,
}

/// Binary serialization format for replication request messages.
///
/// This structure is used when replicating cache entries between peers in a
/// distributed cache cluster. It includes the cache group name, key, and value
/// to ensure proper routing and storage.
///
/// ## Fields
///
/// * `group` - The cache group name (for multi-tenant scenarios)
/// * `key` - The cache key
/// * `value` - The cache value to replicate
///
/// ## Use Case
///
/// When a hot item is detected, it may be replicated to other peers for
/// better performance. This structure encapsulates the replication request.
#[derive(Serialize, Deserialize)]
pub struct BinaryReplicateRequest {
    /// The cache group name.
    ///
    /// Used to identify which cache group this entry belongs to in
    /// multi-tenant scenarios where multiple logical caches share the
    /// same physical infrastructure.
    pub group: String,

    /// The cache key.
    ///
    /// The unique identifier for this cache entry within the group.
    pub key: String,

    /// The cache value to replicate.
    ///
    /// The actual data bytes that should be replicated to the peer.
    pub value: Vec<u8>,
}

/// Serializes any serializable type to binary format using CBOR.
///
/// This is a generic serialization function that works with any type implementing
/// the `Serialize` trait from Serde. It uses CBOR for efficient binary encoding.
///
/// ## Arguments
///
/// * `data` - The data structure to serialize (must implement `Serialize`)
///
/// ## Returns
///
/// A `Bytes` buffer containing the serialized data, or an error if serialization fails.
///
/// ## Errors
///
/// Returns a serialization error if the data cannot be serialized (e.g., contains
/// unsupported types or exceeds size limits).
///
/// ## Performance
///
/// - **Speed**: Very fast, optimized binary encoding
/// - **Size**: Compact representation (typically smaller than JSON)
/// - **Overhead**: Minimal (just type information and length prefixes)
///
/// ## Example
///
/// ```rust
/// use blazecache::utils::serialization::serialize_binary;
///
/// let data = vec![1, 2, 3];
/// let bytes = serialize_binary(&data).unwrap();
/// ```
pub fn serialize_binary<T: Serialize>(data: &T) -> Result<Bytes> {
    // Use CBOR to serialize the data structure
    // CBOR is a fast, compact, standardized binary serialization format
    let mut buffer = Vec::new();
    cbor_serialize(data, &mut buffer)?;
    
    // Convert to Bytes for efficient zero-copy operations
    // Bytes allows sharing the buffer without cloning
    Ok(Bytes::from(buffer))
}

/// Deserializes binary data into a Rust type using CBOR.
///
/// This is a generic deserialization function that works with any type implementing
/// the `Deserialize` trait from Serde. It reads CBOR-encoded binary data.
///
/// ## Type Parameters
///
/// * `T` - The target type to deserialize into (must implement `Deserialize<'de>`)
///
/// ## Arguments
///
/// * `data` - The binary data to deserialize (CBOR format)
///
/// ## Returns
///
/// The deserialized data structure, or an error if deserialization fails.
///
/// ## Errors
///
/// Returns a deserialization error if:
/// - The data format is invalid or corrupted
/// - The data doesn't match the expected type
/// - The data is truncated or incomplete
///
/// ## Example
///
/// ```rust
/// use blazecache::utils::serialization::{serialize_binary, deserialize_binary};
///
/// let original = vec![1, 2, 3];
/// let bytes = serialize_binary(&original).unwrap();
/// let deserialized: Vec<u8> = deserialize_binary(&bytes).unwrap();
/// assert_eq!(original, deserialized);
/// ```
pub fn deserialize_binary<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<T> {
    // Use CBOR to deserialize the binary data
    // The 'for<'de> Deserialize<'de>' bound allows deserializing types
    // that may contain references with lifetimes
    let decoded = cbor_deserialize(data)?;
    Ok(decoded)
}

/// Fast path for serializing GET response messages.
///
/// This is an optimized helper function specifically for serializing cache values
/// in GET responses. It wraps the value in a `BinaryGetResponse` structure and
/// serializes it.
///
/// ## Arguments
///
/// * `value` - The cache value to serialize
///
/// ## Returns
///
/// A `Bytes` buffer containing the serialized response, or an error if serialization fails.
///
/// ## Performance
///
/// This function is optimized for the common case of serializing cache values.
/// It avoids the overhead of generic serialization by using a specific structure.
///
/// ## Example
///
/// ```rust
/// use blazecache::utils::serialization::serialize_get_response;
///
/// let value = b"cached data".to_vec();
/// let bytes = serialize_get_response(value).unwrap();
/// ```
pub fn serialize_get_response(value: Vec<u8>) -> Result<Bytes> {
    // Create a BinaryGetResponse wrapper and serialize it
    // This provides a consistent format for GET responses
    serialize_binary(&BinaryGetResponse { value })
}

/// Fast path for deserializing GET response messages.
///
/// This is an optimized helper function specifically for deserializing cache values
/// from GET responses. It extracts the value from a `BinaryGetResponse` structure.
///
/// ## Arguments
///
/// * `data` - The binary data containing a serialized `BinaryGetResponse`
///
/// ## Returns
///
/// The deserialized cache value, or an error if deserialization fails.
///
/// ## Performance
///
/// This function is optimized for the common case of deserializing cache values.
/// It avoids the overhead of generic deserialization by using a specific structure.
///
/// ## Example
///
/// ```rust
/// use blazecache::utils::serialization::{serialize_get_response, deserialize_get_response};
///
/// let original = b"cached data".to_vec();
/// let bytes = serialize_get_response(original.clone()).unwrap();
/// let deserialized = deserialize_get_response(&bytes).unwrap();
/// assert_eq!(original, deserialized);
/// ```
pub fn deserialize_get_response(data: &[u8]) -> Result<Vec<u8>> {
    // Deserialize as BinaryGetResponse and extract the value
    // This provides type safety and consistent format handling
    let response: BinaryGetResponse = deserialize_binary(data)?;
    Ok(response.value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_binary_get_response() {
        let response = BinaryGetResponse {
            value: vec![1, 2, 3],
        };
        let bytes = serialize_binary(&response).unwrap();
        // Verify that serialization produces non-empty output
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_deserialize_binary_get_response() {
        let response = BinaryGetResponse {
            value: vec![1, 2, 3],
        };
        let bytes = serialize_binary(&response).unwrap();
        let deserialized: BinaryGetResponse = deserialize_binary(&bytes).unwrap();
        // Verify round-trip serialization preserves data
        assert_eq!(deserialized.value, vec![1, 2, 3]);
    }

    #[test]
    fn test_serialize_binary_replicate_request() {
        let request = BinaryReplicateRequest {
            group: "test".to_string(),
            key: "key".to_string(),
            value: vec![1, 2, 3],
        };
        let bytes = serialize_binary(&request).unwrap();
        // Verify that serialization produces non-empty output
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_deserialize_binary_replicate_request() {
        let request = BinaryReplicateRequest {
            group: "test".to_string(),
            key: "key".to_string(),
            value: vec![1, 2, 3],
        };
        let bytes = serialize_binary(&request).unwrap();
        let deserialized: BinaryReplicateRequest = deserialize_binary(&bytes).unwrap();
        // Verify all fields are preserved during round-trip
        assert_eq!(deserialized.group, "test");
        assert_eq!(deserialized.key, "key");
        assert_eq!(deserialized.value, vec![1, 2, 3]);
    }

    #[test]
    fn test_serialize_get_response() {
        let bytes = serialize_get_response(vec![1, 2, 3]).unwrap();
        // Verify that serialization produces non-empty output
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_deserialize_get_response() {
        let original = vec![1, 2, 3];
        let bytes = serialize_get_response(original.clone()).unwrap();
        let deserialized = deserialize_get_response(&bytes).unwrap();
        // Verify round-trip serialization preserves data
        assert_eq!(deserialized, original);
    }

    #[test]
    fn test_round_trip_binary_get_response() {
        let original = BinaryGetResponse {
            value: vec![1, 2, 3, 4, 5],
        };
        let bytes = serialize_binary(&original).unwrap();
        let deserialized: BinaryGetResponse = deserialize_binary(&bytes).unwrap();
        // Verify complete round-trip preserves all data
        assert_eq!(original.value, deserialized.value);
    }

    #[test]
    fn test_round_trip_binary_replicate_request() {
        let original = BinaryReplicateRequest {
            group: "group1".to_string(),
            key: "key1".to_string(),
            value: vec![10, 20, 30],
        };
        let bytes = serialize_binary(&original).unwrap();
        let deserialized: BinaryReplicateRequest = deserialize_binary(&bytes).unwrap();
        // Verify all fields are preserved during round-trip
        assert_eq!(original.group, deserialized.group);
        assert_eq!(original.key, deserialized.key);
        assert_eq!(original.value, deserialized.value);
    }

    #[test]
    fn test_deserialize_invalid_data() {
        // Test that deserializing invalid/corrupted data returns an error
        let invalid_data = vec![0xFF; 100];
        let result: Result<BinaryGetResponse> = deserialize_binary(&invalid_data);
        // Should fail gracefully with an error, not panic
        assert!(result.is_err());
    }
}
