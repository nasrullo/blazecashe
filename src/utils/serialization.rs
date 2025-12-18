use crate::utils::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct BinaryGetResponse {
    pub value: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct BinaryReplicateRequest {
    pub group: String,
    pub key: String,
    pub value: Vec<u8>,
}

pub fn serialize_binary<T: Serialize>(data: &T) -> Result<Bytes> {
    let encoded = bincode::serialize(data)?;
    Ok(Bytes::from(encoded))
}

pub fn deserialize_binary<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<T> {
    let decoded = bincode::deserialize(data)?;
    Ok(decoded)
}

// Fast path for common operations
pub fn serialize_get_response(value: Vec<u8>) -> Result<Bytes> {
    serialize_binary(&BinaryGetResponse { value })
}

pub fn deserialize_get_response(data: &[u8]) -> Result<Vec<u8>> {
    let response: BinaryGetResponse = deserialize_binary(data)?;
    Ok(response.value)
}
