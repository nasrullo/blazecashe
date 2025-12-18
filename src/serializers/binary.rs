use crate::transports::common::{Command, Response};
use crate::transports::Serializer;

pub struct BinarySerializer;

impl Serializer for BinarySerializer {
    fn serialize_command(cmd: &Command) -> Vec<u8> {
        match cmd {
            Command::Get(key) => {
                // Pre-allocate with exact capacity
                let mut buf = Vec::with_capacity(1 + 2 + key.len());
                buf.push(1u8); // GET command
                buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
                buf.extend_from_slice(key.as_bytes());
                buf
            }
            Command::Put(key, value, ttl) => {
                // Pre-allocate with exact capacity
                let mut buf = Vec::with_capacity(1 + 2 + key.len() + 4 + value.len() + 4);
                buf.push(2u8); // PUT command
                buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
                buf.extend_from_slice(value);
                // TTL seconds (u32). 0 means use default/no TTL.
                buf.extend_from_slice(&ttl.to_be_bytes());
                buf
            }
            Command::Delete(key) => {
                // Pre-allocate with exact capacity
                let mut buf = Vec::with_capacity(1 + 2 + key.len());
                buf.push(3u8); // DELETE command
                buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
                buf.extend_from_slice(key.as_bytes());
                buf
            }
            Command::Peer => vec![4u8],
            Command::Ping => vec![0u8],
        }
    }

    fn deserialize_command(
        data: &[u8],
    ) -> Result<Command, Box<dyn std::error::Error + Send + Sync>> {
        if data.is_empty() {
            return Err("Empty data".into());
        }

        match data[0] {
            0 => Ok(Command::Ping),
            1 => {
                if data.len() < 3 {
                    return Err("GET command too short".into());
                }
                let key_len = u16::from_be_bytes([data[1], data[2]]) as usize;
                if data.len() < 3 + key_len {
                    return Err(format!("GET command incomplete: need {} bytes, got {}", 3 + key_len, data.len()).into());
                }
                let key = String::from_utf8(data[3..3 + key_len].to_vec())?;
                Ok(Command::Get(key))
            }
            2 => {
                if data.len() < 3 {
                    return Err("PUT command too short".into());
                }
                let key_len = u16::from_be_bytes([data[1], data[2]]) as usize;
                let key_start = 3;
                let key_end = key_start + key_len;
                if data.len() < key_end {
                    return Err(format!("PUT command incomplete at key: need {} bytes, got {}", key_end, data.len()).into());
                }
                let key = String::from_utf8(data[key_start..key_end].to_vec())?;
                
                let value_len_start = key_end;
                if data.len() < value_len_start + 4 {
                    return Err(format!("PUT command incomplete at value_len: need {} bytes, got {}", value_len_start + 4, data.len()).into());
                }
                let value_len = u32::from_be_bytes([
                    data[value_len_start],
                    data[value_len_start + 1],
                    data[value_len_start + 2],
                    data[value_len_start + 3],
                ]) as usize;
                
                let value_start = value_len_start + 4;
                let value_end = value_start + value_len;
                if data.len() < value_end {
                    return Err(format!("PUT command incomplete at value: need {} bytes, got {}", value_end, data.len()).into());
                }
                let value = data[value_start..value_end].to_vec();
                
                // TTL follows value: 4 bytes
                let ttl_start = value_end;
                let ttl = if data.len() >= ttl_start + 4 {
                    u32::from_be_bytes([
                        data[ttl_start],
                        data[ttl_start + 1],
                        data[ttl_start + 2],
                        data[ttl_start + 3],
                    ])
                } else {
                    0
                };
                Ok(Command::Put(key, value, ttl))
            }
            3 => {
                if data.len() < 3 {
                    return Err("DELETE command too short".into());
                }
                let key_len = u16::from_be_bytes([data[1], data[2]]) as usize;
                if data.len() < 3 + key_len {
                    return Err(format!("DELETE command incomplete: need {} bytes, got {}", 3 + key_len, data.len()).into());
                }
                let key = String::from_utf8(data[3..3 + key_len].to_vec())?;
                Ok(Command::Delete(key))
            }
            4 => Ok(Command::Peer),
            _ => Err("Unknown command".into()),
        }
    }

    fn serialize_response(resp: &Response) -> Vec<u8> {
        match resp {
            Response::Ok(data) => {
                // Pre-allocate with exact capacity to avoid reallocations
                let mut buf = Vec::with_capacity(1 + 4 + data.len());
                buf.push(0u8); // OK
                buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
                buf.extend_from_slice(data);
                buf
            }
            Response::Error(msg) => {
                // Pre-allocate with exact capacity
                let mut buf = Vec::with_capacity(1 + 2 + msg.len());
                buf.push(1u8); // ERROR
                buf.extend_from_slice(&(msg.len() as u16).to_be_bytes());
                buf.extend_from_slice(msg.as_bytes());
                buf
            }
            Response::Pong => vec![2u8], // PONG
        }
    }

    fn deserialize_response(
        data: &[u8],
    ) -> Result<Response, Box<dyn std::error::Error + Send + Sync>> {
        if data.is_empty() {
            return Err("Empty response".into());
        }

        match data[0] {
            0 => {
                let data_len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
                let response_data = data[5..5 + data_len].to_vec();
                Ok(Response::Ok(response_data))
            }
            1 => {
                let msg_len = u16::from_be_bytes([data[1], data[2]]) as usize;
                let msg = String::from_utf8(data[3..3 + msg_len].to_vec())?;
                Ok(Response::Error(msg))
            }
            2 => Ok(Response::Pong),
            _ => Err("Unknown response".into()),
        }
    }
}
