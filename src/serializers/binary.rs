use crate::transports::common::{Command, Response};
use crate::transports::Serializer;
use std::borrow::Cow;

pub struct BinarySerializer;

impl Serializer for BinarySerializer {
    fn serialize_command<'a>(cmd: &Command<'a>) -> Vec<u8> {
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
            Command::Stats => vec![5u8],
            Command::Clear => vec![6u8],
        }
    }

    fn deserialize_command(
        data: &[u8],
    ) -> Result<Command<'static>, Box<dyn std::error::Error + Send + Sync>> {
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
                // OPTIMIZATION: Use from_utf8_lossy to avoid allocation for valid UTF-8
                // For keys, we still need owned String, but avoid double allocation
                let key_bytes = &data[3..3 + key_len];
                let key = String::from_utf8(key_bytes.to_vec())?;
                Ok(Command::Get(Cow::Owned(key)))
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
                // OPTIMIZATION: Avoid double allocation for key
                let key_bytes = &data[key_start..key_end];
                let key = String::from_utf8(key_bytes.to_vec())?;
                
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
                // OPTIMIZATION: Use slice directly, avoid intermediate Vec
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
                Ok(Command::Put(Cow::Owned(key), value, ttl))
            }
            3 => {
                if data.len() < 3 {
                    return Err("DELETE command too short".into());
                }
                let key_len = u16::from_be_bytes([data[1], data[2]]) as usize;
                if data.len() < 3 + key_len {
                    return Err(format!("DELETE command incomplete: need {} bytes, got {}", 3 + key_len, data.len()).into());
                }
                // OPTIMIZATION: Avoid double allocation
                let key_bytes = &data[3..3 + key_len];
                let key = String::from_utf8(key_bytes.to_vec())?;
                Ok(Command::Delete(Cow::Owned(key)))
            }
            4 => Ok(Command::Peer),
            5 => Ok(Command::Stats),
            6 => Ok(Command::Clear),
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
                if data.len() < 5 {
                    return Err("OK response too short".into());
                }
                let data_len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
                if data.len() < 5 + data_len {
                    return Err(format!("OK response incomplete: need {} bytes, got {}", 5 + data_len, data.len()).into());
                }
                let response_data = data[5..5 + data_len].to_vec();
                Ok(Response::Ok(response_data))
            }
            1 => {
                if data.len() < 3 {
                    return Err("ERROR response too short".into());
                }
                let msg_len = u16::from_be_bytes([data[1], data[2]]) as usize;
                if data.len() < 3 + msg_len {
                    return Err(format!("ERROR response incomplete: need {} bytes, got {}", 3 + msg_len, data.len()).into());
                }
                let msg = String::from_utf8(data[3..3 + msg_len].to_vec())?;
                Ok(Response::Error(msg))
            }
            2 => Ok(Response::Pong),
            _ => Err("Unknown response".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    #[test]
    fn test_serialize_ping() {
        let cmd = Command::Ping;
        let data = BinarySerializer::serialize_command(&cmd);
        assert_eq!(data, vec![0u8]);
    }

    #[test]
    fn test_deserialize_ping() {
        let data = vec![0u8];
        let cmd = BinarySerializer::deserialize_command(&data).unwrap();
        match cmd {
            Command::Ping => {}
            _ => panic!("Expected Ping command"),
        }
    }

    #[test]
    fn test_serialize_get() {
        let cmd = Command::Get(Cow::Borrowed("test"));
        let data = BinarySerializer::serialize_command(&cmd);
        assert_eq!(data[0], 1u8);
        assert_eq!(u16::from_be_bytes([data[1], data[2]]), 4);
        assert_eq!(&data[3..], b"test");
    }

    #[test]
    fn test_deserialize_get() {
        let mut data = vec![1u8, 0, 4];
        data.extend_from_slice(b"test");
        let cmd = BinarySerializer::deserialize_command(&data).unwrap();
        match cmd {
            Command::Get(key) => assert_eq!(key, "test"),
            _ => panic!("Expected Get command"),
        }
    }

    #[test]
    fn test_serialize_put() {
        let cmd = Command::Put(Cow::Borrowed("key"), vec![1, 2, 3], 100);
        let data = BinarySerializer::serialize_command(&cmd);
        assert_eq!(data[0], 2u8);
        let key_len = u16::from_be_bytes([data[1], data[2]]);
        assert_eq!(key_len, 3);
        assert_eq!(&data[3..6], b"key");
        let value_len = u32::from_be_bytes([data[6], data[7], data[8], data[9]]);
        assert_eq!(value_len, 3);
        assert_eq!(&data[10..13], &[1, 2, 3]);
        let ttl = u32::from_be_bytes([data[13], data[14], data[15], data[16]]);
        assert_eq!(ttl, 100);
    }

    #[test]
    fn test_deserialize_put() {
        let mut data = vec![2u8, 0, 3];
        data.extend_from_slice(b"key");
        data.extend_from_slice(&3u32.to_be_bytes()); // value length
        data.extend_from_slice(&[1, 2, 3]); // value
        data.extend_from_slice(&100u32.to_be_bytes()); // ttl
        let cmd = BinarySerializer::deserialize_command(&data).unwrap();
        match cmd {
            Command::Put(key, value, ttl) => {
                assert_eq!(key, "key");
                assert_eq!(value, vec![1, 2, 3]);
                assert_eq!(ttl, 100);
            }
            _ => panic!("Expected Put command"),
        }
    }

    #[test]
    fn test_serialize_delete() {
        let cmd = Command::Delete(Cow::Borrowed("key"));
        let data = BinarySerializer::serialize_command(&cmd);
        assert_eq!(data[0], 3u8);
        assert_eq!(u16::from_be_bytes([data[1], data[2]]), 3);
        assert_eq!(&data[3..], b"key");
    }

    #[test]
    fn test_deserialize_delete() {
        let mut data = vec![3u8, 0, 3];
        data.extend_from_slice(b"key");
        let cmd = BinarySerializer::deserialize_command(&data).unwrap();
        match cmd {
            Command::Delete(key) => assert_eq!(key, "key"),
            _ => panic!("Expected Delete command"),
        }
    }

    #[test]
    fn test_serialize_peer() {
        let cmd = Command::Peer;
        let data = BinarySerializer::serialize_command(&cmd);
        assert_eq!(data, vec![4u8]);
    }

    #[test]
    fn test_deserialize_peer() {
        let data = vec![4u8];
        let cmd = BinarySerializer::deserialize_command(&data).unwrap();
        match cmd {
            Command::Peer => {}
            _ => panic!("Expected Peer command"),
        }
    }

    #[test]
    fn test_serialize_stats() {
        let cmd = Command::Stats;
        let data = BinarySerializer::serialize_command(&cmd);
        assert_eq!(data, vec![5u8]);
    }

    #[test]
    fn test_deserialize_stats() {
        let data = vec![5u8];
        let cmd = BinarySerializer::deserialize_command(&data).unwrap();
        match cmd {
            Command::Stats => {}
            _ => panic!("Expected Stats command"),
        }
    }

    #[test]
    fn test_serialize_response_ok() {
        let resp = Response::Ok(vec![1, 2, 3]);
        let data = BinarySerializer::serialize_response(&resp);
        assert_eq!(data[0], 0u8);
        assert_eq!(u32::from_be_bytes([data[1], data[2], data[3], data[4]]), 3);
        assert_eq!(&data[5..], &[1, 2, 3]);
    }

    #[test]
    fn test_deserialize_response_ok() {
        let mut data = vec![0u8];
        data.extend_from_slice(&3u32.to_be_bytes());
        data.extend_from_slice(&[1, 2, 3]);
        let resp = BinarySerializer::deserialize_response(&data).unwrap();
        match resp {
            Response::Ok(value) => assert_eq!(value, vec![1, 2, 3]),
            _ => panic!("Expected Ok response"),
        }
    }

    #[test]
    fn test_serialize_response_error() {
        let resp = Response::Error("test error".to_string());
        let data = BinarySerializer::serialize_response(&resp);
        assert_eq!(data[0], 1u8);
        assert_eq!(u16::from_be_bytes([data[1], data[2]]), 10);
        assert_eq!(&data[3..], b"test error");
    }

    #[test]
    fn test_deserialize_response_error() {
        let mut data = vec![1u8];
        data.extend_from_slice(&10u16.to_be_bytes());
        data.extend_from_slice(b"test error");
        let resp = BinarySerializer::deserialize_response(&data).unwrap();
        match resp {
            Response::Error(msg) => assert_eq!(msg, "test error"),
            _ => panic!("Expected Error response"),
        }
    }

    #[test]
    fn test_serialize_response_pong() {
        let resp = Response::Pong;
        let data = BinarySerializer::serialize_response(&resp);
        assert_eq!(data, vec![2u8]);
    }

    #[test]
    fn test_deserialize_response_pong() {
        let data = vec![2u8];
        let resp = BinarySerializer::deserialize_response(&data).unwrap();
        match resp {
            Response::Pong => {}
            _ => panic!("Expected Pong response"),
        }
    }

    #[test]
    fn test_deserialize_empty_command() {
        let result = BinarySerializer::deserialize_command(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_unknown_command() {
        let data = vec![99u8];
        let result = BinarySerializer::deserialize_command(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_get_incomplete() {
        let data = vec![1u8, 0, 10]; // Says key is 10 bytes but only 3 bytes total
        let result = BinarySerializer::deserialize_command(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_put_incomplete_key() {
        let data = vec![2u8, 0, 10]; // Says key is 10 bytes but only 3 bytes total
        let result = BinarySerializer::deserialize_command(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_put_incomplete_value_len() {
        let mut data = vec![2u8, 0, 3];
        data.extend_from_slice(b"key");
        data.push(0); // Incomplete value length
        let result = BinarySerializer::deserialize_command(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_put_incomplete_value() {
        let mut data = vec![2u8, 0, 3];
        data.extend_from_slice(b"key");
        data.extend_from_slice(&10u32.to_be_bytes()); // Says value is 10 bytes
        data.push(1); // But only 1 byte provided
        let result = BinarySerializer::deserialize_command(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_delete_incomplete() {
        let data = vec![3u8, 0, 10]; // Says key is 10 bytes but only 3 bytes total
        let result = BinarySerializer::deserialize_command(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_response_empty() {
        let result = BinarySerializer::deserialize_response(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_response_unknown() {
        let data = vec![99u8];
        let result = BinarySerializer::deserialize_response(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_response_ok_incomplete() {
        let data = vec![0u8, 0, 0, 0, 10]; // Says data is 10 bytes but only 5 bytes total
        let result = BinarySerializer::deserialize_response(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_response_error_incomplete() {
        let data = vec![1u8, 0, 10]; // Says message is 10 bytes but only 3 bytes total
        let result = BinarySerializer::deserialize_response(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_round_trip_all_commands() {
        let commands = vec![
            Command::Ping,
            Command::Get(Cow::Borrowed("test")),
            Command::Put(Cow::Borrowed("key"), vec![1, 2, 3], 100),
            Command::Delete(Cow::Borrowed("key")),
            Command::Peer,
            Command::Stats,
        ];

        for cmd in commands {
            let serialized = BinarySerializer::serialize_command(&cmd);
            let deserialized = BinarySerializer::deserialize_command(&serialized).unwrap();
            match (&cmd, &deserialized) {
                (Command::Ping, Command::Ping) => {}
                (Command::Get(k1), Command::Get(k2)) => assert_eq!(k1, k2),
                (Command::Put(k1, v1, t1), Command::Put(k2, v2, t2)) => {
                    assert_eq!(k1, k2);
                    assert_eq!(v1, v2);
                    assert_eq!(t1, t2);
                }
                (Command::Delete(k1), Command::Delete(k2)) => assert_eq!(k1, k2),
                (Command::Peer, Command::Peer) => {}
                (Command::Stats, Command::Stats) => {}
                _ => panic!("Round trip failed for {:?}", cmd),
            }
        }
    }

    #[test]
    fn test_round_trip_all_responses() {
        let responses = vec![
            Response::Ok(vec![1, 2, 3]),
            Response::Error("test".to_string()),
            Response::Pong,
        ];

        for resp in responses {
            let serialized = BinarySerializer::serialize_response(&resp);
            let deserialized = BinarySerializer::deserialize_response(&serialized).unwrap();
            match (&resp, &deserialized) {
                (Response::Ok(v1), Response::Ok(v2)) => assert_eq!(v1, v2),
                (Response::Error(m1), Response::Error(m2)) => assert_eq!(m1, m2),
                (Response::Pong, Response::Pong) => {}
                _ => panic!("Round trip failed for {:?}", resp),
            }
        }
    }
}
