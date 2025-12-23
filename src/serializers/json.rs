use crate::transports::common::{Command, Response};
use crate::transports::Serializer;
use base64::{engine::general_purpose, Engine as _};
use serde_json;
use std::borrow::Cow;

pub struct JsonSerializer;

impl Serializer for JsonSerializer {
    fn serialize_command<'a>(cmd: &Command<'a>) -> Vec<u8> {
        let json = match cmd {
            Command::Get(key) => serde_json::json!({
                "type": "get",
                "key": key
            }),
            Command::Put(key, value, ttl) => serde_json::json!({
                "type": "put",
                "key": key,
                "value": general_purpose::STANDARD.encode(value),
                "ttl": ttl
            }),
            Command::Delete(key) => serde_json::json!({
                "type": "delete",
                "key": key
            }),
            Command::Peer => serde_json::json!({
                "type": "peer"
            }),
            Command::Ping => serde_json::json!({
                "type": "ping"
            }),
            Command::Stats => serde_json::json!({
                "type": "stats"
            }),
            Command::Clear => serde_json::json!({
                "type": "clear"
            }),
        };
        json.to_string().into_bytes()
    }

    fn deserialize_command(
        data: &[u8],
    ) -> Result<Command<'static>, Box<dyn std::error::Error + Send + Sync>> {
        let json: serde_json::Value = serde_json::from_slice(data)?;

        match json["type"].as_str() {
            Some("ping") => Ok(Command::Ping),
            Some("get") => {
                let key = json["key"].as_str().ok_or("Missing key")?.to_string();
                Ok(Command::Get(Cow::Owned(key)))
            }
            Some("put") => {
                let key = json["key"].as_str().ok_or("Missing key")?.to_string();
                let value_b64 = json["value"].as_str().ok_or("Missing value")?;
                let value = general_purpose::STANDARD.decode(value_b64)?;
                let ttl:u32 = if let Some(ttl) = json.get("ttl").and_then(|v| v.as_u64()) {
                    ttl as u32
                } else {
                    0
                };

                Ok(Command::Put(Cow::Owned(key), value, ttl))
            }
            Some("delete") => {
                let key = json["key"].as_str().ok_or("Missing key")?.to_string();
                Ok(Command::Delete(Cow::Owned(key)))
            }
            Some("peer") => Ok(Command::Peer),
            Some("stats") => Ok(Command::Stats),
            Some("clear") => Ok(Command::Clear),
            _ => Err("Unknown command type".into()),
        }
    }

    fn serialize_response(resp: &Response) -> Vec<u8> {
        let json = match resp {
            Response::Ok(data) => serde_json::json!({
                "status": "ok",
                "data": general_purpose::STANDARD.encode(data)
            }),
            Response::Error(msg) => serde_json::json!({
                "status": "error",
                "message": msg
            }),
            Response::Pong => serde_json::json!({
                "status": "pong"
            }),
        };
        json.to_string().into_bytes()
    }

    fn deserialize_response(
        data: &[u8],
    ) -> Result<Response, Box<dyn std::error::Error + Send + Sync>> {
        let json: serde_json::Value = serde_json::from_slice(data)?;

        match json["status"].as_str() {
            Some("ok") => {
                let data_b64 = json["data"].as_str().ok_or("Missing data")?;
                let data = general_purpose::STANDARD.decode(data_b64)?;
                Ok(Response::Ok(data))
            }
            Some("error") => {
                let msg = json["message"]
                    .as_str()
                    .ok_or("Missing message")?
                    .to_string();
                Ok(Response::Error(msg))
            }
            Some("pong") => Ok(Response::Pong),
            _ => Err("Unknown response status".into()),
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
        let data = JsonSerializer::serialize_command(&cmd);
        let json: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(json["type"], "ping");
    }

    #[test]
    fn test_deserialize_ping() {
        let json = r#"{"type":"ping"}"#;
        let cmd = JsonSerializer::deserialize_command(json.as_bytes()).unwrap();
        match cmd {
            Command::Ping => {}
            _ => panic!("Expected Ping command"),
        }
    }

    #[test]
    fn test_serialize_get() {
        let cmd = Command::Get(Cow::Borrowed("test"));
        let data = JsonSerializer::serialize_command(&cmd);
        let json: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(json["type"], "get");
        assert_eq!(json["key"], "test");
    }

    #[test]
    fn test_deserialize_get() {
        let json = r#"{"type":"get","key":"test"}"#;
        let cmd = JsonSerializer::deserialize_command(json.as_bytes()).unwrap();
        match cmd {
            Command::Get(key) => assert_eq!(key, "test"),
            _ => panic!("Expected Get command"),
        }
    }

    #[test]
    fn test_serialize_put() {
        let cmd = Command::Put(Cow::Borrowed("key"), vec![1, 2, 3], 100);
        let data = JsonSerializer::serialize_command(&cmd);
        let json: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(json["type"], "put");
        assert_eq!(json["key"], "key");
        assert_eq!(json["ttl"], 100);
        let value = general_purpose::STANDARD.decode(json["value"].as_str().unwrap()).unwrap();
        assert_eq!(value, vec![1, 2, 3]);
    }

    #[test]
    fn test_deserialize_put() {
        let json = r#"{"type":"put","key":"key","value":"AQID","ttl":100}"#;
        let cmd = JsonSerializer::deserialize_command(json.as_bytes()).unwrap();
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
    fn test_deserialize_put_no_ttl() {
        let json = r#"{"type":"put","key":"key","value":"AQID"}"#;
        let cmd = JsonSerializer::deserialize_command(json.as_bytes()).unwrap();
        match cmd {
            Command::Put(key, value, ttl) => {
                assert_eq!(key, "key");
                assert_eq!(value, vec![1, 2, 3]);
                assert_eq!(ttl, 0);
            }
            _ => panic!("Expected Put command"),
        }
    }

    #[test]
    fn test_serialize_delete() {
        let cmd = Command::Delete(Cow::Borrowed("key"));
        let data = JsonSerializer::serialize_command(&cmd);
        let json: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(json["type"], "delete");
        assert_eq!(json["key"], "key");
    }

    #[test]
    fn test_deserialize_delete() {
        let json = r#"{"type":"delete","key":"key"}"#;
        let cmd = JsonSerializer::deserialize_command(json.as_bytes()).unwrap();
        match cmd {
            Command::Delete(key) => assert_eq!(key, "key"),
            _ => panic!("Expected Delete command"),
        }
    }

    #[test]
    fn test_serialize_peer() {
        let cmd = Command::Peer;
        let data = JsonSerializer::serialize_command(&cmd);
        let json: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(json["type"], "peer");
    }

    #[test]
    fn test_deserialize_peer() {
        let json = r#"{"type":"peer"}"#;
        let cmd = JsonSerializer::deserialize_command(json.as_bytes()).unwrap();
        match cmd {
            Command::Peer => {}
            _ => panic!("Expected Peer command"),
        }
    }

    #[test]
    fn test_serialize_stats() {
        let cmd = Command::Stats;
        let data = JsonSerializer::serialize_command(&cmd);
        let json: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(json["type"], "stats");
    }

    #[test]
    fn test_deserialize_stats() {
        let json = r#"{"type":"stats"}"#;
        let cmd = JsonSerializer::deserialize_command(json.as_bytes()).unwrap();
        match cmd {
            Command::Stats => {}
            _ => panic!("Expected Stats command"),
        }
    }

    #[test]
    fn test_serialize_response_ok() {
        let resp = Response::Ok(vec![1, 2, 3]);
        let data = JsonSerializer::serialize_response(&resp);
        let json: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(json["status"], "ok");
        let data_decoded = general_purpose::STANDARD.decode(json["data"].as_str().unwrap()).unwrap();
        assert_eq!(data_decoded, vec![1, 2, 3]);
    }

    #[test]
    fn test_deserialize_response_ok() {
        let json = r#"{"status":"ok","data":"AQID"}"#;
        let resp = JsonSerializer::deserialize_response(json.as_bytes()).unwrap();
        match resp {
            Response::Ok(value) => assert_eq!(value, vec![1, 2, 3]),
            _ => panic!("Expected Ok response"),
        }
    }

    #[test]
    fn test_serialize_response_error() {
        let resp = Response::Error("test error".to_string());
        let data = JsonSerializer::serialize_response(&resp);
        let json: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(json["status"], "error");
        assert_eq!(json["message"], "test error");
    }

    #[test]
    fn test_deserialize_response_error() {
        let json = r#"{"status":"error","message":"test error"}"#;
        let resp = JsonSerializer::deserialize_response(json.as_bytes()).unwrap();
        match resp {
            Response::Error(msg) => assert_eq!(msg, "test error"),
            _ => panic!("Expected Error response"),
        }
    }

    #[test]
    fn test_serialize_response_pong() {
        let resp = Response::Pong;
        let data = JsonSerializer::serialize_response(&resp);
        let json: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(json["status"], "pong");
    }

    #[test]
    fn test_deserialize_response_pong() {
        let json = r#"{"status":"pong"}"#;
        let resp = JsonSerializer::deserialize_response(json.as_bytes()).unwrap();
        match resp {
            Response::Pong => {}
            _ => panic!("Expected Pong response"),
        }
    }

    #[test]
    fn test_deserialize_invalid_json() {
        let data = b"not json";
        let result = JsonSerializer::deserialize_command(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_unknown_command_type() {
        let json = r#"{"type":"unknown"}"#;
        let result = JsonSerializer::deserialize_command(json.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_get_missing_key() {
        let json = r#"{"type":"get"}"#;
        let result = JsonSerializer::deserialize_command(json.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_put_missing_key() {
        let json = r#"{"type":"put","value":"AQID"}"#;
        let result = JsonSerializer::deserialize_command(json.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_put_missing_value() {
        let json = r#"{"type":"put","key":"key"}"#;
        let result = JsonSerializer::deserialize_command(json.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_put_invalid_base64() {
        let json = r#"{"type":"put","key":"key","value":"invalid!"}"#;
        let result = JsonSerializer::deserialize_command(json.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_response_unknown_status() {
        let json = r#"{"status":"unknown"}"#;
        let result = JsonSerializer::deserialize_response(json.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_response_ok_missing_data() {
        let json = r#"{"status":"ok"}"#;
        let result = JsonSerializer::deserialize_response(json.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_response_error_missing_message() {
        let json = r#"{"status":"error"}"#;
        let result = JsonSerializer::deserialize_response(json.as_bytes());
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
            let serialized = JsonSerializer::serialize_command(&cmd);
            let deserialized = JsonSerializer::deserialize_command(&serialized).unwrap();
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
            let serialized = JsonSerializer::serialize_response(&resp);
            let deserialized = JsonSerializer::deserialize_response(&serialized).unwrap();
            match (&resp, &deserialized) {
                (Response::Ok(v1), Response::Ok(v2)) => assert_eq!(v1, v2),
                (Response::Error(m1), Response::Error(m2)) => assert_eq!(m1, m2),
                (Response::Pong, Response::Pong) => {}
                _ => panic!("Round trip failed for {:?}", resp),
            }
        }
    }
}
