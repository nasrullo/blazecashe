use crate::transports::common::{Command, Response};
use crate::transports::Serializer;
use base64::{engine::general_purpose, Engine as _};
use serde_json;

pub struct JsonSerializer;

impl Serializer for JsonSerializer {
    fn serialize_command(cmd: &Command) -> Vec<u8> {
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
        };
        json.to_string().into_bytes()
    }

    fn deserialize_command(
        data: &[u8],
    ) -> Result<Command, Box<dyn std::error::Error + Send + Sync>> {
        let json: serde_json::Value = serde_json::from_slice(data)?;

        match json["type"].as_str() {
            Some("ping") => Ok(Command::Ping),
            Some("get") => {
                let key = json["key"].as_str().ok_or("Missing key")?.to_string();
                Ok(Command::Get(key))
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

                Ok(Command::Put(key, value, ttl))
            }
            Some("delete") => {
                let key = json["key"].as_str().ok_or("Missing key")?.to_string();
                Ok(Command::Delete(key))
            }
            Some("peer") => Ok(Command::Peer),
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
