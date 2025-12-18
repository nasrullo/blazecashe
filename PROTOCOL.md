# BlazeCache Protocol Specification

Version: 1.0  
Date: 2025-12-14

## Overview

BlazeCache uses a binary protocol over TCP and UDP for high-performance cache operations. All multi-byte integers are encoded in big-endian (network) byte order.

## Message Format (BinarySerializer)

### Request Format
```
[command_type:u8][key_length:u16][key:bytes][data_length:u32][data:bytes][ttl_seconds:u32?]
```
- `ttl_seconds` is present only for PUT. It is a u32, interpreted as seconds-to-live. A value of `0` means “use default/no explicit TTL.”

### Response Format
```
[status_code:u8][data_length:u32][data:bytes]
```
Errors are encoded as:
```
[status_code:u8 = 0x01][message_length:u16][message:bytes]
```

## Command Types

| Command | Code | Description |
|---------|------|-------------|
| PING    | 0x00 | Health check |
| GET     | 0x01 | Retrieve value by key |
| PUT     | 0x02 | Store key-value pair (supports optional TTL) |
| DELETE  | 0x03 | Remove key |
| PEER    | 0x04 | List cluster peers (comma-separated list) |

## Status Codes

| Status | Code | Description |
|--------|------|-------------|
| OK     | 0x00 | Operation successful |
| ERROR  | 0x01 | Error (message included) |
| PONG   | 0x02 | Ping response |

## Request Examples

### GET Request
```
[command_type:u8] = 0x01 (GET)
[key_length:u16] = 4
[key:bytes] = "test"
```

### PUT Request
```
[command_type:u8] = 0x02 (PUT)
[key_length:u16] = 4
[key:bytes] = "test"
[data_length:u32] = 5
[data:bytes] = "value"
[ttl_seconds:u32] = 300    (0 to use default/no TTL)
```

### DELETE Request
```
[command_type:u8] = 0x03 (DELETE)
[key_length:u16] = 4
[key:bytes] = "test"
```

### PING Request
```
[command_type:u8] = 0x00 (PING)
```

### PEER Request
```
[command_type:u8] = 0x04 (PEER)
```

## Response Examples

### Successful GET Response
```
[status_code:u8] = 0x00 (OK)
[data_length:u32] = 5
[data:bytes] = "value"
```

### Key Not Found / Error Response
```
[status_code:u8] = 0x01 (ERROR)
[message_length:u16] = 9
[message:bytes] = "Not found"
```

### Successful PUT/DELETE Response
```
[status_code:u8] = 0x00 (OK)
```

### Error Response
```
[status_code:u8] = 0x01 (ERROR)
[message_length:u16] = 15
[message:bytes] = "Key too large"
```

### PONG Response
```
[status_code:u8] = 0x02 (PONG)
```

### PEER Response
```
[status_code:u8] = 0x00 (OK)
[data_length:u32] = N
[data:bytes] = "server1:6784,server2:6784"
```

## Limits

| Field | Maximum Size |
|-------|--------------|
| Key Length | 65,535 bytes (u16 max) |
| Value Length | 4,294,967,295 bytes (u32 max) |
| Message Length | 16 MB (recommended limit) |
| Error Message | 65,535 bytes (u16 max) |

## Connection Handling

### TCP
- Persistent connections supported
- Multiple requests per connection allowed
- Client should handle connection pooling
- Server may close idle connections after timeout

### UDP
- Single request-response per packet
- No connection state maintained
- Maximum packet size: 65,507 bytes
- Client should handle packet loss and retries

## Error Handling

### Client Behavior
- Retry on network errors (connection refused, timeout)
- Do not retry on protocol errors (invalid command, key too large)
- Handle partial reads/writes gracefully
- Validate response format before processing

### Server Behavior
- Return appropriate status codes for all conditions
- Include descriptive error messages when possible
- Close connection on protocol violations
- Log errors for debugging

## Special Commands

### PEER Command
Returns a comma-separated list of cluster peer addresses in the response data field. Used for cluster discovery and health monitoring.

## Versioning

Protocol version is implicit in the connection. Future versions may:
- Add new command types (backward compatible)
- Add optional fields (backward compatible)
- Change message format (requires new version)

Clients should be prepared to handle unknown status codes gracefully by treating them as general errors.
