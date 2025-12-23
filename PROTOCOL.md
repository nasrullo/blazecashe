# BlazeCache Protocol Specification

Version: 1.0  
Date: 2025-12-14

## Overview

BlazeCache uses a binary protocol over TCP and UDP (QUIC) for high-performance cache operations. All multi-byte integers are encoded in big-endian (network) byte order.

**Transport Protocols:**
- **TCP**: Reliable, connection-oriented transport
- **UDP (QUIC)**: Connection-oriented protocol built on UDP, providing reliability, multiplexing, and built-in encryption

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
| STATS   | 0x05 | Get server statistics (JSON format) |
| CLEAR   | 0x06 | Clear all entries from main and hot caches |

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

### STATS Request
```
[command_type:u8] = 0x05 (STATS)
```

### CLEAR Request
```
[command_type:u8] = 0x06 (CLEAR)
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

### STATS Response
```
[status_code:u8] = 0x00 (OK)
[data_length:u32] = N
[data:bytes] = JSON string with statistics
```

Example STATS response data:
```json
{"hits":12345,"misses":234,"puts":5678,"deletes":90,"evictions":12,"hot_items":5,"rejected_items":3,"ttl_evictions":8,"entry_count":1000,"memory_usage":10485760}
```

**Statistics Fields:**
- `hits` - Number of successful cache hits (u64)
- `misses` - Number of cache misses that triggered read-through (u64)
- `puts` - Number of items stored in cache (u64)
- `deletes` - Number of items deleted from cache (u64)
- `evictions` - Number of items evicted due to cache being full (LRU eviction) (u64)
- `hot_items` - Number of hot items currently replicated across nodes (u64)
- `rejected_items` - Number of items rejected (e.g., too large) (u64)
- `ttl_evictions` - Number of items evicted due to TTL expiration (u64)
- `entry_count` - Current number of entries in the cache (u64)
- `memory_usage` - Current memory usage in bytes (u64)

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

### UDP (QUIC)
- **Connection-oriented**: QUIC maintains persistent connections with connection state
- **Reliable delivery**: Built-in retransmission and congestion control (no manual packet loss handling needed)
- **Multiplexing**: Multiple concurrent streams per connection
- **Security**: TLS 1.3 encryption built-in
- **Automatic fragmentation**: Large messages handled automatically by QUIC
- **Low latency**: Optimized for high-performance operations
- **Connection pooling**: Clients can reuse connections for multiple requests
- **Stream-based**: Each request uses a bidirectional stream within the connection

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

**Request:**
- Command code: `0x04`
- No additional parameters required

**Response:**
- Status: `0x00` (OK)
- Data: Comma-separated list of peer addresses (e.g., "server1:6784,server2:6784")

### STATS Command
Returns comprehensive cache statistics in JSON format. Useful for monitoring cache performance, hit rates, and resource usage.

**Request:**
- Command code: `0x05`
- No additional parameters required

**Response:**
- Status: `0x00` (OK)
- Data: JSON string containing statistics (see STATS Response section above)

**Use Cases:**
- Monitoring cache hit/miss rates
- Tracking memory usage and eviction patterns
- Observing hot item replication
- Performance analysis and optimization
- Health checks and alerting

### CLEAR Command
Clears all entries from both the main cache and hot cache on the receiving node. When a node receives a CLEAR command, it also forwards the command to all other peers in the cluster to ensure consistency across all nodes.

**Request:**
- Command code: `0x06`
- No additional parameters required

**Response:**
- Status: `0x00` (OK)
- Data: Empty (operation successful)

**Behavior:**
- Clears all entries from the main cache
- Clears all entries from the hot cache
- Resets entry_count and memory_usage statistics (other stats preserved)
- Forwards CLEAR command to all peers in the cluster (excluding self)
- Peer forwarding is done asynchronously (fire-and-forget)

**Use Cases:**
- Resetting cache state during testing
- Clearing stale data after schema changes
- Freeing memory when cache becomes corrupted
- Administrative cache management

**Note:** This is a destructive operation that cannot be undone. Use with caution in production environments.

## Versioning

Protocol version is implicit in the connection. Future versions may:
- Add new command types (backward compatible)
- Add optional fields (backward compatible)
- Change message format (requires new version)

Clients should be prepared to handle unknown status codes gracefully by treating them as general errors.
