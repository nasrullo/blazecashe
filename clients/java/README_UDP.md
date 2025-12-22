# Java UDP Client with QUIC-like Features

This document describes the UDP client implementation with QUIC-like features for the BlazeCache Java client.

## Features

The UDP client implements several QUIC-inspired optimizations:

1. **Fragmentation and Reassembly** (QUIC Datagram Splitting)
   - Large messages are automatically split into multiple UDP datagrams (fragments)
   - Each fragment contains a header with sequence number, fragment count, and payload length
   - Fragments are reassembled on the receiving end
   - Supports up to 65,535 fragments per message
   - Maximum message size: 4MB (configurable)

2. **Request ID-based Multiplexing** (QUIC Connection ID)
   - Each request has a unique 32-bit request ID
   - Allows multiple concurrent requests on the same UDP socket
   - Responses are matched to requests by request ID

3. **Fast Path for Small Messages** (QUIC 0-RTT Optimization)
   - Messages that fit in a single UDP datagram (< 1200 bytes) bypass fragmentation
   - Direct encoding/decoding without fragment headers
   - Reduces overhead for common small operations (GET, PUT with small values)

4. **Automatic Retry Logic**
   - Timeout-based request handling (5 seconds default)
   - Automatic cleanup of expired reassembly entries (2 seconds)

## Protocol Format

### Single-Datagram Format (Fast Path)
```
[0-1]   Magic (0xBC01)
[2]     Version (1)
[3]     Flags (0 = Request, 1 = Response)
[4-7]   Request ID (u32, big-endian)
[8]     Command (0x01=GET, 0x02=PUT, 0x03=DELETE, 0x04=PING)
[9+]    Command-specific data
```

### Fragment Format (Multi-Datagram)
```
[0-1]   Magic (0xBC01)
[2]     Version (1)
[3]     Flags (bit 0 = Response, other bits reserved)
[4-7]   Request ID (u32, big-endian)
[8-9]   Sequence Number (u16, big-endian, 0-indexed)
[10-11] Fragment Count (u16, big-endian, total fragments)
[12-13] Payload Length (u16, big-endian, bytes in this fragment)
[14+]   Payload data
```

## Usage

### Basic Example

```java
import com.blazecache.UDPClient;
import java.util.Optional;

// Create UDP client
UDPClient client = new UDPClient("127.0.0.1:6793");

try {
    // PING
    client.ping();
    
    // PUT
    client.set("key1", "value1".getBytes());
    
    // GET
    Optional<byte[]> value = client.get("key1");
    if (value.isPresent()) {
        System.out.println(new String(value.get()));
    }
    
    // DELETE
    boolean deleted = client.delete("key1");
    
} catch (IOException e) {
    e.printStackTrace();
} finally {
    client.close();
}
```

### Large Message Example (Automatic Fragmentation)

```java
// Large messages are automatically fragmented
byte[] largeData = new byte[5000]; // Will be split into multiple fragments
client.set("large-key", largeData);

Optional<byte[]> result = client.get("large-key");
// Fragments are automatically reassembled
```

## Comparison with TCP Client

| Feature | TCP Client | UDP Client |
|---------|-----------|------------|
| Transport | TCP (reliable) | UDP (best effort) |
| Connection Pooling | Yes | No (stateless) |
| Fragmentation | No (TCP handles) | Yes (QUIC-like) |
| Fast Path | No | Yes (single datagram) |
| Overhead | Higher | Lower (~9 bytes header) |
| Latency | Higher | Lower |
| Throughput | Good | Excellent |

## Performance Characteristics

- **Small Messages**: Single UDP packet, minimal overhead (~9 bytes header)
- **Large Messages**: Multiple UDP packets, automatic fragmentation/reassembly
- **Overhead**: ~14 bytes per fragment (fragment header)
- **Socket Buffers**: 4MB receive/send buffers for high throughput
- **Timeout**: 5 seconds for requests, 2 seconds for reassembly cleanup

## Thread Safety

The UDP client is thread-safe and can be used concurrently from multiple threads. Each request gets a unique request ID, allowing concurrent operations on the same socket.

## Error Handling

- `IOException` is thrown for network errors, timeouts, and protocol errors
- GET operations return `Optional.empty()` if the key is not found
- DELETE operations return `false` if the key is not found
- PUT operations throw `IOException` on failure

## Limitations

- UDP is best-effort delivery (no guaranteed delivery like TCP)
- No built-in retransmission (relies on application-level retries)
- Maximum message size: 4MB (configurable)
- Maximum fragments: 65,535 per message

## Testing

Run the test client:

```bash
cd clients/java
mvn compile exec:java -Dexec.mainClass="com.blazecache.TestUDPClient" -Dexec.args="127.0.0.1:6793"
```

Or compile and run manually:

```bash
javac -d target/classes src/main/java/com/blazecache/*.java
java -cp target/classes com.blazecache.TestUDPClient 127.0.0.1:6793
```

