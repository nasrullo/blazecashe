# BlazeCache Go Client

A high-performance Go client for BlazeCache, based on the Rust client implementation with connection pooling, retry logic, and peer discovery.

## Features

- ✅ **Connection Pooling**: Reuses connections for better performance (max 10 per server)
- ✅ **Automatic Retry**: Retries failed operations with exponential backoff (3 attempts)
- ✅ **TTL Support**: Set expiration times for cached values
- ✅ **Peer Discovery**: Automatic discovery and refresh of cluster peers
- ✅ **Consistent Hashing**: True consistent hash ring with 150 virtual nodes per server
- ✅ **Round-Robin**: Alternative server selection strategy
- ✅ **Connection Health Tracking**: Automatically detects and replaces dead connections
- ✅ **Thread-Safe**: Safe for concurrent use

## Installation

```bash
go get github.com/blazecache/client
```

## Usage

### Basic Usage

```go
package main

import (
    "fmt"
    blazecache "github.com/blazecache/client"
)

func main() {
    // Create a client
    client, err := blazecache.New("127.0.0.1:6784")
    if err != nil {
        panic(err)
    }
    defer client.Close()

    // Set a value (with TTL in seconds, 0 = no expiration)
    err = client.Set("key", []byte("value"), 0)
    if err != nil {
        panic(err)
    }

    // Get a value
    value, err := client.Get("key")
    if err != nil {
        if err == blazecache.ErrNotFound {
            fmt.Println("Key not found")
        } else {
            panic(err)
        }
    } else {
        fmt.Printf("Value: %s\n", string(value))
    }

    // Delete a key
    err = client.Delete("key")
    if err != nil {
        panic(err)
    }
}
```

### With TTL

```go
// Set a value with 60 second TTL
err = client.Set("key", []byte("value"), 60)
```

### Multiple Servers

```go
// Create client with multiple servers
client, err := blazecache.New(
    "127.0.0.1:6784",
    "127.0.0.1:6785",
    "127.0.0.1:6786",
)
```

### Consistent Hashing

```go
client, err := blazecache.New("127.0.0.1:6784")
client.WithStrategy(blazecache.ConsistentHashing)
```

### Peer Discovery

```go
// Automatically discover and refresh cluster peers
client, err := blazecache.WithDiscovery("127.0.0.1:6784", 30) // refresh every 30 seconds
```

### Custom Timeout

```go
client, err := blazecache.New("127.0.0.1:6784")
client.WithTimeout(10 * time.Second)
```

### Multi-Get

```go
results, err := client.GetMulti([]string{"key1", "key2", "key3"})
for key, value := range results {
    fmt.Printf("%s: %s\n", key, string(value))
}
```

### Ping

```go
err := client.Ping()
if err != nil {
    fmt.Println("Server is not responding")
}
```

## Error Handling

The client provides specific error types:

```go
if err == blazecache.ErrNotFound {
    // Key not found
} else if err == blazecache.ErrTimeout {
    // Operation timeout
} else if clientErr, ok := err.(*blazecache.ClientError); ok {
    switch clientErr.Type {
    case "IO":
        // Network/connection error
    case "Protocol":
        // Protocol error
    case "Timeout":
        // Timeout error
    }
}
```

## Connection Pooling

The client maintains a pool of connections (max 10 per server) to improve performance. Connections are automatically reused and dead connections are detected and replaced.

## Retry Logic

The client automatically retries failed operations up to 3 times with exponential backoff:
- Attempt 1: Immediate
- Attempt 2: After 20ms
- Attempt 3: After 40ms

Only retryable errors (IO errors, timeouts) are retried. Protocol errors are returned immediately.

## Performance

The Go client is optimized for high-throughput scenarios:
- Connection pooling reduces connection overhead
- Automatic retry handles transient failures
- Consistent hashing provides even key distribution
- Thread-safe for concurrent operations

## Comparison with Rust Client

This Go client is based on the Rust client implementation and provides feature parity:
- ✅ Connection pooling
- ✅ Retry logic with exponential backoff
- ✅ TTL support
- ✅ Peer discovery
- ✅ Consistent hashing
- ✅ Connection health tracking

## Examples

See the `examples/` directory for more examples:
- `test_client.go`: Comprehensive test suite
- `loadtest.go`: Load testing example
- `simple_load.go`: Simple load test

## License

MIT

