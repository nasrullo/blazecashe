# BlazeCache

A high-performance, distributed cache server implementation in Rust with automatic peer discovery, persistence, and multi-language client support.

## Quick Start

### Install & Run (Like Memcached)
```bash
# Build and install
./install.sh

# Start server (like memcached)
blazecache -p 6784 -m 64

# Or with custom settings
blazecache -p 6784 -m 128 -d  # daemon mode

# With persistence enabled
blazecache -p 6784 -m 64 -w

# With gossip protocol for automatic peer discovery
blazecache -p 6784 -m 64 --gossip --gossip-seed 192.168.1.100:6784

# As systemd service
sudo systemctl start blazecache
sudo systemctl enable blazecache
```

### Command Line Options
```bash
blazecache [OPTIONS]

OPTIONS:
    -p, --port <PORT>              Port to listen on [default: 6784]
    -m, --memory <MB>              Memory limit in MB [default: 64]
    -d, --daemon                   Run as daemon
    
    Persistence Options (requires -w/--wal):
        -w, --wal                      Enable persistence (WAL + recovery)
        --data-dir <DIR>              Persistence data directory [default: ./cache_data]
        --snapshot-interval <S>       Snapshot interval in seconds [default: 300]
        --wal-max-mb <MB>             Max WAL size before rotation [default: 100]
        --wal-disabled                Disable WAL (snapshots only)
        --no-compress-snapshots       Disable snapshot compression
    
    Gossip Protocol Options (for peer discovery):
        --gossip                      Enable gossip protocol for peer discovery
        --gossip-port <PORT>          Gossip UDP port [default: cache_port + 1]
        --gossip-interval <S>         Gossip interval in seconds [default: 1]
        --gossip-fanout <N>           Number of peers to contact per round [default: 3]
        --gossip-seed <HOST:PORT>     Seed node for bootstrap (can specify multiple)
        --gossip-suspicion-timeout <S> Time before marking peer inactive [default: 15]
        --gossip-failure-timeout <S>  Time before marking peer failed [default: 30]
        --gossip-failure-check-interval <S> Failure check interval [default: 5]
    
    Logging Options:
        --log-level <LEVEL>          Set log level (trace, debug, info, warn, error) [default: info]
                                     Or use RUST_LOG environment variable
    
    -h, --help                       Print help information
    -v, --version                    Print version information
```

## Features

- **Size-limited LRU cache** with individual item size validation
- **Binary protocol** for efficient network communication
- **Automatic peer discovery** via gossip protocol
- **Persistence** with write-ahead logging (WAL) and snapshots
- **Multi-language clients** (Rust, Go, Java)
- **Async operations** with tokio
- **Memory safety** with automatic size validation
- **Load balancing** with consistent hashing
- **Structured logging** with configurable log levels
- **Fault tolerance** with automatic failure detection

## Gossip Protocol

BlazeCache includes a gossip-based membership protocol for automatic peer discovery in distributed clusters. No manual peer configuration needed!

### How It Works

1. **Automatic Discovery**: Nodes periodically exchange membership information with random peers
2. **Eventually Consistent**: All nodes eventually learn about all peers in the cluster
3. **Fault Tolerant**: Handles network partitions and node failures gracefully
4. **Lightweight**: Uses UDP for efficient gossip messages

### Usage Examples

```bash
# Start with gossip enabled and seed nodes
blazecache --gossip \
  --gossip-seed 192.168.1.100:6784 \
  --gossip-seed 192.168.1.101:6784

# Custom gossip configuration
blazecache --gossip \
  --gossip-interval 2 \
  --gossip-fanout 5 \
  --gossip-failure-timeout 45

# Debug gossip activity
RUST_LOG=blazecache::networking::gossip=debug blazecache --gossip
```

### Gossip Configuration

- **gossip-interval**: How often to run gossip rounds (default: 1 second)
- **fanout**: Number of random peers to contact per round (default: 3)
- **suspicion-timeout**: Time before marking peer as inactive (default: 15 seconds)
- **failure-timeout**: Time before marking peer as failed (default: 30 seconds)
- **failure-check-interval**: How often to check for failures (default: 5 seconds)

## Persistence

BlazeCache supports optional persistence with write-ahead logging (WAL) and periodic snapshots for crash recovery.

### Features

- **Write-Ahead Logging**: All writes are logged before being applied
- **Periodic Snapshots**: Full cache dumps at configurable intervals
- **Automatic Recovery**: Restores cache state on startup
- **Configurable**: Customizable data directory, intervals, and compression

### Usage

```bash
# Enable persistence with defaults
blazecache -w

# Custom persistence configuration
blazecache -w \
  --data-dir /var/lib/blazecache \
  --snapshot-interval 600 \
  --wal-max-mb 200

# Snapshots only (no WAL)
blazecache -w --wal-disabled
```

## Logging

BlazeCache uses structured logging with the `tracing` framework. Log levels can be configured via CLI or environment variable.

### Log Levels

- **trace**: Maximum verbosity (ping/pong messages, detailed operations)
- **debug**: Debug information (gossip rounds, peer discovery)
- **info**: General information (server startup, peer status changes)
- **warn**: Warnings (peer failures, recoverable errors)
- **error**: Errors (connection failures, unrecoverable errors)

### Usage

```bash
# Default info level
blazecache --gossip

# Debug level for detailed activity
blazecache --gossip --log-level debug

# Trace level for maximum verbosity
blazecache --gossip --log-level trace

# Environment variable
RUST_LOG=debug blazecache --gossip

# Module-specific logging
RUST_LOG=blazecache::networking::gossip=debug,blazecache::transports=info blazecache
```

### Log Output Format

Structured logs include contextual fields:
```
INFO Starting blazecache server port=6784 memory_mb=64
INFO Gossip protocol enabled gossip_port=6785
DEBUG Received membership message peer_id=192.168.1.100:6784
INFO Discovered new peer peer_id=192.168.1.100:6784 peer_address=192.168.1.100 peer_port=6784
WARN Marking peer as unreachable peer_id=192.168.1.101:6784 last_seen_seconds=35
```

## Performance

Real-world TCP benchmark results:
- **1.7x faster than Redis** (3.7ms vs 2.1ms per operation)
- **Equal to Memcached** (~2.1ms per operation)
- **Network-bound performance** - TCP latency dominates

## Protocol

Binary protocol specification in [PROTOCOL.md](PROTOCOL.md):
```
Request:  [length:u32][command:u8][key_len:u16][key][data_len:u32][data]
Response: [length:u32][status:u8][msg_len:u16][message][data_len:u32][data]
```

Commands: GET, PUT, DELETE, PING, STATS, PEER

## Client Libraries

### Rust
```bash
cd clients/rust && cargo build
```

### Go
```bash
cd clients/go && go build
```

### Java (JDK 25+)
```bash
cd clients/java && mvn compile
```

All clients support:
- Binary protocol compatibility
- Round robin, weighted, and consistent hashing
- Connection management and error handling
- Batch operations

## Testing

```bash
# Run tests
cargo test

# Run benchmarks
cargo bench

# Test with real servers
./corrected_benchmark.sh

# Test gossip protocol
cargo test gossip
```

## Architecture

- **Cache**: LRU eviction with size limits
- **Protocol**: Efficient binary encoding
- **Network**: Async TCP/UDP with connection pooling
- **Gossip**: Automatic peer discovery and membership management
- **Persistence**: WAL and snapshots for durability
- **Clients**: Shared protocol across languages
- **Memory**: Size validation prevents bloat
- **Logging**: Structured logging with configurable levels

## Distributed Cluster Setup

### Example: 3-Node Cluster

**Node 1** (192.168.1.100):
```bash
blazecache -p 6784 -m 256 --gossip
```

**Node 2** (192.168.1.101):
```bash
blazecache -p 6784 -m 256 --gossip \
  --gossip-seed 192.168.1.100:6784
```

**Node 3** (192.168.1.102):
```bash
blazecache -p 6784 -m 256 --gossip \
  --gossip-seed 192.168.1.100:6784 \
  --gossip-seed 192.168.1.101:6784
```

Once started, all nodes will automatically discover each other via gossip protocol.

## Production Deployment

### Recommended Configuration

```bash
# Production server with persistence and gossip
blazecache \
  -p 6784 \
  -m 1024 \
  -d \
  -w \
  --data-dir /var/lib/blazecache \
  --snapshot-interval 300 \
  --gossip \
  --gossip-interval 2 \
  --gossip-fanout 5 \
  --gossip-failure-timeout 60 \
  --log-level info
```

### Systemd Service

Create `/etc/systemd/system/blazecache.service`:
```ini
[Unit]
Description=BlazeCache Server
After=network.target

[Service]
Type=simple
User=blazecache
ExecStart=/usr/local/bin/blazecache -p 6784 -m 1024 -d -w --gossip
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## License

MIT License
