# UDP Docker Compose Setup

This Docker Compose configuration sets up 3 BlazeCache servers with UDP support for testing UDP client improvements.

## Services

- **blazecache-udp-1**: Server 1 (TCP: 6784, UDP: 6793, Gossip: 6785)
- **blazecache-udp-2**: Server 2 (TCP: 6786, UDP: 6794, Gossip: 6787)
- **blazecache-udp-3**: Server 3 (TCP: 6788, UDP: 6795, Gossip: 6789)

## Features

- All servers run with UDP support enabled
- Gossip protocol enabled for peer discovery
- WAL persistence enabled
- Optimized UDP buffer sizes (128MB)
- Health checks configured

## Usage

### Start all services:
```bash
cd docker
docker-compose -f docker-compose-udp.yml up -d
```

### View logs:
```bash
docker-compose -f docker-compose-udp.yml logs -f
```

### Stop all services:
```bash
docker-compose -f docker-compose-udp.yml down
```

### Stop and remove volumes:
```bash
docker-compose -f docker-compose-udp.yml down -v
```

## Testing UDP Client

Once the servers are running, you can test the UDP client:

```bash
cd clients/rust
cargo run --example benchmark_udp --release
```

The benchmark will connect to `127.0.0.1:6793` (first server's UDP port).

## Port Mapping

| Container | TCP Port (Host:Container) | UDP Port (Host:Container) | Gossip Port (Host:Container) |
|-----------|---------------------------|----------------------------|------------------------------|
| blazecache-udp-1 | 6784:6784 | 6793:6793 | 6785:6785 |
| blazecache-udp-2 | 6786:6784 | 6794:6793 | 6787:6785 |
| blazecache-udp-3 | 6788:6784 | 6795:6793 | 6789:6785 |

## Network Configuration

All containers are on the `blazecache-udp-network` bridge network, allowing them to communicate with each other via gossip protocol.

## UDP Optimizations

The containers are configured with:
- `net.core.rmem_max: 134217728` (128MB receive buffer)
- `net.core.wmem_max: 134217728` (128MB send buffer)

These settings help with UDP performance, especially for large fragmented messages.

