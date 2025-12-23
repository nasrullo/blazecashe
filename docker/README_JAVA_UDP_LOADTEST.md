# Java UDP Load Test with Docker Compose

This setup runs a distributed load test using 5 Java UDP client containers against 5 BlazeCache servers (one client per server).

## Quick Start

```bash
cd docker
./run-java-udp-loadtest.sh
```

## Configuration

You can customize the load test by setting environment variables:

```bash
CONCURRENCY=100 VALUE_SIZE=2048 DURATION=120 INTERVAL=5 ./run-java-udp-loadtest.sh
```

- `CONCURRENCY`: Number of concurrent operations per client (default: 100)
- `VALUE_SIZE`: Size of values in bytes (default: 1024)
- `DURATION`: Test duration in seconds (default: 60)
- `INTERVAL`: Stats reporting interval in seconds (default: 5)

**Total Load**: With 5 clients and default concurrency of 100, you get 500 concurrent operations.

## Manual Docker Compose Usage

### Start everything:
```bash
cd docker
docker compose -f docker-compose-java-udp-loadtest.yml up --build
```

### Start in background:
```bash
docker compose -f docker-compose-java-udp-loadtest.yml up -d --build
```

### View logs:
```bash
# All clients
docker compose -f docker-compose-java-udp-loadtest.yml logs -f

# Specific client
docker compose -f docker-compose-java-udp-loadtest.yml logs -f loadtest-java-udp-1

# Server
docker compose -f docker-compose-java-udp-loadtest.yml logs -f blazecache-server
```

### Stop everything:
```bash
docker compose -f docker-compose-java-udp-loadtest.yml down
```

### Stop and remove volumes:
```bash
docker compose -f docker-compose-java-udp-loadtest.yml down -v
```

## Architecture

```
┌─────────────────────┐      ┌─────────────────────┐
│  BlazeCache Server 1│      │  BlazeCache Server 2│
│   (UDP Port 6793)   │      │   (UDP Port 6793)   │
└──────────┬──────────┘      └──────────┬──────────┘
           │                             │
           └─── loadtest-java-udp-1      └─── loadtest-java-udp-2
                (100 concurrent ops)          (100 concurrent ops)

┌─────────────────────┐      ┌─────────────────────┐
│  BlazeCache Server 3│      │  BlazeCache Server 4│
│   (UDP Port 6793)   │      │   (UDP Port 6793)   │
└──────────┬──────────┘      └──────────┬──────────┘
           │                             │
           └─── loadtest-java-udp-3      └─── loadtest-java-udp-4
                (100 concurrent ops)          (100 concurrent ops)

┌─────────────────────┐
│  BlazeCache Server 5│
│   (UDP Port 6793)   │
└──────────┬──────────┘
           │
           └─── loadtest-java-udp-5
                (100 concurrent ops)
```

**Total**: 5 servers, each handling 100 concurrent operations = 500 total concurrent operations across the cluster.

**Server Ports** (host:container):
- Server 1: TCP 6784:6784, UDP 6793:6793
- Server 2: TCP 6785:6784, UDP 6794:6793
- Server 3: TCP 6786:6784, UDP 6795:6793
- Server 4: TCP 6787:6784, UDP 6796:6793
- Server 5: TCP 6788:6784, UDP 6797:6793

## What Gets Tested

Each client performs:
1. **PUT operations**: Setting keys with configurable value sizes
2. **GET operations**: Retrieving the same keys immediately after PUT
3. **Statistics**: Reports RPS, latency, errors every N seconds

The test runs for the specified duration, then prints final statistics.

## Expected Output

Each client will output:
```
=== Java UDP Client Performance Test ===
Server: blazecache-server:6793
Concurrency: 100
Value size: 1024 bytes
Duration: 60 seconds
Interval: 5 seconds

Waiting for server to be ready...
✓ Server is ready (attempt 1)

[Stats] Ops: 1250, Errors: 0, RPS: 250.00, Elapsed: 5s
[Stats] Ops: 2500, Errors: 0, RPS: 250.00, Elapsed: 10s
...

=== Final Results ===
Total operations: 15000
Errors: 0
Time elapsed: 60.00s
Throughput: 250.00 ops/sec
Avg PUT latency: 2.50 ms
Avg GET latency: 2.30 ms
```

## Troubleshooting

### Clients fail to connect
- Ensure all servers are healthy: `docker compose -f docker-compose-java-udp-loadtest.yml ps`
- Check server logs: `docker compose -f docker-compose-java-udp-loadtest.yml logs blazecache-server-1`
- Each client connects to a specific server (client-1 → server-1, etc.)

### Low throughput
- Increase UDP buffer sizes (already configured in docker-compose)
- Check system limits: `ulimit -n`
- Monitor system resources: `docker stats`

### Java compilation errors
- Ensure Docker can access the Java source files
- Check Dockerfile.buildtest for correct paths

## Performance Tuning

For higher load:
1. Increase `CONCURRENCY` per client (e.g., 200)
2. Increase number of client containers (edit docker-compose file)
3. Adjust UDP buffer sizes in docker-compose sysctls
4. Use larger value sizes to test fragmentation

## Files

- `docker-compose-java-udp-loadtest.yml`: Docker Compose configuration
- `run-java-udp-loadtest.sh`: Convenience script to run the test
- `clients/java/Dockerfile.loadtest`: Dockerfile for Java UDP client
- `clients/java/src/main/java/com/blazecache/UDPLoadTest.java`: Load test implementation

