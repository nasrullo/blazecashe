# Blazecache Docker Load Test

This guide explains how to run load tests for Blazecache using Docker.

## Prerequisites

- Docker and Docker Compose installed
- At least 4GB of free memory
- Network access for pulling Docker images

## Quick Start

### 1. Start servers and run load test

```bash
cd docker
./run-loadtest.sh
```

This will:
- Start 2 Blazecache server instances
- Wait for them to be healthy
- Run a load test with 100,000 operations using 100 workers
- Show results and server logs

### 2. Customize load test parameters

```bash
./run-loadtest.sh --ops 500000 --workers 200 --client rust
```

Options:
- `--client TYPE`: Choose client type (`rust` or `go`, default: `rust`)
- `--ops NUMBER`: Number of operations (default: 100000)
- `--workers NUMBER`: Number of worker threads (default: 100)
- `--servers-only`: Only start servers, don't run load test

### 3. Start servers only

```bash
./run-loadtest.sh --servers-only
```

Then run load test manually:

```bash
# Using Rust client
docker-compose -f docker-compose-loadtest.yml run --rm \
    -e NUM_OPS=100000 \
    -e NUM_WORKERS=100 \
    loadtest-rust

# Using Go client
docker-compose -f docker-compose-loadtest.yml run --rm \
    -e NUM_OPS=100000 \
    -e NUM_WORKERS=100 \
    loadtest-go
```

## Manual Docker Compose Usage

### Start servers

```bash
docker-compose -f docker-compose-loadtest.yml up -d blazecache-server-1 blazecache-server-2
```

### Check server status

```bash
docker-compose -f docker-compose-loadtest.yml ps
```

### View server logs

```bash
# All servers
docker-compose -f docker-compose-loadtest.yml logs -f

# Specific server
docker-compose -f docker-compose-loadtest.yml logs -f blazecache-server-1
```

### Run load test

```bash
# Rust client
docker-compose -f docker-compose-loadtest.yml run --rm loadtest-rust

# Go client
docker-compose -f docker-compose-loadtest.yml run --rm loadtest-go
```

### Stop everything

```bash
docker-compose -f docker-compose-loadtest.yml down
```

### Stop and remove volumes (clean slate)

```bash
docker-compose -f docker-compose-loadtest.yml down -v
```

## Load Test Scenarios

### Light Load Test
```bash
./run-loadtest.sh --ops 10000 --workers 10
```

### Medium Load Test
```bash
./run-loadtest.sh --ops 100000 --workers 100
```

### Heavy Load Test
```bash
./run-loadtest.sh --ops 1000000 --workers 500
```

### Stress Test
```bash
./run-loadtest.sh --ops 5000000 --workers 1000
```

## Monitoring

### View real-time server metrics

```bash
# Server 1
docker exec -it blazecache-server-1 ps aux

# Server 2
docker exec -it blazecache-server-2 ps aux
```

### Check network connections

```bash
docker exec -it blazecache-server-1 netstat -an | grep 6784
```

### Monitor resource usage

```bash
docker stats blazecache-server-1 blazecache-server-2
```

## Troubleshooting

### Servers not starting

Check logs:
```bash
docker-compose -f docker-compose-loadtest.yml logs blazecache-server-1
```

### Port conflicts

If ports 6784-6789 are already in use, modify the port mappings in `docker-compose-loadtest.yml`.

### Out of memory

Reduce the number of workers or operations:
```bash
./run-loadtest.sh --ops 50000 --workers 50
```

### Connection errors

Ensure servers are healthy before running load test:
```bash
docker-compose -f docker-compose-loadtest.yml ps
```

All servers should show "healthy" status.

## Performance Tuning

### Increase server limits

Edit `docker-compose-loadtest.yml` and adjust:
- `ulimits.nofile`: File descriptor limits
- `sysctls`: Network buffer sizes

### Use multiple client instances

Start multiple load test containers:
```bash
docker-compose -f docker-compose-loadtest.yml up --scale loadtest-rust=5
```

## Results Interpretation

The load test outputs:
- **Total operations**: Total SET/GET operations performed
- **Successful**: Number of successful operations
- **Errors**: Number of failed operations
- **Time elapsed**: Total test duration
- **Throughput**: Operations per second
- **Avg latency**: Average latency per operation in microseconds

Good performance indicators:
- Success rate > 99%
- Throughput > 10,000 ops/sec (depends on hardware)
- Average latency < 1ms


