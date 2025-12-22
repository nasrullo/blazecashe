# Quick Start: Small Load Test

Run a small load test with 1000 operations and 10 workers:

```bash
cd docker
./quick-loadtest.sh
```

This will:
1. Start 2 Blazecache server instances
2. Wait for them to be ready
3. Run a small load test (1000 operations, 10 workers)
4. Show results
5. Clean up

## Customize Test Size

Set environment variables before running:

```bash
# Very small test
NUM_OPS=100 NUM_WORKERS=5 ./quick-loadtest.sh

# Medium test
NUM_OPS=10000 NUM_WORKERS=50 ./quick-loadtest.sh

# Use Go client instead of Rust
CLIENT_TYPE=go ./quick-loadtest.sh
```

## What Gets Tested

- **SET operations**: Writing key-value pairs
- **GET operations**: Reading back the values
- **Throughput**: Operations per second
- **Latency**: Average time per operation
- **Error rate**: Success vs failure percentage

## Expected Results (Small Test)

For 1000 operations with 10 workers, you should see:
- Success rate: > 99%
- Throughput: 1,000-10,000 ops/sec (depends on hardware)
- Average latency: < 1ms

## Troubleshooting

If servers don't start:
```bash
docker compose -f docker/docker-compose-loadtest.yml logs blazecache-server-1
```

If you see build errors, the images are being built for the first time. This can take 5-10 minutes.


