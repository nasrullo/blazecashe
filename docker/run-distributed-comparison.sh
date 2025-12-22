#!/bin/bash
set -e

# Distributed load test comparison: Rust vs Go UDP clients
# Tests against 5-node Docker Compose cluster

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

NUM_OPS=${NUM_OPS:-100000}
NUM_WORKERS=${NUM_WORKERS:-100}

echo "=== Distributed Load Test Comparison: Rust vs Go UDP Clients ==="
echo "Operations: $NUM_OPS"
echo "Workers: $NUM_WORKERS"
echo ""

# Server addresses for 5-node cluster
SERVER_ADDRS="127.0.0.1:6793,127.0.0.1:6794,127.0.0.1:6795,127.0.0.1:6796,127.0.0.1:6797"

# Start Docker Compose cluster
echo "=== Starting 5-node Docker Compose cluster ==="
cd docker
docker compose -f docker-compose-distributed.yml down 2>/dev/null || true
docker compose -f docker-compose-distributed.yml up -d
cd ..

echo "Waiting for servers to be ready..."
sleep 10

# Verify servers are up
echo "=== Verifying server connectivity ==="
for port in 6793 6794 6795 6796 6797; do
    if nc -z -u 127.0.0.1 $port 2>/dev/null; then
        echo "✓ Server on port $port is ready"
    else
        echo "✗ Server on port $port is not ready"
    fi
done
echo ""

# Test Rust client
echo "=== Testing Optimized Rust UDP Client ==="
export SERVER_ADDR="$SERVER_ADDRS"
export NUM_OPS=$NUM_OPS
export NUM_WORKERS=$NUM_WORKERS

RUST_START=$(date +%s.%N)
cargo run --release --example loadtest_udp_100k 2>&1 | tee /tmp/rust_udp_loadtest.log
RUST_END=$(date +%s.%N)
RUST_TIME=$(echo "$RUST_END - $RUST_START" | bc)

echo ""
echo "Rust client completed in ${RUST_TIME}s"
echo ""

# Extract Rust results
RUST_SUCCESS=$(grep -oP 'Overall: \K\d+' /tmp/rust_udp_loadtest.log | head -1 || echo "0")
RUST_THROUGHPUT=$(grep -oP 'Throughput: \K[\d.]+' /tmp/rust_udp_loadtest.log | head -1 || echo "0")
RUST_LATENCY=$(grep -oP 'Avg latency: \K[\d.]+' /tmp/rust_udp_loadtest.log | head -1 || echo "0")
RUST_ERRORS=$(grep -oP 'errors \([\d.]+%\)' /tmp/rust_udp_loadtest.log | head -1 || echo "0")

# Test Go client
echo "=== Testing Go UDP Client ==="
cd clients/go

# Build Go client
echo "Building Go client..."
go build -o /tmp/go_udp_loadtest ./examples/loadtest_udp.go 2>&1 | tee /tmp/go_build.log || {
    echo "Failed to build Go client. Checking if Go is installed..."
    which go || echo "Go is not installed or not in PATH"
    exit 1
}

GO_START=$(date +%s.%N)
# Go client uses first server address
/tmp/go_udp_loadtest -server="127.0.0.1:6793" -ops=$NUM_OPS -workers=$NUM_WORKERS 2>&1 | tee /tmp/go_udp_loadtest.log
GO_END=$(date +%s.%N)
GO_TIME=$(echo "$GO_END - $GO_START" | bc)

cd "$PROJECT_ROOT"

echo ""
echo "Go client completed in ${GO_TIME}s"
echo ""

# Extract Go results
GO_SUCCESS=$(grep -oP 'Successful: \K\d+' /tmp/go_udp_loadtest.log | head -1 || echo "0")
GO_THROUGHPUT=$(grep -oP 'Throughput: \K[\d.]+' /tmp/go_udp_loadtest.log | head -1 || echo "0")
GO_LATENCY=$(grep -oP 'Avg latency: \K[\d.]+' /tmp/go_udp_loadtest.log | head -1 || echo "0")
GO_ERRORS=$(grep -oP 'Errors: \K\d+' /tmp/go_udp_loadtest.log | head -1 || echo "0")

# Print comparison
echo "=========================================="
echo "=== Load Test Comparison Results ==="
echo "=========================================="
echo ""
echo "Configuration:"
echo "  Operations: $NUM_OPS"
echo "  Workers: $NUM_WORKERS"
echo "  Cluster: 5-node distributed"
echo ""
echo "Rust UDP Client (Optimized):"
echo "  Successful operations: $RUST_SUCCESS"
echo "  Errors: $RUST_ERRORS"
echo "  Throughput: $RUST_THROUGHPUT ops/sec"
echo "  Avg latency: $RUST_LATENCY µs"
echo "  Total time: ${RUST_TIME}s"
echo ""
echo "Go UDP Client:"
echo "  Successful operations: $GO_SUCCESS"
echo "  Errors: $GO_ERRORS"
echo "  Throughput: $GO_THROUGHPUT ops/sec"
echo "  Avg latency: $GO_LATENCY µs"
echo "  Total time: ${GO_TIME}s"
echo ""
echo "Comparison:"
if [ -n "$RUST_THROUGHPUT" ] && [ -n "$GO_THROUGHPUT" ] && [ "$RUST_THROUGHPUT" != "0" ] && [ "$GO_THROUGHPUT" != "0" ]; then
    RUST_OPS=$(echo "$RUST_THROUGHPUT" | cut -d' ' -f1)
    GO_OPS=$(echo "$GO_THROUGHPUT" | cut -d' ' -f1)
    if (( $(echo "$RUST_OPS > $GO_OPS" | bc -l) )); then
        DIFF=$(echo "scale=2; ($RUST_OPS - $GO_OPS) / $GO_OPS * 100" | bc)
        echo "  Rust is ${DIFF}% faster"
    else
        DIFF=$(echo "scale=2; ($GO_OPS - $RUST_OPS) / $RUST_OPS * 100" | bc)
        echo "  Go is ${DIFF}% faster"
    fi
fi
echo ""
echo "Detailed logs:"
echo "  Rust: /tmp/rust_udp_loadtest.log"
echo "  Go: /tmp/go_udp_loadtest.log"
echo ""

# Cleanup
echo "=== Cleaning up ==="
cd docker
docker compose -f docker-compose-distributed.yml down
cd ..

echo "Done!"

