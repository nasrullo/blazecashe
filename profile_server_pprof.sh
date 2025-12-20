#!/bin/bash
# Profile server using pprof (built into Rust binary)

set -e

PROFILE_DURATION=${1:-30}
OUTPUT_DIR="server_profile_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTPUT_DIR"

echo "=== Server Profiling with pprof ==="
echo "Duration: ${PROFILE_DURATION} seconds"
echo "Output directory: $OUTPUT_DIR"
echo ""

# Rebuild with profiling enabled
echo "Building server with profiling support..."
cd /home/nasrullo/workspace/blazecache
PROFILE=1 cargo build --release 2>&1 | tail -3

# Start server with profiling
echo "Starting server with profiling enabled..."
docker stop blazecache-profile 2>/dev/null || true
docker rm blazecache-profile 2>/dev/null || true

# Copy binary to a location we can run
cp target/release/blazecache /tmp/blazecache-profile || true

# Start server directly (not in docker for easier profiling)
PROFILE=1 RUST_LOG=info /tmp/blazecache-profile --port 6784 > "$OUTPUT_DIR/server.log" 2>&1 &
SERVER_PID=$!
sleep 2

if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "Error: Server failed to start. Check $OUTPUT_DIR/server.log"
    exit 1
fi

echo "Server PID: $SERVER_PID"
echo "Starting load test..."

# Start load test
cd clients/go/examples
timeout ${PROFILE_DURATION} go run loadtest_100k.go > /tmp/loadtest.log 2>&1 &
LOAD_TEST_PID=$!

# Wait for profiling duration
sleep ${PROFILE_DURATION}

# Stop server (this will trigger profile dump if pprof is configured)
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

# Wait for load test
wait $LOAD_TEST_PID 2>/dev/null || true

# Check for profile files
if [ -f "server_cpu.pb.gz" ]; then
    mv server_cpu.pb.gz "$OUTPUT_DIR/"
    echo "Profile saved to: $OUTPUT_DIR/server_cpu.pb.gz"
    echo "View with: go tool pprof $OUTPUT_DIR/server_cpu.pb.gz"
fi

echo ""
echo "Profiling complete! Results in: $OUTPUT_DIR"

