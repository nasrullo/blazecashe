#!/bin/bash
# Profile the blazecache server using perf

set -e

SERVER_PID=""
PROFILE_DURATION=${1:-60}  # Default 60 seconds

cleanup() {
    if [ ! -z "$SERVER_PID" ]; then
        echo "Stopping server (PID: $SERVER_PID)..."
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
    fi
    echo "Profiling complete. Results saved to:"
    echo "  - perf.data (raw perf data)"
    echo "  - perf_report.txt (text report)"
    echo "  - perf_flamegraph.svg (flamegraph)"
}

trap cleanup EXIT

echo "=== Starting blazecache server for profiling ==="
echo "Profile duration: ${PROFILE_DURATION} seconds"
echo ""

# Start server in background
docker run -d --name blazecache-profile -p 6792:6784 blazecache blazecache --port 6784 > /dev/null 2>&1
sleep 3

# Get server PID inside container
SERVER_PID=$(docker inspect -f '{{.State.Pid}}' blazecache-profile 2>/dev/null || echo "")

if [ -z "$SERVER_PID" ]; then
    echo "Error: Could not find server PID"
    exit 1
fi

echo "Server PID: $SERVER_PID"
echo "Starting perf profiling..."

# Start load test in background
cd clients/go/examples
timeout ${PROFILE_DURATION} go run loadtest_100k.go > /dev/null 2>&1 &
LOAD_TEST_PID=$!

# Profile the server
perf record -F 99 -p $SERVER_PID -g -- sleep ${PROFILE_DURATION} || {
    echo "Note: perf may require sudo or kernel.perf_event_paranoid = -1"
    echo "Trying with sudo..."
    sudo perf record -F 99 -p $SERVER_PID -g -- sleep ${PROFILE_DURATION}
}

# Wait for load test to finish
wait $LOAD_TEST_PID 2>/dev/null || true

# Generate reports
echo "Generating reports..."
perf report --stdio > perf_report.txt 2>&1 || sudo perf report --stdio > perf_report.txt 2>&1
perf script | stackcollapse-perf.pl | flamegraph.pl > perf_flamegraph.svg 2>&1 || {
    echo "Note: flamegraph tools not found. Install with:"
    echo "  git clone https://github.com/brendangregg/FlameGraph.git"
    echo "  export PATH=\$PATH:\$(pwd)/FlameGraph"
}

echo ""
echo "=== Profiling Results ==="
echo "Top functions (from perf report):"
head -50 perf_report.txt

