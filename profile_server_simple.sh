#!/bin/bash
# Simple server profiling using perf on the host system

set -e

PROFILE_DURATION=${1:-30}  # Default 30 seconds
OUTPUT_DIR="server_profile_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTPUT_DIR"

echo "=== Server Profiling ==="
echo "Duration: ${PROFILE_DURATION} seconds"
echo "Output directory: $OUTPUT_DIR"
echo ""

# Start server
echo "Starting server..."
docker stop blazecache-profile 2>/dev/null || true
docker rm blazecache-profile 2>/dev/null || true
docker run -d --name blazecache-profile -p 6792:6784 blazecache blazecache --port 6784
sleep 3

# Get server PID
SERVER_PID=$(docker inspect -f '{{.State.Pid}}' blazecache-profile 2>/dev/null)
if [ -z "$SERVER_PID" ]; then
    echo "Error: Could not find server PID"
    exit 1
fi

echo "Server PID: $SERVER_PID"
echo "Starting load test and profiling..."

# Start load test in background
cd clients/go/examples
timeout ${PROFILE_DURATION} go run loadtest_100k.go > /tmp/loadtest.log 2>&1 &
LOAD_TEST_PID=$!

# Profile using perf (if available)
if command -v perf >/dev/null 2>&1; then
    echo "Using perf to profile..."
    cd /home/nasrullo/workspace/blazecache
    
    # Try without sudo first
    if perf record -F 99 -p $SERVER_PID -g -o "$OUTPUT_DIR/perf.data" -- sleep ${PROFILE_DURATION} 2>/dev/null; then
        echo "Generating perf report..."
        perf report -i "$OUTPUT_DIR/perf.data" --stdio > "$OUTPUT_DIR/perf_report.txt" 2>&1 || true
        echo "Top 30 functions:"
        head -50 "$OUTPUT_DIR/perf_report.txt"
    else
        echo "Note: perf requires sudo or kernel.perf_event_paranoid = -1"
        echo "Trying with sudo..."
        if sudo perf record -F 99 -p $SERVER_PID -g -o "$OUTPUT_DIR/perf.data" -- sleep ${PROFILE_DURATION} 2>&1; then
            echo "Generating perf report..."
            sudo perf report -i "$OUTPUT_DIR/perf.data" --stdio > "$OUTPUT_DIR/perf_report.txt" 2>&1 || true
            echo "Top 30 functions:"
            head -50 "$OUTPUT_DIR/perf_report.txt"
        else
            echo "perf failed. Trying alternative profiling method..."
        fi
    fi
else
    echo "perf not found. Using alternative method..."
fi

# Wait for load test
wait $LOAD_TEST_PID 2>/dev/null || true

# Get server stats
echo ""
echo "=== Server Statistics ==="
docker stats blazecache-profile --no-stream --format "table {{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}" || true

# Cleanup
docker stop blazecache-profile 2>/dev/null || true
docker rm blazecache-profile 2>/dev/null || true

echo ""
echo "Profiling complete! Results in: $OUTPUT_DIR"

