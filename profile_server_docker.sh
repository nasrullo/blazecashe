#!/bin/bash
# Profile server using Docker stats and basic monitoring

set -e

PROFILE_DURATION=${1:-30}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/server_profile_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTPUT_DIR"
cd "$SCRIPT_DIR"

echo "=== Server Profiling (Docker-based) ==="
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
echo "Starting load test and monitoring..."

# Start load test in background
cd /home/nasrullo/workspace/blazecache/clients/go/examples
timeout ${PROFILE_DURATION} go run loadtest_100k.go > "$OUTPUT_DIR/loadtest.log" 2>&1 &
LOAD_TEST_PID=$!

# Monitor Docker stats
echo "Collecting Docker stats..."
docker stats blazecache-profile --no-stream --format "{{.CPUPerc}},{{.MemUsage}},{{.NetIO}}" > "$OUTPUT_DIR/docker_stats.csv" &
STATS_PID=$!

# Monitor system resources
echo "Collecting system metrics..."
(
    for i in $(seq 1 $PROFILE_DURATION); do
        echo "$(date +%s),$(top -bn1 -p $SERVER_PID | tail -1 | awk '{print $9","$10}')" >> "$OUTPUT_DIR/system_metrics.csv"
        sleep 1
    done
) &
METRICS_PID=$!

# Wait for profiling duration
sleep ${PROFILE_DURATION}

# Stop monitoring
kill $STATS_PID 2>/dev/null || true
kill $METRICS_PID 2>/dev/null || true

# Wait for load test
wait $LOAD_TEST_PID 2>/dev/null || true

# Get final stats
echo ""
echo "=== Final Server Statistics ==="
docker stats blazecache-profile --no-stream || true

# Analyze results
echo ""
echo "=== Analysis ==="
if [ -f "$OUTPUT_DIR/docker_stats.csv" ]; then
    echo "Average CPU usage:"
    awk -F',' '{sum+=$1; count++} END {if(count>0) print sum/count "%"}' "$OUTPUT_DIR/docker_stats.csv" || echo "N/A"
    echo "Peak CPU usage:"
    awk -F',' 'BEGIN{max=0} {if($1+0>max) max=$1+0} END {print max "%"}' "$OUTPUT_DIR/docker_stats.csv" || echo "N/A"
fi

# Check load test results
if [ -f "$OUTPUT_DIR/loadtest.log" ]; then
    echo ""
    echo "=== Load Test Results ==="
    grep -A 10 "Results" "$OUTPUT_DIR/loadtest.log" || tail -20 "$OUTPUT_DIR/loadtest.log"
fi

# Cleanup
docker stop blazecache-profile 2>/dev/null || true
docker rm blazecache-profile 2>/dev/null || true

echo ""
echo "Profiling complete! Results in: $OUTPUT_DIR"
echo "Files:"
ls -lh "$OUTPUT_DIR"/

