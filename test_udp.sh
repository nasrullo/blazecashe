#!/bin/bash

# Test UDP client improvements
# This script starts the server and runs the UDP benchmark

echo "=== UDP Client Test Script ==="
echo ""

# Check if server is running
if ! pgrep -f "blazecache.*6793" > /dev/null; then
    echo "Starting BlazeCache server on UDP port 6793..."
    cargo run --release --bin blazecache -- --udp-port 6793 > /tmp/blazecache_udp.log 2>&1 &
    SERVER_PID=$!
    echo "Server started with PID: $SERVER_PID"
    echo "Waiting for server to be ready..."
    sleep 2
else
    echo "Server already running"
    SERVER_PID=""
fi

echo ""
echo "Running UDP benchmark..."
echo ""

# Run the benchmark
cd clients/rust
cargo run --example benchmark_udp --release

# Cleanup
if [ ! -z "$SERVER_PID" ]; then
    echo ""
    echo "Stopping server (PID: $SERVER_PID)..."
    kill $SERVER_PID 2>/dev/null
fi

echo ""
echo "=== Test Complete ==="

