#!/bin/bash
# Script to run blazecache perf test with automatic server startup

set -e

# Configuration
SERVER_PORT=6784
UDP_PORT=6793
SERVER_ADDR="127.0.0.1:${UDP_PORT}"
LOG_DIR="/tmp/blazecache_perf"
SERVER_LOG="${LOG_DIR}/server.log"
PERF_LOG="${LOG_DIR}/perf.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Create log directory
mkdir -p "${LOG_DIR}"

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    pkill -f "blazecache.*--udp-port ${UDP_PORT}" 2>/dev/null || true
    sleep 2
    echo -e "${GREEN}Cleanup complete${NC}"
}

# Set trap to cleanup on exit
trap cleanup EXIT INT TERM

# Kill any existing server
echo -e "${YELLOW}Killing any existing blazecache servers...${NC}"
pkill -f "blazecache.*--udp-port ${UDP_PORT}" 2>/dev/null || true
sleep 2

# Start server
echo -e "${YELLOW}Starting blazecache server on UDP port ${UDP_PORT}...${NC}"
cd "$(dirname "$0")/.."
RUST_LOG=info cargo run --release --bin blazecache -- --port ${SERVER_PORT} --udp-port ${UDP_PORT} > "${SERVER_LOG}" 2>&1 &
SERVER_PID=$!
echo "Server PID: ${SERVER_PID}"

# Wait for server to be ready
echo -e "${YELLOW}Waiting for server to be ready...${NC}"
MAX_ATTEMPTS=30
ATTEMPT=0
SERVER_READY=false

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    ATTEMPT=$((ATTEMPT + 1))
    
    # Check if server process is still running
    if ! kill -0 ${SERVER_PID} 2>/dev/null; then
        echo -e "${RED}Server process died! Check ${SERVER_LOG} for errors${NC}"
        tail -20 "${SERVER_LOG}"
        exit 1
    fi
    
    # Check if server log shows it's listening
    if grep -q "UDP server instance listening" "${SERVER_LOG}" 2>/dev/null; then
        # Give it a moment to fully initialize
        sleep 2
        SERVER_READY=true
        break
    fi
    
    sleep 1
    echo -n "."
done

echo ""

if [ "$SERVER_READY" = false ]; then
    echo -e "${RED}Server not ready after ${MAX_ATTEMPTS} attempts${NC}"
    echo -e "${YELLOW}Server logs:${NC}"
    tail -30 "${SERVER_LOG}"
    exit 1
fi

echo -e "${GREEN}Server is ready!${NC}"
echo -e "${YELLOW}Server logs location: ${SERVER_LOG}${NC}"

# Run perf test
echo -e "${YELLOW}Running perf test...${NC}"
echo ""

cd perf
cargo run --release --bin blazecache-perf -- "$@" > "${PERF_LOG}" 2>&1 || {
    echo -e "${RED}Perf test failed. Check ${PERF_LOG} for details${NC}"
    exit 1
}

echo ""
echo -e "${GREEN}Perf test completed successfully!${NC}"
echo -e "${YELLOW}Server logs: ${SERVER_LOG}${NC}"
echo -e "${YELLOW}Perf logs: ${PERF_LOG}${NC}"

