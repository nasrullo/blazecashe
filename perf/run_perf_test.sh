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
    # The server logs "Starting UDP (QUIC) server" when it starts accepting connections
    if grep -q "Starting UDP (QUIC) server" "${SERVER_LOG}" 2>/dev/null || \
       grep -q "UDP (QUIC) server enabled" "${SERVER_LOG}" 2>/dev/null; then
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

# Default to 'rust' command if no arguments provided
# Both 'rust' and 'go' commands take server address as positional argument, not --server
# Convert --server ADDR to positional ADDR for convenience
PERF_ARGS=()
if [ $# -eq 0 ]; then
    # No arguments: default to rust client with server address as positional argument
    PERF_ARGS=(rust "${SERVER_ADDR}")
else
    # Process arguments: convert --server to positional argument
    i=1
    while [ $i -le $# ]; do
        arg="${!i}"
        if [ "$arg" = "--server" ] && [ $i -lt $# ]; then
            # Convert --server ADDR to just ADDR (positional)
            next_arg=$((i + 1))
            PERF_ARGS+=("${!next_arg}")
            i=$((i + 2))
        else
            PERF_ARGS+=("$arg")
            i=$((i + 1))
        fi
    done
    
    # If first arg is 'rust' or 'go' and no server address follows, add default server address
    if [ "${PERF_ARGS[0]}" = "rust" ] || [ "${PERF_ARGS[0]}" = "go" ]; then
        # Check if second argument exists and is not a flag (starts with -)
        if [ ${#PERF_ARGS[@]} -eq 1 ] || [ "${PERF_ARGS[1]#-}" != "${PERF_ARGS[1]}" ]; then
            # No server address provided, add default between command and options
            PERF_ARGS=( "${PERF_ARGS[0]}" "${SERVER_ADDR}" "${PERF_ARGS[@]:1}" )
        fi
    fi
fi

# Run the perf test with processed arguments
cargo run --release --bin blazecache-perf -- "${PERF_ARGS[@]}" > "${PERF_LOG}" 2>&1 || {
    echo -e "${RED}Perf test failed. Check ${PERF_LOG} for details${NC}"
    exit 1
}

echo ""
echo -e "${GREEN}Perf test completed successfully!${NC}"
echo -e "${YELLOW}Server logs: ${SERVER_LOG}${NC}"
echo -e "${YELLOW}Perf logs: ${PERF_LOG}${NC}"

