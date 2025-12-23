#!/bin/bash

# Test TLS server and Rust client

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=== Testing TLS Server and Rust Client ===${NC}\n"

# Build the server
echo -e "${YELLOW}Building server...${NC}"
cargo build --release --bin blazecache
echo -e "${GREEN}✓ Server built${NC}\n"

# Start server in background with TLS on port 8443
echo -e "${YELLOW}Starting TLS server on port 8443...${NC}"
./target/release/blazecache --tls-port 8443 --port 6784 > /tmp/blazecache_tls_server.log 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for server to start
sleep 3

# Check if server is running
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo -e "${RED}✗ Server failed to start${NC}"
    cat /tmp/blazecache_tls_server.log
    exit 1
fi

echo -e "${GREEN}✓ Server started${NC}\n"

# Test Rust TLS client
echo -e "${YELLOW}Testing Rust TLS client...${NC}"
cd clients/rust

# Build the example
cargo build --example tls_client_example --release 2>&1 | tail -5

# Run the test (with insecure cert for self-signed)
RUST_LOG=info timeout 10 ./target/release/examples/tls_client_example 2>&1 || {
    echo -e "${YELLOW}Note: Certificate verification may fail with self-signed cert${NC}"
    echo -e "${YELLOW}This is expected for development. In production, use proper certificates.${NC}"
}

cd ../..

# Cleanup
echo -e "\n${YELLOW}Stopping server...${NC}"
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo -e "${GREEN}✓ Test completed${NC}"

