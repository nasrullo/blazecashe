#!/bin/bash

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=== UDP (QUIC) Blazecache Load Test ===${NC}\n"

# Default small test values
NUM_OPS=${NUM_OPS:-100}
NUM_WORKERS=${NUM_WORKERS:-5}

echo -e "${YELLOW}Configuration:${NC}"
echo -e "  Operations: ${NUM_OPS}"
echo -e "  Workers: ${NUM_WORKERS}\n"

# Detect docker compose command
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    echo -e "${RED}Error: docker-compose not found${NC}"
    exit 1
fi

cd "$(dirname "$0")"

# Stop existing
echo -e "${YELLOW}Cleaning up...${NC}"
$DOCKER_COMPOSE -f docker-compose-loadtest.yml down 2>/dev/null || true

# Start servers
echo -e "${GREEN}Starting servers...${NC}"
$DOCKER_COMPOSE -f docker-compose-loadtest.yml up -d blazecache-server-1 blazecache-server-2

# Wait for health
echo -e "${YELLOW}Waiting for servers...${NC}"
for i in {1..60}; do
    if $DOCKER_COMPOSE -f docker-compose-loadtest.yml ps | grep -q "healthy"; then
        echo -e "${GREEN}✓ Servers ready!${NC}\n"
        break
    fi
    if [ $i -eq 60 ]; then
        echo -e "${RED}✗ Servers failed to start${NC}"
        $DOCKER_COMPOSE -f docker-compose-loadtest.yml logs blazecache-server-1 | tail -20
        exit 1
    fi
    sleep 2
done

# Build and run load test locally
echo -e "${GREEN}Building UDP load test client...${NC}"
cd ..
cargo build --release --example loadtest_udp_100k --manifest-path Cargo.toml

echo -e "${GREEN}Running UDP (QUIC) load test...${NC}\n"
SERVER_ADDR="127.0.0.1:6800" \
NUM_OPS=$NUM_OPS \
NUM_WORKERS=$NUM_WORKERS \
./target/release/examples/loadtest_udp_100k

echo -e "\n${GREEN}✓ UDP load test completed!${NC}"

# Show recent logs
echo -e "\n${YELLOW}Recent server logs:${NC}"
cd docker
$DOCKER_COMPOSE -f docker-compose-loadtest.yml logs --tail=10 blazecache-server-1 2>/dev/null || echo "  (Logs not available)"

# Cleanup
echo -e "\n${YELLOW}Stopping servers...${NC}"
$DOCKER_COMPOSE -f docker-compose-loadtest.yml down

echo -e "${GREEN}Done!${NC}"

