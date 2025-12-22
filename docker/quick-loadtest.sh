#!/bin/bash

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=== Quick Blazecache Load Test (Small) ===${NC}\n"

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

# Default small test values
NUM_OPS=${NUM_OPS:-1000}
NUM_WORKERS=${NUM_WORKERS:-10}
CLIENT_TYPE=${CLIENT_TYPE:-rust}

echo -e "${YELLOW}Configuration:${NC}"
echo -e "  Operations: ${NUM_OPS}"
echo -e "  Workers: ${NUM_WORKERS}"
echo -e "  Client: ${CLIENT_TYPE}\n"

# Stop existing
echo -e "${YELLOW}Cleaning up...${NC}"
$DOCKER_COMPOSE -f docker-compose-loadtest.yml down 2>/dev/null || true

# Start servers
echo -e "${GREEN}Starting servers...${NC}"
$DOCKER_COMPOSE -f docker-compose-loadtest.yml up -d blazecache-server-1 blazecache-server-2

# Wait for health
echo -e "${YELLOW}Waiting for servers (this may take a minute on first run)...${NC}"
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
    if [ $((i % 5)) -eq 0 ]; then
        echo -e "  Still waiting... ($i/60)"
    fi
done

# Run load test
echo -e "${GREEN}Running load test...${NC}\n"
$DOCKER_COMPOSE -f docker-compose-loadtest.yml run --rm \
    -e NUM_OPS=$NUM_OPS \
    -e NUM_WORKERS=$NUM_WORKERS \
    -e SERVER_ADDR=blazecache-server-1:6784,blazecache-server-2:6784 \
    loadtest-${CLIENT_TYPE}

echo -e "\n${GREEN}✓ Load test completed!${NC}"

# Show recent logs
echo -e "\n${YELLOW}Recent server logs:${NC}"
$DOCKER_COMPOSE -f docker-compose-loadtest.yml logs --tail=10 blazecache-server-1

# Cleanup
echo -e "\n${YELLOW}Stopping servers...${NC}"
$DOCKER_COMPOSE -f docker-compose-loadtest.yml down

echo -e "${GREEN}Done!${NC}"


