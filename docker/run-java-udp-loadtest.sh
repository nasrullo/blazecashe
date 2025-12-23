#!/bin/bash

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=== Java UDP Blazecache Load Test (3 Servers + 3 Clients) ===${NC}\n"

# Default test values
CONCURRENCY=${CONCURRENCY:-100}
VALUE_SIZE=${VALUE_SIZE:-1024}
DURATION=${DURATION:-60}
INTERVAL=${INTERVAL:-5}

echo -e "${YELLOW}Configuration:${NC}"
echo -e "  BlazeCache servers: 3"
echo -e "  Java UDP clients: 3"
echo -e "  Concurrency per client: ${CONCURRENCY}"
echo -e "  Value size: ${VALUE_SIZE} bytes"
echo -e "  Duration: ${DURATION} seconds"
echo -e "  Stats interval: ${INTERVAL} seconds"
echo -e "  Total concurrency: $((CONCURRENCY * 3))\n"

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
echo -e "${YELLOW}Cleaning up existing containers...${NC}"
$DOCKER_COMPOSE -f docker-compose-java-udp-loadtest.yml down -v 2>/dev/null || true

# Build images
echo -e "${GREEN}Building images...${NC}"
$DOCKER_COMPOSE -f docker-compose-java-udp-loadtest.yml build

# Start all 3 servers
echo -e "${GREEN}Starting 3 BlazeCache servers...${NC}"
$DOCKER_COMPOSE -f docker-compose-java-udp-loadtest.yml up -d \
    blazecache-server-1 \
    blazecache-server-2 \
    blazecache-server-3

# Wait for all servers to be healthy
echo -e "${YELLOW}Waiting for all servers to be ready...${NC}"
for i in {1..60}; do
    HEALTHY_COUNT=$($DOCKER_COMPOSE -f docker-compose-java-udp-loadtest.yml ps 2>/dev/null | grep -c "healthy" || echo "0")
    HEALTHY_COUNT=$(echo "$HEALTHY_COUNT" | head -1 | tr -d '\n')
    if [ "$HEALTHY_COUNT" -ge 3 ]; then
        echo -e "${GREEN}✓ All 3 servers are ready!${NC}\n"
        break
    fi
    if [ $i -eq 60 ]; then
        echo -e "${RED}✗ Not all servers started successfully${NC}"
        echo -e "${YELLOW}Server status:${NC}"
        $DOCKER_COMPOSE -f docker-compose-java-udp-loadtest.yml ps
        echo -e "\n${YELLOW}Server logs:${NC}"
        $DOCKER_COMPOSE -f docker-compose-java-udp-loadtest.yml logs --tail=20 blazecache-server-1
        exit 1
    fi
    echo -e "  Waiting... ($HEALTHY_COUNT/3 servers healthy)"
    sleep 2
done

# Start all 3 load test clients
echo -e "${GREEN}Starting 3 Java UDP load test clients...${NC}"
$DOCKER_COMPOSE -f docker-compose-java-udp-loadtest.yml up -d \
    loadtest-java-udp-1 \
    loadtest-java-udp-2 \
    loadtest-java-udp-3

# Wait a moment for clients to start
sleep 3

# Follow logs from all clients
echo -e "${GREEN}Running load test (following logs)...${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop early${NC}\n"

# Follow logs with timeout
timeout ${DURATION}s $DOCKER_COMPOSE -f docker-compose-java-udp-loadtest.yml logs -f \
    loadtest-java-udp-1 \
    loadtest-java-udp-2 \
    loadtest-java-udp-3 2>/dev/null || true

# Wait for all clients to complete
echo -e "\n${YELLOW}Waiting for all clients to complete...${NC}"
sleep 10

# Show final results from all clients
echo -e "\n${GREEN}=== Final Results from All Clients ===${NC}\n"
for i in {1..3}; do
    echo -e "${YELLOW}Client $i:${NC}"
    $DOCKER_COMPOSE -f docker-compose-java-udp-loadtest.yml logs --tail=20 loadtest-java-udp-$i 2>/dev/null | grep -A 20 "Final Results" || echo "  (No results yet)"
    echo ""
done

# Show server logs
echo -e "${YELLOW}Recent server logs:${NC}"
for i in {1..3}; do
    echo -e "\n${YELLOW}Server $i:${NC}"
    $DOCKER_COMPOSE -f docker-compose-java-udp-loadtest.yml logs --tail=10 blazecache-server-$i 2>/dev/null || echo "  (Logs not available)"
done

# Cleanup option
echo -e "\n${YELLOW}Keep containers running? (y/n)${NC}"
read -t 5 -r KEEP || KEEP="n"
if [[ ! $KEEP =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Stopping containers...${NC}"
    $DOCKER_COMPOSE -f docker-compose-java-udp-loadtest.yml down
    echo -e "${GREEN}Done!${NC}"
else
    echo -e "${GREEN}Containers are still running.${NC}"
    echo -e "To stop: ${YELLOW}cd docker && docker compose -f docker-compose-java-udp-loadtest.yml down${NC}"
fi

