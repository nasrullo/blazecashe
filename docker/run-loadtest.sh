#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Blazecache Docker Load Test ===${NC}\n"

# Default values (small test for initial verification)
CLIENT_TYPE="rust"
NUM_OPS=1000
NUM_WORKERS=10
SERVERS_ONLY=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --client)
            CLIENT_TYPE="$2"
            shift 2
            ;;
        --ops)
            NUM_OPS="$2"
            shift 2
            ;;
        --workers)
            NUM_WORKERS="$2"
            shift 2
            ;;
        --servers-only)
            SERVERS_ONLY=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --client TYPE      Client type: rust or go (default: rust)"
            echo "  --ops NUMBER       Number of operations (default: 100000)"
            echo "  --workers NUMBER   Number of worker threads (default: 100)"
            echo "  --servers-only     Only start servers, don't run load test"
            echo "  --help             Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Change to docker directory
cd "$(dirname "$0")"

# Detect docker compose command (newer versions use 'docker compose' instead of 'docker-compose')
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    echo -e "${RED}Error: docker-compose or 'docker compose' not found${NC}"
    exit 1
fi

# Stop any existing containers
echo -e "${YELLOW}Stopping existing containers...${NC}"
$DOCKER_COMPOSE -f docker-compose-loadtest.yml down 2>/dev/null || true

# Start servers
echo -e "${GREEN}Starting Blazecache servers...${NC}"
$DOCKER_COMPOSE -f docker-compose-loadtest.yml up -d blazecache-server-1 blazecache-server-2

# Wait for servers to be healthy
echo -e "${YELLOW}Waiting for servers to be ready...${NC}"
for i in {1..30}; do
    if $DOCKER_COMPOSE -f docker-compose-loadtest.yml ps | grep -q "healthy"; then
        echo -e "${GREEN}Servers are ready!${NC}\n"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}Servers failed to start within timeout${NC}"
        $DOCKER_COMPOSE -f docker-compose-loadtest.yml logs blazecache-server-1
        exit 1
    fi
    sleep 2
done

if [ "$SERVERS_ONLY" = true ]; then
    echo -e "${GREEN}Servers are running. Use '$DOCKER_COMPOSE -f docker-compose-loadtest.yml up loadtest-${CLIENT_TYPE}' to run load test.${NC}"
    exit 0
fi

# Run load test
echo -e "${GREEN}Running load test with ${CLIENT_TYPE} client...${NC}"
echo -e "  Operations: ${NUM_OPS}"
echo -e "  Workers: ${NUM_WORKERS}\n"

# Update environment variables
export NUM_OPS=$NUM_OPS
export NUM_WORKERS=$NUM_WORKERS

# Run the load test client
$DOCKER_COMPOSE -f docker-compose-loadtest.yml run --rm \
    -e NUM_OPS=$NUM_OPS \
    -e NUM_WORKERS=$NUM_WORKERS \
    loadtest-${CLIENT_TYPE}

# Show server logs
echo -e "\n${YELLOW}=== Server Logs ===${NC}"
$DOCKER_COMPOSE -f docker-compose-loadtest.yml logs --tail=50 blazecache-server-1
$DOCKER_COMPOSE -f docker-compose-loadtest.yml logs --tail=50 blazecache-server-2

# Ask if user wants to keep servers running
echo -e "\n${YELLOW}Load test completed. Keep servers running? (y/n)${NC}"
read -r response
if [[ ! "$response" =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Stopping servers...${NC}"
    $DOCKER_COMPOSE -f docker-compose-loadtest.yml down
fi

