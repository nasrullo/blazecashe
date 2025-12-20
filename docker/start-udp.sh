#!/bin/bash

# Quick start script for UDP Docker Compose setup

set -e

echo "=== Starting BlazeCache UDP Cluster ==="
echo ""

cd "$(dirname "$0")"

echo "Building and starting 3 UDP servers..."
docker-compose -f docker-compose-udp.yml up -d --build

echo ""
echo "Waiting for servers to be ready..."
sleep 5

echo ""
echo "=== Server Status ==="
docker-compose -f docker-compose-udp.yml ps

echo ""
echo "=== Server Endpoints ==="
echo "Server 1 - TCP: localhost:6784, UDP: localhost:6793"
echo "Server 2 - TCP: localhost:6786, UDP: localhost:6794"
echo "Server 3 - TCP: localhost:6788, UDP: localhost:6795"
echo ""
echo "To view logs: docker-compose -f docker-compose-udp.yml logs -f"
echo "To stop: docker-compose -f docker-compose-udp.yml down"
echo ""
echo "=== Ready for UDP Testing ==="

