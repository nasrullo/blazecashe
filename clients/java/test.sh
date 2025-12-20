#!/bin/bash
# Test script for Java client
# Requires Java 21+ to be installed

set -e

echo "=== Testing BlazeCache Java Client ==="

# Check if Java is installed
if ! command -v java &> /dev/null; then
    echo "❌ Java is not installed. Please install Java 21 or later:"
    echo "   sudo apt install openjdk-21-jdk"
    exit 1
fi

# Check Java version
JAVA_VERSION=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | sed '/^1\./s///' | cut -d'.' -f1)
if [ "$JAVA_VERSION" -lt 21 ]; then
    echo "❌ Java 21 or later is required. Found version: $JAVA_VERSION"
    exit 1
fi

echo "✓ Java version: $(java -version 2>&1 | head -1)"

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo "❌ Maven is not installed. Please install Maven:"
    echo "   sudo apt install maven"
    exit 1
fi

echo "✓ Maven version: $(mvn -version 2>&1 | head -1)"

# Compile the project
echo ""
echo "Compiling Java client..."
cd "$(dirname "$0")"
mvn clean compile

# Run TestClient
echo ""
echo "Running TestClient..."
mvn exec:java -Dexec.mainClass="com.blazecache.TestClient" -Dexec.args=""

# Run Benchmark if server is available
if [ "$1" == "benchmark" ]; then
    SERVER="${2:-127.0.0.1:6784}"
    echo ""
    echo "Running Benchmark against $SERVER..."
    mvn exec:java -Dexec.mainClass="com.blazecache.Benchmark" -Dexec.args="$SERVER"
fi

echo ""
echo "✅ Java client testing completed!"

