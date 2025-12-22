#!/bin/bash
# Test script for Java UDP Client

set -e

SERVER_ADDR="${1:-127.0.0.1:6793}"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║     Java UDP Client Test Script                            ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Server: $SERVER_ADDR"
echo ""

# Check if Java is available
if ! command -v javac &> /dev/null; then
    echo "❌ Error: Java compiler (javac) not found"
    echo "   Please install Java 21+ to run tests"
    echo "   Example: sudo apt install openjdk-21-jdk"
    exit 1
fi

if ! command -v java &> /dev/null; then
    echo "❌ Error: Java runtime (java) not found"
    echo "   Please install Java 21+ to run tests"
    exit 1
fi

# Check Java version
JAVA_VERSION=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
if [ "$JAVA_VERSION" -lt 21 ]; then
    echo "⚠️  Warning: Java version $JAVA_VERSION detected. Java 21+ recommended."
fi

echo "✓ Java found: $(java -version 2>&1 | head -1)"
echo ""

# Compile
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Compiling Java UDP Client..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

mkdir -p target/classes

javac -d target/classes -sourcepath src/main/java \
    src/main/java/com/blazecache/UDPClient.java \
    src/main/java/com/blazecache/TestUDPClient.java \
    src/main/java/com/blazecache/TestUDPCommands.java

if [ $? -eq 0 ]; then
    echo "✓ Compilation successful"
else
    echo "✗ Compilation failed"
    exit 1
fi

echo ""

# Run tests
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Running UDP Client Tests..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

java -cp target/classes com.blazecache.TestUDPCommands "$SERVER_ADDR"

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ All tests passed!"
else
    echo "❌ Some tests failed (exit code: $EXIT_CODE)"
fi

exit $EXIT_CODE

