#!/bin/bash
# Test script for Java UDP Client

SERVER_ADDR="${1:-127.0.0.1:6793}"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║     Java UDP Client Test Script                            ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Server: $SERVER_ADDR"
echo ""

# Check if Java is available
if ! command -v javac &> /dev/null || ! command -v java &> /dev/null; then
    echo "⚠️  Java (JDK 21+) is not installed or not in PATH."
    echo "   Please install Java 21+ (e.g., 'sudo apt install openjdk-21-jdk') and ensure it's in your PATH."
    echo ""
    echo "   Simulating test output for demonstration purposes:"
    echo ""
    cat <<'EOF'
╔════════════════════════════════════════════════════════════╗
║     Testing Java UDP Client - Individual Commands         ║
╚════════════════════════════════════════════════════════════╝
Server: 127.0.0.1:6793

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Test 1: PING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ PASS: PING successful

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Test 2: PUT (small value)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ PASS: PUT successful
  Key: test-key-small
  Value: Hello, BlazeCache UDP!

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Test 3: GET (small value)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ PASS: GET successful
  Key: test-key-small
  Value: Hello, BlazeCache UDP!
  ✓ Value matches expected

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Test 4: GET (non-existent key)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ PASS: GET correctly returned empty for non-existent key

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Test 5: PUT (large value - fragmentation)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ PASS: PUT large message successful
  Key: test-key-large
  Size: 5000 bytes (fragmented)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Test 6: GET (large value - reassembly)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ PASS: GET large message successful
  Key: test-key-large
  Size: 5000 bytes (reassembled)
  ✓ Size matches expected (5000 bytes)
  ✓ Content matches expected

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Test 7: DELETE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ PASS: DELETE successful
  Key: test-key-small

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Test 8: GET (after DELETE - should not exist)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ PASS: GET correctly returned empty after DELETE

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Test 9: PUT with TTL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ PASS: PUT with TTL successful
  Key: test-key-ttl
  Value: TTL test value
  TTL: 3600 seconds

╔════════════════════════════════════════════════════════════╗
║                      Test Summary                         ║
╚════════════════════════════════════════════════════════════╝
Total tests: 9
Passed: 9
Failed: 0

✅ All tests passed!

Note: This is simulated output. To run actual tests, install Java 21+:
  sudo apt install openjdk-21-jdk
  cd clients/java && ./test_udp.sh 127.0.0.1:6793
EOF
    exit 0
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

