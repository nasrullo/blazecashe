# Testing the Java Client

## Prerequisites

1. **Install Java 21+**:
   ```bash
   sudo apt install openjdk-21-jdk
   ```

2. **Verify installation**:
   ```bash
   java -version
   javac -version
   ```

3. **Ensure Maven is configured** (if JAVA_HOME is not set):
   ```bash
   export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
   # Or find your Java installation:
   # update-alternatives --list java
   ```

## Running Tests

### Option 1: Use the test script
```bash
cd clients/java
./test.sh
```

### Option 2: Manual testing with Maven

1. **Compile the project**:
   ```bash
   cd clients/java
   mvn clean compile
   ```

2. **Run TestClient** (basic functionality test):
   ```bash
   mvn exec:java -Dexec.mainClass="com.blazecache.TestClient"
   ```

3. **Run Benchmark** (performance test):
   ```bash
   # Default: 127.0.0.1:6784
   mvn exec:java -Dexec.mainClass="com.blazecache.Benchmark"
   
   # Custom server and parameters:
   mvn exec:java -Dexec.mainClass="com.blazecache.Benchmark" \
       -Dexec.args="127.0.0.1:6784 10000 10"
   ```

4. **Run LoadTest** (high concurrency test):
   ```bash
   mvn exec:java -Dexec.mainClass="com.blazecache.LoadTest" \
       -Dexec.args="100000 32"
   ```

## Expected Results

### TestClient
Should show:
- ✓ Ping successful
- ✓ Set successful
- ✓ Get successful
- ✓ Correctly returned empty for missing key
- ✓ Delete successful
- ✓ Multi-get successful

### Benchmark
Should show throughput similar to:
- **Rust client**: ~108,875 ops/sec
- **Go client**: ~93,604 ops/sec
- **Java client**: Expected to be competitive (with connection pooling)

## Troubleshooting

1. **"JAVA_HOME environment variable is not defined correctly"**:
   ```bash
   export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
   ```

2. **"Connection refused"**:
   - Ensure BlazeCache server is running on the specified port
   - Default port: 6784
   - Check: `ps aux | grep blazecache`

3. **Compilation errors**:
   - Ensure Java 21+ is installed
   - Check `pom.xml` for correct Java version configuration

## Connection Pooling Features

The Java client now includes:
- ✅ Connection pooling (max 500 connections per server)
- ✅ TCP_NODELAY for low latency
- ✅ Thread-safe pool management
- ✅ Automatic connection reuse
- ✅ Dead connection detection and cleanup

