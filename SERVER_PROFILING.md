# Server Profiling Guide

## Overview

This document describes how to profile the blazecache server to identify performance bottlenecks.

## Profiling Methods

### 1. Docker Stats Profiling (Recommended - No special permissions)

```bash
./profile_server_docker.sh [duration_seconds]
```

This method:
- Uses Docker stats to monitor CPU, memory, and network usage
- Collects system metrics
- Requires no special permissions
- Works in any environment

### 2. Perf Profiling (Requires sudo or kernel.perf_event_paranoid = -1)

```bash
# First, enable perf for all users (requires root):
sudo sysctl -w kernel.perf_event_paranoid=-1

# Then run:
./profile_server_simple.sh [duration_seconds]
```

This method provides detailed CPU profiling with call graphs.

### 3. pprof Profiling (Built into Rust binary)

The server can be built with pprof support:

```bash
PROFILE=1 cargo build --release
PROFILE=1 ./target/release/blazecache --port 6784
```

Profile will be saved to `server_cpu.pb.gz` on exit.

## Current Profiling Results

### Go Client Profiling (from previous analysis)

**Key Findings:**
- **Network I/O is the bottleneck** (62.53% of CPU time)
- Connection pooling overhead is minimal (< 2%)
- System is I/O bound (5.42% CPU usage)
- Current throughput: ~714 ops/sec
- Target throughput: ~78k ops/sec
- **Gap: ~109x slower than target**

### Server-Side Observations

From Docker stats during load testing:
- Server CPU usage: Very low (< 1%)
- Server memory usage: ~1.5MB
- Network I/O: Active but low

**This suggests:**
1. Server is not CPU-bound
2. Server may be waiting on I/O
3. Possible network latency issues
4. Possible protocol overhead

## Next Steps

1. **Profile the server** using one of the methods above
2. **Compare server CPU usage** with client CPU usage
3. **Identify server-side bottlenecks** (if any)
4. **Check network latency** between client and server
5. **Analyze protocol overhead**

## Analysis Tools

### View perf reports:
```bash
perf report -i perf.data
```

### View pprof profiles:
```bash
go tool pprof server_cpu.pb.gz
```

### Docker stats:
```bash
docker stats blazecache-profile
```

## Expected Bottlenecks

Based on client profiling, likely server-side issues:
1. **Network I/O latency** - TCP connection overhead
2. **Protocol parsing** - Binary serialization/deserialization
3. **Cache operations** - Lock contention in cache operations
4. **Connection handling** - TCP connection management overhead

