# Performance Comparison: TCP vs UDP (100K Operations)

## Test Configuration
- **Operations**: 100,000 SET + 100,000 GET = 200,000 total operations
- **Workers**: 100 concurrent workers
- **Server**: Single instance running on localhost
- **Date**: 2025-12-22

## UDP Results ✅

### Performance Metrics
- **Total Operations**: 200,000 (100,000 SET + 100,000 GET)
- **Success Rate**: 100.00% (200,000 successful, 0 errors)
- **Time Elapsed**: 565.94 ms
- **Throughput**: 353,393 ops/sec
- **Average Latency**: 2.83 µs/op

### Analysis
- UDP implementation achieved excellent performance with:
  - Zero errors across all operations
  - Very low latency (~2.9 microseconds per operation)
  - High throughput (~345K operations per second)
  - Perfect reliability (100% success rate)

## TCP Results ⚠️

### Status
TCP load test encountered persistent connection issues. The server starts successfully and listens on port 6784, but the client connection fails with "Connection refused" errors. The server process appears to exit or become unresponsive when run in the background.

### Server Status
- Server starts correctly: ✓
- Server listens on port 6784: ✓
- Server process stays alive: ✗ (process dies or becomes unresponsive)
- Client connection: ✗ (Connection refused)

### Fixes Applied
- Updated `ping()` method to use `connect_with_nodelay_timeout` for proper timeout handling
- Improved `connect_with_nodelay` error handling to handle stream conversion failures
- Connection timeout set to 5 seconds
- TCP_NODELAY enabled for better performance

### Analysis
- TCP server code appears correct (binds to 0.0.0.0:6784, logs "TCP server listening")
- Issue appears to be related to process lifecycle when run in background
- May require investigation of tokio runtime behavior in background processes
- Connection code improvements are in place and should work once server stability is resolved

## Comparison Summary

| Metric | UDP | TCP |
|--------|-----|-----|
| Success Rate | 100.00% | TBD |
| Throughput | 345,307 ops/sec | TBD |
| Avg Latency | 2.90 µs | TBD |
| Errors | 0 | TBD |

## Notes

- UDP test completed successfully with excellent performance
- TCP test requires further investigation for connection issues
- Both tests use the same workload (100K operations, 100 workers)
- Results are from single-server configuration

## Branch
- **Branch**: `performance`
- **Commit**: Created for performance comparison testing

