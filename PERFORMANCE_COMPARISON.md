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
- **Time Elapsed**: 579.19 ms
- **Throughput**: 345,307.22 ops/sec
- **Average Latency**: 2.90 µs/op

### Analysis
- UDP implementation achieved excellent performance with:
  - Zero errors across all operations
  - Very low latency (~2.9 microseconds per operation)
  - High throughput (~345K operations per second)
  - Perfect reliability (100% success rate)

## TCP Results ⚠️

### Status
TCP load test encountered connection issues during test execution. The server starts successfully and listens on port 6784, but the client connection test fails with "Connection refused" errors.

### Server Status
- Server starts correctly: ✓
- Server listens on port 6784: ✓
- Client connection: ✗ (Connection refused)

### Next Steps
1. Investigate TCP client connection timing issues
2. Verify TCP server binding and network configuration
3. Re-run TCP test once connection issues are resolved

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

