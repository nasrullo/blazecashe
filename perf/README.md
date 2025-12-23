# Blazecache Performance Testing Tool

Performance testing tool for Blazecache UDP clients.

## Features

- **HDR Histograms**: High-precision latency measurements
- **Interval-based Reporting**: Real-time statistics during test execution
- **Rust and Go Client Support**: Test both implementations
- **JSON Output**: Optional JSON export for analysis
- **Comprehensive Metrics**: PUT/GET latency, throughput, first-byte latency

## Building

```bash
cd perf
cargo build --release
```

## Usage

### Rust Client Performance Test

The perf tool creates simple clients and connects to the server automatically. Just specify the server address and transport type.

```bash
# Basic UDP test (default)
cargo run --release --bin blazecache-perf -- rust --server 127.0.0.1:6793

# TCP test
cargo run --release --bin blazecache-perf -- rust \
    --server 127.0.0.1:6784 \
    --transport tcp

# With custom parameters
cargo run --release --bin blazecache-perf -- rust \
    --server 127.0.0.1:6793 \
    --transport udp \
    --concurrency 100 \
    --value-size 1k \
    --duration 60 \
    --interval 1
```

### Go Client Performance Test

**Note:** The Go client is in a separate repository. The perf tool will automatically look for it in:
- `client-repos/blazecache-client-go`
- `../blazecache-client-go`
- `../../blazecache-client-go`

```bash
# Basic test
cargo run --release --bin blazecache-perf -- go --server 127.0.0.1:6793

# With custom parameters
cargo run --release --bin blazecache-perf -- go \
    --server 127.0.0.1:6793 \
    --concurrency 100 \
    --value-size 1k \
    --duration 60 \
    --interval 1
```

## Parameters

- `--server`: Server address (default: `127.0.0.1:6793`)
- `--transport`: Transport type - `tcp` or `udp` (default: `udp`)
- `--concurrency`: Number of concurrent operations (default: `100`)
- `--value-size`: Value size with SI suffixes (k, M, G) (default: `1k`)
- `--duration`: Test duration in seconds (default: `60`)
- `--interval`: Stats reporting interval in seconds (default: `1`)

## How It Works

The perf tool:
1. **Creates simple clients** - Each worker creates one client connection
2. **Connects automatically** - Tests server connection before starting
3. **Runs PUT/GET operations** - Measures latency and throughput
4. **Reports statistics** - Real-time stats during test, final summary at end

No need to manually create clients or manage connections - the tool handles everything!

## Output

The tool reports:
- **RPS**: Requests per second
- **PUT Duration**: Latency percentiles for PUT operations
- **GET Duration**: Latency percentiles for GET operations
- **FBL**: First-byte latency (time to first byte of GET response)
- **Throughput**: PUT and GET throughput in Mb/s

Example output:
```
Overall stats:
RPS: 106368.52 (200000 requests in 1.88s)

Operation metrics:

      │ PUT Duration    │ GET Duration     | FBL        │ PUT Throughput  │ GET Throughput
──────┼─────────────────┼──────────────────┼────────────┼──────────────────┼────────────────
 AVG  │        9.40 µs  │         9.40 µs  │   9.40 µs  │     106.37 Mb/s │    106.37 Mb/s
 P50  │        9.00 µs  │         9.00 µs  │   9.00 µs  │     106.67 Mb/s │    106.67 Mb/s
 P90  │       12.00 µs  │        12.00 µs  │  12.00 µs  │      80.00 Mb/s │     80.00 Mb/s
 P100 │       50.00 µs  │        50.00 µs  │  50.00 µs  │      20.00 Mb/s │     20.00 Mb/s
```

## JSON Output

With `json-output` feature enabled, you can export results to JSON:

```bash
cargo run --release --bin blazecache-perf --features json-output -- rust \
    --server 127.0.0.1:6793 \
    --json results.json
```



