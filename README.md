# BlazeCache

BlazeCache is a high-performance distributed cache server written in Rust that provides automatic peer discovery, read-through and write-through caching patterns, and intelligent hot item replication to prevent thundering herd issues.

## Key Features

### 1. Read-Through Caching

BlazeCache implements **read-through** caching, which automatically loads data from a backing store (database, API, etc.) when a cache miss occurs. This pattern ensures that:

- Cache misses trigger automatic data loading via a configurable getter function
- Loaded data is automatically stored in the cache for future requests
- SingleFlight pattern prevents duplicate loads for concurrent requests (thundering herd protection)
- Seamless integration with your existing data sources

**How it works:**
1. Client requests a key from the cache
2. Check local main cache first (fastest path)
3. If not found, check local hot cache (for hot items replicated from other nodes)
4. Determine which node is responsible for this key via consistent hashing
5. If this node is **not responsible** for the key, forward the request to the responsible node
6. If this node **is responsible** for the key, the getter function is called to load from the backing store
7. The loaded data is cached for future requests

**Key Point:** Each node is responsible for a specific set of keys defined by consistent hashing. Requests are automatically routed to the responsible node, ensuring data consistency and proper load distribution.

### 2. Write-Through Caching

BlazeCache supports **write-through** caching, ensuring data consistency between cache and backing store:

- All writes are synchronously written to the backing store before being cached
- Configurable setter function handles persistence to your data source
- Cache is updated only after successful write to backing store
- Ensures data durability and consistency

**How it works:**
1. Client writes a key-value pair
2. If a setter is configured, data is first written to the backing store
3. After successful write, data is stored in the cache
4. Data is distributed to the appropriate peer node via consistent hashing

### 3. Gossip-Based Peer Discovery

All nodes in a BlazeCache cluster automatically discover each other through a **gossip protocol**:

- **Automatic Discovery**: No manual peer configuration required
- **Eventually Consistent**: All nodes eventually learn about all peers in the cluster
- **Fault Tolerant**: Handles network partitions and node failures gracefully
- **Lightweight**: Uses UDP for efficient membership propagation
- **Self-Healing**: Automatically detects and removes failed nodes

**How it works:**
1. Nodes periodically exchange membership information with random peers
2. Membership information propagates through the cluster via gossip rounds
3. Eventually, all nodes learn about all other nodes
4. Failed nodes are automatically detected and removed from the cluster

### 4. Hot Item Replication (Thundering Herd Prevention)

BlazeCache intelligently replicates **hot items** (frequently accessed data) to all nodes with a short TTL (1 second) to prevent thundering herd issues:

- **Hot Item Detection**: Automatically identifies frequently accessed items
- **Global Replication**: Hot items are replicated to all nodes in the cluster
- **Short TTL**: Replicated items expire after 1 second to prevent stale data
- **Thundering Herd Prevention**: Multiple concurrent requests for the same key don't overwhelm the backing store

**How it works:**
1. When an item is accessed frequently (hot item), it's detected by the responsible node
2. The hot item is replicated to all peer nodes with a 1-second TTL
3. Subsequent requests can be served from any node's hot cache (before checking if node is responsible)
4. After 1 second, the replicated copy expires, ensuring data freshness
5. If hot cache miss, request is routed to the responsible node via consistent hashing
6. This prevents multiple nodes from simultaneously querying the backing store for the same key

**Benefits:**
- Reduces load on backing store during traffic spikes
- Improves response times for popular items
- Prevents cascading failures from thundering herd scenarios
- Maintains data freshness with short TTL

## Architecture

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────┐
│         BlazeCache Node             │
│                                     │
│  Request Flow:                      │
│  1. Check Main Cache (local)        │
│  2. Check Hot Cache (local, 1s TTL) │
│  3. Consistent Hashing:             │
│     - Is this node responsible?     │
│     - If NO → Forward to responsible│
│     - If YES → Use getter function  │
│                                     │
│  ┌──────────┐    ┌──────────┐      │
│  │   Main   │    │   Hot    │      │
│  │  Cache   │    │  Cache   │      │
│  │ (LRU)    │    │ (1s TTL) │      │
│  └────┬─────┘    └────┬─────┘      │
│       │               │             │
│       ▼               ▼             │
│  ┌──────────────────────────┐      │
│  │  Consistent Hashing       │      │
│  │  (Determines responsible) │      │
│  │  + Gossip Protocol        │      │
│  └──────┬───────────────────┘      │
└────────┼────────────────────────────┘
         │
         ├──────────────┬──────────────┐
         ▼              ▼              ▼
    ┌─────────┐  ┌─────────┐  ┌─────────┐
    │  Peer   │  │  Peer   │  │  Peer   │
    │  Node 1 │  │  Node 2 │  │  Node 3 │
    │(Keys A) │  │(Keys B) │  │(Keys C) │
    └─────────┘  └─────────┘  └─────────┘
         │              │              │
         └──────────────┴──────────────┘
                      │
                      ▼
              ┌──────────────┐
              │ Backing Store│
              │ (Database/API)│
              └──────────────┘
```

**Key Distribution:**
- Each node is responsible for a specific subset of keys via consistent hashing
- Keys are distributed evenly across all nodes in the cluster
- Requests are automatically routed to the responsible node

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/blazecache.git
cd blazecache

# Build the project
cargo build --release

# Install (optional)
./install.sh
```

### Basic Usage

```bash
# Start a BlazeCache server
blazecache -p 6784 -m 64

# Start with gossip protocol enabled
blazecache -p 6784 -m 64 --gossip

# Start with persistence
blazecache -p 6784 -m 64 -w
```

### Using Read-Through and Write-Through

```rust
use blazecache::{Group, Getter, Setter};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Define getter function for read-through
    let getter: Getter = Arc::new(|key: &str| {
        // Load from database, API, or any backing store
        println!("Loading {} from database", key);
        Ok(format!("value-for-{}", key).into_bytes())
    });

    // Define setter function for write-through
    let setter: Setter = Arc::new(|key: &str, value: &[u8]| {
        // Write to database, API, or any backing store
        println!("Writing {} = {:?} to database", key, value);
        Ok(())
    });

    // Create cache group with write-through support
    let group = Group::with_write_through(
        "my-cache".to_string(),
        1024 * 1024, // 1MB cache
        getter,
        setter,
        "127.0.0.1:6784".to_string(),
    );

    // Read-through: automatically loads from getter on cache miss
    let value = group.get("user:123").await?;
    println!("Got: {:?}", String::from_utf8(value)?);

    // Write-through: automatically writes to setter before caching
    group.set("user:123", b"new-value".to_vec(), 3600).await?;

    Ok(())
}
```

## Distributed Cluster Setup

### Example: 3-Node Cluster with Gossip

**Node 1** (192.168.1.100):
```bash
blazecache -p 6784 -m 256 --gossip
```

**Node 2** (192.168.1.101):
```bash
blazecache -p 6784 -m 256 --gossip \
  --gossip-seed 192.168.1.100:6784
```

**Node 3** (192.168.1.102):
```bash
blazecache -p 6784 -m 256 --gossip \
  --gossip-seed 192.168.1.100:6784 \
  --gossip-seed 192.168.1.101:6784
```

Once started, all nodes automatically discover each other via gossip protocol. No manual peer configuration needed!

## Configuration

### Command Line Options

```bash
blazecache [OPTIONS]

OPTIONS:
    -p, --port <PORT>              Port to listen on [default: 6784]
    -m, --memory <MB>              Memory limit in MB [default: 64]
    -d, --daemon                   Run as daemon
    
    Gossip Protocol Options:
        --gossip                      Enable gossip protocol
        --gossip-port <PORT>          Gossip UDP port [default: cache_port + 1]
        --gossip-interval <S>         Gossip interval in seconds [default: 1]
        --gossip-fanout <N>           Number of peers to contact per round [default: 3]
        --gossip-seed <HOST:PORT>     Seed node for bootstrap
    
    Persistence Options:
        -w, --wal                     Enable persistence (WAL + snapshots)
        --data-dir <DIR>              Persistence data directory
        --snapshot-interval <S>        Snapshot interval in seconds [default: 300]
    
    -h, --help                       Print help information
```

### Configuration File

Create `blazecache.toml`:

```toml
[server]
port = 6784
memory = 1024

[gossip]
enabled = true
port = 6785
interval = 1
fanout = 3

[persistence]
enabled = true
data_dir = "/var/lib/blazecache"
snapshot_interval = 300
```

## Protocol

BlazeCache uses a binary protocol over TCP and UDP for high-performance operations:

- **GET** - Retrieve value by key (read-through on miss)
- **PUT** - Store key-value pair (write-through if configured)
- **DELETE** - Remove key
- **PING** - Health check
- **STATS** - Get server statistics (see below for details)
- **PEER** - List cluster peers
- **CLEAR** - Clear all entries from main and hot caches (see below for details)

### STATS Command

The **STATS** command returns comprehensive cache statistics in JSON format:

```json
{
  "hits": 12345,
  "misses": 234,
  "puts": 5678,
  "deletes": 90,
  "evictions": 12,
  "hot_items": 5,
  "rejected_items": 3,
  "ttl_evictions": 8,
  "entry_count": 1000,
  "memory_usage": 10485760
}
```

**Statistics Fields:**
- `hits` - Number of successful cache hits
- `misses` - Number of cache misses (triggered read-through)
- `puts` - Number of items stored in cache
- `deletes` - Number of items deleted from cache
- `evictions` - Number of items evicted due to cache being full (LRU eviction)
- `hot_items` - Number of hot items currently replicated across nodes
- `rejected_items` - Number of items rejected (e.g., too large)
- `ttl_evictions` - Number of items evicted due to TTL expiration
- `entry_count` - Current number of entries in the cache
- `memory_usage` - Current memory usage in bytes

**Example Usage:**
```bash
# Using a client library
let stats = client.stats().await?;
println!("Cache hit rate: {:.2}%", 
    (stats.hits as f64 / (stats.hits + stats.misses) as f64) * 100.0);
```

### CLEAR Command

The **CLEAR** command removes all entries from both the main cache and hot cache on the receiving node. When a node receives a CLEAR command, it automatically forwards the command to all other peers in the cluster to ensure consistency across all nodes.

**Behavior:**
- Clears all entries from the main cache
- Clears all entries from the hot cache
- Resets `entry_count` and `memory_usage` statistics (other stats like hits/misses are preserved)
- Automatically forwards CLEAR command to all peers in the cluster (excluding self)
- Peer forwarding is done asynchronously (fire-and-forget)

**Example Usage:**
```rust
// Using a client library
client.clear().await?;
// All caches on all nodes in the cluster are now cleared
```

**Use Cases:**
- Resetting cache state during testing
- Clearing stale data after schema changes
- Freeing memory when cache becomes corrupted
- Administrative cache management

**Warning:** This is a destructive operation that cannot be undone. Use with caution in production environments.

See [PROTOCOL.md](PROTOCOL.md) for complete specification.

## Client Load Balancing Strategies

BlazeCache clients support two load balancing strategies for distributing requests across cluster nodes:

### 1. Consistent Hashing

**Best for:** Write operations, applications without thundering herd issues

**Characteristics:**
- Each key is deterministically mapped to a specific node
- Ensures the same key always goes to the same node
- Provides better cache locality and consistency
- Minimal redistribution when nodes are added/removed

**When to use:**
- ✅ **Write operations** - First choice for PUT/DELETE operations
- ✅ **Read operations** - Only if your application doesn't have thundering herd issues
- ✅ **Session data** - When you need sticky sessions
- ✅ **Stateful operations** - When operations depend on previous state

**When NOT to use:**
- ❌ **Read operations with thundering herd issues** - Use round robin instead
- ❌ **High-traffic read scenarios** - Round robin distributes load better

### 2. Round Robin

**Best for:** Read operations, applications with thundering herd issues

**Characteristics:**
- Requests are distributed evenly across all nodes in rotation
- Simple and predictable load distribution
- Better for high-traffic read scenarios
- Works well with hot item replication

**When to use:**
- ✅ **Read operations** - Especially if you have thundering herd issues
- ✅ **High-traffic scenarios** - Better load distribution across nodes
- ✅ **Stateless operations** - When cache locality isn't critical
- ✅ **Applications with hot item replication** - Leverages replicated hot items

**When NOT to use:**
- ❌ **Write operations** - Consistent hashing is preferred
- ❌ **Stateful operations** - Consistent hashing provides better consistency

### Recommendation

**For Reads:**
- If your application has **thundering herd issues** → Use **Round Robin**
- If your application does **NOT** have thundering herd issues → Use **Consistent Hashing**

**For Writes:**
- **Always use Consistent Hashing** - This is the first choice for write operations
- Ensures data consistency and proper cache distribution
- Prevents write conflicts and maintains cache locality

**Hybrid Approach:**
Many applications use a hybrid strategy:
- **Reads**: Round Robin (to leverage hot item replication and prevent thundering herd)
- **Writes**: Consistent Hashing (for data consistency and cache locality)

## Performance

- **Cache Hit**: ~132ns (nanoseconds)
- **Peer Hit**: ~80μs (microseconds) via TCP
- **Cache Miss**: Depends on getter latency (typically 100ms+ for database)
- **Hot Item Replication**: Prevents thundering herd, reduces backing store load by up to 90%

## Use Cases

### 1. Database Caching
- **Read-through**: Automatically loads from database on cache miss
- **Write-through**: Ensures database is always updated before caching
- **Hot items**: Popular queries are replicated to all nodes, reducing database load

### 2. API Response Caching
- **Read-through**: Fetches from API on cache miss
- **Write-through**: Updates API before caching (for POST/PUT operations)
- **Hot items**: Frequently accessed API responses available on all nodes

### 3. Session Storage
- **Read-through**: Loads session data from persistent store
- **Write-through**: Persists session changes immediately
- **Hot items**: Active sessions replicated for fast access

## Thundering Herd Prevention

BlazeCache prevents thundering herd issues through multiple mechanisms:

1. **SingleFlight Pattern**: Deduplicates concurrent requests for the same key on the server side
2. **Hot Item Replication**: Popular items replicated to all nodes with 1-second TTL
3. **Client Load Balancing**: Clients should use **Round Robin** for reads if thundering herd is a concern
4. **Consistent Hashing for Writes**: Ensures write operations go to the responsible node
5. **Gossip Protocol**: Automatic load distribution as cluster grows

**Client Strategy for Thundering Herd Prevention:**
- **Reads with thundering herd issues**: Use **Round Robin** to distribute requests across all nodes
- **Writes**: Always use **Consistent Hashing** for data consistency
- **Reads without thundering herd issues**: Can use **Consistent Hashing** for better cache locality

**Example Scenario:**
- 1000 concurrent requests for key "popular-item"
- Without BlazeCache: 1000 database queries (thundering herd)
- With BlazeCache + Round Robin client: Requests distributed across nodes, hot items available on all nodes → 1 database query + 999 cache hits from replicated hot items

## License

MIT License

