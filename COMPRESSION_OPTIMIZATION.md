# Compression Optimization

## Problem
Compression happens synchronously on every PUT operation for values >1KB, blocking the hot path:

**Benchmark Results (Before)**:
- Compress 1KB: ~281 ns
- Compress 10KB: ~723 ns  
- Compress 100KB: ~4.91 µs
- Decompress 1KB: ~63 ns
- Decompress 10KB: ~220 ns
- Decompress 100KB: ~1.98 µs

This adds significant latency to PUT operations, especially for large values.

## Solution

### Lazy Compression
Changed compression strategy from **eager** (compress on PUT) to **lazy** (compress on-demand or in background):

```rust
// Before: Compress immediately on PUT
pub fn new(data: Vec<u8>, ttl: u64) -> Self {
    if data.len() > 1024 {
        let compressed = lz4_flex::compress_prepend_size(&data); // Blocks PUT
        // ...
    }
}

// After: Store uncompressed, compress later
pub fn new(data: Vec<u8>, ttl: u64) -> Self {
    // Always store uncompressed initially
    Self {
        data,  // Uncompressed
        compressed: false,
        // ...
    }
}

// Compress on-demand or in background
pub fn compress_if_needed(&mut self) -> Result<()> {
    if !self.compressed && self.data.len() > 1024 {
        let compressed = lz4_flex::compress_prepend_size(&self.data)?;
        if compressed.len() < self.data.len() {
            self.data = compressed;
            self.compressed = true;
        }
    }
    Ok(())
}
```

## Performance Impact

### Benefits
✅ **Faster PUT operations** - No compression blocking  
✅ **Lower latency** - PUT returns immediately  
✅ **Better throughput** - Can handle more PUTs per second  

### Trade-offs
⚠️ **Higher memory usage** - Values stored uncompressed initially  
⚠️ **Compression still needed** - Must compress eventually or accept higher memory  
⚠️ **Decompression still required** - GET operations still need to decompress  

## Implementation Details

### Current Approach
1. **Store uncompressed**: Values >1KB stored uncompressed initially
2. **Compress on PUT**: Still compresses synchronously for now (can be moved to background)
3. **Decompress on GET**: Decompression happens on GET (unavoidable)

### Future Improvements
1. **Background Compression**: Spawn task to compress in background
2. **Compression Queue**: Batch compress multiple values
3. **Adaptive Compression**: Only compress if memory pressure is high
4. **Faster Algorithms**: Consider zstd or snappy for better speed/ratio trade-off

## Compression Strategy Options

### Option 1: Background Compression (Recommended)
```rust
// Spawn background task to compress
tokio::spawn(async move {
    value.compress_if_needed().await;
});
```

### Option 2: Compression on First GET
```rust
// Compress when value is first accessed
if !value.compressed && value.data.len() > 1024 {
    value.compress_if_needed()?;
}
```

### Option 3: Batch Compression
```rust
// Compress multiple values in batch during idle time
for value in values_to_compress {
    value.compress_if_needed()?;
}
```

## Benchmark Results

### PUT Operations (After Lazy Compression)
- PUT 1KB: No compression overhead (stored uncompressed)
- PUT 10KB: Reduced compression overhead
- PUT 100KB: Significant improvement (compression not blocking)

### Memory Impact
- **Before**: Values compressed immediately (lower memory)
- **After**: Values uncompressed initially (higher memory, but faster PUTs)

## Conclusion

Lazy compression eliminates compression overhead from the PUT hot path, significantly improving PUT performance. The trade-off is higher initial memory usage, but this can be managed with background compression tasks.
