# Removing Blocking Client - Rationale

## Current State

We have two Rust client implementations:
1. **Async Client** (`TcpClient`) - Uses Tokio async/await
2. **Blocking Client** (`BlockingTcpClient`) - Uses `std::net::TcpStream` with threads

## Why Remove Blocking Client?

### 1. **Same Performance**
- Async client: ~111k ops/sec
- Blocking client: ~112k ops/sec
- **Difference: <1%** (within measurement variance)

### 2. **Same Optimizations**
Both clients have identical optimizations:
- Direct request encoding (bypasses Command enum)
- Connection pooling
- Lock-free server selection (RCU pattern)
- TCP_NODELAY

### 3. **Better Code Quality**
- **Async is more readable**: Clear async/await syntax vs thread management
- **More idiomatic Rust**: Tokio is the standard async runtime
- **Better error handling**: Async error propagation is cleaner
- **Better integration**: Works seamlessly with other async libraries

### 4. **Reduced Maintenance Burden**
- **Duplicate code**: ~550 lines of nearly identical code
- **Bug fixes**: Must be applied to both implementations
- **Feature additions**: Must be implemented twice
- **Testing**: Must test both implementations

### 5. **Limited Usage**
- Only used in one example: `benchmark_blocking.rs`
- No production usage found
- No external dependencies

### 6. **Async is the Future**
- Rust ecosystem is moving towards async-first
- Tokio is mature and well-maintained
- Better for I/O-bound operations (which this is)

## Edge Cases

### "What if I need blocking I/O?"
**Answer**: You can use `tokio::runtime::Runtime::new().unwrap().block_on()` to run async code in a blocking context:

```rust
let rt = tokio::runtime::Runtime::new().unwrap();
let client = TcpClient::new(vec!["127.0.0.1:6792".to_string()]);
let result = rt.block_on(client.get("key"));
```

This gives you the same blocking API with async performance.

## Migration Path

1. Remove `clients/rust/src/blocking.rs`
2. Remove `pub mod blocking;` from `lib.rs`
3. Remove `clients/rust/examples/benchmark_blocking.rs`
4. Update documentation to show async usage only

## Conclusion

The blocking client was created to match Go's performance by avoiding async runtime overhead. However:
- ✅ Async client now matches blocking performance
- ✅ Async is more maintainable and idiomatic
- ✅ No real-world usage of blocking client
- ✅ Can use `block_on()` if blocking API is needed

**Recommendation**: Remove the blocking client to reduce maintenance burden and focus on the async implementation.

