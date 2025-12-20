# Consistent Hashing Fix

## Issue Identified

The server's consistent hashing implementation had a mismatch between:
1. **Internal hash function**: `ConsistentHash::hash()` uses `FnvHasher` (FNV-1a)
2. **Exposed hash function**: `hash_key()` was using `DefaultHasher` (different algorithm)
3. **Go client**: Uses `fnv.New64a()` (FNV-1a)

This mismatch could cause inconsistent peer selection between server and clients.

## Fix Applied

### 1. Unified Hash Function
- Changed `hash_key()` to use `FnvHasher` instead of `DefaultHasher`
- Both `ConsistentHash::hash()` and `hash_key()` now use the same FNV-1a algorithm
- This matches Go client's `fnv.New64a()` implementation

### 2. Documentation Updates
- Added comments clarifying that FNV-1a is used to match Go client
- Updated function documentation to emphasize consistency

## Verification

The server's consistent hashing now:
- Uses FNV-1a internally (`FnvHasher`)
- Exposes FNV-1a via `hash_key()` for client-side use
- Matches Go client's `fnv.New64a()` implementation

## Files Modified

- `src/networking/consistent_hash.rs`:
  - `hash_key()`: Changed from `DefaultHasher` to `FnvHasher`
  - Updated documentation to clarify FNV-1a usage

## Testing Recommendations

1. Verify that server and client select the same peer for a given key
2. Test with multiple peers to ensure consistent distribution
3. Verify that adding/removing peers causes minimal key redistribution

