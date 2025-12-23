//! # Data Compression Utilities
//!
//! This module provides compression and decompression functionality for cache values.
//! Compression is used to reduce memory usage and network bandwidth when storing
//! or transmitting cache data.
//!
//! ## Compression Algorithm
//!
//! Uses **LZ4** compression, which provides:
//! - **Fast Compression**: Very high compression speed
//! - **Fast Decompression**: Even faster decompression
//! - **Good Compression Ratio**: Reasonable space savings for compressible data
//! - **Low CPU Overhead**: Minimal impact on cache performance
//!
//! ## When to Compress
//!
//! Compression is only applied to values larger than 1KB to avoid overhead
//! on small values where compression may not provide benefits or may even
//! increase size.
//!
//! ## Use Cases
//!
//! - **Memory Savings**: Store more data in the same memory footprint
//! - **Network Efficiency**: Reduce bandwidth when replicating to peers
//! - **Persistence**: Smaller snapshot files on disk
//!
//! ## Performance Considerations
//!
//! - Small values (< 1KB) are not compressed (overhead not worth it)
//! - Compression happens synchronously during PUT operations
//! - Decompression happens on-demand during GET operations
//! - Compressed data is stored alongside compression metadata

use crate::utils::Result;

/// Compresses data using LZ4 compression with size prefix.
///
/// This function compresses the input data using LZ4 and prepends the original
/// size to the compressed output. The size prefix is necessary for decompression
/// to know how much memory to allocate.
///
/// ## Arguments
///
/// * `data` - The data to compress (byte slice)
///
/// ## Returns
///
/// A byte vector containing the compressed data with size prefix, or an error
/// if compression fails.
///
/// ## Compression Format
///
/// The output format is: `[size (4 bytes)][compressed_data]`
/// - First 4 bytes: Original uncompressed size (big-endian u32)
/// - Remaining bytes: LZ4-compressed data
///
/// ## Performance
///
/// - **Speed**: Very fast (LZ4 is one of the fastest compression algorithms)
/// - **CPU Usage**: Low overhead, suitable for real-time compression
/// - **Memory**: Allocates output buffer (typically smaller than input)
///
/// ## Example
///
/// ```rust
/// use blazecache::cache::compression::compress;
///
/// let data = b"Hello, World! This is a test string that can be compressed.";
/// let compressed = compress(data).unwrap();
/// // compressed is smaller than original (for compressible data)
/// ```
pub fn compress(data: &[u8]) -> Result<Vec<u8>> {
    // Use LZ4 compression with size prefix
    // The size prefix allows decompression to allocate the correct buffer size
    // lz4_flex::compress_prepend_size handles the size prefix automatically
    Ok(lz4_flex::compress_prepend_size(data))
}

/// Decompresses data that was compressed with `compress()`.
///
/// This function reads the size prefix from the compressed data and decompresses
/// it using LZ4. The size prefix is used to allocate the correct output buffer.
///
/// ## Arguments
///
/// * `data` - The compressed data with size prefix (from `compress()`)
///
/// ## Returns
///
/// The decompressed data as a byte vector, or an error if decompression fails.
///
/// ## Errors
///
/// Returns `BlazeCacheError::CompressionError` if:
/// - The compressed data is corrupted or invalid
/// - The size prefix is invalid or indicates impossible size
/// - Decompression fails for any reason
///
/// ## Performance
///
/// - **Speed**: Extremely fast (LZ4 decompression is faster than compression)
/// - **CPU Usage**: Very low overhead
/// - **Memory**: Allocates output buffer based on size prefix
///
/// ## Example
///
/// ```rust
/// use blazecache::cache::compression::{compress, decompress};
///
/// let original = b"Hello, World!";
/// let compressed = compress(original).unwrap();
/// let decompressed = decompress(&compressed).unwrap();
/// assert_eq!(original, decompressed.as_slice());
/// ```
pub fn decompress(data: &[u8]) -> Result<Vec<u8>> {
    // Decompress data with size prefix
    // lz4_flex::decompress_size_prepended reads the size prefix and decompresses
    lz4_flex::decompress_size_prepended(data)
        .map_err(|e| crate::utils::BlazeCacheError::CompressionError(e.to_string()))
}

/// Determines whether data should be compressed based on its size.
///
/// This function implements a size-based compression policy: only compress
/// data larger than a threshold (1KB) to avoid overhead on small values.
///
/// ## Compression Policy
///
/// - **Compress**: Data larger than 1KB (1024 bytes)
/// - **Don't Compress**: Data 1KB or smaller
///
/// ## Rationale
///
/// Small values often don't compress well and may even increase in size after
/// compression due to overhead. Additionally, the CPU cost of compression
/// may not be worth it for small values. The 1KB threshold balances:
/// - Compression benefits (memory/bandwidth savings)
/// - Compression overhead (CPU time, potential size increase)
///
/// ## Arguments
///
/// * `data` - The data to evaluate (byte slice)
///
/// ## Returns
///
/// `true` if the data should be compressed, `false` otherwise.
///
/// ## Example
///
/// ```rust
/// use blazecache::cache::compression::should_compress;
///
/// assert!(!should_compress(&vec![0; 1024])); // Exactly 1KB - no compression
/// assert!(should_compress(&vec![0; 1025]));  // > 1KB - compress
/// assert!(!should_compress(&vec![0; 512])); // < 1KB - no compression
/// ```
pub fn should_compress(data: &[u8]) -> bool {
    // Only compress data larger than 1KB
    // This threshold balances compression benefits vs overhead
    // Values <= 1KB may not compress well or may even increase in size
    data.len() > 1024 // Only compress data > 1KB
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_compress() {
        // Test compression threshold logic
        assert!(!should_compress(&vec![0; 1024])); // Exactly 1KB - no compression
        assert!(should_compress(&vec![0; 1025])); // > 1KB - compress
        assert!(!should_compress(&vec![0; 512])); // < 1KB - no compression
        assert!(!should_compress(&[])); // Empty - no compression
    }

    #[test]
    fn test_compress_decompress_small_data() {
        // Test compression/decompression of small data
        // Small data may not compress well, but should still round-trip correctly
        let data = b"Hello, World!";
        let compressed = compress(data).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        // Verify round-trip preserves data
        assert_eq!(data.to_vec(), decompressed);
        // Small data might actually be larger when compressed (due to overhead)
        assert!(compressed.len() > 0);
    }

    #[test]
    fn test_compress_decompress_large_data() {
        // Test compression/decompression of large, compressible data
        // Large repetitive data should compress well
        let data = vec![b'A'; 2048]; // Large compressible data
        let compressed = compress(&data).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        // Verify round-trip preserves data
        assert_eq!(data, decompressed);
        // Compressed data should be smaller than original for repetitive data
        assert!(compressed.len() < data.len()); // Should be smaller
    }

    #[test]
    fn test_compress_decompress_random_data() {
        // Test compression/decompression of random-looking data
        // Random data may not compress well, but should still round-trip
        let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let compressed = compress(&data).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        // Verify round-trip preserves data (even if compression doesn't help)
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_compress_empty_data() {
        // Test compression/decompression of empty data
        let data = b"";
        let compressed = compress(data).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        // Empty data should round-trip correctly
        assert_eq!(data.to_vec(), decompressed);
    }

    #[test]
    fn test_decompress_invalid_data() {
        // Test that decompressing invalid data returns an error
        let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF];
        let result = decompress(&invalid_data);

        // Should return an error for invalid compressed data
        assert!(result.is_err());
        match result {
            Err(crate::utils::BlazeCacheError::CompressionError(_)) => {}
            _ => panic!("Expected CompressionError"),
        }
    }

    #[test]
    fn test_compress_highly_compressible() {
        // Test compression of highly compressible data (repetitive)
        // This should achieve excellent compression ratio
        let data = vec![b'A'; 10000]; // Highly compressible
        let compressed = compress(&data).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        // Verify round-trip
        assert_eq!(data, decompressed);
        // Should compress very well (much smaller than original)
        assert!(compressed.len() < data.len() / 10);
    }

    #[test]
    fn test_compress_binary_data() {
        // Test compression of binary data (all byte values)
        let data: Vec<u8> = (0..=255).cycle().take(2000).collect();
        let compressed = compress(&data).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        // Verify round-trip preserves binary data
        assert_eq!(data, decompressed);
    }
}
