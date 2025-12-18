use crate::utils::Result;

pub fn compress(data: &[u8]) -> Result<Vec<u8>> {
    Ok(lz4_flex::compress_prepend_size(data))
}

pub fn decompress(data: &[u8]) -> Result<Vec<u8>> {
    lz4_flex::decompress_size_prepended(data)
        .map_err(|e| crate::utils::BlazeCacheError::CompressionError(e.to_string()))
}

pub fn should_compress(data: &[u8]) -> bool {
    data.len() > 1024 // Only compress data > 1KB
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_compress() {
        assert!(!should_compress(&vec![0; 1024])); // Exactly 1KB - no compression
        assert!(should_compress(&vec![0; 1025])); // > 1KB - compress
        assert!(!should_compress(&vec![0; 512])); // < 1KB - no compression
        assert!(!should_compress(&[])); // Empty - no compression
    }

    #[test]
    fn test_compress_decompress_small_data() {
        let data = b"Hello, World!";
        let compressed = compress(data).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(data.to_vec(), decompressed);
        // Small data might actually be larger when compressed
        assert!(compressed.len() > 0);
    }

    #[test]
    fn test_compress_decompress_large_data() {
        let data = vec![b'A'; 2048]; // Large compressible data
        let compressed = compress(&data).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(data, decompressed);
        assert!(compressed.len() < data.len()); // Should be smaller
    }

    #[test]
    fn test_compress_decompress_random_data() {
        let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let compressed = compress(&data).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_compress_empty_data() {
        let data = b"";
        let compressed = compress(data).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(data.to_vec(), decompressed);
    }

    #[test]
    fn test_decompress_invalid_data() {
        let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF];
        let result = decompress(&invalid_data);

        assert!(result.is_err());
        match result {
            Err(crate::utils::BlazeCacheError::CompressionError(_)) => {}
            _ => panic!("Expected CompressionError"),
        }
    }

    #[test]
    fn test_compress_highly_compressible() {
        let data = vec![b'A'; 10000]; // Highly compressible
        let compressed = compress(&data).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(data, decompressed);
        // Should compress very well
        assert!(compressed.len() < data.len() / 10);
    }

    #[test]
    fn test_compress_binary_data() {
        let data: Vec<u8> = (0..=255).cycle().take(2000).collect();
        let compressed = compress(&data).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(data, decompressed);
    }
}
