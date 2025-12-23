pub mod stats;
pub mod client_rust;
pub mod client_go;

use std::num::ParseIntError;
use std::str::FromStr;

/// Parse byte size with SI suffixes (e.g., "1M", "10k", "100G")
pub fn parse_byte_size(s: &str) -> Result<u64, ParseIntError> {
    let s = s.trim();

    let multiplier = match s.chars().last() {
        Some('T') | Some('t') => 1024 * 1024 * 1024 * 1024,
        Some('G') | Some('g') => 1024 * 1024 * 1024,
        Some('M') | Some('m') => 1024 * 1024,
        Some('K') | Some('k') => 1024,
        _ => 1,
    };

    let s = match multiplier {
        1 => s,
        _ => &s[..s.len() - 1],
    };

    Ok(u64::from_str(s)? * multiplier)
}








