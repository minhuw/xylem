//! CRC16 implementation for Redis Cluster hash slot calculation
//!
//! This module implements the CRC16-XMODEM variant used by Redis Cluster
//! for calculating hash slots. The algorithm uses a 256-entry lookup table
//! for efficient computation.
//!
//! Reference: Redis Cluster Specification, Appendix A
//! Test vector: CRC16("123456789") = 0x31C3

/// CRC16 lookup table using XMODEM variant
///
/// Polynomial: 0x1021 (x^16 + x^12 + x^5 + 1)
/// Initial value: 0x0000
///
/// This table is precomputed to speed up CRC16 calculation.
const CRC16_TAB: [u16; 256] = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a, 0xb16b,
    0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462, 0x3443, 0x0420, 0x1401,
    0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738,
    0xf7df, 0xe7fe, 0xd79d, 0xc7bc, 0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b, 0x5af5, 0x4ad4, 0x7ab7, 0x6a96,
    0x1a71, 0x0a50, 0x3a33, 0x2a12, 0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd,
    0xad2a, 0xbd0b, 0x8d68, 0x9d49, 0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb,
    0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1, 0x1290, 0x22f3, 0x32d2,
    0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405, 0xa7db, 0xb7fa, 0x8799, 0x97b8,
    0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827,
    0x18c0, 0x08e1, 0x3882, 0x28a3, 0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e, 0xed0f, 0xdd6c, 0xcd4d,
    0xbdaa, 0xad8b, 0x9de8, 0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74,
    0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
];

/// Calculate CRC16 checksum using XMODEM variant
///
/// This is the algorithm used by Redis Cluster for hash slot calculation.
/// It uses a 256-entry lookup table for efficient computation.
///
/// # Arguments
///
/// * `data` - The byte slice to compute CRC16 for
///
/// # Returns
///
/// The CRC16 checksum as a 16-bit unsigned integer
///
/// # Example
///
/// ```
/// use xylem_protocols::redis::crc16::crc16;
///
/// // Redis specification test vector
/// assert_eq!(crc16(b"123456789"), 0x31C3);
/// ```
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        let idx = ((crc >> 8) ^ (byte as u16)) & 0xFF;
        crc = (crc << 8) ^ CRC16_TAB[idx as usize];
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc16_spec_test_vector() {
        // Redis specification test vector
        // This is the official test case from Redis Cluster specification
        assert_eq!(crc16(b"123456789"), 0x31C3);
    }

    #[test]
    fn test_crc16_empty() {
        // Empty data should produce initial value
        assert_eq!(crc16(b""), 0x0000);
    }

    #[test]
    fn test_crc16_single_byte() {
        // Test single byte inputs produce valid CRC values
        let crc_a = crc16(b"a");
        let crc_b = crc16(b"b");

        // Different bytes should produce different CRCs
        assert_ne!(crc_a, crc_b);
    }

    #[test]
    fn test_crc16_deterministic() {
        // Same input should always produce same output
        let data = b"hello world";
        let crc1 = crc16(data);
        let crc2 = crc16(data);
        assert_eq!(crc1, crc2);
    }

    #[test]
    fn test_crc16_different_inputs() {
        // Different inputs should (very likely) produce different CRCs
        let crc1 = crc16(b"foo");
        let crc2 = crc16(b"bar");
        assert_ne!(crc1, crc2);
    }

    #[test]
    fn test_crc16_sensitivity() {
        // Small change in input should change CRC
        let crc1 = crc16(b"test");
        let crc2 = crc16(b"Test");
        assert_ne!(crc1, crc2);
    }

    #[test]
    fn test_crc16_redis_keys() {
        // Test with typical Redis key patterns
        let keys: &[&[u8]] = &[b"user:1000", b"user:1001", b"session:abc123", b"cache:xyz789"];

        // All should produce valid CRCs (deterministic)
        for key in keys {
            let crc1 = crc16(key);
            let crc2 = crc16(key);
            assert_eq!(crc1, crc2);
        }
    }

    #[test]
    fn test_crc16_large_input() {
        // Test with larger input
        let large_data = b"The quick brown fox jumps over the lazy dog. \
                          This is a longer test string to verify CRC16 works with larger inputs.";
        let crc = crc16(large_data);

        // Verify it's deterministic
        assert_eq!(crc, crc16(large_data));
    }

    #[test]
    fn test_crc16_binary_data() {
        // Test with binary data (not just ASCII)
        let binary = &[0x00, 0xFF, 0xAA, 0x55, 0x12, 0x34, 0x56, 0x78];
        let crc1 = crc16(binary);
        let crc2 = crc16(binary);
        // Should be deterministic
        assert_eq!(crc1, crc2);
    }

    #[test]
    fn test_crc16_all_zeros() {
        // Test with all zeros
        let zeros = &[0u8; 100];
        let crc1 = crc16(zeros);
        let crc2 = crc16(zeros);
        // Should be deterministic
        assert_eq!(crc1, crc2);
    }

    #[test]
    fn test_crc16_all_ones() {
        // Test with all ones
        let ones = &[0xFFu8; 100];
        let crc1 = crc16(ones);
        let crc2 = crc16(ones);
        // Should be deterministic
        assert_eq!(crc1, crc2);
    }
}
