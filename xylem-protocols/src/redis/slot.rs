//! Redis Cluster hash slot calculation
//!
//! This module implements hash slot calculation for Redis Cluster, including
//! support for hash tags that allow multiple keys to be placed in the same slot.
//!
//! Redis Cluster divides the key space into 16,384 hash slots (2^14).
//! Each key is mapped to a slot using: HASH_SLOT = CRC16(key) mod 16384
//!
//! Hash tags allow grouping related keys in the same slot by hashing only
//! the substring between { and } characters.

use super::crc16::crc16;

/// Number of hash slots in Redis Cluster (2^14)
pub const CLUSTER_SLOTS: u16 = 16384;

/// Calculate the hash slot for a Redis key
///
/// Implements Redis Cluster's slot calculation algorithm with hash tag support.
///
/// # Hash Tags
///
/// If the key contains a `{...}` pattern, only the substring between the first
/// `{` and the first `}` after it is hashed. This allows multiple keys to be
/// placed in the same slot for multi-key operations.
///
/// # Arguments
///
/// * `key` - The Redis key as a byte slice
///
/// # Returns
///
/// The hash slot number (0-16383)
///
/// # Example
///
/// ```
/// use xylem_protocols::redis::slot::calculate_slot;
///
/// // Regular key
/// let slot = calculate_slot(b"user:1000");
/// assert!(slot < 16384);
///
/// // Hash tags - these will hash to the same slot
/// let slot1 = calculate_slot(b"{user:1000}.profile");
/// let slot2 = calculate_slot(b"{user:1000}.settings");
/// assert_eq!(slot1, slot2);
/// ```
pub fn calculate_slot(key: &[u8]) -> u16 {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    crc16(hash_key) % CLUSTER_SLOTS
}

/// Calculate slot from a key string (convenience wrapper)
///
/// # Example
///
/// ```
/// use xylem_protocols::redis::slot::calculate_slot_str;
///
/// let slot = calculate_slot_str("user:1000");
/// assert!(slot < 16384);
/// ```
pub fn calculate_slot_str(key: &str) -> u16 {
    calculate_slot(key.as_bytes())
}

/// Extract hash tag from key if present
///
/// Returns the substring between the first `{` and the first `}` after it.
/// If no valid hash tag is found, returns None and the entire key should be hashed.
///
/// # Hash Tag Rules
///
/// - Must have both `{` and `}`
/// - The `}` must come after the `{`
/// - The substring between them must not be empty
/// - Only the first `{...}` pair is considered
///
/// # Arguments
///
/// * `key` - The Redis key as a byte slice
///
/// # Returns
///
/// - `Some(&[u8])` - The hash tag substring (without the braces)
/// - `None` - No valid hash tag found, hash the entire key
///
/// # Example
///
/// ```
/// use xylem_protocols::redis::slot::extract_hash_tag;
///
/// assert_eq!(extract_hash_tag(b"{user}key"), Some(b"user".as_ref()));
/// assert_eq!(extract_hash_tag(b"key{user}"), Some(b"user".as_ref()));
/// assert_eq!(extract_hash_tag(b"{user"), None);
/// assert_eq!(extract_hash_tag(b"user}"), None);
/// assert_eq!(extract_hash_tag(b"{}key"), None); // Empty tag
/// assert_eq!(extract_hash_tag(b"key"), None);   // No tag
/// ```
pub fn extract_hash_tag(key: &[u8]) -> Option<&[u8]> {
    // Find the first '{'
    let start = key.iter().position(|&b| b == b'{')?;

    // Find the first '}' after '{'
    let remaining = &key[start + 1..];
    let end = remaining.iter().position(|&b| b == b'}')?;

    // Empty hash tag like "{}" is not valid
    if end == 0 {
        return None;
    }

    // Return the substring between { and }
    Some(&key[start + 1..start + 1 + end])
}

/// Check if a key contains a hash tag
///
/// # Example
///
/// ```
/// use xylem_protocols::redis::slot::has_hash_tag;
///
/// assert!(has_hash_tag(b"{user:1000}.profile"));
/// assert!(!has_hash_tag(b"user:1000"));
/// ```
pub fn has_hash_tag(key: &[u8]) -> bool {
    extract_hash_tag(key).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_calculation_in_range() {
        // All slots should be in valid range [0, 16384)
        let keys: &[&[u8]] = &[b"mykey", b"user:1000", b"session:abc123", b"cache:xyz789", b""];

        for key in keys {
            let slot = calculate_slot(key);
            assert!(slot < CLUSTER_SLOTS, "Slot {} is out of range for key {:?}", slot, key);
        }
    }

    #[test]
    fn test_slot_deterministic() {
        // Same key should always produce same slot
        let key = b"test:123";
        let slot1 = calculate_slot(key);
        let slot2 = calculate_slot(key);
        assert_eq!(slot1, slot2);
    }

    #[test]
    fn test_hash_tag_same_slot() {
        // Keys with same hash tag should map to same slot
        let slot1 = calculate_slot(b"{user:1000}.profile");
        let slot2 = calculate_slot(b"{user:1000}.settings");
        let slot3 = calculate_slot(b"{user:1000}.preferences");

        assert_eq!(slot1, slot2);
        assert_eq!(slot2, slot3);
    }

    #[test]
    fn test_hash_tag_different_from_full_key() {
        // Hash tag should produce different slot than full key
        // (This is true for most keys, though not guaranteed by algorithm)
        let slot_full = calculate_slot(b"user:1000.profile");
        let slot_tag = calculate_slot(b"{user:1000}.profile");

        // They will likely be different (not guaranteed, but very likely)
        // We can't assert inequality here, but we can verify they're both valid
        assert!(slot_full < CLUSTER_SLOTS);
        assert!(slot_tag < CLUSTER_SLOTS);
    }

    #[test]
    fn test_hash_tag_extraction() {
        assert_eq!(extract_hash_tag(b"{user}key"), Some(b"user".as_ref()));
        assert_eq!(extract_hash_tag(b"key{user}"), Some(b"user".as_ref()));
        assert_eq!(extract_hash_tag(b"{user:1000}.profile"), Some(b"user:1000".as_ref()));
        assert_eq!(extract_hash_tag(b"prefix{tag}suffix"), Some(b"tag".as_ref()));
    }

    #[test]
    fn test_hash_tag_extraction_invalid() {
        // No closing brace
        assert_eq!(extract_hash_tag(b"{user"), None);

        // No opening brace
        assert_eq!(extract_hash_tag(b"user}"), None);

        // Empty tag
        assert_eq!(extract_hash_tag(b"{}key"), None);

        // No braces at all
        assert_eq!(extract_hash_tag(b"key"), None);

        // Empty key
        assert_eq!(extract_hash_tag(b""), None);
    }

    #[test]
    fn test_hash_tag_only_first_pair() {
        // Only the first {...} pair should be considered
        let key = b"{first}middle{second}end";
        assert_eq!(extract_hash_tag(key), Some(b"first".as_ref()));

        let slot = calculate_slot(key);
        let slot_first = calculate_slot(b"{first}");
        assert_eq!(slot, slot_first);
    }

    #[test]
    fn test_hash_tag_nested_braces() {
        // Nested braces - should take first } after first {
        let key = b"{foo{bar}baz}";
        // This extracts "foo{bar" (up to first })
        assert_eq!(extract_hash_tag(key), Some(b"foo{bar".as_ref()));
    }

    #[test]
    fn test_has_hash_tag() {
        assert!(has_hash_tag(b"{user:1000}.profile"));
        assert!(has_hash_tag(b"{tag}"));
        assert!(!has_hash_tag(b"regular_key"));
        assert!(!has_hash_tag(b"{}empty"));
        assert!(!has_hash_tag(b"{no_closing"));
    }

    #[test]
    fn test_calculate_slot_str() {
        let slot1 = calculate_slot_str("user:1000");
        let slot2 = calculate_slot(b"user:1000");
        assert_eq!(slot1, slot2);
    }

    #[test]
    fn test_slot_distribution() {
        // Test that slots are reasonably distributed across the space
        // Generate 1000 keys and verify they don't all map to the same slot
        let mut slots = std::collections::HashSet::new();

        for i in 0..1000 {
            let key = format!("key:{}", i);
            let slot = calculate_slot_str(&key);
            slots.insert(slot);
        }

        // With 1000 keys and 16384 slots, we should see good distribution
        // Expect at least 500 unique slots (conservative estimate)
        assert!(slots.len() > 500, "Poor slot distribution: only {} unique slots", slots.len());
    }

    #[test]
    fn test_redis_cluster_examples() {
        // Test with realistic Redis Cluster key patterns

        // User data grouped by user ID
        let user_profile = calculate_slot(b"{user:1000}.profile");
        let user_settings = calculate_slot(b"{user:1000}.settings");
        assert_eq!(user_profile, user_settings);

        // Session data grouped by session ID
        let session_data1 = calculate_slot(b"{session:abc}.data");
        let session_data2 = calculate_slot(b"{session:abc}.metadata");
        assert_eq!(session_data1, session_data2);

        // Different users should (likely) map to different slots
        let user1 = calculate_slot(b"{user:1000}.profile");
        let user2 = calculate_slot(b"{user:2000}.profile");
        // Not guaranteed to be different, but very likely
        // Just verify both are valid
        assert!(user1 < CLUSTER_SLOTS);
        assert!(user2 < CLUSTER_SLOTS);
    }

    #[test]
    fn test_empty_key() {
        // Empty key should still produce valid slot
        let slot = calculate_slot(b"");
        assert!(slot < CLUSTER_SLOTS);
    }

    #[test]
    fn test_special_characters() {
        // Keys with special characters
        let keys: &[&[u8]] = &[
            b"key:with:colons",
            b"key-with-dashes",
            b"key_with_underscores",
            b"key.with.dots",
            b"key with spaces",
            b"key\xFFwith\xFFbinary",
        ];

        for key in keys {
            let slot = calculate_slot(key);
            assert!(slot < CLUSTER_SLOTS);
        }
    }

    #[test]
    fn test_long_keys() {
        // Test with very long keys
        let long_key = b"a".repeat(1000);
        let slot = calculate_slot(&long_key);
        assert!(slot < CLUSTER_SLOTS);
    }

    #[test]
    fn test_unicode_keys() {
        // Redis keys can contain UTF-8
        let unicode_keys = [
            "用户:1000",         // Chinese
            "пользователь:1000", // Russian
            "🔑:1000",           // Emoji
        ];

        for key in &unicode_keys {
            let slot = calculate_slot_str(key);
            assert!(slot < CLUSTER_SLOTS);
        }
    }
}
