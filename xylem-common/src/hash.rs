//! A fast hasher for the maps the measurement loop touches per request
//!
//! The standard library's SipHash resists hash flooding, which keys that
//! xylem picks itself (connection IDs, request IDs, command IDs) do not need,
//! and costs a sizeable part of a request's processing. [`FastHasher`] is
//! FxHash with a final rotation (as in `rustc-hash` 2): one add and multiply
//! per word.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

/// Odd multiplier with well-spread bits, from `rustc-hash` 2
const MULTIPLIER: u64 = 0xf135_7aea_2e62_a9c5;

/// A word-at-a-time multiplicative hasher for keys xylem chooses; not
/// resistant to hash flooding
#[derive(Debug, Default, Clone, Copy)]
pub struct FastHasher(u64);

impl FastHasher {
    #[inline]
    fn add(&mut self, word: u64) {
        self.0 = self.0.wrapping_add(word).wrapping_mul(MULTIPLIER);
    }
}

impl Hasher for FastHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let (words, rest) = bytes.as_chunks::<8>();
        for &word in words {
            self.add(u64::from_le_bytes(word));
        }
        if !rest.is_empty() {
            let mut word = [0; 8];
            word[..rest.len()].copy_from_slice(rest);
            self.add(u64::from_le_bytes(word));
        }
    }

    #[inline]
    fn write_u8(&mut self, value: u8) {
        self.add(u64::from(value));
    }

    #[inline]
    fn write_u16(&mut self, value: u16) {
        self.add(u64::from(value));
    }

    #[inline]
    fn write_u32(&mut self, value: u32) {
        self.add(u64::from(value));
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.add(value);
    }

    #[inline]
    fn write_usize(&mut self, value: usize) {
        self.add(value as u64);
    }

    #[inline]
    fn finish(&self) -> u64 {
        // The multiply leaves the low bits, which pick the bucket, the least
        // mixed: rotate the well-mixed high bits down
        self.0.rotate_left(26)
    }
}

/// Builds [`FastHasher`]s
pub type FastBuildHasher = BuildHasherDefault<FastHasher>;

/// A `HashMap` with [`FastHasher`]
pub type FastHashMap<K, V> = HashMap<K, V, FastBuildHasher>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::hash::BuildHasher;

    #[test]
    fn hashes_depend_on_every_word_and_on_order() {
        let hash = |value: &dyn Fn(&mut FastHasher)| {
            let mut hasher = FastBuildHasher::default().build_hasher();
            value(&mut hasher);
            hasher.finish()
        };
        let pair = |a: usize, b: u64| hash(&|h| (a, b).hash(h));
        use std::hash::Hash;
        assert_ne!(pair(1, 2), pair(2, 1));
        assert_ne!(pair(0, 1), pair(0, 2));
        // A trailing partial word counts
        assert_ne!(hash(&|h| h.write(&[1; 9])), hash(&|h| h.write(&[1; 8])));
        // Consecutive keys spread over the low bits, which pick buckets
        let buckets: HashSet<u64> = (0..64usize).map(|id| hash(&|h| id.hash(h)) & 63).collect();
        assert!(buckets.len() > 32, "{} of 64 buckets", buckets.len());
    }

    #[test]
    fn maps_work_with_tuple_keys() {
        let mut map = FastHashMap::default();
        for id in 0..1000u64 {
            map.insert((id as usize % 7, id), id);
        }
        assert_eq!(map.len(), 1000);
        assert_eq!(map[&(3, 10)], 10);
    }
}
