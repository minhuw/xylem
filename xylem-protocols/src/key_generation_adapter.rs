//! Adapter to make workload::KeyGeneration work with command_selector::KeyGenerator trait

use crate::redis::command_selector::KeyGenerator;
use crate::workload::{KeyGeneration, KeyGeneratorTrait};

/// Implement KeyGenerator trait for KeyGeneration
impl KeyGenerator for KeyGeneration {
    fn next_key(&mut self) -> u64 {
        KeyGeneratorTrait::next_key(self)
    }

    fn reset(&mut self) {
        KeyGeneratorTrait::reset(self)
    }
}
