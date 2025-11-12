//! Adapter to make xylem_core::workload::KeyGeneration work with command_selector::KeyGenerator trait

use crate::redis::command_selector::KeyGenerator;
use xylem_core::workload::KeyGeneration;

/// Implement KeyGenerator trait for KeyGeneration
impl KeyGenerator for KeyGeneration {
    fn next_key(&mut self) -> u64 {
        // Call the inherent next_key method from KeyGeneration
        self.next_key()
    }

    fn reset(&mut self) {
        // Call the inherent reset method from KeyGeneration
        self.reset()
    }
}
