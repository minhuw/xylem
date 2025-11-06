//! Memcached Binary protocol

use crate::Protocol;
use anyhow::Result;

pub struct MemcachedBinaryProtocol {
    // TODO: Add fields
}

impl MemcachedBinaryProtocol {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for MemcachedBinaryProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl Protocol for MemcachedBinaryProtocol {
    fn generate_request(&mut self, _key: u64, _value_size: usize) -> Vec<u8> {
        // TODO: Implement
        Vec::new()
    }

    fn parse_response(&mut self, _data: &[u8]) -> Result<()> {
        // TODO: Implement
        Ok(())
    }

    fn name(&self) -> &'static str {
        "memcached-binary"
    }

    fn reset(&mut self) {
        // TODO: Implement
    }
}
