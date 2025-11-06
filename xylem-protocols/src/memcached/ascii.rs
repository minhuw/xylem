//! Memcached ASCII protocol

use crate::Protocol;
use anyhow::Result;

pub struct MemcachedAsciiProtocol {
    // TODO: Add fields
}

impl MemcachedAsciiProtocol {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for MemcachedAsciiProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl Protocol for MemcachedAsciiProtocol {
    fn generate_request(&mut self, _key: u64, _value_size: usize) -> Vec<u8> {
        // TODO: Implement
        Vec::new()
    }

    fn parse_response(&mut self, _data: &[u8]) -> Result<()> {
        // TODO: Implement
        Ok(())
    }

    fn name(&self) -> &'static str {
        "memcached-ascii"
    }

    fn reset(&mut self) {
        // TODO: Implement
    }
}
