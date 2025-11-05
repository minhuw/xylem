//! Synthetic protocol implementation

use crate::Protocol;
use anyhow::Result;
use async_trait::async_trait;

pub struct SyntheticProtocol {
    service_time_ns: u64,
}

impl SyntheticProtocol {
    pub fn new(service_time_ns: u64) -> Self {
        Self { service_time_ns }
    }
}

#[async_trait]
impl Protocol for SyntheticProtocol {
    fn generate_request(&mut self, _key: u64, _value_size: usize) -> Vec<u8> {
        format!("{}\n", self.service_time_ns).into_bytes()
    }

    fn parse_response(&mut self, _data: &[u8]) -> Result<()> {
        // Synthetic protocol accepts any response
        Ok(())
    }

    fn name(&self) -> &'static str {
        "synthetic"
    }

    fn reset(&mut self) {
        // No state to reset
    }
}
