//! Echo protocol implementation

use crate::Protocol;
use anyhow::{anyhow, Result};

pub struct EchoProtocol {
    payload_size: usize,
}

impl EchoProtocol {
    pub fn new(payload_size: usize) -> Self {
        Self { payload_size }
    }
}

impl Protocol for EchoProtocol {
    fn generate_request(&mut self, _key: u64, _value_size: usize) -> Vec<u8> {
        vec![0u8; self.payload_size]
    }

    fn parse_response(&mut self, data: &[u8]) -> Result<()> {
        if data.len() == self.payload_size {
            Ok(())
        } else {
            Err(anyhow!("Invalid echo response"))
        }
    }

    fn name(&self) -> &'static str {
        "echo"
    }

    fn reset(&mut self) {
        // No state to reset
    }
}
