//! Redis protocol (RESP) implementation

use crate::Protocol;
use anyhow::{anyhow, Result};

#[derive(Debug, Clone, Copy)]
pub enum RedisOp {
    Get,
    Set,
    Incr,
}

pub struct RedisProtocol {
    operation: RedisOp,
}

impl RedisProtocol {
    pub fn new(operation: RedisOp) -> Self {
        Self { operation }
    }
}

impl Protocol for RedisProtocol {
    fn generate_request(&mut self, key: u64, value_size: usize) -> Vec<u8> {
        match self.operation {
            RedisOp::Get => {
                format!("*2\r\n$3\r\nGET\r\n${}\r\nkey:{}\r\n", key.to_string().len() + 4, key)
                    .into_bytes()
            }
            RedisOp::Set => {
                let value = "x".repeat(value_size);
                format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\nkey:{}\r\n${}\r\n{}\r\n",
                    key.to_string().len() + 4,
                    key,
                    value_size,
                    value
                )
                .into_bytes()
            }
            RedisOp::Incr => {
                format!("*2\r\n$4\r\nINCR\r\n${}\r\nkey:{}\r\n", key.to_string().len() + 4, key)
                    .into_bytes()
            }
        }
    }

    fn parse_response(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Err(anyhow!("Empty response"));
        }

        match data[0] {
            b'+' | b':' | b'$' => Ok(()),
            b'-' => Err(anyhow!("Redis error: {}", String::from_utf8_lossy(&data[1..]))),
            _ => Err(anyhow!("Invalid RESP response")),
        }
    }

    fn name(&self) -> &'static str {
        "redis"
    }

    fn reset(&mut self) {
        // No state to reset
    }
}
