//! HTTP/1.1 protocol implementation

use crate::Protocol;
use anyhow::{anyhow, Result};

pub struct HttpProtocol {
    method: String,
    path: String,
    host: String,
}

impl HttpProtocol {
    pub fn new(method: String, path: String, host: String) -> Self {
        Self { method, path, host }
    }
}

impl Protocol for HttpProtocol {
    fn generate_request(&mut self, _key: u64, _value_size: usize) -> Vec<u8> {
        format!(
            "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: keep-alive\r\n\r\n",
            self.method, self.path, self.host
        )
        .into_bytes()
    }

    fn parse_response(&mut self, data: &[u8]) -> Result<()> {
        if data.starts_with(b"HTTP/1.1 ") || data.starts_with(b"HTTP/1.0 ") {
            Ok(())
        } else {
            Err(anyhow!("Invalid HTTP response"))
        }
    }

    fn name(&self) -> &'static str {
        "http"
    }

    fn reset(&mut self) {
        // No state to reset
    }
}
