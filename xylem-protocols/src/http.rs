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
    type RequestId = (usize, u64);

    fn generate_request(
        &mut self,
        conn_id: usize,
        _key: u64,
        _value_size: usize,
    ) -> (Vec<u8>, Self::RequestId) {
        let data = format!(
            "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: keep-alive\r\n\r\n",
            self.method, self.path, self.host
        )
        .into_bytes();
        // TODO: Implement proper sequence tracking per connection
        (data, (conn_id, 0))
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)> {
        if data.starts_with(b"HTTP/1.1 ") || data.starts_with(b"HTTP/1.0 ") {
            // TODO: Implement proper sequence tracking per connection
            Ok((data.len(), Some((conn_id, 0))))
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
