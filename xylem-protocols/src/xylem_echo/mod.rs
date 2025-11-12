//! Xylem Echo Protocol
//!
//! A custom protocol designed for validating Xylem's latency measurement infrastructure.
//!
//! ## Protocol Format
//!
//! Request (binary, little-endian):
//! ```text
//! [request_id: u64][delay_us: u64]
//! ```
//!
//! Response (echoes the request):
//! ```text
//! [request_id: u64][delay_us: u64]
//! ```
//!
//! The server waits for `delay_us` microseconds before echoing the message back.
//! This allows controlled latency testing with explicit request ID tracking.

use crate::Protocol;
use anyhow::Result;
use zeropool::BufferPool;

const MESSAGE_SIZE: usize = 16; // 8 bytes request_id + 8 bytes delay_us

pub struct XylemEchoProtocol {
    /// Default delay in microseconds if not specified
    default_delay_us: u64,
    /// Buffer pool for request generation
    pool: BufferPool,
}

impl XylemEchoProtocol {
    pub fn new(default_delay_us: u64) -> Self {
        Self {
            default_delay_us,
            pool: BufferPool::new(),
        }
    }
}

impl Default for XylemEchoProtocol {
    fn default() -> Self {
        Self::new(0) // No delay by default
    }
}

impl Protocol for XylemEchoProtocol {
    type RequestId = u64;

    fn generate_request(
        &mut self,
        conn_id: usize,
        key: u64,
        _value_size: usize,
    ) -> (Vec<u8>, Self::RequestId) {
        // Use a composite ID: high 32 bits = conn_id, low 32 bits = key
        let request_id = ((conn_id as u64) << 32) | (key & 0xFFFFFFFF);

        let mut buf = self.pool.get(MESSAGE_SIZE);
        buf.clear();

        // Write request_id (8 bytes, little-endian)
        buf.extend_from_slice(&request_id.to_le_bytes());

        // Write delay_us (8 bytes, little-endian)
        buf.extend_from_slice(&self.default_delay_us.to_le_bytes());

        (buf.to_vec(), request_id)
    }

    fn parse_response(
        &mut self,
        _conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)> {
        if data.len() < MESSAGE_SIZE {
            // Incomplete response
            return Ok((0, None));
        }

        // Parse request_id from first 8 bytes
        let request_id = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);

        // We could also parse delay_us from bytes 8-15 if needed for validation
        // let delay_us = u64::from_le_bytes([...]);

        Ok((MESSAGE_SIZE, Some(request_id)))
    }

    fn name(&self) -> &'static str {
        "xylem-echo"
    }

    fn reset(&mut self) {
        // No state to reset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_format() {
        let mut protocol = XylemEchoProtocol::new(100);

        // Generate a request
        let (request, req_id) = protocol.generate_request(0, 42, 0);

        assert_eq!(request.len(), MESSAGE_SIZE);

        // Check request_id encoding
        let request_id = u64::from_le_bytes([
            request[0], request[1], request[2], request[3], request[4], request[5], request[6],
            request[7],
        ]);
        assert_eq!(request_id, req_id);

        // Check delay_us encoding
        let delay_us = u64::from_le_bytes([
            request[8],
            request[9],
            request[10],
            request[11],
            request[12],
            request[13],
            request[14],
            request[15],
        ]);
        assert_eq!(delay_us, 100);
    }

    #[test]
    fn test_response_parsing() {
        let mut protocol = XylemEchoProtocol::new(100);

        // Create a response (echo of request)
        let request_id = 12345u64;
        let delay_us = 100u64;

        let mut response = Vec::new();
        response.extend_from_slice(&request_id.to_le_bytes());
        response.extend_from_slice(&delay_us.to_le_bytes());

        // Parse the response
        let (consumed, parsed_id) = protocol.parse_response(0, &response).unwrap();

        assert_eq!(consumed, MESSAGE_SIZE);
        assert_eq!(parsed_id, Some(request_id));
    }

    #[test]
    fn test_incomplete_response() {
        let mut protocol = XylemEchoProtocol::new(100);

        // Incomplete response (only 10 bytes)
        let incomplete = vec![0u8; 10];

        let (consumed, parsed_id) = protocol.parse_response(0, &incomplete).unwrap();

        assert_eq!(consumed, 0);
        assert_eq!(parsed_id, None);
    }

    #[test]
    fn test_multiple_connections() {
        let mut protocol = XylemEchoProtocol::new(50);

        // Connection 0, key 1
        let (_, id0) = protocol.generate_request(0, 1, 0);

        // Connection 1, key 1 (same key, different connection)
        let (_, id1) = protocol.generate_request(1, 1, 0);

        // IDs should be different
        assert_ne!(id0, id1);

        // High 32 bits should encode connection ID
        assert_eq!(id0 >> 32, 0);
        assert_eq!(id1 >> 32, 1);

        // Low 32 bits should be the key
        assert_eq!(id0 & 0xFFFFFFFF, 1);
        assert_eq!(id1 & 0xFFFFFFFF, 1);
    }
}
