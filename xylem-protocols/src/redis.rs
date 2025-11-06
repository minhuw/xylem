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
    /// Per-connection sequence numbers for send
    conn_send_seq: std::collections::HashMap<usize, u64>,
    /// Per-connection sequence numbers for receive
    conn_recv_seq: std::collections::HashMap<usize, u64>,
}

impl RedisProtocol {
    pub fn new(operation: RedisOp) -> Self {
        Self {
            operation,
            conn_send_seq: std::collections::HashMap::new(),
            conn_recv_seq: std::collections::HashMap::new(),
        }
    }

    /// Get next sequence number for a connection (for sending)
    fn next_send_seq(&mut self, conn_id: usize) -> u64 {
        let seq = self.conn_send_seq.entry(conn_id).or_insert(0);
        let result = *seq;
        *seq += 1;
        result
    }

    /// Get next sequence number for a connection (for receiving)
    fn next_recv_seq(&mut self, conn_id: usize) -> u64 {
        let seq = self.conn_recv_seq.entry(conn_id).or_insert(0);
        let result = *seq;
        *seq += 1;
        result
    }
}

impl Protocol for RedisProtocol {
    type RequestId = (usize, u64); // (conn_id, sequence)

    fn generate_request(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
    ) -> (Vec<u8>, Self::RequestId) {
        let seq = self.next_send_seq(conn_id);
        let id = (conn_id, seq);
        let request_data = match self.operation {
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
        };
        (request_data, id)
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)> {
        if data.is_empty() {
            return Ok((0, None));
        }

        // Parse RESP response and find complete message
        let consumed = self.find_resp_message_end(data)?;
        if consumed == 0 {
            // Incomplete response
            return Ok((0, None));
        }

        // Validate response
        match data[0] {
            b'+' | b':' | b'$' | b'*' => {
                // Valid response - return the next expected ID for this connection
                // (Redis guarantees FIFO ordering per connection)
                let seq = self.next_recv_seq(conn_id);
                Ok((consumed, Some((conn_id, seq))))
            }
            b'-' => Err(anyhow!("Redis error: {}", String::from_utf8_lossy(&data[1..consumed]))),
            _ => Err(anyhow!("Invalid RESP response")),
        }
    }

    fn name(&self) -> &'static str {
        "redis"
    }

    fn reset(&mut self) {
        self.conn_send_seq.clear();
        self.conn_recv_seq.clear();
    }
}

impl RedisProtocol {
    /// Find the end of a complete RESP message, returns bytes consumed
    fn find_resp_message_end(&self, data: &[u8]) -> Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }

        match data[0] {
            b'+' | b'-' | b':' => {
                // Simple strings, errors, integers end with \r\n
                if let Some(pos) = data.windows(2).position(|w| w == b"\r\n") {
                    Ok(pos + 2)
                } else {
                    Ok(0) // Incomplete
                }
            }
            b'$' => {
                // Bulk string: $<length>\r\n<data>\r\n
                if let Some(first_crlf) = data.windows(2).position(|w| w == b"\r\n") {
                    let length_str = std::str::from_utf8(&data[1..first_crlf])
                        .map_err(|e| anyhow!("Invalid bulk string length: {e}"))?;
                    let length: i64 = length_str
                        .parse()
                        .map_err(|e| anyhow!("Failed to parse bulk string length: {e}"))?;

                    if length == -1 {
                        // Null bulk string
                        return Ok(first_crlf + 2);
                    }

                    let expected_total = first_crlf + 2 + length as usize + 2;
                    if data.len() >= expected_total {
                        Ok(expected_total)
                    } else {
                        Ok(0) // Incomplete
                    }
                } else {
                    Ok(0) // Incomplete
                }
            }
            b'*' => {
                // Array - for simplicity, just find the end marker
                // This is simplified; real implementation would need recursive parsing
                if let Some(pos) = data.windows(2).position(|w| w == b"\r\n") {
                    Ok(pos + 2)
                } else {
                    Ok(0) // Incomplete
                }
            }
            _ => Err(anyhow!("Invalid RESP message type")),
        }
    }
}
