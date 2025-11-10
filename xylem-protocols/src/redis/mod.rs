//! Redis protocol (RESP) implementation

pub mod command_selector;
pub mod command_template;

use crate::Protocol;
use anyhow::{anyhow, Result};
use command_selector::CommandSelector;
use command_template::CommandTemplate;

#[derive(Debug, Clone, PartialEq)]
pub enum RedisOp {
    Get,
    Set,
    Incr,
    /// Multi-get - fetch multiple keys at once
    MGet {
        count: usize,
    },
    /// WAIT for replication
    Wait {
        num_replicas: usize,
        timeout_ms: u64,
    },
    /// Custom command from template
    Custom(CommandTemplate),
}

pub struct RedisProtocol {
    command_selector: Box<dyn CommandSelector<RedisOp>>,
    /// Per-connection sequence numbers for send
    conn_send_seq: std::collections::HashMap<usize, u64>,
    /// Per-connection sequence numbers for receive
    conn_recv_seq: std::collections::HashMap<usize, u64>,
}

impl RedisProtocol {
    /// Create a new Redis protocol with a command selector
    pub fn new(command_selector: Box<dyn CommandSelector<RedisOp>>) -> Self {
        Self {
            command_selector,
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

        // Select which command to execute
        let operation = self.command_selector.next_command();

        let request_data = match operation {
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
            RedisOp::MGet { count } => {
                // MGET key:0 key:1 key:2 ... key:(count-1)
                // Format: *{count+1}\r\n$4\r\nMGET\r\n${len}\r\nkey:{n}\r\n...
                let mut request = format!("*{}\r\n$4\r\nMGET\r\n", count + 1);
                for i in 0..count {
                    let curr_key = key.wrapping_add(i as u64);
                    let key_str = format!("key:{}", curr_key);
                    request.push_str(&format!("${}\r\n{}\r\n", key_str.len(), key_str));
                }
                request.into_bytes()
            }
            RedisOp::Wait { num_replicas, timeout_ms } => {
                // WAIT num_replicas timeout_ms
                // Format: *3\r\n$4\r\nWAIT\r\n${len}\r\n{num}\r\n${len}\r\n{timeout}\r\n
                let num_str = num_replicas.to_string();
                let timeout_str = timeout_ms.to_string();
                format!(
                    "*3\r\n$4\r\nWAIT\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    num_str.len(),
                    num_str,
                    timeout_str.len(),
                    timeout_str
                )
                .into_bytes()
            }
            RedisOp::Custom(template) => template.generate_request(key, value_size),
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
        self.command_selector.reset();
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

#[cfg(test)]
mod tests {
    use super::*;
    use command_selector::FixedCommandSelector;

    #[test]
    fn test_protocol_name() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let protocol = RedisProtocol::new(selector);
        assert_eq!(protocol.name(), "redis");
    }

    #[test]
    fn test_generate_get_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let (request, id) = protocol.generate_request(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request);

        assert_eq!(id, (0, 0)); // First request on connection 0
        assert!(request_str.contains("GET"));
        assert!(request_str.contains("key:42"));
        assert!(request_str.starts_with("*2\r\n")); // Array with 2 elements
    }

    #[test]
    fn test_generate_set_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let mut protocol = RedisProtocol::new(selector);

        let (request, id) = protocol.generate_request(0, 100, 5);
        let request_str = String::from_utf8_lossy(&request);

        assert_eq!(id, (0, 0));
        assert!(request_str.contains("SET"));
        assert!(request_str.contains("key:100"));
        assert!(request_str.contains("xxxxx")); // 5 bytes of value
        assert!(request_str.starts_with("*3\r\n")); // Array with 3 elements
    }

    #[test]
    fn test_generate_incr_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Incr));
        let mut protocol = RedisProtocol::new(selector);

        let (request, _) = protocol.generate_request(0, 999, 64);
        let request_str = String::from_utf8_lossy(&request);

        assert!(request_str.contains("INCR"));
        assert!(request_str.contains("key:999"));
        assert!(request_str.starts_with("*2\r\n"));
    }

    #[test]
    fn test_generate_mget_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::MGet { count: 3 }));
        let mut protocol = RedisProtocol::new(selector);

        let (request, _) = protocol.generate_request(0, 10, 64);
        let request_str = String::from_utf8_lossy(&request);

        assert!(request_str.contains("MGET"));
        assert!(request_str.contains("key:10"));
        assert!(request_str.contains("key:11"));
        assert!(request_str.contains("key:12"));
        assert!(request_str.starts_with("*4\r\n")); // MGET + 3 keys
    }

    #[test]
    fn test_generate_wait_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Wait {
            num_replicas: 2,
            timeout_ms: 1000,
        }));
        let mut protocol = RedisProtocol::new(selector);

        let (request, _) = protocol.generate_request(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request);

        assert!(request_str.contains("WAIT"));
        assert!(request_str.contains("2")); // num_replicas
        assert!(request_str.contains("1000")); // timeout_ms
        assert!(request_str.starts_with("*3\r\n"));
    }

    #[test]
    fn test_generate_custom_request() {
        let template = CommandTemplate::parse("HSET myhash __key__ __data__").unwrap();
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Custom(template)));
        let mut protocol = RedisProtocol::new(selector);

        let (request, _) = protocol.generate_request(0, 5, 3);
        let request_str = String::from_utf8_lossy(&request);

        assert!(request_str.contains("HSET"));
        assert!(request_str.contains("myhash"));
        assert!(request_str.contains("key:5"));
        assert!(request_str.contains("xxx")); // 3 bytes of value
    }

    #[test]
    fn test_sequence_numbers() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Generate multiple requests on same connection
        let (_, id1) = protocol.generate_request(0, 1, 64);
        let (_, id2) = protocol.generate_request(0, 2, 64);
        let (_, id3) = protocol.generate_request(0, 3, 64);

        assert_eq!(id1, (0, 0));
        assert_eq!(id2, (0, 1));
        assert_eq!(id3, (0, 2));

        // Different connection has independent sequence
        let (_, id4) = protocol.generate_request(1, 1, 64);
        assert_eq!(id4, (1, 0));
    }

    #[test]
    fn test_parse_simple_string_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"+OK\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 5);
        assert_eq!(id, Some((0, 0)));
    }

    #[test]
    fn test_parse_integer_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Incr));
        let mut protocol = RedisProtocol::new(selector);

        let response = b":42\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 5);
        assert_eq!(id, Some((0, 0)));
    }

    #[test]
    fn test_parse_bulk_string_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"$5\r\nhello\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 11);
        assert_eq!(id, Some((0, 0)));
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"$-1\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 5);
    }

    #[test]
    fn test_parse_array_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"*3\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 4);
    }

    #[test]
    fn test_parse_error_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"-ERR unknown command\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ERR unknown command"));
    }

    #[test]
    fn test_parse_incomplete_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"+OK"; // Missing \r\n
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(id, None);
    }

    #[test]
    fn test_parse_empty_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(id, None);
    }

    #[test]
    fn test_parse_invalid_response_type() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"X invalid\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_incomplete_bulk_string() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"$10\r\nhello"; // Only 5 bytes of 10
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 0); // Incomplete
        assert_eq!(id, None);
    }

    #[test]
    fn test_reset() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Generate some requests to build up sequence numbers
        protocol.generate_request(0, 1, 64);
        protocol.generate_request(0, 2, 64);
        protocol.generate_request(1, 1, 64);

        // Reset should clear sequence numbers
        protocol.reset();

        // Next request should start from sequence 0 again
        let (_, id) = protocol.generate_request(0, 1, 64);
        assert_eq!(id, (0, 0));
    }

    #[test]
    fn test_multiple_connections() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Generate requests on different connections
        let (_, id1) = protocol.generate_request(0, 1, 64);
        let (_, id2) = protocol.generate_request(1, 1, 64);
        let (_, id3) = protocol.generate_request(2, 1, 64);

        assert_eq!(id1, (0, 0));
        assert_eq!(id2, (1, 0));
        assert_eq!(id3, (2, 0));

        // Each connection maintains independent sequence
        let (_, id4) = protocol.generate_request(0, 2, 64);
        let (_, id5) = protocol.generate_request(1, 2, 64);

        assert_eq!(id4, (0, 1));
        assert_eq!(id5, (1, 1));
    }

    #[test]
    fn test_response_sequence_tracking() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Parse multiple responses on same connection
        let response = b"+OK\r\n";

        let (_, id1) = protocol.parse_response(0, response).unwrap();
        let (_, id2) = protocol.parse_response(0, response).unwrap();
        let (_, id3) = protocol.parse_response(0, response).unwrap();

        assert_eq!(id1, Some((0, 0)));
        assert_eq!(id2, Some((0, 1)));
        assert_eq!(id3, Some((0, 2)));
    }

    #[test]
    fn test_parse_invalid_resp_type() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Invalid RESP type byte (not +, -, :, $, or *)
        let invalid_response = b"@INVALID\r\n";
        let result = protocol.parse_response(0, invalid_response);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid RESP"));
    }

    #[test]
    fn test_parse_incomplete_array() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Incomplete array (no \r\n)
        let incomplete = b"*2";
        let result = protocol.parse_response(0, incomplete).unwrap();
        assert_eq!(result, (0, None)); // Should indicate incomplete
    }

    #[test]
    fn test_parse_incomplete_bulk_length() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Incomplete bulk string (no \r\n after length)
        let incomplete = b"$10";
        let result = protocol.parse_response(0, incomplete).unwrap();
        assert_eq!(result, (0, None)); // Should indicate incomplete
    }

    #[test]
    fn test_find_resp_message_end_empty() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let protocol = RedisProtocol::new(selector);

        // Empty data should return 0
        let result = protocol.find_resp_message_end(&[]).unwrap();
        assert_eq!(result, 0);
    }
}
