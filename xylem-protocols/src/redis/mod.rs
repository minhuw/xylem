//! Redis protocol (RESP) implementation

pub mod cluster;
pub mod command_selector;
pub mod command_template;
pub mod crc16;
pub mod slot;

use crate::Protocol;
use anyhow::{anyhow, Result};
use cluster::{parse_redirect, RedirectType};
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
    /// AUTH - Authenticate with Redis
    Auth {
        username: Option<String>, // For Redis 6.0+ ACL
        password: String,
    },
    /// SELECT - Select database
    SelectDb {
        db: u32,
    },
    /// HELLO - Set RESP protocol version
    Hello {
        version: u8, // 2 or 3
    },
    /// SETEX - Set with expiry in seconds
    SetEx {
        ttl_seconds: u32,
    },
    /// SETRANGE - Write at specific offset
    SetRange {
        offset: usize,
    },
    /// GETRANGE - Read from offset to end
    GetRange {
        offset: usize,
        end: i64, // -1 for end of string
    },
    /// CLUSTER SLOTS - Query cluster topology
    ClusterSlots,
    /// Custom command from template
    Custom(CommandTemplate),
}

pub struct RedisProtocol {
    command_selector: Box<dyn CommandSelector<RedisOp>>,
    /// Per-connection sequence numbers for send
    conn_send_seq: std::collections::HashMap<usize, u64>,
    /// Per-connection sequence numbers for receive
    conn_recv_seq: std::collections::HashMap<usize, u64>,
    /// Track redirect statistics (for cluster support)
    moved_redirects: u64,
    ask_redirects: u64,
    /// RESP protocol version (2 or 3)
    #[allow(dead_code)] // Reserved for future RESP3-specific features
    resp_version: u8,
}

impl RedisProtocol {
    /// Create a new Redis protocol with a command selector
    pub fn new(command_selector: Box<dyn CommandSelector<RedisOp>>) -> Self {
        Self {
            command_selector,
            conn_send_seq: std::collections::HashMap::new(),
            conn_recv_seq: std::collections::HashMap::new(),
            moved_redirects: 0,
            ask_redirects: 0,
            resp_version: 2, // Default to RESP2
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

    /// Get redirect statistics (for monitoring cluster behavior)
    pub fn redirect_stats(&self) -> (u64, u64) {
        (self.moved_redirects, self.ask_redirects)
    }

    /// Reset redirect statistics
    pub fn reset_redirect_stats(&mut self) {
        self.moved_redirects = 0;
        self.ask_redirects = 0;
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
            RedisOp::Auth { username, password } => {
                if let Some(user) = username {
                    // Redis 6.0+ ACL: AUTH username password
                    format!(
                        "*3\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        user.len(),
                        user,
                        password.len(),
                        password
                    )
                    .into_bytes()
                } else {
                    // Legacy: AUTH password
                    format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", password.len(), password)
                        .into_bytes()
                }
            }
            RedisOp::SelectDb { db } => {
                let db_str = db.to_string();
                format!("*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n", db_str.len(), db_str).into_bytes()
            }
            RedisOp::Hello { version } => {
                format!("*2\r\n$5\r\nHELLO\r\n$1\r\n{}\r\n", version).into_bytes()
            }
            RedisOp::SetEx { ttl_seconds } => {
                let value = "x".repeat(value_size);
                let ttl_str = ttl_seconds.to_string();
                format!(
                    "*4\r\n$5\r\nSETEX\r\n${}\r\nkey:{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key.to_string().len() + 4,
                    key,
                    ttl_str.len(),
                    ttl_str,
                    value_size,
                    value
                )
                .into_bytes()
            }
            RedisOp::SetRange { offset } => {
                let value = "x".repeat(value_size);
                let offset_str = offset.to_string();
                format!(
                    "*4\r\n$8\r\nSETRANGE\r\n${}\r\nkey:{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key.to_string().len() + 4,
                    key,
                    offset_str.len(),
                    offset_str,
                    value_size,
                    value
                )
                .into_bytes()
            }
            RedisOp::GetRange { offset, end } => {
                let offset_str = offset.to_string();
                let end_str = end.to_string();
                format!(
                    "*4\r\n$8\r\nGETRANGE\r\n${}\r\nkey:{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key.to_string().len() + 4,
                    key,
                    offset_str.len(),
                    offset_str,
                    end_str.len(),
                    end_str
                )
                .into_bytes()
            }
            RedisOp::ClusterSlots => b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n".to_vec(),
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
            b'+' | b':' | b'$' | b'*' | b'_' | b',' | b'#' | b'(' | b'%' | b'~' | b'|' | b'!'
            | b'=' => {
                // Valid response (RESP2 and RESP3 types) - return the next expected ID for this connection
                // (Redis guarantees FIFO ordering per connection)
                let seq = self.next_recv_seq(conn_id);
                Ok((consumed, Some((conn_id, seq))))
            }
            b'-' => {
                // Check if this is a cluster redirect (MOVED or ASK)
                match parse_redirect(data) {
                    Ok(Some(RedirectType::Moved { slot, addr })) => {
                        // MOVED redirect - permanent slot reassignment
                        self.moved_redirects += 1;
                        Err(anyhow!(
                            "MOVED redirect: slot {} moved to {} (total MOVED: {})",
                            slot,
                            addr,
                            self.moved_redirects
                        ))
                    }
                    Ok(Some(RedirectType::Ask { slot, addr })) => {
                        // ASK redirect - temporary migration state
                        self.ask_redirects += 1;
                        Err(anyhow!(
                            "ASK redirect: slot {} temporarily at {} (total ASK: {})",
                            slot,
                            addr,
                            self.ask_redirects
                        ))
                    }
                    Ok(None) | Err(_) => {
                        // Regular Redis error (not a redirect)
                        Err(anyhow!("Redis error: {}", String::from_utf8_lossy(&data[1..consumed])))
                    }
                }
            }
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
    /// Supports both RESP2 and RESP3 types
    fn find_resp_message_end(&self, data: &[u8]) -> Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }

        match data[0] {
            b'+' | b'-' | b':' => {
                // RESP2: Simple strings, errors, integers end with \r\n
                if let Some(pos) = data.windows(2).position(|w| w == b"\r\n") {
                    Ok(pos + 2)
                } else {
                    Ok(0) // Incomplete
                }
            }
            b'$' | b'!' | b'=' => {
                // RESP2: Bulk string ($)
                // RESP3: Blob error (!), Verbatim string (=)
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
            b'*' | b'%' | b'~' | b'|' => {
                // RESP2: Array (*)
                // RESP3: Map (%), Set (~), Attribute (|)
                // For simplicity, just find the end marker
                // This is simplified; real implementation would need recursive parsing
                if let Some(pos) = data.windows(2).position(|w| w == b"\r\n") {
                    Ok(pos + 2)
                } else {
                    Ok(0) // Incomplete
                }
            }
            b'_' => {
                // RESP3: Null (_\r\n)
                if data.len() >= 3 && &data[0..3] == b"_\r\n" {
                    Ok(3)
                } else {
                    Ok(0) // Incomplete
                }
            }
            b',' | b'(' => {
                // RESP3: Double (,<floating-point-number>\r\n)
                // RESP3: Big number ((<big-number>\r\n)
                if let Some(pos) = data.windows(2).position(|w| w == b"\r\n") {
                    Ok(pos + 2)
                } else {
                    Ok(0) // Incomplete
                }
            }
            b'#' => {
                // RESP3: Boolean (#t\r\n or #f\r\n)
                if data.len() >= 4 && &data[2..4] == b"\r\n" {
                    Ok(4)
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

    #[test]
    fn test_moved_redirect_detection() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let moved_error = b"-MOVED 3999 127.0.0.1:7001\r\n";
        let result = protocol.parse_response(0, moved_error);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("MOVED redirect"));
        assert!(err.contains("3999"));
        assert!(err.contains("127.0.0.1:7001"));

        // Check statistics
        let (moved, ask) = protocol.redirect_stats();
        assert_eq!(moved, 1);
        assert_eq!(ask, 0);
    }

    #[test]
    fn test_ask_redirect_detection() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let ask_error = b"-ASK 5000 127.0.0.1:7002\r\n";
        let result = protocol.parse_response(0, ask_error);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("ASK redirect"));
        assert!(err.contains("5000"));
        assert!(err.contains("127.0.0.1:7002"));

        // Check statistics
        let (moved, ask) = protocol.redirect_stats();
        assert_eq!(moved, 0);
        assert_eq!(ask, 1);
    }

    #[test]
    fn test_multiple_redirects_statistics() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Simulate multiple MOVED redirects
        let _ = protocol.parse_response(0, b"-MOVED 100 127.0.0.1:7001\r\n");
        let _ = protocol.parse_response(0, b"-MOVED 200 127.0.0.1:7002\r\n");

        // Simulate ASK redirects
        let _ = protocol.parse_response(0, b"-ASK 300 127.0.0.1:7003\r\n");

        let (moved, ask) = protocol.redirect_stats();
        assert_eq!(moved, 2);
        assert_eq!(ask, 1);

        // Reset stats
        protocol.reset_redirect_stats();
        let (moved, ask) = protocol.redirect_stats();
        assert_eq!(moved, 0);
        assert_eq!(ask, 0);
    }

    #[test]
    fn test_regular_error_vs_redirect() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Regular error should not increment redirect stats
        let regular_error = b"-ERR unknown command\r\n";
        let result = protocol.parse_response(0, regular_error);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Redis error"));
        assert!(!err.contains("redirect"));

        // Stats should be zero
        let (moved, ask) = protocol.redirect_stats();
        assert_eq!(moved, 0);
        assert_eq!(ask, 0);
    }

    // Tests for new commands (AUTH, SELECT, HELLO, etc.)

    #[test]
    fn test_generate_auth_simple() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Auth {
            username: None,
            password: "mypassword".to_string(),
        }));
        let mut protocol = RedisProtocol::new(selector);

        let (request, id) = protocol.generate_request(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request);

        assert_eq!(id, (0, 0));
        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("AUTH"));
        assert!(request_str.contains("mypassword"));
        assert!(!request_str.contains("username")); // Simple auth has no username
    }

    #[test]
    fn test_generate_auth_acl() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Auth {
            username: Some("myuser".to_string()),
            password: "mypassword".to_string(),
        }));
        let mut protocol = RedisProtocol::new(selector);

        let (request, id) = protocol.generate_request(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request);

        assert_eq!(id, (0, 0));
        assert!(request_str.starts_with("*3\r\n")); // 3 elements for ACL auth
        assert!(request_str.contains("AUTH"));
        assert!(request_str.contains("myuser"));
        assert!(request_str.contains("mypassword"));
    }

    #[test]
    fn test_generate_select_db() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::SelectDb { db: 5 }));
        let mut protocol = RedisProtocol::new(selector);

        let (request, _) = protocol.generate_request(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("SELECT"));
        assert!(request_str.contains("5"));
    }

    #[test]
    fn test_generate_hello_resp2() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Hello { version: 2 }));
        let mut protocol = RedisProtocol::new(selector);

        let (request, _) = protocol.generate_request(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("HELLO"));
        assert!(request_str.contains("2"));
    }

    #[test]
    fn test_generate_hello_resp3() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Hello { version: 3 }));
        let mut protocol = RedisProtocol::new(selector);

        let (request, _) = protocol.generate_request(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("HELLO"));
        assert!(request_str.contains("3"));
    }

    #[test]
    fn test_generate_setex() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::SetEx { ttl_seconds: 300 }));
        let mut protocol = RedisProtocol::new(selector);

        let (request, _) = protocol.generate_request(0, 100, 10);
        let request_str = String::from_utf8_lossy(&request);

        assert!(request_str.starts_with("*4\r\n")); // 4 elements: SETEX key ttl value
        assert!(request_str.contains("SETEX"));
        assert!(request_str.contains("key:100"));
        assert!(request_str.contains("300")); // TTL
        assert!(request_str.contains("xxxxxxxxxx")); // 10 x's
    }

    #[test]
    fn test_generate_setrange() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::SetRange { offset: 5 }));
        let mut protocol = RedisProtocol::new(selector);

        let (request, _) = protocol.generate_request(0, 100, 3);
        let request_str = String::from_utf8_lossy(&request);

        assert!(request_str.starts_with("*4\r\n"));
        assert!(request_str.contains("SETRANGE"));
        assert!(request_str.contains("key:100"));
        assert!(request_str.contains("5")); // offset
        assert!(request_str.contains("xxx")); // 3 x's
    }

    #[test]
    fn test_generate_getrange() {
        let selector =
            Box::new(FixedCommandSelector::new(RedisOp::GetRange { offset: 10, end: -1 }));
        let mut protocol = RedisProtocol::new(selector);

        let (request, _) = protocol.generate_request(0, 100, 64);
        let request_str = String::from_utf8_lossy(&request);

        assert!(request_str.starts_with("*4\r\n"));
        assert!(request_str.contains("GETRANGE"));
        assert!(request_str.contains("key:100"));
        assert!(request_str.contains("10")); // offset
        assert!(request_str.contains("-1")); // end
    }

    #[test]
    fn test_generate_cluster_slots() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::ClusterSlots));
        let mut protocol = RedisProtocol::new(selector);

        let (request, _) = protocol.generate_request(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("CLUSTER"));
        assert!(request_str.contains("SLOTS"));
    }

    // RESP3 response parsing tests

    #[test]
    fn test_parse_resp3_null() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"_\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 3);
        assert_eq!(id, Some((0, 0)));
    }

    #[test]
    fn test_parse_resp3_boolean_true() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"#t\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 4);
        assert_eq!(id, Some((0, 0)));
    }

    #[test]
    fn test_parse_resp3_boolean_false() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"#f\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 4);
    }

    #[test]
    fn test_parse_resp3_double() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b",3.14159\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 10);
    }

    #[test]
    fn test_parse_resp3_big_number() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"(3492890328409238509324850943850943825024385\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 46); // 1 + 43 digits + 2 (\r\n)
    }

    #[test]
    fn test_parse_resp3_blob_error() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"!21\r\nSYNTAX invalid syntax\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 28);
    }

    #[test]
    fn test_parse_resp3_verbatim_string() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"=15\r\ntxt:Some string\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 22); // =15\r\n (4) + txt:Some string (15) + \r\n (2) + 1 = 22
    }

    #[test]
    fn test_parse_resp3_map() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"%2\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 4);
    }

    #[test]
    fn test_parse_resp3_set() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"~5\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 4);
    }

    #[test]
    fn test_parse_resp3_attribute() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"|1\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 4);
    }

    #[test]
    fn test_parse_resp3_incomplete_null() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"_\r"; // Missing \n
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 0); // Incomplete
        assert_eq!(id, None);
    }

    #[test]
    fn test_parse_resp3_incomplete_boolean() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"#t"; // Missing \r\n
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 0); // Incomplete
        assert_eq!(id, None);
    }

    #[test]
    fn test_resp_version_default() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let protocol = RedisProtocol::new(selector);

        assert_eq!(protocol.resp_version, 2); // Default to RESP2
    }
}
