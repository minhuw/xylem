//! Masstree protocol implementation
//!
//! Masstree uses MessagePack (msgpack) for serialization.
//! Based on the lancet implementation.
//!
//! Request format:
//! - GET: [seq, 2, key_str]
//! - SET/REPLACE: [seq, 8, key_str, value_str]
//! - HANDSHAKE: [0, 14, {"core": -1, "maxkeylen": 255}]
//!
//! Response format:
//! - GET response: [seq, 3, value_str]
//! - SET response: [seq, 9, result_u8]
//! - HANDSHAKE response: [0, 15, success_bool, max_seq_u32, version_str]
//!
//! Command codes:
//! - Cmd_Get = 2 (response: Cmd_Get + 1 = 3)
//! - Cmd_Replace = 8 (response: Cmd_Replace + 1 = 9)
//! - Cmd_Handshake = 14 (response: Cmd_Handshake + 1 = 15)

use crate::Protocol;
use anyhow::{anyhow, Result};
use rmp::decode;
use rmp::encode;
use std::collections::HashMap;
use std::io::Cursor;

// Masstree command codes (from lancet)
const CMD_GET: u8 = 2;
const CMD_REPLACE: u8 = 8;
const CMD_HANDSHAKE: u8 = 14;

#[derive(Debug, Clone, Copy)]
pub enum MasstreeOp {
    Get,
    Set,
}

pub struct MasstreeProtocol {
    operation: MasstreeOp,
    /// Per-connection sequence numbers
    conn_send_seq: HashMap<usize, u16>,
    /// Per-connection handshake state
    conn_handshake_done: HashMap<usize, bool>,
}

impl MasstreeProtocol {
    pub fn new(operation: MasstreeOp) -> Self {
        Self {
            operation,
            conn_send_seq: HashMap::new(),
            conn_handshake_done: HashMap::new(),
        }
    }

    fn next_send_seq(&mut self, conn_id: usize) -> u16 {
        let seq = self.conn_send_seq.entry(conn_id).or_insert(0);
        let result = *seq;
        *seq = seq.wrapping_add(1);
        result
    }

    fn is_handshake_done(&self, conn_id: usize) -> bool {
        *self.conn_handshake_done.get(&conn_id).unwrap_or(&false)
    }

    fn mark_handshake_done(&mut self, conn_id: usize) {
        self.conn_handshake_done.insert(conn_id, true);
    }

    /// Build a handshake request: [0, 14, {"core": -1, "maxkeylen": 255}]
    fn build_handshake(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // Array of 3 elements
        encode::write_array_len(&mut buf, 3)?;

        // seq = 0 (handshake always uses seq 0)
        encode::write_u32(&mut buf, 0)?;

        // cmd = 14 (Cmd_Handshake)
        encode::write_u8(&mut buf, CMD_HANDSHAKE)?;

        // Map with 2 entries: {"core": -1, "maxkeylen": 255}
        encode::write_map_len(&mut buf, 2)?;

        // "core": -1
        encode::write_str(&mut buf, "core")?;
        encode::write_sint(&mut buf, -1)?;

        // "maxkeylen": 255
        encode::write_str(&mut buf, "maxkeylen")?;
        encode::write_uint(&mut buf, 255)?;

        Ok(buf)
    }

    /// Build a GET request: [seq, 2, key]
    fn build_get_request(&self, seq: u16, key: &str) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // Array of 3 elements
        encode::write_array_len(&mut buf, 3)?;

        // seq
        encode::write_u32(&mut buf, seq as u32)?;

        // cmd = 2 (Cmd_Get)
        encode::write_u8(&mut buf, CMD_GET)?;

        // key as string
        encode::write_str(&mut buf, key)?;

        Ok(buf)
    }

    /// Build a SET/REPLACE request: [seq, 8, key, value]
    fn build_set_request(&self, seq: u16, key: &str, value: &[u8]) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // Array of 4 elements
        encode::write_array_len(&mut buf, 4)?;

        // seq
        encode::write_u32(&mut buf, seq as u32)?;

        // cmd = 8 (Cmd_Replace)
        encode::write_u8(&mut buf, CMD_REPLACE)?;

        // key as string
        encode::write_str(&mut buf, key)?;

        // value as binary string
        encode::write_str_len(&mut buf, value.len() as u32)?;
        buf.extend_from_slice(value);

        Ok(buf)
    }
}

impl Default for MasstreeProtocol {
    fn default() -> Self {
        Self::new(MasstreeOp::Get)
    }
}

impl Protocol for MasstreeProtocol {
    type RequestId = (usize, u16);

    fn generate_request(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
    ) -> (Vec<u8>, Self::RequestId) {
        // Check if handshake is done for this connection
        if !self.is_handshake_done(conn_id) {
            let handshake = self.build_handshake().expect("Failed to build handshake request");
            return (handshake, (conn_id, 0));
        }

        let seq = self.next_send_seq(conn_id);
        let key_str = format!("key{:010}", key); // key format: "key0000000001"

        let request = match self.operation {
            MasstreeOp::Get => {
                self.build_get_request(seq, &key_str).expect("Failed to build GET request")
            }
            MasstreeOp::Set => {
                // Generate value filled with 'x' characters
                let value = vec![b'x'; value_size];
                self.build_set_request(seq, &key_str, &value)
                    .expect("Failed to build SET request")
            }
        };

        (request, (conn_id, seq))
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<(usize, u16)>)> {
        if data.is_empty() {
            return Ok((0, None));
        }

        let mut cursor = Cursor::new(data);

        // Try to parse MessagePack array
        let _array_len = match decode::read_array_len(&mut cursor) {
            Ok(len) if len >= 2 => len,
            _ => return Ok((0, None)),
        };

        // Read sequence number
        let seq = match decode::read_int::<u32, _>(&mut cursor) {
            Ok(s) => s,
            Err(_) => return Ok((0, None)),
        };

        // Read command
        let cmd = match decode::read_int::<u8, _>(&mut cursor) {
            Ok(c) => c,
            Err(_) => return Ok((0, None)),
        };

        match cmd {
            // GET response: [seq, 3, value_str]
            3 => self.parse_get_response(conn_id, seq, &mut cursor, data),
            // SET response: [seq, 9, result]
            9 => self.parse_set_response(conn_id, seq, &mut cursor),
            // HANDSHAKE response: [0, 15, success, max_seq, version]
            15 => self.parse_handshake_response(conn_id, &mut cursor, data),
            _ => Err(anyhow!("Unknown Masstree response command: {}", cmd)),
        }
    }

    fn name(&self) -> &'static str {
        "masstree"
    }

    fn reset(&mut self) {
        self.conn_send_seq.clear();
        self.conn_handshake_done.clear();
    }
}

// Helper methods for parsing different response types
impl MasstreeProtocol {
    fn parse_get_response(
        &mut self,
        conn_id: usize,
        seq: u32,
        cursor: &mut Cursor<&[u8]>,
        data: &[u8],
    ) -> Result<(usize, Option<(usize, u16)>)> {
        let str_len = match decode::read_str_len(cursor) {
            Ok(len) => len,
            Err(_) => return Ok((0, None)),
        };

        let position = cursor.position() as usize;
        if position + str_len as usize <= data.len() {
            Ok((position + str_len as usize, Some((conn_id, seq as u16))))
        } else {
            Ok((0, None))
        }
    }

    fn parse_set_response(
        &mut self,
        conn_id: usize,
        seq: u32,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<(usize, Option<(usize, u16)>)> {
        match decode::read_int::<u8, _>(cursor) {
            Ok(_result) => {
                let bytes_consumed = cursor.position() as usize;
                Ok((bytes_consumed, Some((conn_id, seq as u16))))
            }
            Err(_) => Ok((0, None)),
        }
    }

    fn parse_handshake_response(
        &mut self,
        conn_id: usize,
        cursor: &mut Cursor<&[u8]>,
        data: &[u8],
    ) -> Result<(usize, Option<(usize, u16)>)> {
        // Read success boolean
        let _success = decode::read_bool(cursor).unwrap_or(false);

        // Read max_seq
        let _max_seq = decode::read_int::<u32, _>(cursor).unwrap_or(0);

        // Read version string
        let str_len = match decode::read_str_len(cursor) {
            Ok(len) => len,
            Err(_) => return Ok((0, None)),
        };

        let position = cursor.position() as usize;
        if position + str_len as usize <= data.len() {
            self.mark_handshake_done(conn_id);
            Ok((position + str_len as usize, Some((conn_id, 0))))
        } else {
            Ok((0, None))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handshake_request() {
        let mut proto = MasstreeProtocol::new(MasstreeOp::Get);
        let (req, (conn_id, seq)) = proto.generate_request(0, 1, 64);

        assert_eq!(conn_id, 0);
        assert_eq!(seq, 0); // Handshake uses seq 0

        // Verify it's a valid MessagePack array
        assert!(!req.is_empty());
        assert_eq!(req[0] & 0xf0, 0x90); // fixarray marker
    }

    #[test]
    fn test_get_request() {
        let mut proto = MasstreeProtocol::new(MasstreeOp::Get);

        // Mark handshake as done
        proto.mark_handshake_done(0);

        let (req, (conn_id, seq)) = proto.generate_request(0, 123, 64);

        assert_eq!(conn_id, 0);
        assert_eq!(seq, 0); // First request after handshake

        // Verify it's a valid MessagePack array
        assert!(!req.is_empty());
    }

    #[test]
    fn test_set_request() {
        let mut proto = MasstreeProtocol::new(MasstreeOp::Set);

        // Mark handshake as done
        proto.mark_handshake_done(0);

        let (req, (conn_id, seq)) = proto.generate_request(0, 456, 128);

        assert_eq!(conn_id, 0);
        assert_eq!(seq, 0);

        // SET requests should be larger (include value)
        assert!(req.len() > 100);
    }

    #[test]
    fn test_sequence_increment() {
        let mut proto = MasstreeProtocol::new(MasstreeOp::Get);
        proto.mark_handshake_done(0);

        let (_, (_, seq1)) = proto.generate_request(0, 1, 64);
        let (_, (_, seq2)) = proto.generate_request(0, 2, 64);
        let (_, (_, seq3)) = proto.generate_request(0, 3, 64);

        assert_eq!(seq1, 0);
        assert_eq!(seq2, 1);
        assert_eq!(seq3, 2);
    }

    #[test]
    fn test_per_connection_sequences() {
        let mut proto = MasstreeProtocol::new(MasstreeOp::Get);
        proto.mark_handshake_done(0);
        proto.mark_handshake_done(1);

        let (_, (_, seq_conn0_1)) = proto.generate_request(0, 1, 64);
        let (_, (_, seq_conn1_1)) = proto.generate_request(1, 1, 64);
        let (_, (_, seq_conn0_2)) = proto.generate_request(0, 2, 64);

        assert_eq!(seq_conn0_1, 0);
        assert_eq!(seq_conn1_1, 0); // Each connection has independent sequence
        assert_eq!(seq_conn0_2, 1);
    }
}
