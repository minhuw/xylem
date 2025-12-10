//! Masstree protocol implementation
//!
//! Masstree uses MessagePack (msgpack) for serialization.
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
use std::io::{Cursor, Write};
use zeropool::BufferPool;

// Masstree command codes
const CMD_GET: u8 = 2;
const CMD_SCAN: u8 = 4;
const CMD_PUT: u8 = 6;
const CMD_REPLACE: u8 = 8;
const CMD_REMOVE: u8 = 10;
const CMD_CHECKPOINT: u8 = 12;
const CMD_HANDSHAKE: u8 = 14;

/// Masstree result codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum ResultCode {
    NotFound = -2,
    Retry = -1,
    OutOfDate = 0,
    Inserted = 1,
    Updated = 2,
    Found = 3,
    ScanDone = 4,
}

impl ResultCode {
    pub fn from_i8(value: i8) -> Option<Self> {
        match value {
            -2 => Some(ResultCode::NotFound),
            -1 => Some(ResultCode::Retry),
            0 => Some(ResultCode::OutOfDate),
            1 => Some(ResultCode::Inserted),
            2 => Some(ResultCode::Updated),
            3 => Some(ResultCode::Found),
            4 => Some(ResultCode::ScanDone),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum MasstreeOp {
    Get,
    Set,
    /// Put with column-based update: [(col_idx, value), ...]
    Put {
        columns: Vec<(u32, Vec<u8>)>,
    },
    Remove,
    /// Scan from firstkey, return up to count key-value pairs
    /// Optional field indices to return specific fields
    Scan {
        firstkey: String,
        count: u32,
        fields: Vec<u32>,
    },
    Checkpoint,
}

/// Insert phase state for data population before measurement
#[derive(Debug)]
pub struct InsertPhaseState {
    /// Total number of keys to insert
    key_count: u64,
    /// Next key to insert
    next_key: u64,
    /// Value size for inserts
    value_size: usize,
    /// Whether insert phase is complete
    complete: bool,
}

impl InsertPhaseState {
    /// Create new insert phase state
    pub fn new(key_count: u64, value_size: usize) -> Self {
        Self {
            key_count,
            next_key: 0,
            value_size,
            complete: false,
        }
    }

    /// Check if insert phase is complete
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// Get next key to insert, returns None if insert phase is complete
    pub fn next_key(&mut self) -> Option<u64> {
        if self.next_key >= self.key_count {
            self.complete = true;
            None
        } else {
            let key = self.next_key;
            self.next_key += 1;
            Some(key)
        }
    }
}

/// Handshake state for a connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandshakeState {
    /// Handshake not yet sent
    NotSent,
    /// Handshake sent, waiting for response
    Pending,
    /// Handshake complete
    Done,
}

pub struct MasstreeProtocol {
    operation: MasstreeOp,
    /// Per-connection sequence numbers
    conn_send_seq: HashMap<usize, u16>,
    /// Per-connection handshake state
    conn_handshake_state: HashMap<usize, HandshakeState>,
    /// Buffer pool for request generation
    pool: BufferPool,
    /// Key generator for next_request()
    key_gen: Option<crate::workload::KeyGeneration>,
    /// Value size for next_request()
    value_size: usize,
    /// Key prefix for generated keys (default: "key:")
    key_prefix: String,
    /// Whether to use random data for values instead of repeated 'x'
    random_data: bool,
    /// RNG for random data generation (only used when random_data is true)
    rng: Option<rand::rngs::SmallRng>,
    /// Insert phase state (None = no insert phase, go straight to measurement)
    insert_phase: Option<InsertPhaseState>,
}

impl MasstreeProtocol {
    pub fn new(operation: MasstreeOp) -> Self {
        Self {
            operation,
            conn_send_seq: HashMap::new(),
            conn_handshake_state: HashMap::new(),
            pool: BufferPool::new(),
            key_gen: None,
            value_size: 64,
            key_prefix: "key:".to_string(),
            random_data: false,
            rng: None,
            insert_phase: None,
        }
    }

    /// Create with embedded workload generator
    pub fn with_workload(
        operation: MasstreeOp,
        key_gen: crate::workload::KeyGeneration,
        value_size: usize,
    ) -> Self {
        Self::with_workload_and_options(
            operation,
            key_gen,
            value_size,
            "key:".to_string(),
            false,
            None,
            None,
        )
    }

    /// Create with embedded workload generator and custom options
    pub fn with_workload_and_options(
        operation: MasstreeOp,
        key_gen: crate::workload::KeyGeneration,
        value_size: usize,
        key_prefix: String,
        random_data: bool,
        seed: Option<u64>,
        insert_phase: Option<InsertPhaseState>,
    ) -> Self {
        use rand::SeedableRng;

        let rng = if random_data {
            Some(match seed {
                Some(s) => rand::rngs::SmallRng::seed_from_u64(s),
                None => rand::rngs::SmallRng::seed_from_u64(rand::random()),
            })
        } else {
            None
        };

        Self {
            operation,
            conn_send_seq: HashMap::new(),
            conn_handshake_state: HashMap::new(),
            pool: BufferPool::new(),
            key_gen: Some(key_gen),
            value_size,
            key_prefix,
            random_data,
            rng,
            insert_phase,
        }
    }

    /// Set the insert phase configuration
    pub fn with_insert_phase(mut self, key_count: u64, value_size: usize) -> Self {
        self.insert_phase = Some(InsertPhaseState::new(key_count, value_size));
        self
    }

    /// Check if currently in insert phase (warmup)
    pub fn is_in_insert_phase(&self) -> bool {
        self.insert_phase.as_ref().is_some_and(|p| !p.is_complete())
    }

    fn next_send_seq(&mut self, conn_id: usize) -> u16 {
        let seq = self.conn_send_seq.entry(conn_id).or_insert(0);
        let result = *seq;
        *seq = seq.wrapping_add(1);
        result
    }

    fn get_handshake_state(&self, conn_id: usize) -> HandshakeState {
        *self.conn_handshake_state.get(&conn_id).unwrap_or(&HandshakeState::NotSent)
    }

    fn is_handshake_done(&self, conn_id: usize) -> bool {
        self.get_handshake_state(conn_id) == HandshakeState::Done
    }

    fn set_handshake_pending(&mut self, conn_id: usize) {
        self.conn_handshake_state.insert(conn_id, HandshakeState::Pending);
    }

    /// Mark a connection's handshake as complete
    /// This is useful for testing to skip the handshake phase
    pub fn mark_handshake_done(&mut self, conn_id: usize) {
        self.conn_handshake_state.insert(conn_id, HandshakeState::Done);
    }

    /// Get the key prefix
    pub fn key_prefix(&self) -> &str {
        &self.key_prefix
    }

    /// Check if random data is enabled
    pub fn random_data(&self) -> bool {
        self.random_data
    }

    /// Format a key with the configured prefix
    fn format_key(&self, key: u64) -> String {
        format!("{}{}", self.key_prefix, key)
    }

    /// Generate value data of the specified size
    fn generate_value(&mut self, size: usize) -> Vec<u8> {
        if self.random_data {
            if let Some(ref mut rng) = self.rng {
                use rand::Rng;
                // Generate printable ASCII characters (33-126)
                (0..size).map(|_| rng.random_range(33u8..127u8)).collect()
            } else {
                // Fallback to 'x' if RNG not available
                vec![b'x'; size]
            }
        } else {
            vec![b'x'; size]
        }
    }

    /// Build a handshake request: [0, 14, {"core": -1, "maxkeylen": 255}]
    fn build_handshake(&self) -> Result<Vec<u8>> {
        let mut buf = self.pool.get(128);
        buf.clear(); // Clear any old data

        // Array of 3 elements: [seq, cmd, map]
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

        Ok(buf.to_vec())
    }

    /// Build a GET request: [seq, 2, key_str]
    fn build_get_request(&self, seq: u16, key: &str) -> Result<Vec<u8>> {
        let mut buf = self.pool.get(256);
        buf.clear(); // Clear any old data

        // Array of 3 elements: [seq, cmd, key]
        encode::write_array_len(&mut buf, 3)?;

        // seq (u32)
        encode::write_u32(&mut buf, seq as u32)?;

        // cmd = 2 (Cmd_Get)
        encode::write_u8(&mut buf, CMD_GET)?;

        // key as string
        encode::write_str(&mut buf, key)?;

        Ok(buf.to_vec())
    }

    /// Build a SET/REPLACE request: [seq, 8, key_str, value_str]
    fn build_set_request(&self, seq: u16, key: &str, value: &[u8]) -> Result<Vec<u8>> {
        let mut buf = self.pool.get(256 + value.len());
        buf.clear(); // Clear any old data

        // Array of 4 elements: [seq, cmd, key, value]
        encode::write_array_len(&mut buf, 4)?;

        // seq (u32)
        encode::write_u32(&mut buf, seq as u32)?;

        // cmd = 8 (Cmd_Replace)
        encode::write_u8(&mut buf, CMD_REPLACE)?;

        // key as string
        encode::write_str(&mut buf, key)?;

        // value as string (not binary, to match lancet implementation)
        encode::write_str_len(&mut buf, value.len() as u32)?;
        buf.write_all(value)?;

        Ok(buf.to_vec())
    }

    /// Build a PUT request: [seq, 6, key_str, col_idx, value, col_idx, value, ...]
    fn build_put_request(
        &self,
        seq: u16,
        key: &str,
        columns: &[(u32, Vec<u8>)],
    ) -> Result<Vec<u8>> {
        // Estimate size: header + key + column data
        let total_value_size: usize = columns.iter().map(|(_, v)| v.len()).sum();
        let mut buf = self.pool.get(256 + total_value_size);
        buf.clear(); // Clear any old data

        // Array size: seq, cmd, key, + (col_idx, value) pairs
        let array_len = 3 + (columns.len() * 2);
        encode::write_array_len(&mut buf, array_len as u32)?;

        // seq (u32)
        encode::write_u32(&mut buf, seq as u32)?;

        // cmd = 6 (Cmd_Put)
        encode::write_u8(&mut buf, CMD_PUT)?;

        // key as string
        encode::write_str(&mut buf, key)?;

        // Write column index and value pairs
        for (col_idx, value) in columns {
            encode::write_u32(&mut buf, *col_idx)?;
            encode::write_str_len(&mut buf, value.len() as u32)?;
            buf.write_all(value)?;
        }

        Ok(buf.to_vec())
    }

    /// Build a REMOVE request: [seq, 10, key_str]
    fn build_remove_request(&self, seq: u16, key: &str) -> Result<Vec<u8>> {
        let mut buf = self.pool.get(256);
        buf.clear(); // Clear any old data

        // Array of 3 elements: [seq, cmd, key]
        encode::write_array_len(&mut buf, 3)?;

        // seq (u32)
        encode::write_u32(&mut buf, seq as u32)?;

        // cmd = 10 (Cmd_Remove)
        encode::write_u8(&mut buf, CMD_REMOVE)?;

        // key as string
        encode::write_str(&mut buf, key)?;

        Ok(buf.to_vec())
    }

    /// Build a SCAN request: [seq, 4, firstkey_str, count_i32, field1_idx, field2_idx, ...]
    fn build_scan_request(
        &self,
        seq: u16,
        firstkey: &str,
        count: u32,
        fields: &[u32],
    ) -> Result<Vec<u8>> {
        let mut buf = self.pool.get(256 + (fields.len() * 4));
        buf.clear(); // Clear any old data

        // Array size: seq, cmd, firstkey, count, + field indices
        let array_len = 4 + fields.len();
        encode::write_array_len(&mut buf, array_len as u32)?;

        // seq (u32)
        encode::write_u32(&mut buf, seq as u32)?;

        // cmd = 4 (Cmd_Scan)
        encode::write_u8(&mut buf, CMD_SCAN)?;

        // firstkey as string
        encode::write_str(&mut buf, firstkey)?;

        // count (must be > 0)
        encode::write_u32(&mut buf, count)?;

        // Optional field indices
        for field_idx in fields {
            encode::write_u32(&mut buf, *field_idx)?;
        }

        Ok(buf.to_vec())
    }

    /// Build a CHECKPOINT request: [seq, 12]
    fn build_checkpoint_request(&self, seq: u16) -> Result<Vec<u8>> {
        let mut buf = self.pool.get(64);
        buf.clear(); // Clear any old data

        // Array of 2 elements: [seq, cmd]
        encode::write_array_len(&mut buf, 2)?;

        // seq (u32)
        encode::write_u32(&mut buf, seq as u32)?;

        // cmd = 12 (Cmd_Checkpoint)
        encode::write_u8(&mut buf, CMD_CHECKPOINT)?;

        Ok(buf.to_vec())
    }
}

impl Default for MasstreeProtocol {
    fn default() -> Self {
        Self::new(MasstreeOp::Get)
    }
}

impl MasstreeProtocol {
    /// Internal method to generate a request with specific key and value size
    /// Note: Caller should check handshake state before calling this
    fn generate_request_internal(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
    ) -> crate::Request<(usize, u16)> {
        let seq = self.next_send_seq(conn_id);
        // Format key with configurable prefix
        let key_str = self.format_key(key);

        let request = match &self.operation {
            MasstreeOp::Get => {
                self.build_get_request(seq, &key_str).expect("Failed to build GET request")
            }
            MasstreeOp::Set => {
                // Generate value using configurable data generation
                let value = self.generate_value(value_size);
                self.build_set_request(seq, &key_str, &value)
                    .expect("Failed to build SET request")
            }
            MasstreeOp::Put { columns } => self
                .build_put_request(seq, &key_str, columns)
                .expect("Failed to build PUT request"),
            MasstreeOp::Remove => self
                .build_remove_request(seq, &key_str)
                .expect("Failed to build REMOVE request"),
            MasstreeOp::Scan { firstkey, count, fields } => self
                .build_scan_request(seq, firstkey, *count, fields)
                .expect("Failed to build SCAN request"),
            MasstreeOp::Checkpoint => {
                self.build_checkpoint_request(seq).expect("Failed to build CHECKPOINT request")
            }
        };

        crate::Request::measurement(request, (conn_id, seq))
    }
}

impl Protocol for MasstreeProtocol {
    type RequestId = (usize, u16);

    fn next_request(&mut self, conn_id: usize) -> crate::Request<Self::RequestId> {
        // Phase 1: Handshake (warmup)
        if !self.is_handshake_done(conn_id) {
            let handshake = self.build_handshake().expect("Failed to build handshake request");
            // Mark handshake as pending to prevent duplicate handshakes
            self.set_handshake_pending(conn_id);
            return crate::Request::warmup(handshake, (conn_id, 0));
        }

        // Phase 2: Insert phase (warmup) - populate data before measurement
        if let Some(ref mut insert_state) = self.insert_phase {
            if let Some(key) = insert_state.next_key() {
                let value_size = insert_state.value_size;
                let seq = self.next_send_seq(conn_id);
                let key_str = self.format_key(key);
                let value = self.generate_value(value_size);
                let request = self
                    .build_set_request(seq, &key_str, &value)
                    .expect("Failed to build SET request for insert phase");
                return crate::Request::warmup(request, (conn_id, seq));
            }
        }

        // Phase 3: Normal measurement phase
        let key = self.key_gen.as_mut().map(|g| g.next_key()).unwrap_or(0);
        self.generate_request_internal(conn_id, key, self.value_size)
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
        let start_position = cursor.position();

        // Try to parse MessagePack array header
        let array_len = match decode::read_array_len(&mut cursor) {
            Ok(len) if len >= 2 => len,
            Ok(_) => return Err(anyhow!("Invalid Masstree response: array too short")),
            Err(_) => return Ok((0, None)), // Incomplete data
        };

        // Read sequence number (u32)
        let seq = match decode::read_int::<u32, _>(&mut cursor) {
            Ok(s) => s,
            Err(_) => return Ok((0, None)), // Incomplete data
        };

        // Read command (u8)
        let cmd = match decode::read_int::<u8, _>(&mut cursor) {
            Ok(c) => c,
            Err(_) => return Ok((0, None)), // Incomplete data
        };

        // Parse based on command type (response = request + 1)
        match cmd {
            // GET response: [seq, 3, value_str]
            3 => {
                if array_len != 3 {
                    return Err(anyhow!(
                        "Invalid GET response: expected 3 elements, got {}",
                        array_len
                    ));
                }
                self.parse_get_response(conn_id, seq, &mut cursor, data, start_position)
            }
            // SCAN response: [seq, 5, key1_str, value1, key2_str, value2, ...]
            5 => {
                // SCAN response has variable length (2 + 2*count)
                self.parse_scan_response(conn_id, seq, &mut cursor, data, start_position, array_len)
            }
            // PUT response: [seq, 7, result_u8]
            7 => {
                if array_len != 3 {
                    return Err(anyhow!(
                        "Invalid PUT response: expected 3 elements, got {}",
                        array_len
                    ));
                }
                self.parse_put_response(conn_id, seq, &mut cursor)
            }
            // REPLACE response: [seq, 9, result_u8]
            9 => {
                if array_len != 3 {
                    return Err(anyhow!(
                        "Invalid REPLACE response: expected 3 elements, got {}",
                        array_len
                    ));
                }
                self.parse_replace_response(conn_id, seq, &mut cursor)
            }
            // REMOVE response: [seq, 11, removed_bool]
            11 => {
                if array_len != 3 {
                    return Err(anyhow!(
                        "Invalid REMOVE response: expected 3 elements, got {}",
                        array_len
                    ));
                }
                self.parse_remove_response(conn_id, seq, &mut cursor)
            }
            // CHECKPOINT response: [seq, 13]
            13 => {
                if array_len != 2 {
                    return Err(anyhow!(
                        "Invalid CHECKPOINT response: expected 2 elements, got {}",
                        array_len
                    ));
                }
                // Checkpoint response is just [seq, cmd], no additional data
                let bytes_consumed = cursor.position() as usize;
                Ok((bytes_consumed, Some((conn_id, seq as u16))))
            }
            // HANDSHAKE response: [0, 15, success_bool, thread_id_u32, version_str]
            15 => {
                if array_len != 5 {
                    return Err(anyhow!(
                        "Invalid HANDSHAKE response: expected 5 elements, got {}",
                        array_len
                    ));
                }
                self.parse_handshake_response(conn_id, &mut cursor, data, start_position)
            }
            _ => Err(anyhow!("Unknown Masstree response command: {}", cmd)),
        }
    }

    fn name(&self) -> &'static str {
        "masstree"
    }

    fn reset(&mut self) {
        self.conn_send_seq.clear();
        self.conn_handshake_state.clear();
        if let Some(ref mut key_gen) = self.key_gen {
            key_gen.reset();
        }
    }

    fn can_send(&self, conn_id: usize) -> bool {
        // Allow sending unless handshake is pending (sent but not responded)
        //
        // This prevents duplicate handshake requests with the same ID (conn_id, 0)
        // when max_pending > 1:
        //
        // State machine:
        // - NotSent → can send (will send handshake)
        // - Pending → CANNOT send (waiting for handshake response)
        // - Done → can send (normal operation)
        let state = self.get_handshake_state(conn_id);
        state != HandshakeState::Pending
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
        _start_position: u64,
    ) -> Result<(usize, Option<(usize, u16)>)> {
        // Read value string length
        let str_len = match decode::read_str_len(cursor) {
            Ok(len) => len as usize,
            Err(_) => return Ok((0, None)), // Incomplete data
        };

        let position = cursor.position() as usize;
        // Check if we have the full string data
        if position + str_len <= data.len() {
            // Advance cursor past the string data
            cursor.set_position((position + str_len) as u64);
            let total_consumed = cursor.position() as usize;
            Ok((total_consumed, Some((conn_id, seq as u16))))
        } else {
            Ok((0, None)) // Incomplete data
        }
    }

    fn parse_replace_response(
        &mut self,
        conn_id: usize,
        seq: u32,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<(usize, Option<(usize, u16)>)> {
        // Read result byte (ResultCode enum)
        match decode::read_int::<i8, _>(cursor) {
            Ok(_result) => {
                let bytes_consumed = cursor.position() as usize;
                Ok((bytes_consumed, Some((conn_id, seq as u16))))
            }
            Err(_) => Ok((0, None)), // Incomplete data
        }
    }

    fn parse_put_response(
        &mut self,
        conn_id: usize,
        seq: u32,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<(usize, Option<(usize, u16)>)> {
        // Read result byte (ResultCode enum)
        match decode::read_int::<i8, _>(cursor) {
            Ok(_result) => {
                let bytes_consumed = cursor.position() as usize;
                Ok((bytes_consumed, Some((conn_id, seq as u16))))
            }
            Err(_) => Ok((0, None)), // Incomplete data
        }
    }

    fn parse_remove_response(
        &mut self,
        conn_id: usize,
        seq: u32,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<(usize, Option<(usize, u16)>)> {
        // Read removed boolean
        match decode::read_bool(cursor) {
            Ok(_removed) => {
                let bytes_consumed = cursor.position() as usize;
                Ok((bytes_consumed, Some((conn_id, seq as u16))))
            }
            Err(_) => Ok((0, None)), // Incomplete data
        }
    }

    fn parse_scan_response(
        &mut self,
        conn_id: usize,
        seq: u32,
        cursor: &mut Cursor<&[u8]>,
        data: &[u8],
        _start_position: u64,
        array_len: u32,
    ) -> Result<(usize, Option<(usize, u16)>)> {
        // SCAN response: [seq, 5, key1_str, value1, key2_str, value2, ...]
        // Array length = 2 + 2*count (seq, cmd, then key-value pairs)

        if array_len < 2 {
            return Err(anyhow!("Invalid SCAN response: array too short"));
        }

        // We already read seq and cmd, now read key-value pairs
        let num_pairs = (array_len - 2) / 2;

        for _ in 0..num_pairs {
            // Read key string
            let key_len = match decode::read_str_len(cursor) {
                Ok(len) => len as usize,
                Err(_) => return Ok((0, None)), // Incomplete data
            };

            let position = cursor.position() as usize;
            if position + key_len > data.len() {
                return Ok((0, None)); // Incomplete data
            }
            cursor.set_position((position + key_len) as u64);

            // Read value string
            let value_len = match decode::read_str_len(cursor) {
                Ok(len) => len as usize,
                Err(_) => return Ok((0, None)), // Incomplete data
            };

            let position = cursor.position() as usize;
            if position + value_len > data.len() {
                return Ok((0, None)); // Incomplete data
            }
            cursor.set_position((position + value_len) as u64);
        }

        let total_consumed = cursor.position() as usize;
        Ok((total_consumed, Some((conn_id, seq as u16))))
    }

    fn parse_handshake_response(
        &mut self,
        conn_id: usize,
        cursor: &mut Cursor<&[u8]>,
        data: &[u8],
        _start_position: u64,
    ) -> Result<(usize, Option<(usize, u16)>)> {
        // Read success boolean
        let _success = match decode::read_bool(cursor) {
            Ok(s) => s,
            Err(_) => return Ok((0, None)), // Incomplete data
        };

        // Read max_seq (u32)
        let _max_seq = match decode::read_int::<u32, _>(cursor) {
            Ok(s) => s,
            Err(_) => return Ok((0, None)), // Incomplete data
        };

        // Read version string length
        let str_len = match decode::read_str_len(cursor) {
            Ok(len) => len as usize,
            Err(_) => return Ok((0, None)), // Incomplete data
        };

        let position = cursor.position() as usize;
        // Check if we have the full string data
        if position + str_len <= data.len() {
            self.mark_handshake_done(conn_id);
            // Advance cursor past the string data
            let total_consumed = position + str_len;
            Ok((total_consumed, Some((conn_id, 0))))
        } else {
            Ok((0, None)) // Incomplete data
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handshake_request() {
        let mut proto = MasstreeProtocol::new(MasstreeOp::Get);
        let request = proto.next_request(0);
        let (conn_id, seq) = request.request_id;

        assert_eq!(conn_id, 0);
        assert_eq!(seq, 0); // Handshake uses seq 0

        // Verify it's a valid MessagePack array
        assert!(!request.data.is_empty());
        assert_eq!(request.data[0] & 0xf0, 0x90); // fixarray marker
    }

    #[test]
    fn test_get_request() {
        let mut proto = MasstreeProtocol::new(MasstreeOp::Get);

        // Mark handshake as done
        proto.mark_handshake_done(0);

        let request = proto.next_request(0);
        let (conn_id, seq) = request.request_id;

        assert_eq!(conn_id, 0);
        assert_eq!(seq, 0); // First request after handshake

        // Verify it's a valid MessagePack array
        assert!(!request.data.is_empty());
    }

    #[test]
    fn test_set_request() {
        let mut proto = MasstreeProtocol::new(MasstreeOp::Set);

        // Mark handshake as done
        proto.mark_handshake_done(0);

        let request = proto.next_request(0);
        let (conn_id, seq) = request.request_id;

        assert_eq!(conn_id, 0);
        assert_eq!(seq, 0);

        // SET requests should be larger (include value)
        // Default value_size is 64, plus overhead for key, seq, cmd
        assert!(request.data.len() > 64, "SET request too small: {} bytes", request.data.len());
    }

    #[test]
    fn test_sequence_increment() {
        let mut proto = MasstreeProtocol::new(MasstreeOp::Get);
        proto.mark_handshake_done(0);

        // Same conn_id, sequence increments
        let request1 = proto.next_request(0);
        let (_, seq1) = request1.request_id;
        let request2 = proto.next_request(0);
        let (_, seq2) = request2.request_id;
        let request3 = proto.next_request(0);
        let (_, seq3) = request3.request_id;

        assert_eq!(seq1, 0);
        assert_eq!(seq2, 1);
        assert_eq!(seq3, 2);
    }

    #[test]
    fn test_per_connection_sequences() {
        let mut proto = MasstreeProtocol::new(MasstreeOp::Get);
        proto.mark_handshake_done(0);
        proto.mark_handshake_done(1);

        let request0_1 = proto.next_request(0);
        let (conn0, seq_conn0_1) = request0_1.request_id;
        let request1_1 = proto.next_request(1);
        let (conn1, seq_conn1_1) = request1_1.request_id;
        let request0_2 = proto.next_request(0);
        let (_, seq_conn0_2) = request0_2.request_id;

        assert_eq!(conn0, 0);
        assert_eq!(conn1, 1);
        assert_eq!(seq_conn0_1, 0);
        assert_eq!(seq_conn1_1, 0); // Each connection has independent sequence
        assert_eq!(seq_conn0_2, 1);
    }

    #[test]
    fn test_remove_request() {
        let mut proto = MasstreeProtocol::new(MasstreeOp::Remove);
        proto.mark_handshake_done(0);

        let request = proto.next_request(0);
        let (conn_id, seq) = request.request_id;

        assert_eq!(conn_id, 0);
        assert_eq!(seq, 0);

        // Verify it's a valid MessagePack array
        assert!(!request.data.is_empty());
        // REMOVE requests are small (just seq, cmd, key)
        assert!(request.data.len() < 50);
    }

    #[test]
    fn test_put_request() {
        let columns = vec![(0, b"value0".to_vec()), (1, b"value1".to_vec())];
        let mut proto = MasstreeProtocol::new(MasstreeOp::Put { columns });
        proto.mark_handshake_done(0);

        let request = proto.next_request(0);
        let (conn_id, seq) = request.request_id;

        assert_eq!(conn_id, 0);
        assert_eq!(seq, 0);

        // PUT requests have seq, cmd, key, + column pairs
        assert!(!request.data.is_empty());
        assert!(request.data.len() > 20); // Should have reasonable size
    }

    #[test]
    fn test_scan_request() {
        let scan_op = MasstreeOp::Scan {
            firstkey: "start_key".to_string(),
            count: 10,
            fields: vec![0, 1, 2],
        };
        let mut proto = MasstreeProtocol::new(scan_op);
        proto.mark_handshake_done(0);

        let request = proto.next_request(0);
        let (conn_id, seq) = request.request_id;

        assert_eq!(conn_id, 0);
        assert_eq!(seq, 0);

        // SCAN requests have seq, cmd, firstkey, count, field indices
        assert!(!request.data.is_empty());
        let req_str = String::from_utf8_lossy(&request.data);
        assert!(req_str.contains("start_key"));
    }

    #[test]
    fn test_checkpoint_request() {
        let mut proto = MasstreeProtocol::new(MasstreeOp::Checkpoint);
        proto.mark_handshake_done(0);

        let request = proto.next_request(0);
        let (conn_id, seq) = request.request_id;

        assert_eq!(conn_id, 0);
        assert_eq!(seq, 0);

        // CHECKPOINT requests are minimal (just seq, cmd)
        assert!(!request.data.is_empty());
        assert!(request.data.len() < 20);
    }

    #[test]
    fn test_result_code_conversion() {
        assert_eq!(ResultCode::from_i8(-2), Some(ResultCode::NotFound));
        assert_eq!(ResultCode::from_i8(-1), Some(ResultCode::Retry));
        assert_eq!(ResultCode::from_i8(0), Some(ResultCode::OutOfDate));
        assert_eq!(ResultCode::from_i8(1), Some(ResultCode::Inserted));
        assert_eq!(ResultCode::from_i8(2), Some(ResultCode::Updated));
        assert_eq!(ResultCode::from_i8(3), Some(ResultCode::Found));
        assert_eq!(ResultCode::from_i8(4), Some(ResultCode::ScanDone));
        assert_eq!(ResultCode::from_i8(99), None);
    }

    #[test]
    fn test_multiple_operations() {
        // Test that different operations work on same protocol instance
        let mut proto1 = MasstreeProtocol::new(MasstreeOp::Get);
        proto1.mark_handshake_done(0);
        let req1 = proto1.next_request(0);

        let mut proto2 = MasstreeProtocol::new(MasstreeOp::Remove);
        proto2.mark_handshake_done(0);
        let req2 = proto2.next_request(0);

        // Requests should be different
        assert_ne!(req1, req2);
    }

    #[test]
    fn test_custom_key_prefix() {
        use crate::workload::KeyGeneration;

        let key_gen = KeyGeneration::sequential(0);
        let mut proto = MasstreeProtocol::with_workload_and_options(
            MasstreeOp::Get,
            key_gen,
            64,
            "memtier-".to_string(),
            false,
            None,
            None, // no insert phase
        );
        proto.mark_handshake_done(0);

        let request = proto.next_request(0);
        let req_str = String::from_utf8_lossy(&request.data);

        assert!(req_str.contains("memtier-"), "Expected custom key prefix 'memtier-'");
        assert!(!req_str.contains("key:"), "Should not contain default prefix 'key:'");
    }

    #[test]
    fn test_custom_key_prefix_set() {
        use crate::workload::KeyGeneration;

        let key_gen = KeyGeneration::sequential(100);
        let mut proto = MasstreeProtocol::with_workload_and_options(
            MasstreeOp::Set,
            key_gen,
            10,
            "test:".to_string(),
            false,
            None,
            None, // no insert phase
        );
        proto.mark_handshake_done(0);

        let request = proto.next_request(0);
        let req_str = String::from_utf8_lossy(&request.data);

        assert!(req_str.contains("test:100"), "Expected custom key prefix 'test:'");
    }

    #[test]
    fn test_random_data_generation() {
        use crate::workload::KeyGeneration;

        let key_gen = KeyGeneration::sequential(0);
        let mut proto = MasstreeProtocol::with_workload_and_options(
            MasstreeOp::Set,
            key_gen,
            100,
            "key:".to_string(),
            true,     // Enable random data
            Some(42), // Use fixed seed for reproducibility
            None,     // no insert phase
        );
        proto.mark_handshake_done(0);

        let request = proto.next_request(0);

        // Count occurrences of 'x' - random data should have fewer consecutive x's
        let x_count = request.data.iter().filter(|&&b| b == b'x').count();
        // With random data of size 100, we shouldn't have 100 consecutive x's
        // (statistically very unlikely with random ASCII 33-126)
        assert!(
            x_count < 50,
            "Random data should not contain many 'x' characters, found {}",
            x_count
        );
    }

    #[test]
    fn test_random_data_reproducibility() {
        use crate::workload::KeyGeneration;

        // Create two protocols with the same seed
        let key_gen1 = KeyGeneration::sequential(0);
        let mut proto1 = MasstreeProtocol::with_workload_and_options(
            MasstreeOp::Set,
            key_gen1,
            50,
            "key:".to_string(),
            true,
            Some(12345),
            None, // no insert phase
        );
        proto1.mark_handshake_done(0);

        let key_gen2 = KeyGeneration::sequential(0);
        let mut proto2 = MasstreeProtocol::with_workload_and_options(
            MasstreeOp::Set,
            key_gen2,
            50,
            "key:".to_string(),
            true,
            Some(12345),
            None, // no insert phase
        );
        proto2.mark_handshake_done(0);

        // Generate requests - they should be identical with the same seed
        let req1 = proto1.next_request(0);
        let req2 = proto2.next_request(0);

        assert_eq!(req1, req2, "Same seed should produce same random data");
    }

    #[test]
    fn test_key_prefix_accessor() {
        use crate::workload::KeyGeneration;

        let key_gen = KeyGeneration::sequential(0);
        let proto = MasstreeProtocol::with_workload_and_options(
            MasstreeOp::Get,
            key_gen,
            64,
            "custom:".to_string(),
            false,
            None,
            None, // no insert phase
        );

        assert_eq!(proto.key_prefix(), "custom:");
        assert!(!proto.random_data());
    }

    #[test]
    fn test_random_data_accessor() {
        use crate::workload::KeyGeneration;

        let key_gen = KeyGeneration::sequential(0);
        let proto = MasstreeProtocol::with_workload_and_options(
            MasstreeOp::Set,
            key_gen,
            64,
            "key:".to_string(),
            true,
            Some(42),
            None, // no insert phase
        );

        assert!(proto.random_data());
    }

    #[test]
    fn test_default_key_prefix() {
        let proto = MasstreeProtocol::new(MasstreeOp::Get);
        assert_eq!(proto.key_prefix(), "key:");
        assert!(!proto.random_data());
    }

    #[test]
    fn test_insert_phase_warmup() {
        use crate::workload::KeyGeneration;

        // Create protocol with insert phase of 10 keys
        let key_gen = KeyGeneration::sequential(0);
        let insert_phase = InsertPhaseState::new(10, 64); // 10 keys, value_size 64
        let mut proto = MasstreeProtocol::with_workload_and_options(
            MasstreeOp::Get,
            key_gen,
            64,
            "key:".to_string(),
            false,
            None,
            Some(insert_phase),
        );

        // First request is handshake (seq 0), should be warmup
        let request1 = proto.next_request(0);
        let (_, seq) = request1.request_id;
        assert_eq!(seq, 0, "First request should be handshake with seq 0");
        assert!(request1.metadata.is_warmup, "Handshake should be warmup");

        // Mark handshake done
        proto.mark_handshake_done(0);

        // Insert phase requests should be warmup and SET operations
        for i in 0..10 {
            let request = proto.next_request(0);
            assert!(request.metadata.is_warmup, "Insert phase request {} should be warmup", i);
            // The request should be a SET operation during insert phase
            let req_str = String::from_utf8_lossy(&request.data);
            assert!(req_str.contains(&format!("key:{}", i)), "Should use sequential keys");
        }

        // After insert phase, requests should not be warmup
        let request_after = proto.next_request(0);
        assert!(
            !request_after.metadata.is_warmup,
            "After insert phase, requests should not be warmup"
        );
    }

    #[test]
    fn test_insert_phase_with_builder() {
        // Test the builder method
        let proto = MasstreeProtocol::new(MasstreeOp::Get).with_insert_phase(100, 128);

        assert!(proto.is_in_insert_phase());

        // Create without insert phase
        let proto2 = MasstreeProtocol::new(MasstreeOp::Get);
        assert!(!proto2.is_in_insert_phase());
    }
}
