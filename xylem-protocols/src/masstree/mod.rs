//! Masstree protocol implementation
//!
//! Masstree (`mtd`, `kvproto.hh`) speaks MessagePack: every request and
//! response is one array that starts with the sequence number and the
//! command; a response's command is the request's plus one, and its
//! sequence number is the request's.
//!
//! Requests:
//! - HANDSHAKE: `[0, 14, {"core": -1, "maxkeylen": 255}]`
//! - GET: `[seq, 2, key]`
//! - SCAN: `[seq, 4, firstkey, count, column...]`, the `count` keys from
//!   `firstkey` on (inclusive), with the listed columns (all if none)
//! - PUT: `[seq, 6, key, column, value, column, value, ...]`
//! - REPLACE (Xylem's SET): `[seq, 8, key, value]`
//! - REMOVE: `[seq, 10, key]`
//! - CHECKPOINT: `[seq, 12]`
//!
//! Responses:
//! - HANDSHAKE: `[0, 15, true, thread_index, row_type]`, or
//!   `[0, 15, false, reason]` when it fails, after which the server closes
//!   the connection
//! - GET: `[seq, 3, column...]` with every column of a hit, or the request
//!   echoed (`[seq, 3, key]`) for a miss
//! - SCAN: `[seq, 5, key, value, ...]`, each value a string for a
//!   one-column row and an array of columns otherwise
//! - PUT, REPLACE: `[seq, 7 or 9, result]`, a [`ResultCode`]
//! - REMOVE: `[seq, 11, removed]`
//! - CHECKPOINT: `[seq, 13]`
//! - a command the server does not run: `[seq, -1]`
//!
//! A PUT or REPLACE response whose result code is Retry or OutOfDate, or a
//! code outside [`ResultCode`], is an error reply named by the code, and so
//! is `[seq, -1]` (`Unsupported`; a Stackbench `mtd` without blocking calls
//! answers CHECKPOINT so). A missing key is not an error: the server
//! answers a GET miss with the key itself, and NotFound is a successful
//! result. A failed handshake fails the run.

mod config;

use crate::workload::{CommandSelector, FixedCommandSelector, ValueSizeGenerator};
use crate::{ErrorReply, Parsed, Protocol};
use anyhow::{anyhow, bail, ensure, Result};
use rmp::encode;
use std::collections::HashMap;
use std::io::Write;

// Masstree command codes
const CMD_GET: u8 = 2;
const CMD_SCAN: u8 = 4;
const CMD_PUT: u8 = 6;
const CMD_REPLACE: u8 = 8;
const CMD_REMOVE: u8 = 10;
const CMD_CHECKPOINT: u8 = 12;
const CMD_HANDSHAKE: u8 = 14;
/// Command of the response to a request the server does not run
const CMD_UNSUPPORTED: i64 = -1;

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

/// The error reply of a PUT or REPLACE result code, `None` for a success
fn result_error(code: i64) -> Option<ErrorReply<'static>> {
    const RETRY: i64 = ResultCode::Retry as i64;
    const OUT_OF_DATE: i64 = ResultCode::OutOfDate as i64;
    let kind = match code {
        RETRY => "Retry",
        OUT_OF_DATE => "OutOfDate",
        // NotFound, Inserted, Updated, Found, ScanDone
        code if (ResultCode::NotFound as i64..=ResultCode::ScanDone as i64).contains(&code) => {
            return None
        }
        _ => "UnknownResult",
    };
    Some(ErrorReply::new(kind, kind))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MasstreeOp {
    Get,
    /// Replace the whole row with one value
    Set,
    /// Write columns `0..columns`, each a value of the value size
    Put {
        columns: u32,
    },
    Remove,
    /// Scan `count` rows from the key on, returning the `fields` columns
    /// (all if empty)
    Scan {
        count: u32,
        fields: Vec<u32>,
    },
    Checkpoint,
}

impl MasstreeOp {
    /// Command name, as `value_size.per_command` keys sizes
    pub fn name(&self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Set => "SET",
            Self::Put { .. } => "PUT",
            Self::Remove => "REMOVE",
            Self::Scan { .. } => "SCAN",
            Self::Checkpoint => "CHECKPOINT",
        }
    }
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
}

impl InsertPhaseState {
    /// Create new insert phase state
    pub fn new(key_count: u64, value_size: usize) -> Self {
        Self { key_count, next_key: 0, value_size }
    }

    /// Check if every key has been handed out
    pub fn is_complete(&self) -> bool {
        self.next_key >= self.key_count
    }

    /// Get next key to insert, returns None if insert phase is complete
    pub fn next_key(&mut self) -> Option<u64> {
        if self.is_complete() {
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
    /// Operation of each measurement request
    selector: Box<dyn CommandSelector<MasstreeOp>>,
    /// Per-connection sequence numbers
    conn_send_seq: HashMap<usize, u16>,
    /// Per-connection handshake state
    conn_handshake_state: HashMap<usize, HandshakeState>,
    /// Key generator for next_request()
    key_gen: Option<crate::workload::KeyGeneration>,
    /// Value size for next_request() without a size generator
    value_size: usize,
    /// Value sizes of SET and PUT columns, by command name
    value_sizes: Option<Box<dyn ValueSizeGenerator>>,
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
            selector: Box::new(FixedCommandSelector::new(operation)),
            conn_send_seq: HashMap::new(),
            conn_handshake_state: HashMap::new(),
            key_gen: None,
            value_size: 64,
            value_sizes: None,
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
            selector: Box::new(FixedCommandSelector::new(operation)),
            conn_send_seq: HashMap::new(),
            conn_handshake_state: HashMap::new(),
            key_gen: Some(key_gen),
            value_size,
            value_sizes: None,
            key_prefix,
            random_data,
            rng,
            insert_phase,
        }
    }

    /// Select each measurement operation with `selector` instead of the one
    /// operation
    pub fn with_operations(mut self, selector: Box<dyn CommandSelector<MasstreeOp>>) -> Self {
        self.selector = selector;
        self
    }

    /// Draw the size of every SET value and PUT column from `sizes`, by
    /// command name
    pub fn with_value_size_generator(mut self, sizes: Box<dyn ValueSizeGenerator>) -> Self {
        self.value_sizes = Some(sizes);
        self
    }

    /// Set the insert phase configuration
    pub fn with_insert_phase(mut self, key_count: u64, value_size: usize) -> Self {
        self.insert_phase = Some(InsertPhaseState::new(key_count, value_size));
        self
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

    /// The size of the next value of `op`
    fn next_value_size(&mut self, op: &MasstreeOp) -> usize {
        match &mut self.value_sizes {
            Some(sizes) => sizes.next_size_for_command(op.name()),
            None => self.value_size,
        }
    }

    /// Build a handshake request: [0, 14, {"core": -1, "maxkeylen": 255}]
    fn build_handshake(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(128);

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

        Ok(buf)
    }

    /// Start a request array of `len` elements: [seq, cmd, ...]
    fn start_request(buf: &mut Vec<u8>, len: u32, seq: u16, cmd: u8) -> Result<()> {
        encode::write_array_len(buf, len)?;
        encode::write_u32(buf, seq as u32)?;
        encode::write_u8(buf, cmd)?;
        Ok(())
    }

    /// Write `value` as a msgpack string, as mtclient sends values
    fn write_value(buf: &mut Vec<u8>, value: &[u8]) -> Result<()> {
        encode::write_str_len(buf, value.len() as u32)?;
        buf.write_all(value)?;
        Ok(())
    }

    /// Build a GET request: [seq, 2, key_str]
    fn build_get_request(&self, seq: u16, key: &str) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(32 + key.len());
        Self::start_request(&mut buf, 3, seq, CMD_GET)?;
        encode::write_str(&mut buf, key)?;
        Ok(buf)
    }

    /// Build a SET/REPLACE request: [seq, 8, key_str, value_str]
    fn build_set_request(&self, seq: u16, key: &str, value: &[u8]) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(32 + key.len() + value.len());
        Self::start_request(&mut buf, 4, seq, CMD_REPLACE)?;
        encode::write_str(&mut buf, key)?;
        Self::write_value(&mut buf, value)?;
        Ok(buf)
    }

    /// Build a PUT request: [seq, 6, key_str, col_idx, value, col_idx, value, ...]
    fn build_put_request(&self, seq: u16, key: &str, columns: &[Vec<u8>]) -> Result<Vec<u8>> {
        let total_value_size: usize = columns.iter().map(Vec::len).sum();
        let mut buf = Vec::with_capacity(32 + key.len() + total_value_size + columns.len() * 10);
        Self::start_request(&mut buf, 3 + 2 * columns.len() as u32, seq, CMD_PUT)?;
        encode::write_str(&mut buf, key)?;
        for (col_idx, value) in columns.iter().enumerate() {
            encode::write_u32(&mut buf, col_idx as u32)?;
            Self::write_value(&mut buf, value)?;
        }
        Ok(buf)
    }

    /// Build a REMOVE request: [seq, 10, key_str]
    fn build_remove_request(&self, seq: u16, key: &str) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(32 + key.len());
        Self::start_request(&mut buf, 3, seq, CMD_REMOVE)?;
        encode::write_str(&mut buf, key)?;
        Ok(buf)
    }

    /// Build a SCAN request: [seq, 4, firstkey_str, count, field1_idx, field2_idx, ...]
    fn build_scan_request(
        &self,
        seq: u16,
        firstkey: &str,
        count: u32,
        fields: &[u32],
    ) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(32 + firstkey.len() + fields.len() * 5);
        Self::start_request(&mut buf, 4 + fields.len() as u32, seq, CMD_SCAN)?;
        encode::write_str(&mut buf, firstkey)?;
        // count (must be > 0: mtd asserts it)
        encode::write_u32(&mut buf, count)?;
        for field_idx in fields {
            encode::write_u32(&mut buf, *field_idx)?;
        }
        Ok(buf)
    }

    /// Build a CHECKPOINT request: [seq, 12]
    fn build_checkpoint_request(&self, seq: u16) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(8);
        Self::start_request(&mut buf, 2, seq, CMD_CHECKPOINT)?;
        Ok(buf)
    }
}

impl Default for MasstreeProtocol {
    fn default() -> Self {
        Self::new(MasstreeOp::Get)
    }
}

impl MasstreeProtocol {
    /// Encode the request of `op` on `key`
    /// Note: Caller should check handshake state before calling this
    fn encode(&mut self, op: &MasstreeOp, seq: u16, key: u64) -> Result<Vec<u8>> {
        // Format key with configurable prefix
        let key_str = self.format_key(key);
        match op {
            MasstreeOp::Get => self.build_get_request(seq, &key_str),
            MasstreeOp::Set => {
                let size = self.next_value_size(op);
                let value = self.generate_value(size);
                self.build_set_request(seq, &key_str, &value)
            }
            MasstreeOp::Put { columns } => {
                let values: Vec<_> = (0..*columns)
                    .map(|_| {
                        let size = self.next_value_size(op);
                        self.generate_value(size)
                    })
                    .collect();
                self.build_put_request(seq, &key_str, &values)
            }
            MasstreeOp::Remove => self.build_remove_request(seq, &key_str),
            MasstreeOp::Scan { count, fields } => {
                self.build_scan_request(seq, &key_str, *count, fields)
            }
            MasstreeOp::Checkpoint => self.build_checkpoint_request(seq),
        }
    }
}

impl Protocol for MasstreeProtocol {
    type RequestId = (usize, u16);

    fn next_request(&mut self, conn_id: usize) -> crate::Request<Self::RequestId> {
        // Phase 1: Handshake (connection setup)
        if !self.is_handshake_done(conn_id) {
            let handshake = self.build_handshake().expect("Failed to build handshake request");
            // Mark handshake as pending to prevent duplicate handshakes
            self.set_handshake_pending(conn_id);
            return crate::Request::setup(handshake, (conn_id, 0));
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
        let op = self.selector.next_command();
        let key = if self.selector.has_per_command_keys() {
            self.selector
                .generate_key_for_command(&op)
                .expect("a selector with per-command keys generates them")
        } else {
            self.key_gen.as_mut().map(|g| g.next_key()).unwrap_or(0)
        };
        let seq = self.next_send_seq(conn_id);
        let request = self
            .encode(&op, seq, key)
            .unwrap_or_else(|e| panic!("Failed to build {} request: {e}", op.name()));
        crate::Request::measurement(request, (conn_id, seq))
    }

    fn parse_response<'a>(
        &mut self,
        conn_id: usize,
        data: &'a [u8],
    ) -> Result<Parsed<'a, (usize, u16)>> {
        let Some(response) = Response::parse(data)? else {
            return Ok(Parsed::Incomplete);
        };
        let id = (conn_id, response.seq as u16);
        let error = match response.body {
            Body::Done => None,
            Body::Result(code) => result_error(code),
            Body::Unsupported => Some(ErrorReply::new("Unsupported", "Unsupported command")),
            Body::Handshake(Ok(())) => {
                self.mark_handshake_done(conn_id);
                None
            }
            Body::Handshake(Err(reason)) => {
                bail!("Masstree handshake failed on connection {conn_id}: {reason}")
            }
        };
        Ok(Parsed::Complete { consumed: response.len, id, error })
    }

    fn name(&self) -> &'static str {
        "masstree"
    }

    fn reset(&mut self) {
        self.conn_send_seq.clear();
        self.conn_handshake_state.clear();
        self.selector.reset();
        if let Some(ref mut key_gen) = self.key_gen {
            key_gen.reset();
        }
        if let Some(sizes) = &mut self.value_sizes {
            sizes.reset();
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

    fn setup_pending(&self, conn_id: usize) -> bool {
        // The handshake is the connection's setup
        !self.is_handshake_done(conn_id)
    }

    fn preload_pending(&self, _conn_id: usize) -> bool {
        // The connections share the insert phase
        self.insert_phase.as_ref().is_some_and(|insert| !insert.is_complete())
    }
}

/// One complete response
#[derive(Debug, PartialEq)]
struct Response {
    /// Its length in bytes
    len: usize,
    seq: u32,
    body: Body,
}

/// What a response says
#[derive(Debug, PartialEq)]
enum Body {
    /// GET, SCAN, REMOVE and CHECKPOINT: done, whatever they found
    Done,
    /// PUT and REPLACE: a result code
    Result(i64),
    /// `[seq, -1]`: the server did not run the command
    Unsupported,
    /// HANDSHAKE: success, or the server's reason for failing
    Handshake(std::result::Result<(), String>),
}

impl Response {
    /// The response at the start of `data`, `None` while it is incomplete
    fn parse(data: &[u8]) -> Result<Option<Self>> {
        let mut reader = Reader { data, pos: 0, read: 0 };
        let Some(array_len) = reader.array_len()? else {
            return Ok(None);
        };
        ensure!(array_len >= 2, "Invalid Masstree response: array of {array_len} elements");
        let Some(seq) = reader.int()? else {
            return Ok(None);
        };
        let seq = u32::try_from(seq)
            .map_err(|_| anyhow!("Invalid Masstree response sequence number {seq}"))?;
        let Some(cmd) = reader.int()? else {
            return Ok(None);
        };
        let expect_len = |expected: u32, what: &str| {
            ensure!(
                array_len == expected,
                "Invalid {what} response: expected {expected} elements, got {array_len}"
            );
            Ok(())
        };
        let body = match cmd {
            // [seq, 3, column...] for a hit, the request echoed for a miss
            3 => {
                ensure!(array_len >= 3, "Invalid GET response: {array_len} elements");
                Body::Done
            }
            // [seq, 5, key, value, ...]
            5 => {
                ensure!(array_len % 2 == 0, "Invalid SCAN response: {array_len} elements");
                Body::Done
            }
            7 | 9 => {
                expect_len(3, if cmd == 7 { "PUT" } else { "REPLACE" })?;
                match reader.int()? {
                    Some(code) => Body::Result(code),
                    None => return Ok(None),
                }
            }
            11 => {
                expect_len(3, "REMOVE")?;
                match reader.bool()? {
                    Some(_removed) => Body::Done,
                    None => return Ok(None),
                }
            }
            13 => {
                expect_len(2, "CHECKPOINT")?;
                Body::Done
            }
            CMD_UNSUPPORTED => {
                expect_len(2, "unsupported command")?;
                Body::Unsupported
            }
            15 => match reader.bool()? {
                None => return Ok(None),
                // [0, 15, true, thread_index, row_type]
                Some(true) => {
                    expect_len(5, "HANDSHAKE")?;
                    Body::Handshake(Ok(()))
                }
                // [0, 15, false, reason]; the reason is the first string
                Some(false) => match reader.first_str_of_rest(array_len)? {
                    Some(reason) => Body::Handshake(Err(
                        reason.unwrap_or_else(|| "no reason given".to_string())
                    )),
                    None => return Ok(None),
                },
            },
            _ => bail!("Unknown Masstree response command: {cmd}"),
        };
        // Skip the elements the body does not read
        while reader.read < array_len {
            if reader.skip()?.is_none() {
                return Ok(None);
            }
        }
        Ok(Some(Self { len: reader.pos, seq, body }))
    }
}

/// Reads the elements of one response array from a buffer that may end
/// early: every read returns `None` when the data ends first
struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
    /// Array elements read so far
    read: u32,
}

impl<'a> Reader<'a> {
    /// The `n` bytes at `at`, `None` if the data ends first
    fn bytes(&self, at: usize, n: usize) -> Option<&'a [u8]> {
        self.data.get(at..at.checked_add(n)?)
    }

    /// The big-endian unsigned integer of `n` bytes at `at`
    fn uint(&self, at: usize, n: usize) -> Option<u64> {
        Some(self.bytes(at, n)?.iter().fold(0, |value, &byte| value << 8 | u64::from(byte)))
    }

    /// The length of the array that starts the response
    fn array_len(&mut self) -> Result<Option<u32>> {
        use rmp::Marker;
        let Some(&byte) = self.data.first() else {
            return Ok(None);
        };
        let (len, header) = match Marker::from_u8(byte) {
            Marker::FixArray(n) => (u64::from(n), 1),
            Marker::Array16 => match self.uint(1, 2) {
                Some(n) => (n, 3),
                None => return Ok(None),
            },
            Marker::Array32 => match self.uint(1, 4) {
                Some(n) => (n, 5),
                None => return Ok(None),
            },
            marker => bail!("Invalid Masstree response: {marker:?} instead of an array"),
        };
        self.pos = header;
        Ok(Some(len as u32))
    }

    /// The next element, an integer
    fn int(&mut self) -> Result<Option<i64>> {
        use rmp::Marker;
        let Some(&byte) = self.data.get(self.pos) else {
            return Ok(None);
        };
        let (value, len) = match Marker::from_u8(byte) {
            Marker::FixPos(n) => (i64::from(n), 1),
            Marker::FixNeg(n) => (i64::from(n), 1),
            marker @ (Marker::U8 | Marker::U16 | Marker::U32 | Marker::U64) => {
                let n = match marker {
                    Marker::U8 => 1,
                    Marker::U16 => 2,
                    Marker::U32 => 4,
                    _ => 8,
                };
                let Some(value) = self.uint(self.pos + 1, n) else {
                    return Ok(None);
                };
                let value = i64::try_from(value)
                    .map_err(|_| anyhow!("Masstree response integer {value} out of range"))?;
                (value, 1 + n)
            }
            marker @ (Marker::I8 | Marker::I16 | Marker::I32 | Marker::I64) => {
                let n = match marker {
                    Marker::I8 => 1,
                    Marker::I16 => 2,
                    Marker::I32 => 4,
                    _ => 8,
                };
                let Some(value) = self.uint(self.pos + 1, n) else {
                    return Ok(None);
                };
                // Sign-extend the n-byte two's complement value
                let shift = 64 - 8 * n as u32;
                (((value << shift) as i64) >> shift, 1 + n)
            }
            marker => bail!("Invalid Masstree response: {marker:?} instead of an integer"),
        };
        self.pos += len;
        self.read += 1;
        Ok(Some(value))
    }

    /// The next element, a boolean
    fn bool(&mut self) -> Result<Option<bool>> {
        use rmp::Marker;
        let Some(&byte) = self.data.get(self.pos) else {
            return Ok(None);
        };
        let value = match Marker::from_u8(byte) {
            Marker::True => true,
            Marker::False => false,
            marker => bail!("Invalid Masstree response: {marker:?} instead of a boolean"),
        };
        self.pos += 1;
        self.read += 1;
        Ok(Some(value))
    }

    /// The next element, a string
    fn str(&mut self) -> Result<Option<&'a [u8]>> {
        let start = self.pos;
        match skip_value(self.data, start)? {
            Some(end) if matches!(self.data[start], 0xa0..=0xbf | 0xd9..=0xdb) => {
                let header = match self.data[start] {
                    0xa0..=0xbf => 1,
                    0xd9 => 2,
                    0xda => 3,
                    _ => 5,
                };
                self.pos = end;
                self.read += 1;
                Ok(Some(&self.data[start + header..end]))
            }
            Some(_) => {
                bail!("Invalid Masstree response: 0x{:02x} instead of a string", self.data[start])
            }
            None => Ok(None),
        }
    }

    /// The first string among the rest of the `array_len` elements, which
    /// it skips; `None` while they are incomplete
    fn first_str_of_rest(&mut self, array_len: u32) -> Result<Option<Option<String>>> {
        let mut found = None;
        while self.read < array_len {
            let is_str = matches!(self.data.get(self.pos), Some(0xa0..=0xbf | 0xd9..=0xdb));
            if is_str && found.is_none() {
                let Some(text) = self.str()? else {
                    return Ok(None);
                };
                found = Some(String::from_utf8_lossy(text).into_owned());
            } else if self.skip()?.is_none() {
                return Ok(None);
            }
        }
        Ok(Some(found))
    }

    /// Skip the next element, whatever it is
    fn skip(&mut self) -> Result<Option<()>> {
        let Some(end) = skip_value(self.data, self.pos)? else {
            return Ok(None);
        };
        self.pos = end;
        self.read += 1;
        Ok(Some(()))
    }
}

/// The end of the msgpack value at `pos` in `data`, `None` if the data ends
/// first
fn skip_value(data: &[u8], pos: usize) -> Result<Option<usize>> {
    use rmp::Marker;
    let reader = Reader { data, pos, read: 0 };
    let Some(&byte) = data.get(pos) else {
        return Ok(None);
    };
    // Bytes after the marker: a length field of `len_bytes` bytes, then
    // `fixed` bytes plus `len` bytes, then `len * items_per_len + items` values
    let (len_bytes, fixed, len_is_bytes, items): (usize, usize, bool, u64) =
        match Marker::from_u8(byte) {
            Marker::FixPos(_) | Marker::FixNeg(_) | Marker::Null | Marker::True | Marker::False => {
                (0, 0, false, 0)
            }
            Marker::U8 | Marker::I8 => (0, 1, false, 0),
            Marker::U16 | Marker::I16 => (0, 2, false, 0),
            Marker::U32 | Marker::I32 | Marker::F32 => (0, 4, false, 0),
            Marker::U64 | Marker::I64 | Marker::F64 => (0, 8, false, 0),
            Marker::FixStr(n) => (0, usize::from(n), false, 0),
            Marker::Str8 | Marker::Bin8 => (1, 0, true, 0),
            Marker::Str16 | Marker::Bin16 => (2, 0, true, 0),
            Marker::Str32 | Marker::Bin32 => (4, 0, true, 0),
            Marker::FixArray(n) => (0, 0, false, u64::from(n)),
            Marker::FixMap(n) => (0, 0, false, 2 * u64::from(n)),
            Marker::Array16 | Marker::Map16 | Marker::Array32 | Marker::Map32 => {
                let len_bytes = if matches!(Marker::from_u8(byte), Marker::Array16 | Marker::Map16)
                {
                    2
                } else {
                    4
                };
                let Some(n) = reader.uint(pos + 1, len_bytes) else {
                    return Ok(None);
                };
                let per = if matches!(Marker::from_u8(byte), Marker::Map16 | Marker::Map32) {
                    2
                } else {
                    1
                };
                (len_bytes, 0, false, per * n)
            }
            Marker::FixExt1 => (0, 2, false, 0),
            Marker::FixExt2 => (0, 3, false, 0),
            Marker::FixExt4 => (0, 5, false, 0),
            Marker::FixExt8 => (0, 9, false, 0),
            Marker::FixExt16 => (0, 17, false, 0),
            Marker::Ext8 => (1, 1, true, 0),
            Marker::Ext16 => (2, 1, true, 0),
            Marker::Ext32 => (4, 1, true, 0),
            Marker::Reserved => bail!("Invalid Masstree response: reserved msgpack marker 0xc1"),
        };
    let mut end = pos + 1;
    let mut payload = fixed;
    if len_is_bytes {
        let Some(len) = reader.uint(end, len_bytes) else {
            return Ok(None);
        };
        payload += usize::try_from(len)?;
    }
    end += len_bytes;
    end = end
        .checked_add(payload)
        .ok_or_else(|| anyhow!("Masstree value length overflow"))?;
    if data.len() < end {
        return Ok(None);
    }
    for _ in 0..items {
        match skip_value(data, end)? {
            Some(next) => end = next,
            None => return Ok(None),
        }
    }
    Ok(Some(end))
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
    fn put_writes_every_column_with_a_value() {
        use crate::workload::KeyGeneration;
        let mut proto = MasstreeProtocol::with_workload(
            MasstreeOp::Put { columns: 2 },
            KeyGeneration::sequential(5),
            3,
        );
        proto.mark_handshake_done(0);
        let request = proto.next_request(0);
        // [0, 6, "key:5", 0, "xxx", 1, "xxx"]
        let mut expected = vec![0x97, 0xce, 0, 0, 0, 0, 0xcc, CMD_PUT, 0xa5];
        expected.extend_from_slice(b"key:5");
        expected.extend_from_slice(&[0xce, 0, 0, 0, 0, 0xa3, b'x', b'x', b'x']);
        expected.extend_from_slice(&[0xce, 0, 0, 0, 1, 0xa3, b'x', b'x', b'x']);
        assert_eq!(request.data, expected);
    }

    #[test]
    fn scan_starts_at_the_generated_key() {
        use crate::workload::KeyGeneration;
        let scan_op = MasstreeOp::Scan { count: 10, fields: vec![0, 1, 2] };
        let mut proto = MasstreeProtocol::with_workload(scan_op, KeyGeneration::sequential(42), 8);
        proto.mark_handshake_done(0);

        for key in [42, 43] {
            let request = proto.next_request(0);
            // [seq, 4, "key:N", 10, 0, 1, 2]
            let mut expected = vec![0x97, 0xce, 0, 0, 0, (key - 42) as u8, 0xcc, CMD_SCAN, 0xa6];
            expected.extend_from_slice(format!("key:{key}").as_bytes());
            expected.extend_from_slice(&[0xce, 0, 0, 0, 10]);
            for field in 0..3 {
                expected.extend_from_slice(&[0xce, 0, 0, 0, field]);
            }
            assert_eq!(request.data, expected);
        }
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
        assert!(request1.metadata.is_setup, "Handshake should be connection setup");

        // Mark handshake done
        proto.mark_handshake_done(0);

        // Insert phase requests should be warmup and SET operations
        for i in 0..10 {
            // Every connection shares the insert phase
            assert!(proto.preload_pending(0) && proto.preload_pending(1));
            let request = proto.next_request(0);
            assert!(request.metadata.is_warmup, "Insert phase request {} should be warmup", i);
            assert!(!request.metadata.is_setup, "Insert phase request {} is not setup", i);
            // The request should be a SET operation during insert phase
            let req_str = String::from_utf8_lossy(&request.data);
            assert!(req_str.contains(&format!("key:{}", i)), "Should use sequential keys");
        }

        // After insert phase, requests should not be warmup
        assert!(!proto.preload_pending(0) && !proto.preload_pending(1));
        let request_after = proto.next_request(0);
        assert!(
            !request_after.metadata.is_warmup,
            "After insert phase, requests should not be warmup"
        );
    }

    #[test]
    fn test_insert_phase_with_builder() {
        // Test the builder method: after the handshake, the first request is an insert
        let mut proto = MasstreeProtocol::new(MasstreeOp::Get).with_insert_phase(100, 128);
        proto.mark_handshake_done(0);
        assert!(proto.next_request(0).metadata.is_warmup);

        // Create without insert phase
        let mut proto2 = MasstreeProtocol::new(MasstreeOp::Get);
        proto2.mark_handshake_done(0);
        assert!(!proto2.next_request(0).metadata.is_warmup);
    }

    /// Responses as Stackbench's mtd sends them (captured from it)
    #[test]
    fn responses_of_every_command_are_framed() {
        fn parse(data: &[u8]) -> Result<Parsed<'_, (usize, u16)>> {
            MasstreeProtocol::new(MasstreeOp::Get).parse_response(3, data)
        }
        for (response, seq) in [
            // Handshake: [0, 15, true, 0, "Bag"]
            (&b"\x95\x00\x0f\xc3\x00\xa3Bag"[..], 0),
            // PUT Inserted, REPLACE Updated
            (b"\x93\x01\x07\x01", 1),
            (b"\x93\x01\x09\x02", 1),
            // GET hit of a two-column row, and of a one-column row
            (b"\x94\x02\x03\xa2aa\xa2bb", 2),
            (b"\x93\x02\x03\xa2aa", 2),
            // GET miss: the request echoed
            (b"\x93\x03\x03\xa1z", 3),
            // SCAN: a multi-column row's value is an array, none found
            (b"\x94\x04\x05\xa1k\x92\xa2aa\xa2bb", 4),
            (b"\x96\x04\x05\xa1k\xa2aa\xa1l\xa2bb", 4),
            (b"\x92\x04\x05", 4),
            // REMOVE, CHECKPOINT
            (b"\x93\x06\x0b\xc3", 6),
            (b"\x92\x05\x0d", 5),
            // A sequence number past 127 is a msgpack uint
            (b"\x93\xcd\x01\x2c\x0b\xc2", 300),
        ] {
            assert_eq!(
                parse(response).unwrap(),
                Parsed::ok(response.len(), (3, seq)),
                "{response:?}"
            );
            let mut two = response.to_vec();
            two.extend_from_slice(b"\x92\x05\x0d");
            assert_eq!(parse(&two).unwrap(), Parsed::ok(response.len(), (3, seq)));
            for len in 0..response.len() {
                assert_eq!(parse(&response[..len]).unwrap(), Parsed::Incomplete, "{response:?}");
            }
        }
    }

    #[test]
    fn unsupported_commands_and_failed_results_are_error_replies() {
        let mut proto = MasstreeProtocol::new(MasstreeOp::Get);
        // [5, -1]: mtd did not run the command (a checkpoint on a backend
        // without blocking calls)
        assert_eq!(
            proto.parse_response(0, b"\x92\x05\xff").unwrap(),
            Parsed::error(3, (0, 5), ErrorReply::new("Unsupported", "Unsupported command"))
        );
        for (code, kind) in [(0xff, "Retry"), (0x00, "OutOfDate"), (0x09, "UnknownResult")] {
            let response = [0x93, 0x01, 0x09, code];
            assert_eq!(
                proto.parse_response(0, &response).unwrap(),
                Parsed::error(4, (0, 1), ErrorReply::new(kind, kind))
            );
        }
        // NotFound is a result
        assert_eq!(proto.parse_response(0, b"\x93\x01\x07\xfe").unwrap(), Parsed::ok(4, (0, 1)));
    }

    #[test]
    fn a_failed_handshake_fails_with_the_servers_reason() {
        let mut proto = MasstreeProtocol::new(MasstreeOp::Get);
        proto.next_request(0);
        let error = proto
            .parse_response(0, b"\x94\x00\x0f\xc2\xadbad maxkeylen")
            .unwrap_err()
            .to_string();
        assert_eq!(error, "Masstree handshake failed on connection 0: bad maxkeylen");
        assert!(proto.setup_pending(0));
        let error = proto.parse_response(0, b"\x93\x00\x0f\xc2").unwrap_err().to_string();
        assert_eq!(error, "Masstree handshake failed on connection 0: no reason given");
        assert!(proto.parse_response(0, b"\x95\x00\x0f\xc3\x00\xa3Bag").is_ok());
        assert!(!proto.setup_pending(0));
    }

    #[test]
    fn malformed_responses_are_errors() {
        for response in [
            &b"\x93\x01\x2a\x00"[..], // unknown command 42
            b"\x91\x01",              // array too short
            b"\xa1x",                 // not an array
            b"\x92\x01\x03",          // GET without a column or key
            b"\x93\x01\x05\xa1k",     // SCAN key without a value
            b"\x94\x01\x07\x01\x01",  // PUT with an extra element
            b"\x93\x01\x07\xa1x",     // PUT result not an integer
            b"\x93\x01\x0b\x01",      // REMOVE result not a boolean
            b"\x93\xff\x03\xa1k",     // negative sequence number
            b"\x93\x01\x03\xc1",      // reserved marker
        ] {
            let mut proto = MasstreeProtocol::new(MasstreeOp::Get);
            assert!(proto.parse_response(0, response).is_err(), "{response:?}");
        }
    }

    #[test]
    fn skip_value_crosses_every_msgpack_type() {
        let mut values: Vec<Vec<u8>> = Vec::new();
        let mut buf = Vec::new();
        encode::write_nil(&mut buf).unwrap();
        values.push(std::mem::take(&mut buf));
        for value in [0u64, 200, 70_000, 5_000_000_000] {
            encode::write_uint(&mut buf, value).unwrap();
            values.push(std::mem::take(&mut buf));
        }
        for value in [-1i64, -100, -40_000, -3_000_000_000] {
            encode::write_sint(&mut buf, value).unwrap();
            values.push(std::mem::take(&mut buf));
        }
        encode::write_f64(&mut buf, 1.5).unwrap();
        values.push(std::mem::take(&mut buf));
        for len in [3, 40, 300, 70_000] {
            encode::write_str_len(&mut buf, len as u32).unwrap();
            buf.resize(buf.len() + len, b'v');
            values.push(std::mem::take(&mut buf));
            encode::write_bin(&mut buf, &vec![1; len]).unwrap();
            values.push(std::mem::take(&mut buf));
        }
        encode::write_ext_meta(&mut buf, 4, 7).unwrap();
        buf.extend_from_slice(&[0; 4]);
        values.push(std::mem::take(&mut buf));
        encode::write_ext_meta(&mut buf, 5, 7).unwrap();
        buf.extend_from_slice(&[0; 5]);
        values.push(std::mem::take(&mut buf));
        // An array of 20 and a map of every value so far
        encode::write_array_len(&mut buf, 20).unwrap();
        for _ in 0..20 {
            buf.extend_from_slice(&values[1]);
        }
        values.push(std::mem::take(&mut buf));
        encode::write_map_len(&mut buf, values.len() as u32).unwrap();
        for value in values.clone() {
            buf.extend_from_slice(&values[0]);
            buf.extend_from_slice(&value);
        }
        values.push(std::mem::take(&mut buf));
        for value in values {
            assert_eq!(skip_value(&value, 0).unwrap(), Some(value.len()), "{value:?}");
            for len in 0..value.len() {
                assert_eq!(skip_value(&value[..len], 0).unwrap(), None);
            }
        }
    }

    #[test]
    fn value_sizes_follow_the_generator_per_command() {
        use crate::workload::{FixedSize, KeyGeneration, PerCommandSize, ValueSizeGenerator};
        let mut sizes: HashMap<String, Box<dyn ValueSizeGenerator>> = HashMap::new();
        sizes.insert("put".to_string(), Box::new(FixedSize::new(2)));
        let mut proto =
            MasstreeProtocol::with_workload(MasstreeOp::Set, KeyGeneration::sequential(0), 9)
                .with_value_size_generator(Box::new(PerCommandSize::new(
                    sizes,
                    Box::new(FixedSize::new(4)),
                )));
        proto.mark_handshake_done(0);
        assert!(proto.next_request(0).data.ends_with(b"\xa4xxxx"));
        let mut proto = proto
            .with_operations(Box::new(FixedCommandSelector::new(MasstreeOp::Put { columns: 1 })));
        assert!(proto.next_request(0).data.ends_with(b"\xa2xx"));
    }
}
