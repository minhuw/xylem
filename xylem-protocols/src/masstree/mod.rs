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
use std::io::Write;
use std::ops::Range;
use xylem_common::{FastHashMap, VerificationMismatch, VerificationReport};

const FNV1A_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h = FNV1A_OFFSET_BASIS;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV1A_PRIME);
    }
    h
}

const GOLDEN_RATIO: u64 = 0x9e37_79b9_7f4a_7c15;
/// Maximum bytes consumed in a single `parse_response` step, matching
/// `xylem-core`'s `MAX_PAYLOAD` (64 KiB).
const MAX_STREAM_STEP: usize = 65536;

#[inline(always)]
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(GOLDEN_RATIO);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[inline(always)]
fn verify_word(base: u64, word_idx: u64) -> u64 {
    splitmix64(base.wrapping_add(word_idx.wrapping_mul(GOLDEN_RATIO)))
}

#[inline]
fn pattern_base(seed: u64, key: &[u8], value_size: usize, col_idx: u32) -> u64 {
    splitmix64(
        seed ^ fnv1a_64(key)
            ^ (value_size as u64).wrapping_mul(GOLDEN_RATIO)
            ^ u64::from(col_idx).wrapping_mul(0xbf58_476d_1ce4_e5b9),
    )
}

#[cold]
#[inline(never)]
fn first_word_mismatch(word_start_offset: u64, expected: u64, actual: u64) -> (u64, u8, u8) {
    let diff = expected ^ actual;
    let byte_idx = (diff.trailing_zeros() / 8) as usize;
    let exp = expected.to_le_bytes()[byte_idx];
    let act = actual.to_le_bytes()[byte_idx];
    (word_start_offset + byte_idx as u64, exp, act)
}

/// Fill `out` with the deterministic Masstree verification pattern for
/// `(seed, key, value_size, col_idx)`.
pub fn fill_verify_pattern(seed: u64, key: &[u8], value_size: usize, col_idx: u32, out: &mut [u8]) {
    let base = pattern_base(seed, key, value_size, col_idx);
    let mut w = 0u64;

    let (chunks32, rem32) = out.as_chunks_mut::<32>();
    for block in chunks32 {
        let w0 = verify_word(base, w).to_le_bytes();
        let w1 = verify_word(base, w + 1).to_le_bytes();
        let w2 = verify_word(base, w + 2).to_le_bytes();
        let w3 = verify_word(base, w + 3).to_le_bytes();
        block[0..8].copy_from_slice(&w0);
        block[8..16].copy_from_slice(&w1);
        block[16..24].copy_from_slice(&w2);
        block[24..32].copy_from_slice(&w3);
        w += 4;
    }

    let (chunks8, tail) = rem32.as_chunks_mut::<8>();
    for block in chunks8 {
        block.copy_from_slice(&verify_word(base, w).to_le_bytes());
        w += 1;
    }

    if !tail.is_empty() {
        let bytes = verify_word(base, w).to_le_bytes();
        tail.copy_from_slice(&bytes[..tail.len()]);
    }
}

/// Compare `chunk` starting at byte `offset` against the deterministic
/// Masstree verification pattern for `base`; returns
/// `Some((offset, expected, actual))` on the first differing byte.
pub fn check_verify_pattern_chunk(base: u64, offset: usize, chunk: &[u8]) -> Option<(u64, u8, u8)> {
    if chunk.is_empty() {
        return None;
    }
    let mut rest = chunk;
    let mut off = offset;

    // Unaligned head up to the next 8-byte word boundary
    let rem_head = off & 7;
    if rem_head != 0 {
        let exp = verify_word(base, (off >> 3) as u64).to_le_bytes();
        let take = (8 - rem_head).min(rest.len());
        for i in 0..take {
            let expected = exp[rem_head + i];
            let actual = rest[i];
            if actual != expected {
                return Some(((off + i) as u64, expected, actual));
            }
        }
        rest = &rest[take..];
        off += take;
    }

    let mut w = (off >> 3) as u64;

    // 4x-unrolled 32-byte body
    let (chunks32, rem32) = rest.as_chunks::<32>();
    for block in chunks32 {
        let e0 = verify_word(base, w);
        let e1 = verify_word(base, w + 1);
        let e2 = verify_word(base, w + 2);
        let e3 = verify_word(base, w + 3);
        let a0 = u64::from_le_bytes(block[0..8].try_into().unwrap());
        let a1 = u64::from_le_bytes(block[8..16].try_into().unwrap());
        let a2 = u64::from_le_bytes(block[16..24].try_into().unwrap());
        let a3 = u64::from_le_bytes(block[24..32].try_into().unwrap());
        if ((e0 ^ a0) | (e1 ^ a1) | (e2 ^ a2) | (e3 ^ a3)) != 0 {
            if e0 != a0 {
                return Some(first_word_mismatch(w << 3, e0, a0));
            }
            if e1 != a1 {
                return Some(first_word_mismatch((w + 1) << 3, e1, a1));
            }
            if e2 != a2 {
                return Some(first_word_mismatch((w + 2) << 3, e2, a2));
            }
            return Some(first_word_mismatch((w + 3) << 3, e3, a3));
        }
        w += 4;
    }

    // Remaining full 8-byte words
    let (chunks8, tail) = rem32.as_chunks::<8>();
    for block in chunks8 {
        let e = verify_word(base, w);
        let a = u64::from_le_bytes(*block);
        if e != a {
            return Some(first_word_mismatch(w << 3, e, a));
        }
        w += 1;
    }

    // Trailing partial word (< 8 bytes)
    if !tail.is_empty() {
        let exp = verify_word(base, w).to_le_bytes();
        for (i, &actual) in tail.iter().enumerate() {
            if actual != exp[i] {
                return Some(((w << 3) + i as u64, exp[i], actual));
            }
        }
    }

    None
}

/// Compare `actual` against the expected verification pattern; returns
/// `Some((offset, expected, actual))` on the first differing byte.
pub fn check_verify_pattern(
    seed: u64,
    key: &[u8],
    value_size: usize,
    col_idx: u32,
    actual: &[u8],
) -> Option<(u64, u8, u8)> {
    let base = pattern_base(seed, key, value_size, col_idx);
    check_verify_pattern_chunk(base, 0, actual)
}

/// Incremental verification state for a streamed GET response
struct StreamVerifyState {
    key_num: u64,
    base: u64,
    expected_size: usize,
    /// True while all bytes of `col0` seen so far match the key (only possible
    /// for single-column GET responses with `col0_len == key_bytes.len()`)
    maybe_miss: bool,
    /// First pattern mismatch within `[0..expected_size)`
    first_byte_mismatch: Option<(u64, u8, u8)>,
    /// The byte at offset `expected_size` when `col0_len > expected_size`
    extra_byte: Option<u8>,
}

impl StreamVerifyState {
    fn feed_chunk(&mut self, key_bytes: &[u8], col0_len: usize, off: usize, chunk: &[u8]) {
        if chunk.is_empty() {
            return;
        }
        if self.maybe_miss && key_bytes.get(off..off + chunk.len()) != Some(chunk) {
            self.maybe_miss = false;
        }
        if col0_len >= self.expected_size
            && self.first_byte_mismatch.is_none()
            && off < self.expected_size
        {
            let check_end = (off + chunk.len()).min(self.expected_size);
            if let Some(m) = check_verify_pattern_chunk(self.base, off, &chunk[..check_end - off]) {
                self.first_byte_mismatch = Some(m);
            }
        }
        if col0_len > self.expected_size
            && self.extra_byte.is_none()
            && off <= self.expected_size
            && self.expected_size < off + chunk.len()
        {
            self.extra_byte = Some(chunk[self.expected_size - off]);
        }
    }
}

/// Per-connection receive state for streaming GET responses larger than
/// [`MAX_STREAM_STEP`]. Only populated while such a response is in flight.
struct MasstreeRxState {
    seq: u16,
    /// Declared byte length of `col0`
    col0_len: usize,
    /// Bytes of `col0` already consumed
    col0_offset: usize,
    /// Bytes of `col0` still remaining on the wire
    remaining_col0: usize,
    /// Remaining array elements after `col0` (non-zero for multi-column GET)
    remaining_elements: u32,
    /// Remaining payload bytes when streaming-skipping a large trailing element
    remaining_skip_bytes: usize,
    /// Incremental verification state, if verification was enabled for this GET
    verify: Option<StreamVerifyState>,
}

/// Opt-in payload verification state for [`MasstreeProtocol`]
struct VerifyState {
    seed: u64,
    expected_size: usize,
    pending_gets: FastHashMap<usize, FastHashMap<u16, u64>>,
    report: VerificationReport,
}

impl VerifyState {
    fn new(seed: u64, expected_size: usize) -> Self {
        Self {
            seed,
            expected_size,
            pending_gets: FastHashMap::default(),
            report: VerificationReport::default(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record_failure(
        &mut self,
        kind: &'static str,
        key: &str,
        offset: u64,
        expected: Option<u8>,
        actual: Option<u8>,
        expected_len: Option<usize>,
        actual_len: Option<usize>,
        detail: &'static str,
    ) -> ErrorReply<'static> {
        self.report.failures += 1;
        if self.report.first_mismatch.is_none() {
            self.report.first_mismatch = Some(VerificationMismatch {
                kind: kind.to_string(),
                key: Some(key.to_string()),
                offset,
                expected,
                actual,
                expected_len,
                actual_len,
                detail: detail.to_string(),
            });
        }
        ErrorReply::new(kind, detail)
    }

    fn verify_get(
        &mut self,
        conn_id: usize,
        seq: u16,
        key_prefix: &str,
        key_buf: &mut String,
        first_col: Option<&[u8]>,
        col_count: u32,
    ) -> Option<ErrorReply<'static>> {
        let key_num = self.pending_gets.get_mut(&conn_id)?.remove(&seq)?;
        let key_str = format_key_into(key_prefix, key_num, key_buf);
        let key_bytes = key_str.as_bytes();
        let expected_size = self.expected_size;

        let Some(actual) = first_col else {
            return Some(self.record_failure(
                "ValueMismatch",
                key_str,
                0,
                None,
                None,
                Some(expected_size),
                None,
                "GET column is not a byte string",
            ));
        };

        if col_count == 1
            && actual == key_bytes
            && (actual.len() != expected_size
                || check_verify_pattern(self.seed, key_bytes, expected_size, 0, actual).is_some())
        {
            return Some(self.record_failure(
                "UnexpectedMiss",
                key_str,
                0,
                None,
                None,
                Some(expected_size),
                Some(actual.len()),
                "GET key not found (miss)",
            ));
        }

        if actual.len() < expected_size {
            return Some(self.record_failure(
                "ShortValue",
                key_str,
                actual.len() as u64,
                None,
                None,
                Some(expected_size),
                Some(actual.len()),
                "GET value shorter than expected",
            ));
        }

        if let Some((offset, expected, actual_byte)) =
            check_verify_pattern(self.seed, key_bytes, expected_size, 0, &actual[..expected_size])
        {
            return Some(self.record_failure(
                "ValueMismatch",
                key_str,
                offset,
                Some(expected),
                Some(actual_byte),
                Some(expected_size),
                Some(actual.len()),
                "GET value mismatch",
            ));
        }

        if actual.len() > expected_size {
            return Some(self.record_failure(
                "ValueMismatch",
                key_str,
                expected_size as u64,
                None,
                Some(actual[expected_size]),
                Some(expected_size),
                Some(actual.len()),
                "GET value longer than expected",
            ));
        }

        self.report.verified += 1;
        None
    }

    fn start_stream(
        &mut self,
        conn_id: usize,
        seq: u16,
        key_prefix: &str,
        key_buf: &mut String,
        col0_len: usize,
        col_count: u32,
    ) -> Option<StreamVerifyState> {
        let key_num = self.pending_gets.get_mut(&conn_id)?.remove(&seq)?;
        let key_str = format_key_into(key_prefix, key_num, key_buf);
        let key_bytes = key_str.as_bytes();
        let expected_size = self.expected_size;
        let base = pattern_base(self.seed, key_bytes, expected_size, 0);
        let maybe_miss = col_count == 1 && col0_len == key_bytes.len();
        Some(StreamVerifyState {
            key_num,
            base,
            expected_size,
            maybe_miss,
            first_byte_mismatch: None,
            extra_byte: None,
        })
    }

    fn finalize_stream(
        &mut self,
        key_prefix: &str,
        key_buf: &mut String,
        col0_len: usize,
        sv: StreamVerifyState,
    ) -> Option<ErrorReply<'static>> {
        let key_str = format_key_into(key_prefix, sv.key_num, key_buf);
        let expected_size = sv.expected_size;

        if sv.maybe_miss && (col0_len != expected_size || sv.first_byte_mismatch.is_some()) {
            return Some(self.record_failure(
                "UnexpectedMiss",
                key_str,
                0,
                None,
                None,
                Some(expected_size),
                Some(col0_len),
                "GET key not found (miss)",
            ));
        }

        if col0_len < expected_size {
            return Some(self.record_failure(
                "ShortValue",
                key_str,
                col0_len as u64,
                None,
                None,
                Some(expected_size),
                Some(col0_len),
                "GET value shorter than expected",
            ));
        }

        if let Some((offset, expected, actual_byte)) = sv.first_byte_mismatch {
            return Some(self.record_failure(
                "ValueMismatch",
                key_str,
                offset,
                Some(expected),
                Some(actual_byte),
                Some(expected_size),
                Some(col0_len),
                "GET value mismatch",
            ));
        }

        if col0_len > expected_size {
            return Some(self.record_failure(
                "ValueMismatch",
                key_str,
                expected_size as u64,
                None,
                sv.extra_byte,
                Some(expected_size),
                Some(col0_len),
                "GET value longer than expected",
            ));
        }

        self.report.verified += 1;
        None
    }
}

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
    conn_send_seq: FastHashMap<usize, u16>,
    /// Per-connection handshake state
    conn_handshake_state: FastHashMap<usize, HandshakeState>,
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
    /// The last formatted key, kept for its allocation
    key_buf: String,
    /// Opt-in deterministic value verification state (`None` when disabled)
    verify: Option<VerifyState>,
    /// Per-connection streaming receive state for GET responses exceeding
    /// [`MAX_STREAM_STEP`]; empty on the small-response fast path.
    conn_rx: FastHashMap<usize, MasstreeRxState>,
}

impl MasstreeProtocol {
    pub fn new(operation: MasstreeOp) -> Self {
        Self {
            selector: Box::new(FixedCommandSelector::new(operation)),
            conn_send_seq: FastHashMap::default(),
            conn_handshake_state: FastHashMap::default(),
            key_gen: None,
            value_size: 64,
            value_sizes: None,
            key_prefix: "key:".to_string(),
            random_data: false,
            rng: None,
            insert_phase: None,
            key_buf: String::new(),
            verify: None,
            conn_rx: FastHashMap::default(),
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
            conn_send_seq: FastHashMap::default(),
            conn_handshake_state: FastHashMap::default(),
            key_gen: Some(key_gen),
            value_size,
            value_sizes: None,
            key_prefix,
            random_data,
            rng,
            insert_phase,
            key_buf: String::new(),
            verify: None,
            conn_rx: FastHashMap::default(),
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

    /// Enable opt-in deterministic value verification for GET/SET/PUT/insert_phase
    pub fn with_verify(mut self, seed: u64, expected_size: usize) -> Self {
        self.verify = Some(VerifyState::new(seed, expected_size));
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

    /// The key `key` with the configured prefix, written into the
    /// protocol's key buffer, which the caller takes and gives back
    fn format_key(&mut self, key: u64) -> String {
        let mut out = std::mem::take(&mut self.key_buf);
        out.clear();
        out.push_str(&self.key_prefix);
        push_decimal(&mut out, key);
        out
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
        encode::write_str_len(buf, u32::try_from(value.len())?)?;
        buf.write_all(value)?;
        Ok(())
    }

    /// Write a deterministic verification value directly into `buf` without an
    /// intermediate allocation or copy.
    fn write_verify_value(
        buf: &mut Vec<u8>,
        seed: u64,
        key: &[u8],
        value_size: usize,
        col_idx: u32,
    ) -> Result<()> {
        encode::write_str_len(buf, u32::try_from(value_size)?)?;
        let start = buf.len();
        buf.resize(start + value_size, 0);
        fill_verify_pattern(seed, key, value_size, col_idx, &mut buf[start..start + value_size]);
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

    /// Build a SET/REPLACE request with deterministic verification payload
    /// written directly into the request buffer.
    fn build_set_verify_request(
        &self,
        seq: u16,
        key: &str,
        seed: u64,
        value_size: usize,
    ) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(32 + key.len() + value_size);
        Self::start_request(&mut buf, 4, seq, CMD_REPLACE)?;
        encode::write_str(&mut buf, key)?;
        Self::write_verify_value(&mut buf, seed, key.as_bytes(), value_size, 0)?;
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

    /// Build a PUT request with deterministic verification payloads written
    /// directly into the request buffer.
    fn build_put_verify_request(
        &self,
        seq: u16,
        key: &str,
        seed: u64,
        column_sizes: &[usize],
    ) -> Result<Vec<u8>> {
        let total_value_size: usize = column_sizes.iter().sum();
        let mut buf =
            Vec::with_capacity(32 + key.len() + total_value_size + column_sizes.len() * 10);
        Self::start_request(&mut buf, 3 + 2 * column_sizes.len() as u32, seq, CMD_PUT)?;
        encode::write_str(&mut buf, key)?;
        for (col_idx, &size) in column_sizes.iter().enumerate() {
            encode::write_u32(&mut buf, col_idx as u32)?;
            Self::write_verify_value(&mut buf, seed, key.as_bytes(), size, col_idx as u32)?;
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
    fn encode(&mut self, conn_id: usize, op: &MasstreeOp, seq: u16, key: u64) -> Result<Vec<u8>> {
        // Format key with configurable prefix
        let key_str = self.format_key(key);
        let request = match op {
            MasstreeOp::Get => {
                if let Some(ref mut v) = self.verify {
                    v.pending_gets.entry(conn_id).or_default().insert(seq, key);
                }
                self.build_get_request(seq, &key_str)
            }
            MasstreeOp::Set => {
                let size = self.next_value_size(op);
                if let Some(seed) = self.verify.as_ref().map(|v| v.seed) {
                    self.build_set_verify_request(seq, &key_str, seed, size)
                } else {
                    let value = self.generate_value(size);
                    self.build_set_request(seq, &key_str, &value)
                }
            }
            MasstreeOp::Put { columns } => {
                if let Some(seed) = self.verify.as_ref().map(|v| v.seed) {
                    let sizes: Vec<usize> =
                        (0..*columns).map(|_| self.next_value_size(op)).collect();
                    self.build_put_verify_request(seq, &key_str, seed, &sizes)
                } else {
                    let values: Vec<_> = (0..*columns)
                        .map(|_| {
                            let size = self.next_value_size(op);
                            self.generate_value(size)
                        })
                        .collect();
                    self.build_put_request(seq, &key_str, &values)
                }
            }
            MasstreeOp::Remove => self.build_remove_request(seq, &key_str),
            MasstreeOp::Scan { count, fields } => {
                self.build_scan_request(seq, &key_str, *count, fields)
            }
            MasstreeOp::Checkpoint => self.build_checkpoint_request(seq),
        };
        self.key_buf = key_str;
        request
    }

    fn complete_parsed_response<'a>(
        &mut self,
        conn_id: usize,
        data: &'a [u8],
        response: Response,
    ) -> Result<Parsed<'a, (usize, u16)>> {
        let id = (conn_id, response.seq as u16);
        let error = match response.body {
            Body::Get { first_col, col_count } => self.verify.as_mut().and_then(|v| {
                v.verify_get(
                    conn_id,
                    response.seq as u16,
                    &self.key_prefix,
                    &mut self.key_buf,
                    first_col.map(|r| &data[r]),
                    col_count,
                )
            }),
            Body::Done => None,
            Body::Result(code) => result_error(code),
            Body::Unsupported => {
                if let Some(ref mut v) = self.verify {
                    if let Some(m) = v.pending_gets.get_mut(&conn_id) {
                        m.remove(&(response.seq as u16));
                    }
                }
                Some(ErrorReply::new("Unsupported", "Unsupported command"))
            }
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

    fn finalize_streaming_rx(&mut self, rx: MasstreeRxState) -> Option<ErrorReply<'static>> {
        let sv = rx.verify?;
        let v = self.verify.as_mut()?;
        v.finalize_stream(&self.key_prefix, &mut self.key_buf, rx.col0_len, sv)
    }

    fn try_start_streaming_get(
        &mut self,
        conn_id: usize,
        data: &[u8],
        response_exceeded_max: bool,
    ) -> Result<Option<Parsed<'static, (usize, u16)>>> {
        let mut reader = Reader { data, pos: 0, read: 0 };
        let Some(array_len) = reader.array_len()? else {
            return Ok(None);
        };
        if array_len < 3 {
            return Ok(None);
        }
        let Some(seq) = reader.int()? else {
            return Ok(None);
        };
        let seq = u32::try_from(seq)
            .map_err(|_| anyhow!("Invalid Masstree response sequence number {seq}"))?
            as u16;
        let Some(cmd) = reader.int()? else {
            return Ok(None);
        };
        if cmd != 3 {
            return Ok(None);
        }
        let Some((hdr_len, col0_len)) = parse_bytes_header(data, reader.pos) else {
            return Ok(None);
        };
        let col0_start = reader.pos + hdr_len;
        let col0_end = col0_start
            .checked_add(col0_len)
            .ok_or_else(|| anyhow!("Masstree value length overflow"))?;
        let remaining_elements = array_len - 3;

        if col0_end <= MAX_STREAM_STEP
            && !(remaining_elements > 0 && (data.len() >= MAX_STREAM_STEP || response_exceeded_max))
        {
            return Ok(None);
        }

        let mut verify_state = self.verify.as_mut().and_then(|v| {
            v.start_stream(
                conn_id,
                seq,
                &self.key_prefix,
                &mut self.key_buf,
                col0_len,
                array_len - 2,
            )
        });

        let max_col0_now = MAX_STREAM_STEP.saturating_sub(col0_start);
        let take = (data.len() - col0_start).min(col0_len).min(max_col0_now);
        let consumed = col0_start + take;

        if let Some(ref mut sv) = verify_state {
            let key_bytes = if sv.maybe_miss {
                self.key_buf.as_bytes()
            } else {
                &[]
            };
            sv.feed_chunk(key_bytes, col0_len, 0, &data[col0_start..col0_start + take]);
        }

        let rx = MasstreeRxState {
            seq,
            col0_len,
            col0_offset: take,
            remaining_col0: col0_len - take,
            remaining_elements,
            remaining_skip_bytes: 0,
            verify: verify_state,
        };
        if rx.remaining_col0 == 0 && rx.remaining_elements == 0 {
            let error = self.finalize_streaming_rx(rx);
            return Ok(Some(Parsed::Complete { consumed, id: (conn_id, seq), error }));
        }
        self.conn_rx.insert(conn_id, rx);
        Ok(Some(Parsed::Absorbed(consumed)))
    }

    fn parse_streaming(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<Parsed<'static, (usize, u16)>> {
        if data.is_empty() {
            return Ok(Parsed::Incomplete);
        }
        let mut rx = self.conn_rx.remove(&conn_id).expect("caller checked conn_rx entry");

        if rx.remaining_col0 > 0 {
            let n = data.len().min(rx.remaining_col0).min(MAX_STREAM_STEP);
            if let Some(ref mut sv) = rx.verify {
                let key_bytes = if sv.maybe_miss {
                    format_key_into(&self.key_prefix, sv.key_num, &mut self.key_buf).as_bytes()
                } else {
                    &[]
                };
                sv.feed_chunk(key_bytes, rx.col0_len, rx.col0_offset, &data[..n]);
            }
            rx.col0_offset += n;
            rx.remaining_col0 -= n;
            if rx.remaining_col0 == 0 && rx.remaining_elements == 0 {
                let seq = rx.seq;
                let error = self.finalize_streaming_rx(rx);
                return Ok(Parsed::Complete { consumed: n, id: (conn_id, seq), error });
            }
            self.conn_rx.insert(conn_id, rx);
            return Ok(Parsed::Absorbed(n));
        }

        if rx.remaining_skip_bytes > 0 {
            let n = data.len().min(rx.remaining_skip_bytes).min(MAX_STREAM_STEP);
            rx.remaining_skip_bytes -= n;
            if rx.remaining_skip_bytes == 0 {
                rx.remaining_elements -= 1;
            }
            if rx.remaining_skip_bytes == 0 && rx.remaining_elements == 0 {
                let seq = rx.seq;
                let error = self.finalize_streaming_rx(rx);
                return Ok(Parsed::Complete { consumed: n, id: (conn_id, seq), error });
            }
            self.conn_rx.insert(conn_id, rx);
            return Ok(Parsed::Absorbed(n));
        }

        // Skip remaining elements after col0 (multi-column GET)
        if let Some(end) = skip_value(data, 0)? {
            if end <= MAX_STREAM_STEP {
                rx.remaining_elements -= 1;
                if rx.remaining_elements == 0 {
                    let seq = rx.seq;
                    let error = self.finalize_streaming_rx(rx);
                    return Ok(Parsed::Complete { consumed: end, id: (conn_id, seq), error });
                }
                self.conn_rx.insert(conn_id, rx);
                return Ok(Parsed::Absorbed(end));
            }
        }

        if let Some((hdr_len, payload_len)) = parse_bytes_header(data, 0) {
            let end = hdr_len
                .checked_add(payload_len)
                .ok_or_else(|| anyhow!("Masstree value length overflow"))?;
            if end > MAX_STREAM_STEP || end > data.len() {
                let max_now = MAX_STREAM_STEP.saturating_sub(hdr_len);
                let take = (data.len() - hdr_len).min(payload_len).min(max_now);
                let consumed = hdr_len + take;
                rx.remaining_skip_bytes = payload_len - take;
                if rx.remaining_skip_bytes == 0 {
                    rx.remaining_elements -= 1;
                }
                if rx.remaining_skip_bytes == 0 && rx.remaining_elements == 0 {
                    let seq = rx.seq;
                    let error = self.finalize_streaming_rx(rx);
                    return Ok(Parsed::Complete { consumed, id: (conn_id, seq), error });
                }
                self.conn_rx.insert(conn_id, rx);
                return Ok(Parsed::Absorbed(consumed));
            }
        }

        self.conn_rx.insert(conn_id, rx);
        Ok(Parsed::Incomplete)
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
                let request = if let Some(seed) = self.verify.as_ref().map(|v| v.seed) {
                    self.build_set_verify_request(seq, &key_str, seed, value_size)
                } else {
                    let value = self.generate_value(value_size);
                    self.build_set_request(seq, &key_str, &value)
                }
                .expect("Failed to build SET request for insert phase");
                self.key_buf = key_str;
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
            .encode(conn_id, &op, seq, key)
            .unwrap_or_else(|e| panic!("Failed to build {} request: {e}", op.name()));
        crate::Request::measurement(request, (conn_id, seq))
    }

    fn parse_response<'a>(
        &mut self,
        conn_id: usize,
        data: &'a [u8],
    ) -> Result<Parsed<'a, (usize, u16)>> {
        if !self.conn_rx.is_empty() && self.conn_rx.contains_key(&conn_id) {
            return self.parse_streaming(conn_id, data);
        }

        let mut response_exceeded_max = false;
        if let Some(response) = Response::parse(data)? {
            if response.len <= MAX_STREAM_STEP {
                return self.complete_parsed_response(conn_id, data, response);
            }
            response_exceeded_max = true;
        }

        if let Some(parsed) = self.try_start_streaming_get(conn_id, data, response_exceeded_max)? {
            return Ok(parsed);
        }

        Ok(Parsed::Incomplete)
    }

    fn verification_report(&self) -> Option<VerificationReport> {
        self.verify.as_ref().map(|v| v.report.clone())
    }

    fn name(&self) -> &'static str {
        "masstree"
    }

    fn reset(&mut self) {
        self.conn_send_seq.clear();
        self.conn_handshake_state.clear();
        self.conn_rx.clear();
        self.selector.reset();
        if let Some(ref mut key_gen) = self.key_gen {
            key_gen.reset();
        }
        if let Some(sizes) = &mut self.value_sizes {
            sizes.reset();
        }
        if let Some(ref mut v) = self.verify {
            v.pending_gets.clear();
            v.report = VerificationReport::default();
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

/// Format `prefix` + decimal `key` into `buf` without allocating.
fn format_key_into<'a>(prefix: &str, key: u64, buf: &'a mut String) -> &'a str {
    buf.clear();
    buf.push_str(prefix);
    push_decimal(buf, key);
    buf.as_str()
}

/// Append `n` in decimal to `out`, as `{n}` formats it
fn push_decimal(out: &mut String, mut n: u64) {
    let mut digits = [0u8; 20];
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    out.push_str(std::str::from_utf8(&digits[start..]).expect("decimal digits are ASCII"));
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
    /// GET: byte range of element 2 (first column or echoed key) in `data` if it
    /// is a msgpack `str` or `bin`, plus the number of trailing elements
    Get {
        first_col: Option<Range<usize>>,
        col_count: u32,
    },
    /// SCAN, REMOVE and CHECKPOINT: done, whatever they found
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
                let Some(first_col) = reader.next_bytes_range()? else {
                    return Ok(None);
                };
                Body::Get { first_col, col_count: array_len - 2 }
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

    /// Read the next element, returning its payload byte range in `self.data`
    /// if it is a msgpack `str` or `bin`, or `None` if it is another valid
    /// msgpack type. Returns `Ok(None)` when the element is incomplete.
    fn next_bytes_range(&mut self) -> Result<Option<Option<Range<usize>>>> {
        let start = self.pos;
        let Some(end) = skip_value(self.data, start)? else {
            return Ok(None);
        };
        let range = match self.data[start] {
            0xa0..=0xbf => Some(start + 1..end),
            0xd9 | 0xc4 => Some(start + 2..end),
            0xda | 0xc5 => Some(start + 3..end),
            0xdb | 0xc6 => Some(start + 5..end),
            _ => None,
        };
        self.pos = end;
        self.read += 1;
        Ok(Some(range))
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

/// If `data[pos..]` starts with a complete msgpack `str` or `bin` header,
/// return `Some((header_len, payload_len))`.
fn parse_bytes_header(data: &[u8], pos: usize) -> Option<(usize, usize)> {
    let &byte = data.get(pos)?;
    match byte {
        0xa0..=0xbf => Some((1, usize::from(byte & 0x1f))),
        0xd9 | 0xc4 => {
            let len = usize::from(*data.get(pos + 1)?);
            Some((2, len))
        }
        0xda | 0xc5 => {
            let b = data.get(pos + 1..pos + 3)?;
            let len = usize::from(u16::from_be_bytes([b[0], b[1]]));
            Some((3, len))
        }
        0xdb | 0xc6 => {
            let b = data.get(pos + 1..pos + 5)?;
            let len = u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize;
            Some((5, len))
        }
        _ => None,
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
    use std::collections::HashMap;

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
    fn keys_are_the_prefix_and_the_decimal_number() {
        let mut proto = MasstreeProtocol::new(MasstreeOp::Get);
        for key in [0, 7, 9, 10, 99_999, 100_000, 1 << 32, u64::MAX - 1, u64::MAX] {
            let formatted = proto.format_key(key);
            assert_eq!(formatted, format!("key:{key}"));
            proto.key_buf = formatted;
        }
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

    fn build_get_response_str(seq: u32, col: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        encode::write_array_len(&mut buf, 3).unwrap();
        encode::write_uint(&mut buf, u64::from(seq)).unwrap();
        encode::write_uint(&mut buf, 3).unwrap();
        encode::write_str_len(&mut buf, col.len() as u32).unwrap();
        buf.extend_from_slice(col);
        buf
    }

    fn build_get_response_bin(seq: u32, col: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        encode::write_array_len(&mut buf, 3).unwrap();
        encode::write_uint(&mut buf, u64::from(seq)).unwrap();
        encode::write_uint(&mut buf, 3).unwrap();
        encode::write_bin(&mut buf, col).unwrap();
        buf
    }

    #[test]
    fn verify_pattern_matches_fixed_reference_vectors() {
        let mut buf = [0u8; 16];
        fill_verify_pattern(0, b"key:0", 16, 0, &mut buf);
        assert_eq!(
            buf,
            [
                0x56, 0x96, 0x68, 0xf3, 0x49, 0xbb, 0x71, 0x14, 0x1f, 0x4e, 0xd0, 0xa2, 0x9d, 0x44,
                0x67, 0x07
            ]
        );
        assert_eq!(check_verify_pattern(0, b"key:0", 16, 0, &buf), None);

        fill_verify_pattern(42, b"key:7", 16, 0, &mut buf);
        assert_eq!(
            buf,
            [
                0x8b, 0xf6, 0x5f, 0x1d, 0xd6, 0x79, 0xce, 0xa9, 0x4d, 0x64, 0x53, 0xfe, 0xf9, 0x50,
                0x90, 0x84
            ]
        );

        fill_verify_pattern(0, b"key:0", 16, 1, &mut buf);
        assert_eq!(
            buf,
            [
                0x9a, 0x78, 0xd6, 0x50, 0x84, 0x13, 0x3c, 0xfb, 0xd0, 0xc3, 0x4f, 0xda, 0x7a, 0x77,
                0xda, 0x77
            ]
        );

        let mut buf71 = [0u8; 71];
        fill_verify_pattern(0, b"key:0", 71, 0, &mut buf71);
        assert_eq!(
            &buf71[..16],
            &[
                0xc4, 0x4d, 0xf1, 0x40, 0x9f, 0x37, 0x21, 0x16, 0x88, 0x54, 0xbe, 0x22, 0xff, 0x1b,
                0xb4, 0x66
            ]
        );
        assert_eq!(&buf71[63..], &[0x61, 0xd4, 0xc8, 0xf1, 0x07, 0x75, 0x99, 0x28]);
        assert_eq!(check_verify_pattern(0, b"key:0", 71, 0, &buf71), None);
    }

    #[test]
    fn verify_writes_identical_col0_for_set_put_and_insert_phase() {
        use crate::workload::KeyGeneration;
        let seed = 1337u64;
        for value_size in [64usize, 71, 512, 4096] {
            let mut expected_val = vec![0u8; value_size];
            fill_verify_pattern(seed, b"key:0", value_size, 0, &mut expected_val);

            // SET (REPLACE)
            let mut set_proto = MasstreeProtocol::with_workload(
                MasstreeOp::Set,
                KeyGeneration::sequential(0),
                value_size,
            )
            .with_verify(seed, value_size);
            set_proto.mark_handshake_done(0);
            let set_req = set_proto.next_request(0);
            assert!(
                set_req.data.ends_with(&expected_val),
                "SET value did not match expected pattern for size {value_size}"
            );

            // insert_phase (also REPLACE)
            let mut ins_proto = MasstreeProtocol::with_workload(
                MasstreeOp::Get,
                KeyGeneration::sequential(0),
                value_size,
            )
            .with_insert_phase(1, value_size)
            .with_verify(seed, value_size);
            ins_proto.mark_handshake_done(0);
            let ins_req = ins_proto.next_request(0);
            assert_eq!(ins_req.data, set_req.data, "insert_phase must match SET byte-for-byte");

            // PUT (1 column => col 0)
            let mut put_proto = MasstreeProtocol::with_workload(
                MasstreeOp::Put { columns: 1 },
                KeyGeneration::sequential(0),
                value_size,
            )
            .with_verify(seed, value_size);
            put_proto.mark_handshake_done(0);
            let put_req = put_proto.next_request(0);
            assert!(
                put_req.data.ends_with(&expected_val),
                "PUT col 0 value did not match expected pattern for size {value_size}"
            );
        }
    }

    #[test]
    fn verify_get_accepts_valid_values_across_sizes_and_str_or_bin() {
        use crate::workload::KeyGeneration;
        let seed = 99u64;
        for value_size in [64usize, 71, 512, 4096] {
            let mut proto = MasstreeProtocol::with_workload(
                MasstreeOp::Get,
                KeyGeneration::sequential(10),
                value_size,
            )
            .with_verify(seed, value_size);
            proto.mark_handshake_done(0);

            for (i, use_bin) in [false, true].into_iter().enumerate() {
                let req = proto.next_request(0);
                let seq = req.request_id.1;
                let key = format!("key:{}", 10 + i);
                let mut val = vec![0u8; value_size];
                fill_verify_pattern(seed, key.as_bytes(), value_size, 0, &mut val);
                let resp = if use_bin {
                    build_get_response_bin(u32::from(seq), &val)
                } else {
                    build_get_response_str(u32::from(seq), &val)
                };
                assert_eq!(
                    proto.parse_response(0, &resp).unwrap(),
                    Parsed::ok(resp.len(), (0, seq)),
                    "size={value_size} bin={use_bin}"
                );
            }

            let report = proto.verification_report().unwrap();
            assert_eq!(report.verified, 2);
            assert_eq!(report.failures, 0);
            assert_eq!(report.first_mismatch, None);
        }
    }

    #[test]
    fn verify_detects_bit_corruption_at_start_middle_and_end() {
        use crate::workload::KeyGeneration;
        let seed = 55u64;
        let value_size = 71usize;
        for bad_offset in [0usize, 35, value_size - 1] {
            let mut proto = MasstreeProtocol::with_workload(
                MasstreeOp::Get,
                KeyGeneration::sequential(7),
                value_size,
            )
            .with_verify(seed, value_size);
            proto.mark_handshake_done(0);

            let req = proto.next_request(0);
            let seq = req.request_id.1;
            let mut val = vec![0u8; value_size];
            fill_verify_pattern(seed, b"key:7", value_size, 0, &mut val);
            let expected_byte = val[bad_offset];
            let actual_byte = expected_byte ^ 0x01;
            val[bad_offset] = actual_byte;
            let resp = build_get_response_str(u32::from(seq), &val);

            assert_eq!(
                proto.parse_response(0, &resp).unwrap(),
                Parsed::error(
                    resp.len(),
                    (0, seq),
                    ErrorReply::new("ValueMismatch", "GET value mismatch")
                )
            );
            let report = proto.verification_report().unwrap();
            assert_eq!(report.verified, 0);
            assert_eq!(report.failures, 1);
            assert_eq!(
                report.first_mismatch,
                Some(VerificationMismatch {
                    kind: "ValueMismatch".to_string(),
                    key: Some("key:7".to_string()),
                    offset: bad_offset as u64,
                    expected: Some(expected_byte),
                    actual: Some(actual_byte),
                    expected_len: Some(value_size),
                    actual_len: Some(value_size),
                    detail: "GET value mismatch".to_string(),
                })
            );
        }
    }

    #[test]
    fn verify_detects_short_long_miss_and_non_bytes() {
        use crate::workload::KeyGeneration;
        let seed = 11u64;
        let value_size = 64usize;

        // 1. ShortValue
        {
            let mut proto = MasstreeProtocol::with_workload(
                MasstreeOp::Get,
                KeyGeneration::sequential(1),
                value_size,
            )
            .with_verify(seed, value_size);
            proto.mark_handshake_done(0);
            let seq = proto.next_request(0).request_id.1;
            let mut val = vec![0u8; value_size];
            fill_verify_pattern(seed, b"key:1", value_size, 0, &mut val);
            val.truncate(20);
            let resp = build_get_response_str(u32::from(seq), &val);
            assert_eq!(
                proto.parse_response(0, &resp).unwrap(),
                Parsed::error(
                    resp.len(),
                    (0, seq),
                    ErrorReply::new("ShortValue", "GET value shorter than expected")
                )
            );
            let m = proto.verification_report().unwrap().first_mismatch.unwrap();
            assert_eq!(m.kind, "ShortValue");
            assert_eq!(m.key.as_deref(), Some("key:1"));
            assert_eq!(m.offset, 20);
            assert_eq!(m.expected_len, Some(64));
            assert_eq!(m.actual_len, Some(20));
        }

        // 2. Overly long value (valid prefix + extra byte)
        {
            let mut proto = MasstreeProtocol::with_workload(
                MasstreeOp::Get,
                KeyGeneration::sequential(2),
                value_size,
            )
            .with_verify(seed, value_size);
            proto.mark_handshake_done(0);
            let seq = proto.next_request(0).request_id.1;
            let mut val = vec![0u8; value_size];
            fill_verify_pattern(seed, b"key:2", value_size, 0, &mut val);
            val.push(0xab);
            let resp = build_get_response_str(u32::from(seq), &val);
            assert_eq!(
                proto.parse_response(0, &resp).unwrap(),
                Parsed::error(
                    resp.len(),
                    (0, seq),
                    ErrorReply::new("ValueMismatch", "GET value longer than expected")
                )
            );
            let m = proto.verification_report().unwrap().first_mismatch.unwrap();
            assert_eq!(m.kind, "ValueMismatch");
            assert_eq!(m.offset, 64);
            assert_eq!(m.expected, None);
            assert_eq!(m.actual, Some(0xab));
            assert_eq!(m.expected_len, Some(64));
            assert_eq!(m.actual_len, Some(65));
        }

        // 3. UnexpectedMiss (server echoes [seq, 3, key])
        {
            let mut proto = MasstreeProtocol::with_workload(
                MasstreeOp::Get,
                KeyGeneration::sequential(3),
                value_size,
            )
            .with_verify(seed, value_size);
            proto.mark_handshake_done(0);
            let seq = proto.next_request(0).request_id.1;
            let resp = build_get_response_str(u32::from(seq), b"key:3");
            assert_eq!(
                proto.parse_response(0, &resp).unwrap(),
                Parsed::error(
                    resp.len(),
                    (0, seq),
                    ErrorReply::new("UnexpectedMiss", "GET key not found (miss)")
                )
            );
            let m = proto.verification_report().unwrap().first_mismatch.unwrap();
            assert_eq!(m.kind, "UnexpectedMiss");
            assert_eq!(m.key.as_deref(), Some("key:3"));
            assert_eq!(m.offset, 0);
            assert_eq!(m.expected_len, Some(64));
            assert_eq!(m.actual_len, Some(5));
        }

        // 4. Non-bytes element in GET column
        {
            let mut proto = MasstreeProtocol::with_workload(
                MasstreeOp::Get,
                KeyGeneration::sequential(4),
                value_size,
            )
            .with_verify(seed, value_size);
            proto.mark_handshake_done(0);
            let seq = proto.next_request(0).request_id.1;
            let mut resp = Vec::new();
            encode::write_array_len(&mut resp, 3).unwrap();
            encode::write_uint(&mut resp, u64::from(seq)).unwrap();
            encode::write_uint(&mut resp, 3).unwrap();
            encode::write_uint(&mut resp, 123).unwrap();
            assert_eq!(
                proto.parse_response(0, &resp).unwrap(),
                Parsed::error(
                    resp.len(),
                    (0, seq),
                    ErrorReply::new("ValueMismatch", "GET column is not a byte string")
                )
            );
            let m = proto.verification_report().unwrap().first_mismatch.unwrap();
            assert_eq!(m.kind, "ValueMismatch");
            assert_eq!(m.actual_len, None);
        }

        // 5. Reset clears verification counts and disabled verify returns None
        {
            let disabled = MasstreeProtocol::new(MasstreeOp::Get);
            assert_eq!(disabled.verification_report(), None);

            let mut proto = MasstreeProtocol::with_workload(
                MasstreeOp::Get,
                KeyGeneration::sequential(0),
                value_size,
            )
            .with_verify(seed, value_size);
            proto.mark_handshake_done(0);
            let seq = proto.next_request(0).request_id.1;
            let resp = build_get_response_str(u32::from(seq), b"key:0");
            proto.parse_response(0, &resp).unwrap();
            assert_eq!(proto.verification_report().unwrap().failures, 1);
            proto.reset();
            assert_eq!(proto.verification_report(), Some(VerificationReport::default()));
        }
    }

    /// Naive reference implementation of the documented Masstree verify formula.
    fn naive_verify_pattern(seed: u64, key: &[u8], value_size: usize, col_idx: u32) -> Vec<u8> {
        let base = pattern_base(seed, key, value_size, col_idx);
        let mut out = vec![0u8; value_size];
        let mut off = 0usize;
        let mut w = 0u64;
        while off < value_size {
            let word = splitmix64(base.wrapping_add(w.wrapping_mul(0x9e37_79b9_7f4a_7c15)));
            let bytes = word.to_le_bytes();
            let n = (value_size - off).min(8);
            out[off..off + n].copy_from_slice(&bytes[..n]);
            off += n;
            w += 1;
        }
        out
    }

    fn check_chunked(base: u64, data: &[u8], chunk_size: usize) -> Option<(u64, u8, u8)> {
        let mut off = 0usize;
        while off < data.len() {
            let end = (off + chunk_size).min(data.len());
            if let Some(m) = check_verify_pattern_chunk(base, off, &data[off..end]) {
                return Some(m);
            }
            off = end;
        }
        None
    }

    #[test]
    fn fast_verify_pattern_matches_naive_across_lengths_and_chunk_splits() {
        let lengths = [0usize, 1, 7, 8, 9, 15, 16, 31, 32, 33, 64, 16384, 65536, 65537, 131072];
        let chunk_sizes = [13usize, 777, 16384, 65536];
        let seed = 0x1234_5678_9abc_def0u64;
        let key = b"key:98765";

        for &len in &lengths {
            let expected = naive_verify_pattern(seed, key, len, 0);
            let mut fast = vec![0xaa; len];
            fill_verify_pattern(seed, key, len, 0, &mut fast);
            assert_eq!(fast, expected, "fill_verify_pattern mismatch at len={len}");
            assert_eq!(
                check_verify_pattern(seed, key, len, 0, &fast),
                None,
                "check_verify_pattern false positive at len={len}"
            );

            let base = pattern_base(seed, key, len, 0);
            for &cs in &chunk_sizes {
                assert_eq!(
                    check_chunked(base, &fast, cs),
                    None,
                    "chunk verify failed at len={len} cs={cs}"
                );
            }

            if len == 0 {
                continue;
            }
            // Flip 1 byte at start, near middle, and end; check both full and chunked
            for bad_pos in [0usize, len / 2, len - 1] {
                let mut corrupted = fast.clone();
                let exp_b = corrupted[bad_pos];
                let act_b = exp_b ^ 0x55;
                corrupted[bad_pos] = act_b;
                assert_eq!(
                    check_verify_pattern(seed, key, len, 0, &corrupted),
                    Some((bad_pos as u64, exp_b, act_b)),
                    "full mismatch detection failed at len={len} bad_pos={bad_pos}"
                );
                for &cs in &chunk_sizes {
                    assert_eq!(
                        check_chunked(base, &corrupted, cs),
                        Some((bad_pos as u64, exp_b, act_b)),
                        "chunk mismatch detection failed at len={len} cs={cs} bad_pos={bad_pos}"
                    );
                }
            }
        }
    }

    type StreamedCompletion = (usize, (usize, u16), Option<(String, String)>);

    /// Feed `wire` into `proto.parse_response(conn_id, ...)` in slices of at
    /// most `read_chunk` bytes, enforcing the `MAX_STREAM_STEP` contract
    /// (`consumed <= 65536` and never `Incomplete` with `available > 65536`).
    fn feed_streamed(
        proto: &mut MasstreeProtocol,
        conn_id: usize,
        wire: &[u8],
        read_chunk: usize,
    ) -> Vec<StreamedCompletion> {
        let mut buf = Vec::new();
        let mut absorbed = 0usize;
        let mut completions = Vec::new();
        let mut pos = 0usize;

        while pos < wire.len() {
            let end = (pos + read_chunk).min(wire.len());
            buf.extend_from_slice(&wire[pos..end]);
            pos = end;

            let mut cursor = 0usize;
            while cursor < buf.len() {
                let avail = buf.len() - cursor;
                let parsed = proto.parse_response(conn_id, &buf[cursor..]).unwrap();
                let consumed = parsed.consumed();
                assert!(consumed <= avail, "consumed {consumed} > avail {avail}");
                assert!(
                    consumed <= MAX_STREAM_STEP,
                    "consumed {consumed} > MAX_STREAM_STEP ({MAX_STREAM_STEP})"
                );
                assert!(
                    !(consumed == 0 && avail > MAX_STREAM_STEP),
                    "Incomplete with avail {avail} > MAX_STREAM_STEP"
                );
                if consumed == 0 {
                    break;
                }
                cursor += consumed;
                absorbed += consumed;
                if let Parsed::Complete { id, error, .. } = parsed {
                    completions.push((
                        absorbed,
                        id,
                        error.map(|e| (e.kind.to_string(), e.message.to_string())),
                    ));
                    absorbed = 0;
                }
            }
            buf.drain(..cursor);
        }
        assert!(buf.is_empty(), "leftover unparsed bytes: {}", buf.len());
        completions
    }

    fn assert_streaming_pair(seed: u64, value_size: usize, verify: bool, read_chunk: usize) {
        use crate::workload::KeyGeneration;
        let mut proto = MasstreeProtocol::with_workload(
            MasstreeOp::Get,
            KeyGeneration::sequential(0),
            value_size,
        );
        if verify {
            proto = proto.with_verify(seed, value_size);
        }
        proto.mark_handshake_done(0);

        // Pipeline 2 large GETs back-to-back to verify clean state reset between responses
        let seq0 = proto.next_request(0).request_id.1;
        let seq1 = proto.next_request(0).request_id.1;
        let mut val0 = vec![0u8; value_size];
        let mut val1 = vec![0u8; value_size];
        fill_verify_pattern(seed, b"key:0", value_size, 0, &mut val0);
        fill_verify_pattern(seed, b"key:1", value_size, 0, &mut val1);
        let resp0 = build_get_response_str(u32::from(seq0), &val0);
        let resp1 = build_get_response_bin(u32::from(seq1), &val1);

        let mut wire = Vec::with_capacity(resp0.len() + resp1.len());
        wire.extend_from_slice(&resp0);
        wire.extend_from_slice(&resp1);

        let done = feed_streamed(&mut proto, 0, &wire, read_chunk);
        assert_eq!(
            done,
            vec![(resp0.len(), (0, seq0), None), (resp1.len(), (0, seq1), None)],
            "size={value_size} verify={verify} chunk={read_chunk}"
        );
        assert!(proto.conn_rx.is_empty());
        if verify {
            let rep = proto.verification_report().unwrap();
            assert_eq!(rep.verified, 2);
            assert_eq!(rep.failures, 0);
        }
    }

    #[test]
    fn streaming_get_responses_64kib_and_128kib_with_and_without_verify() {
        let seed = 42u64;
        for value_size in [65536usize, 65537, 131072] {
            for verify in [false, true] {
                for read_chunk in [13usize, 4096, 16384, 65536, 200_000] {
                    assert_streaming_pair(seed, value_size, verify, read_chunk);
                }
            }
        }
    }

    #[test]
    fn streaming_get_128kib_detects_corruption_short_long_and_multicolumn() {
        use crate::workload::KeyGeneration;
        let seed = 99u64;
        let value_size = 131072usize;

        // 1. Corruption past 64 KiB boundary (offset 70000)
        {
            let mut proto = MasstreeProtocol::with_workload(
                MasstreeOp::Get,
                KeyGeneration::sequential(5),
                value_size,
            )
            .with_verify(seed, value_size);
            proto.mark_handshake_done(0);
            let seq = proto.next_request(0).request_id.1;
            let mut val = vec![0u8; value_size];
            fill_verify_pattern(seed, b"key:5", value_size, 0, &mut val);
            let bad_off = 70_000usize;
            let exp_b = val[bad_off];
            let act_b = exp_b ^ 0x42;
            val[bad_off] = act_b;
            let resp = build_get_response_str(u32::from(seq), &val);

            let done = feed_streamed(&mut proto, 0, &resp, 16384);
            assert_eq!(done.len(), 1);
            assert_eq!(done[0].0, resp.len());
            assert_eq!(done[0].1, (0, seq));
            assert_eq!(
                done[0].2,
                Some(("ValueMismatch".to_string(), "GET value mismatch".to_string()))
            );
            let m = proto.verification_report().unwrap().first_mismatch.unwrap();
            assert_eq!(m.kind, "ValueMismatch");
            assert_eq!(m.key.as_deref(), Some("key:5"));
            assert_eq!(m.offset, bad_off as u64);
            assert_eq!(m.expected, Some(exp_b));
            assert_eq!(m.actual, Some(act_b));
            assert_eq!(m.expected_len, Some(value_size));
            assert_eq!(m.actual_len, Some(value_size));
            assert!(proto.conn_rx.is_empty());
        }

        // 2. ShortValue (>64KiB actual but <128KiB expected)
        {
            let mut proto = MasstreeProtocol::with_workload(
                MasstreeOp::Get,
                KeyGeneration::sequential(6),
                value_size,
            )
            .with_verify(seed, value_size);
            proto.mark_handshake_done(0);
            let seq = proto.next_request(0).request_id.1;
            let mut val = vec![0u8; value_size];
            fill_verify_pattern(seed, b"key:6", value_size, 0, &mut val);
            val.truncate(70_000);
            let resp = build_get_response_str(u32::from(seq), &val);

            let done = feed_streamed(&mut proto, 0, &resp, 16384);
            assert_eq!(done[0].2.as_ref().unwrap().0, "ShortValue");
            let m = proto.verification_report().unwrap().first_mismatch.unwrap();
            assert_eq!(m.offset, 70_000);
            assert_eq!(m.actual_len, Some(70_000));
            assert_eq!(m.expected_len, Some(value_size));
        }

        // 3. Longer than expected (>128KiB)
        {
            let mut proto = MasstreeProtocol::with_workload(
                MasstreeOp::Get,
                KeyGeneration::sequential(7),
                value_size,
            )
            .with_verify(seed, value_size);
            proto.mark_handshake_done(0);
            let seq = proto.next_request(0).request_id.1;
            let mut val = vec![0u8; value_size];
            fill_verify_pattern(seed, b"key:7", value_size, 0, &mut val);
            val.push(0xef);
            let resp = build_get_response_str(u32::from(seq), &val);

            let done = feed_streamed(&mut proto, 0, &resp, 16384);
            assert_eq!(done[0].2.as_ref().unwrap().0, "ValueMismatch");
            let m = proto.verification_report().unwrap().first_mismatch.unwrap();
            assert_eq!(m.offset, value_size as u64);
            assert_eq!(m.actual, Some(0xef));
            assert_eq!(m.actual_len, Some(value_size + 1));
        }

        // 4. Multi-column GET where both col0 and col1 are 64 KiB
        {
            let col_size = 65536usize;
            let mut proto = MasstreeProtocol::with_workload(
                MasstreeOp::Get,
                KeyGeneration::sequential(8),
                col_size,
            )
            .with_verify(seed, col_size);
            proto.mark_handshake_done(0);
            let seq = proto.next_request(0).request_id.1;
            let mut col0 = vec![0u8; col_size];
            fill_verify_pattern(seed, b"key:8", col_size, 0, &mut col0);
            let col1 = vec![b'z'; col_size];

            let mut resp = Vec::new();
            encode::write_array_len(&mut resp, 4).unwrap();
            encode::write_uint(&mut resp, u64::from(seq)).unwrap();
            encode::write_uint(&mut resp, 3).unwrap();
            encode::write_str_len(&mut resp, col0.len() as u32).unwrap();
            resp.extend_from_slice(&col0);
            encode::write_str_len(&mut resp, col1.len() as u32).unwrap();
            resp.extend_from_slice(&col1);

            let done = feed_streamed(&mut proto, 0, &resp, 16384);
            assert_eq!(done, vec![(resp.len(), (0, seq), None)]);
            assert_eq!(proto.verification_report().unwrap().verified, 1);
            assert!(proto.conn_rx.is_empty());
        }
    }

    #[test]
    fn set_and_insert_phase_64kib_and_128kib_encode_str32_and_match_pattern() {
        use crate::workload::KeyGeneration;
        let seed = 77u64;
        for value_size in [65536usize, 131072] {
            let mut expected = vec![0u8; value_size];
            fill_verify_pattern(seed, b"key:0", value_size, 0, &mut expected);

            let mut set_proto = MasstreeProtocol::with_workload(
                MasstreeOp::Set,
                KeyGeneration::sequential(0),
                value_size,
            )
            .with_verify(seed, value_size);
            set_proto.mark_handshake_done(0);
            let set_req = set_proto.next_request(0);
            // Verify str32 marker (0xdb + 4-byte BE length) right before the value payload
            let hdr_pos = set_req.data.len() - value_size - 5;
            assert_eq!(set_req.data[hdr_pos], 0xdb);
            assert_eq!(&set_req.data[hdr_pos + 1..hdr_pos + 5], &(value_size as u32).to_be_bytes());
            assert_eq!(&set_req.data[hdr_pos + 5..], expected.as_slice());

            let mut insert_proto = MasstreeProtocol::with_workload(
                MasstreeOp::Get,
                KeyGeneration::sequential(0),
                value_size,
            )
            .with_insert_phase(1, value_size)
            .with_verify(seed, value_size);
            insert_proto.mark_handshake_done(0);
            let ins_req = insert_proto.next_request(0);
            assert!(ins_req.metadata.is_warmup);
            assert_eq!(ins_req.data, set_req.data);

            let mut put_proto = MasstreeProtocol::with_workload(
                MasstreeOp::Put { columns: 1 },
                KeyGeneration::sequential(0),
                value_size,
            )
            .with_verify(seed, value_size);
            put_proto.mark_handshake_done(0);
            let put_req = put_proto.next_request(0);
            let put_hdr_pos = put_req.data.len() - value_size - 5;
            assert_eq!(put_req.data[put_hdr_pos], 0xdb);
            assert_eq!(
                &put_req.data[put_hdr_pos + 1..put_hdr_pos + 5],
                &(value_size as u32).to_be_bytes()
            );
            assert_eq!(&put_req.data[put_hdr_pos + 5..], expected.as_slice());
        }
    }

    #[test]
    #[ignore = "microbenchmark: run with cargo test --release -- --ignored --nocapture"]
    fn bench_verify_pattern_16kib_and_64kib() {
        use std::hint::black_box;
        use std::time::Instant;

        let seed = 42u64;
        let key = b"key:12345";
        let iters = 20_000u32;

        for size in [16384usize, 65536] {
            let mut buf = vec![0u8; size];
            let t0 = Instant::now();
            for i in 0..iters {
                fill_verify_pattern(
                    black_box(seed ^ u64::from(i)),
                    black_box(key),
                    size,
                    0,
                    black_box(&mut buf),
                );
            }
            let fill_us = t0.elapsed().as_secs_f64() * 1e6 / f64::from(iters);

            fill_verify_pattern(seed, key, size, 0, &mut buf);
            let t1 = Instant::now();
            for _ in 0..iters {
                let res =
                    check_verify_pattern(black_box(seed), black_box(key), size, 0, black_box(&buf));
                assert!(black_box(res).is_none());
            }
            let check_us = t1.elapsed().as_secs_f64() * 1e6 / f64::from(iters);

            println!(
                "masstree verify bench size={size}B: fill={fill_us:.3} us/call, check={check_us:.3} us/call"
            );
        }
    }
}
