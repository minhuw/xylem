//! Memcached Binary protocol
//!
//! Implements the Memcached binary protocol (memcache_bin.h).

use super::udp::UdpFraming;
use super::{
    error_reply, frame_udp_request, next_conn_seq, parse_udp_response, InsertPhaseState, NextOp,
    ResponseEnd, Workload,
};
use crate::workload::{CommandSelector, ValueSizeGenerator};
use crate::{Parsed, Protocol};
use anyhow::Result;
use std::collections::HashMap;

pub use super::MemcachedOp;

// Command opcodes
const CMD_SET: u8 = 0x01;
const CMD_ADD: u8 = 0x02;
const CMD_REPLACE: u8 = 0x03;
const CMD_DELETE: u8 = 0x04;
const CMD_INCREMENT: u8 = 0x05;
const CMD_DECREMENT: u8 = 0x06;
const CMD_GETQ: u8 = 0x09;
const CMD_NOOP: u8 = 0x0a;
const CMD_GETK: u8 = 0x0c;
const CMD_GETKQ: u8 = 0x0d;
const CMD_TOUCH: u8 = 0x1c;

/// Magic byte of a response
const MAGIC_RESPONSE: u8 = 0x81;

/// Response status of a missing key
const STATUS_KEY_ENOENT: u16 = 0x0001;
/// Response status of a present key where an absent one was required
const STATUS_KEY_EEXISTS: u16 = 0x0002;
/// Response status of a storage command whose condition failed
const STATUS_NOT_STORED: u16 = 0x0005;

/// Incr and decr expiration that makes a missing key fail with `KEY_ENOENT`
/// instead of being created, as in the ASCII protocol
const DELTA_NO_CREATE: u32 = 0xffff_ffff;

/// Name of a non-zero response status, as in memcached's `protocol_binary.h`
fn status_name(status: u16) -> &'static str {
    match status {
        STATUS_KEY_ENOENT => "KEY_ENOENT",
        STATUS_KEY_EEXISTS => "KEY_EEXISTS",
        0x0003 => "E2BIG",
        0x0004 => "EINVAL",
        STATUS_NOT_STORED => "NOT_STORED",
        0x0006 => "DELTA_BADVAL",
        0x0007 => "NOT_MY_VBUCKET",
        0x0020 => "AUTH_ERROR",
        0x0021 => "AUTH_CONTINUE",
        0x0081 => "UNKNOWN_COMMAND",
        0x0082 => "ENOMEM",
        0x0083 => "NOT_SUPPORTED",
        0x0084 => "EINTERNAL",
        0x0085 => "EBUSY",
        0x0086 => "ETMPFAIL",
        _ => "UNKNOWN_STATUS",
    }
}

/// Memcached binary protocol header (24 bytes)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
struct BmcHeader {
    magic: u8,
    opcode: u8,
    key_len: u16, // big-endian
    extra_len: u8,
    data_type: u8,
    vbucket_or_status: u16, // big-endian, vbucket for request, status for response
    body_len: u32,          // big-endian
    opaque: u32,            // big-endian (not used in Lancet)
    version: u64,           // big-endian (not used in Lancet)
}

impl BmcHeader {
    const SIZE: usize = 24;

    fn new_request(opcode: u8, key_len: u16, extra_len: u8, body_len: u32) -> Self {
        Self {
            magic: 0x80, // Request magic
            opcode,
            key_len: key_len.to_be(),
            extra_len,
            data_type: 0x00,
            vbucket_or_status: 0u16.to_be(),
            body_len: body_len.to_be(),
            opaque: 0,
            version: 0,
        }
    }

    fn as_bytes(&self) -> [u8; Self::SIZE] {
        unsafe { std::mem::transmute::<BmcHeader, [u8; Self::SIZE]>(*self) }
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < Self::SIZE {
            return None;
        }
        unsafe {
            let mut header_bytes = [0u8; Self::SIZE];
            header_bytes.copy_from_slice(&bytes[0..Self::SIZE]);
            Some(std::mem::transmute::<[u8; Self::SIZE], BmcHeader>(header_bytes))
        }
    }

    fn body_len(&self) -> u32 {
        u32::from_be(self.body_len)
    }

    fn status(&self) -> u16 {
        u16::from_be(self.vbucket_or_status)
    }

    /// Offset of the value in the message: after the header, extras and key
    fn value_offset(&self) -> usize {
        Self::SIZE + usize::from(self.extra_len) + usize::from(u16::from_be(self.key_len))
    }
}

pub struct MemcachedBinaryProtocol {
    /// Operations, keys and value sizes of the measurement phase
    workload: Workload,
    /// Per-connection sequence numbers for send
    conn_send_seq: HashMap<usize, u64>,
    /// Per-connection sequence numbers for receive
    conn_recv_seq: HashMap<usize, u64>,
    key_prefix: String,
    /// Insert phase state (None = no insert phase, go straight to measurement)
    insert_phase: Option<InsertPhaseState>,
    /// UDP framing and reassembly; `None` over byte streams
    udp: Option<UdpFraming>,
}

impl MemcachedBinaryProtocol {
    pub fn new(operation: MemcachedOp) -> Self {
        Self::from_workload(Workload::new(operation, None, 64))
    }

    /// Create with embedded workload generator
    pub fn with_workload(
        operation: MemcachedOp,
        key_gen: crate::workload::KeyGeneration,
        value_size: usize,
    ) -> Self {
        Self::from_workload(Workload::new(operation, Some(key_gen), value_size))
    }

    fn from_workload(workload: Workload) -> Self {
        Self {
            workload,
            conn_send_seq: HashMap::new(),
            conn_recv_seq: HashMap::new(),
            key_prefix: "key:".to_string(),
            insert_phase: None,
            udp: None,
        }
    }

    /// Select each measurement operation with `selector` instead of the one
    /// operation
    pub fn with_operations(mut self, selector: Box<dyn CommandSelector<MemcachedOp>>) -> Self {
        self.workload.selector = selector;
        self
    }

    /// Draw the value size of every storage operation from `sizes`, by
    /// command name (SET, ADD, REPLACE)
    pub fn with_value_size_generator(mut self, sizes: Box<dyn ValueSizeGenerator>) -> Self {
        self.workload.value_sizes = Some(sizes);
        self
    }

    /// Set the prefix used by all generated keys, including warmup requests.
    pub fn with_key_prefix(mut self, prefix: String) -> Self {
        self.key_prefix = prefix;
        self
    }

    /// Frame requests and responses for UDP (memcached's 8-byte frame header)
    pub fn with_udp_framing(mut self) -> Self {
        self.udp = Some(UdpFraming::default());
        self
    }

    /// Set up insert phase for data population before measurement
    pub fn with_insert_phase(mut self, key_count: u64, value_size: usize) -> Self {
        let insert_key_gen = self.workload.key_gen.clone();
        self.insert_phase = Some(InsertPhaseState::new(key_count, value_size, insert_key_gen));
        self
    }

    /// Check if currently in insert phase (warmup)
    pub fn is_in_insert_phase(&self) -> bool {
        self.insert_phase.as_ref().is_some_and(|p| !p.is_complete())
    }

    /// Append one request message: header, `extras`, the key (if any) and
    /// `value_size` bytes of value
    fn push_message(
        &self,
        buf: &mut Vec<u8>,
        opcode: u8,
        extras: &[u8],
        key: Option<u64>,
        value_size: usize,
    ) {
        let key = key.map(|key| format!("{}{key}", self.key_prefix)).unwrap_or_default();
        let key_len = key.len() as u16;
        let body_len = (extras.len() + key.len() + value_size) as u32;
        let header = BmcHeader::new_request(opcode, key_len, extras.len() as u8, body_len);
        buf.reserve(BmcHeader::SIZE + body_len as usize);
        buf.extend_from_slice(&header.as_bytes());
        buf.extend_from_slice(extras);
        buf.extend_from_slice(key.as_bytes());
        buf.resize(buf.len() + value_size, b'x');
    }

    /// Encode a storage request for a specific key and value size; its
    /// extras are 4 bytes of flags and 4 of expiration, all 0
    fn encode_store(&self, opcode: u8, key: u64, value_size: usize) -> Vec<u8> {
        let mut buf = Vec::new();
        self.push_message(&mut buf, opcode, &[0u8; 8], Some(key), value_size);
        buf
    }

    fn encode(&self, NextOp { op, keys, value_size }: &NextOp) -> Vec<u8> {
        let mut buf = Vec::new();
        match *op {
            // GETK echoes the key
            MemcachedOp::Get => self.push_message(&mut buf, CMD_GETK, &[], Some(keys[0]), 0),
            MemcachedOp::Set => return self.encode_store(CMD_SET, keys[0], *value_size),
            MemcachedOp::Add => return self.encode_store(CMD_ADD, keys[0], *value_size),
            MemcachedOp::Replace => return self.encode_store(CMD_REPLACE, keys[0], *value_size),
            MemcachedOp::Delete => self.push_message(&mut buf, CMD_DELETE, &[], Some(keys[0]), 0),
            MemcachedOp::Incr { delta } | MemcachedOp::Decr { delta } => {
                let opcode = if matches!(op, MemcachedOp::Incr { .. }) {
                    CMD_INCREMENT
                } else {
                    CMD_DECREMENT
                };
                // Extras: delta, initial value, expiration
                let mut extras = [0u8; 20];
                extras[..8].copy_from_slice(&delta.to_be_bytes());
                extras[16..].copy_from_slice(&DELTA_NO_CREATE.to_be_bytes());
                self.push_message(&mut buf, opcode, &extras, Some(keys[0]), 0);
            }
            MemcachedOp::Touch { exptime } => {
                self.push_message(&mut buf, CMD_TOUCH, &exptime.to_be_bytes(), Some(keys[0]), 0)
            }
            // A quiet GETKQ per key answers only hits; the NOOP after them
            // ends the response
            MemcachedOp::MultiGet { .. } => {
                for &key in keys {
                    self.push_message(&mut buf, CMD_GETKQ, &[], Some(key), 0);
                }
                self.push_message(&mut buf, CMD_NOOP, &[], None, 0);
            }
        }
        buf
    }
}

impl Default for MemcachedBinaryProtocol {
    fn default() -> Self {
        Self::new(MemcachedOp::Get)
    }
}

impl Protocol for MemcachedBinaryProtocol {
    type RequestId = (usize, u64);

    fn next_request(&mut self, conn_id: usize) -> crate::Request<Self::RequestId> {
        // Phase 1: Insert phase (warmup) - populate data before measurement
        if let Some(ref mut insert_state) = self.insert_phase {
            if let Some(key) = insert_state.next_key() {
                let value_size = insert_state.value_size;
                let seq = next_conn_seq(&mut self.conn_send_seq, conn_id);
                let request_data = self.encode_store(CMD_SET, key, value_size);
                let request_data = frame_udp_request(seq, request_data, self.udp.as_ref());
                return crate::Request::warmup(request_data, (conn_id, seq));
            }
        }

        // Phase 2: Normal measurement phase
        let next = self.workload.next();
        let seq = next_conn_seq(&mut self.conn_send_seq, conn_id);
        let request_data = self.encode(&next);
        let request_data = frame_udp_request(seq, request_data, self.udp.as_ref());

        crate::Request::measurement(request_data, (conn_id, seq))
    }

    fn parse_response<'a>(
        &mut self,
        conn_id: usize,
        data: &'a [u8],
    ) -> Result<Parsed<'a, Self::RequestId>> {
        if let Some(framing) = &mut self.udp {
            return parse_udp_response(framing, &self.conn_send_seq, conn_id, data, response_end);
        }
        let Some((total_len, error)) = response_end(data)? else {
            return Ok(Parsed::Incomplete);
        };

        // Complete response found
        let seq = next_conn_seq(&mut self.conn_recv_seq, conn_id);
        Ok(Parsed::Complete {
            consumed: total_len,
            id: (conn_id, seq),
            error: error.map(error_reply),
        })
    }

    fn preload_pending(&self, _conn_id: usize) -> bool {
        // The connections share the insert phase
        self.is_in_insert_phase()
    }

    fn name(&self) -> &'static str {
        "memcached-binary"
    }

    fn reset(&mut self) {
        self.conn_send_seq.clear();
        self.conn_recv_seq.clear();
        self.workload.reset();
        if let Some(framing) = &mut self.udp {
            framing.reset();
        }
    }
}

/// The end of the complete response at the start of `data`, `None` if it is
/// incomplete
///
/// A response is one message, or for a multi-get, the messages of its quiet
/// gets (only their hits) up to the message of the NOOP that ends it. A
/// non-zero status is an error reply named by the status, with the value
/// (memcached's error text) as its text, except a miss or a failed condition
/// (`KEY_ENOENT`, `KEY_EEXISTS`, `NOT_STORED`): a result. A response with
/// several errors reports the first.
fn response_end(data: &[u8]) -> Result<Option<ResponseEnd<'_>>> {
    let mut pos = 0;
    let mut first_error = None;
    loop {
        // Need at least the header
        let Some(header) = BmcHeader::from_bytes(&data[pos..]) else {
            return Ok(None);
        };
        anyhow::ensure!(
            header.magic == MAGIC_RESPONSE,
            "Invalid Memcached binary response magic 0x{:02x}",
            header.magic
        );
        let start = pos;
        let end = pos + BmcHeader::SIZE + header.body_len() as usize;
        // Check if we have the complete message
        if data.len() < end {
            return Ok(None);
        }
        pos = end;
        let status = header.status();
        let result =
            matches!(status, 0 | STATUS_KEY_ENOENT | STATUS_KEY_EEXISTS | STATUS_NOT_STORED);
        if !result && first_error.is_none() {
            let name = status_name(status);
            let text = data.get(start + header.value_offset()..end).filter(|text| !text.is_empty());
            first_error = Some((name, text.unwrap_or(name.as_bytes())));
        }
        if status == 0 && matches!(header.opcode, CMD_INCREMENT | CMD_DECREMENT) {
            anyhow::ensure!(
                header.body_len() == 8,
                "Memcached incr/decr response body of {} bytes, not an 8-byte value",
                header.body_len()
            );
        }
        // A quiet get's hit is followed by the rest of its multi-get
        if !matches!(header.opcode, CMD_GETQ | CMD_GETKQ) {
            return Ok(Some((pos, first_error)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::KeyGeneration;
    use crate::ErrorReply;

    fn protocol(op: MemcachedOp) -> MemcachedBinaryProtocol {
        MemcachedBinaryProtocol::with_workload(op, KeyGeneration::sequential(7), 3)
    }

    /// A message with `opcode`, `status`, `extras`, `key` and `value`
    fn message(
        magic: u8,
        opcode: u8,
        status: u16,
        extras: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Vec<u8> {
        let mut buf = vec![magic, opcode];
        buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
        buf.push(extras.len() as u8);
        buf.push(0);
        buf.extend_from_slice(&status.to_be_bytes());
        buf.extend_from_slice(&((extras.len() + key.len() + value.len()) as u32).to_be_bytes());
        buf.extend_from_slice(&[0; 12]);
        for part in [extras, key, value] {
            buf.extend_from_slice(part);
        }
        buf
    }

    fn request(opcode: u8, extras: &[u8], key: &[u8], value: &[u8]) -> Vec<u8> {
        message(0x80, opcode, 0, extras, key, value)
    }

    fn response(opcode: u8, status: u16, extras: &[u8], key: &[u8], value: &[u8]) -> Vec<u8> {
        message(MAGIC_RESPONSE, opcode, status, extras, key, value)
    }

    #[test]
    fn every_operation_encodes_its_command() {
        let mut incr = [0u8; 20];
        incr[7] = 5;
        incr[16..].fill(0xff);
        let mut decr = incr;
        decr[7] = 1;
        let multi_get = [
            request(CMD_GETKQ, &[], b"key:7", b""),
            request(CMD_GETKQ, &[], b"key:8", b""),
            request(CMD_NOOP, &[], b"", b""),
        ]
        .concat();
        for (op, expected) in [
            (MemcachedOp::Get, request(CMD_GETK, &[], b"key:7", b"")),
            (MemcachedOp::Set, request(CMD_SET, &[0; 8], b"key:7", b"xxx")),
            (MemcachedOp::Add, request(CMD_ADD, &[0; 8], b"key:7", b"xxx")),
            (MemcachedOp::Replace, request(CMD_REPLACE, &[0; 8], b"key:7", b"xxx")),
            (MemcachedOp::Delete, request(CMD_DELETE, &[], b"key:7", b"")),
            (MemcachedOp::Incr { delta: 5 }, request(CMD_INCREMENT, &incr, b"key:7", b"")),
            (MemcachedOp::Decr { delta: 1 }, request(CMD_DECREMENT, &decr, b"key:7", b"")),
            (
                MemcachedOp::Touch { exptime: 60 },
                request(CMD_TOUCH, &[0, 0, 0, 60], b"key:7", b""),
            ),
            (MemcachedOp::MultiGet { count: 2 }, multi_get),
        ] {
            assert_eq!(protocol(op).next_request(0).data, expected, "{op:?}");
        }
    }

    #[test]
    fn replies_of_every_operation_are_framed() {
        let hit = |opcode| response(opcode, 0, &[0; 4], b"key:7", b"abc");
        let miss = |opcode| response(opcode, STATUS_KEY_ENOENT, &[], b"", b"Not found");
        let noop = response(CMD_NOOP, 0, &[], b"", b"");
        for reply in [
            hit(CMD_GETK),
            miss(CMD_GETK),
            miss(0x00), // GET
            response(CMD_SET, 0, &[], b"", b""),
            response(CMD_ADD, STATUS_KEY_EEXISTS, &[], b"", b"Data exists for key."),
            response(CMD_REPLACE, STATUS_NOT_STORED, &[], b"", b"Not stored."),
            response(CMD_REPLACE, STATUS_KEY_ENOENT, &[], b"", b"Not found"),
            response(CMD_DELETE, 0, &[], b"", b""),
            miss(CMD_DELETE),
            response(CMD_INCREMENT, 0, &[], b"", &6u64.to_be_bytes()),
            miss(CMD_DECREMENT),
            response(CMD_TOUCH, 0, &[], b"", b""),
            // Multi-gets: no hit, some hits, all hits
            noop.clone(),
            [hit(CMD_GETKQ), noop.clone()].concat(),
            [hit(CMD_GETKQ), hit(CMD_GETKQ), noop.clone()].concat(),
        ] {
            let mut protocol = protocol(MemcachedOp::Get);
            assert_eq!(
                protocol.parse_response(0, &reply).unwrap(),
                Parsed::ok(reply.len(), (0, 0))
            );
            let mut two = reply.clone();
            two.extend_from_slice(&noop);
            assert_eq!(protocol.parse_response(0, &two).unwrap(), Parsed::ok(reply.len(), (0, 1)));
            for len in 0..reply.len() {
                assert_eq!(protocol.parse_response(0, &reply[..len]).unwrap(), Parsed::Incomplete);
            }
        }
    }

    #[test]
    fn error_statuses_are_error_replies() {
        let non_numeric = b"Non-numeric server-side value for incr or decr";
        for (reply, kind, text) in [
            (
                response(CMD_INCREMENT, 0x0006, &[], b"", non_numeric),
                "DELTA_BADVAL",
                non_numeric.as_slice(),
            ),
            (response(CMD_SET, 0x0003, &[], b"", b"Too large."), "E2BIG", b"Too large."),
            (
                response(CMD_TOUCH, 0x0081, &[], b"", b""),
                "UNKNOWN_COMMAND",
                b"UNKNOWN_COMMAND",
            ),
            // A quiet get's error is the multi-get's
            (
                [
                    response(CMD_GETKQ, 0x0082, &[], b"", b"Out of memory"),
                    response(CMD_GETKQ, 0x0084, &[], b"", b"Internal"),
                    response(CMD_NOOP, 0, &[], b"", b""),
                ]
                .concat(),
                "ENOMEM",
                b"Out of memory",
            ),
        ] {
            let mut protocol = protocol(MemcachedOp::Get);
            assert_eq!(
                protocol.parse_response(0, &reply).unwrap(),
                Parsed::error(reply.len(), (0, 0), ErrorReply::from_bytes(kind.as_bytes(), text))
            );
        }
    }

    #[test]
    fn malformed_replies_are_errors() {
        let mut protocol = protocol(MemcachedOp::Get);
        let request_magic = request(CMD_GETK, &[], b"k", b"");
        assert!(protocol.parse_response(0, &request_magic).is_err());
        let short_incr = response(CMD_INCREMENT, 0, &[], b"", b"6");
        assert!(protocol.parse_response(0, &short_incr).is_err());
    }
}
