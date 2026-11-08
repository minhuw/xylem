//! Memcached Binary protocol
//!
//! Implements the Memcached binary protocol (memcache_bin.h).

use super::udp::UdpFraming;
use super::{
    error_reply, frame_udp_request, next_conn_seq, parse_udp_response, InsertPhaseState,
    ResponseEnd,
};
use crate::{Parsed, Protocol};
use anyhow::Result;
use std::collections::HashMap;

pub use super::MemcachedOp;

// Command opcodes
const CMD_GET: u8 = 0x00;
const CMD_GETQ: u8 = 0x09;
const CMD_GETK: u8 = 0x0c;
const CMD_GETKQ: u8 = 0x0d;
const CMD_SET: u8 = 0x01;

/// Response status of a missing key
const STATUS_KEY_ENOENT: u16 = 0x0001;

/// Name of a non-zero response status, as in memcached's `protocol_binary.h`
fn status_name(status: u16) -> &'static str {
    match status {
        STATUS_KEY_ENOENT => "KEY_ENOENT",
        0x0002 => "KEY_EEXISTS",
        0x0003 => "E2BIG",
        0x0004 => "EINVAL",
        0x0005 => "NOT_STORED",
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
    operation: MemcachedOp,
    /// Per-connection sequence numbers for send
    conn_send_seq: HashMap<usize, u64>,
    /// Per-connection sequence numbers for receive
    conn_recv_seq: HashMap<usize, u64>,
    /// Key generator for next_request()
    key_gen: Option<crate::workload::KeyGeneration>,
    /// Value size for next_request()
    value_size: usize,
    key_prefix: String,
    /// Insert phase state (None = no insert phase, go straight to measurement)
    insert_phase: Option<InsertPhaseState>,
    /// UDP framing and reassembly; `None` over byte streams
    udp: Option<UdpFraming>,
}

impl MemcachedBinaryProtocol {
    pub fn new(operation: MemcachedOp) -> Self {
        Self {
            operation,
            conn_send_seq: HashMap::new(),
            conn_recv_seq: HashMap::new(),
            key_gen: None,
            value_size: 64,
            key_prefix: "key:".to_string(),
            insert_phase: None,
            udp: None,
        }
    }

    /// Create with embedded workload generator
    pub fn with_workload(
        operation: MemcachedOp,
        key_gen: crate::workload::KeyGeneration,
        value_size: usize,
    ) -> Self {
        Self {
            operation,
            conn_send_seq: HashMap::new(),
            conn_recv_seq: HashMap::new(),
            key_gen: Some(key_gen),
            value_size,
            key_prefix: "key:".to_string(),
            insert_phase: None,
            udp: None,
        }
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
        let insert_key_gen = self.key_gen.clone();
        self.insert_phase = Some(InsertPhaseState::new(key_count, value_size, insert_key_gen));
        self
    }

    /// Check if currently in insert phase (warmup)
    pub fn is_in_insert_phase(&self) -> bool {
        self.insert_phase.as_ref().is_some_and(|p| !p.is_complete())
    }

    /// Encode a SET request for a specific key and value size
    fn encode_set(&self, key: u64, value_size: usize) -> Vec<u8> {
        let key_str = format!("{}{key}", self.key_prefix);
        let key_bytes = key_str.as_bytes();
        let key_len = key_bytes.len() as u16;

        // SET: header (24) + extras (8: flags + expiration) + key + value
        let extras_len = 8u8;
        let body_len = extras_len as u32 + key_len as u32 + value_size as u32;
        let header = BmcHeader::new_request(CMD_SET, key_len, extras_len, body_len);

        let mut buf = Vec::with_capacity(
            BmcHeader::SIZE + extras_len as usize + key_len as usize + value_size,
        );
        buf.extend_from_slice(&header.as_bytes());

        // Extras: 4 bytes flags (0) + 4 bytes expiration (0)
        buf.extend_from_slice(&[0u8; 8]);

        // Key
        buf.extend_from_slice(key_bytes);

        // Value
        buf.resize(buf.len() + value_size, b'x');

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
                let request_data = self.encode_set(key, value_size);
                let request_data = frame_udp_request(seq, request_data, self.udp.as_ref());
                return crate::Request::warmup(request_data, (conn_id, seq));
            }
        }

        // Phase 2: Normal measurement phase
        let key = self.key_gen.as_mut().map(|g| g.next_key()).unwrap_or(0);
        let seq = next_conn_seq(&mut self.conn_send_seq, conn_id);

        let request_data = match self.operation {
            MemcachedOp::Get => {
                // GET/GETK: header + key
                let key_str = format!("{}{key}", self.key_prefix);
                let key_bytes = key_str.as_bytes();
                let key_len = key_bytes.len() as u16;
                let body_len = key_len as u32;
                let header = BmcHeader::new_request(CMD_GETK, key_len, 0, body_len);

                let mut buf = Vec::with_capacity(BmcHeader::SIZE + key_len as usize);
                buf.extend_from_slice(&header.as_bytes());
                buf.extend_from_slice(key_bytes);
                buf
            }
            MemcachedOp::Set => self.encode_set(key, self.value_size),
        };
        let request_data = frame_udp_request(seq, request_data, self.udp.as_ref());

        crate::Request::measurement(request_data, (conn_id, seq))
    }

    fn parse_response<'a>(
        &mut self,
        conn_id: usize,
        data: &'a [u8],
    ) -> Result<Parsed<'a, Self::RequestId>> {
        if let Some(framing) = &mut self.udp {
            return parse_udp_response(framing, &self.conn_send_seq, conn_id, data, |data| {
                Ok(response_end(data))
            });
        }
        let Some((total_len, error)) = response_end(data) else {
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

    fn name(&self) -> &'static str {
        "memcached-binary"
    }

    fn reset(&mut self) {
        self.conn_send_seq.clear();
        self.conn_recv_seq.clear();
        if let Some(ref mut key_gen) = self.key_gen {
            key_gen.reset();
        }
        if let Some(framing) = &mut self.udp {
            framing.reset();
        }
    }
}

/// The end of the complete response at the start of `data`, `None` if it is
/// incomplete
///
/// A non-zero status is an error reply named by the status, with the value
/// (memcached's error text) as its text, except a missing key on a GET: a miss.
fn response_end(data: &[u8]) -> Option<ResponseEnd<'_>> {
    // Need at least the header
    let header = BmcHeader::from_bytes(data)?;
    let total_len = BmcHeader::SIZE + header.body_len() as usize;
    // Check if we have the complete message
    if data.len() < total_len {
        return None;
    }
    let status = header.status();
    let miss = status == STATUS_KEY_ENOENT
        && matches!(header.opcode, CMD_GET | CMD_GETQ | CMD_GETK | CMD_GETKQ);
    let error = (status != 0 && !miss).then(|| {
        let name = status_name(status);
        let text = data.get(header.value_offset()..total_len).filter(|text| !text.is_empty());
        (name, text.unwrap_or(name.as_bytes()))
    });
    Some((total_len, error))
}
