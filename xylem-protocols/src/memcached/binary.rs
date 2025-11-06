//! Memcached Binary protocol
//!
//! Implements the Memcached binary protocol (memcache_bin.h).

use crate::Protocol;
use anyhow::Result;
use std::collections::HashMap;

// Command opcodes
const CMD_GETK: u8 = 0x0c;
const CMD_SET: u8 = 0x01;

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
}

#[derive(Debug, Clone, Copy)]
pub enum MemcachedOp {
    Get,
    Set,
}

pub struct MemcachedBinaryProtocol {
    operation: MemcachedOp,
    /// Per-connection sequence numbers for send
    conn_send_seq: HashMap<usize, u64>,
    /// Per-connection sequence numbers for receive
    conn_recv_seq: HashMap<usize, u64>,
}

impl MemcachedBinaryProtocol {
    pub fn new(operation: MemcachedOp) -> Self {
        Self {
            operation,
            conn_send_seq: HashMap::new(),
            conn_recv_seq: HashMap::new(),
        }
    }

    fn next_send_seq(&mut self, conn_id: usize) -> u64 {
        let seq = self.conn_send_seq.entry(conn_id).or_insert(0);
        let result = *seq;
        *seq += 1;
        result
    }

    fn next_recv_seq(&mut self, conn_id: usize) -> u64 {
        let seq = self.conn_recv_seq.entry(conn_id).or_insert(0);
        let result = *seq;
        *seq += 1;
        result
    }
}

impl Default for MemcachedBinaryProtocol {
    fn default() -> Self {
        Self::new(MemcachedOp::Get)
    }
}

impl Protocol for MemcachedBinaryProtocol {
    type RequestId = (usize, u64);

    fn generate_request(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
    ) -> (Vec<u8>, Self::RequestId) {
        let seq = self.next_send_seq(conn_id);
        let key_str = format!("key:{key}");
        let key_bytes = key_str.as_bytes();
        let key_len = key_bytes.len() as u16;

        let request_data = match self.operation {
            MemcachedOp::Get => {
                // GET/GETK: header + key
                let body_len = key_len as u32;
                let header = BmcHeader::new_request(CMD_GETK, key_len, 0, body_len);

                let mut buf = Vec::with_capacity(BmcHeader::SIZE + key_len as usize);
                buf.extend_from_slice(&header.as_bytes());
                buf.extend_from_slice(key_bytes);
                buf
            }
            MemcachedOp::Set => {
                // SET: header + extras (8 bytes: flags + expiration) + key + value
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
        };

        (request_data, (conn_id, seq))
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)> {
        if data.is_empty() {
            return Ok((0, None));
        }

        // Need at least the header
        if data.len() < BmcHeader::SIZE {
            return Ok((0, None));
        }

        // Parse header
        let header = match BmcHeader::from_bytes(data) {
            Some(h) => h,
            None => return Ok((0, None)),
        };

        let body_len = header.body_len() as usize;
        let total_len = BmcHeader::SIZE + body_len;

        // Check if we have the complete message
        if data.len() < total_len {
            return Ok((0, None));
        }

        // Complete response found
        let seq = self.next_recv_seq(conn_id);
        Ok((total_len, Some((conn_id, seq))))
    }

    fn name(&self) -> &'static str {
        "memcached-binary"
    }

    fn reset(&mut self) {
        self.conn_send_seq.clear();
        self.conn_recv_seq.clear();
    }
}
