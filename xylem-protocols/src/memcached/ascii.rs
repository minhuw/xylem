//! Memcached ASCII protocol
//!
//! Implements the Memcached text protocol with GET and SET commands.

use crate::Protocol;
use anyhow::Result;
use std::collections::HashMap;
use zeropool::BufferPool;

#[derive(Debug, Clone, Copy)]
pub enum MemcachedOp {
    Get,
    Set,
}

pub struct MemcachedAsciiProtocol {
    operation: MemcachedOp,
    /// Per-connection sequence numbers for send
    conn_send_seq: HashMap<usize, u64>,
    /// Per-connection sequence numbers for receive
    conn_recv_seq: HashMap<usize, u64>,
    /// Buffer pool for request generation
    pool: BufferPool,
    /// Key generator for next_request()
    key_gen: Option<crate::workload::KeyGeneration>,
    /// Value size for next_request()
    value_size: usize,
}

impl MemcachedAsciiProtocol {
    pub fn new(operation: MemcachedOp) -> Self {
        Self {
            operation,
            conn_send_seq: HashMap::new(),
            conn_recv_seq: HashMap::new(),
            pool: BufferPool::new(),
            key_gen: None,
            value_size: 64,
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
            pool: BufferPool::new(),
            key_gen: Some(key_gen),
            value_size,
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

impl Default for MemcachedAsciiProtocol {
    fn default() -> Self {
        Self::new(MemcachedOp::Get)
    }
}

impl Protocol for MemcachedAsciiProtocol {
    type RequestId = (usize, u64);

    fn next_request(&mut self, conn_id: usize) -> (Vec<u8>, Self::RequestId) {
        let key = self.key_gen.as_mut().map(|g| g.next_key()).unwrap_or(0);
        self.generate_request(conn_id, key, self.value_size)
    }

    fn generate_request(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
    ) -> (Vec<u8>, Self::RequestId) {
        let seq = self.next_send_seq(conn_id);
        let request_data = match self.operation {
            MemcachedOp::Get => {
                // Format: get key:N\r\n
                let mut buf = self.pool.get(64);
                buf.clear();
                buf.extend_from_slice(b"get key:");
                buf.extend_from_slice(key.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.to_vec()
            }
            MemcachedOp::Set => {
                // Format: set key:N 0 0 <value_len>\r\n<value>\r\n
                let mut buf = self.pool.get(256 + value_size);
                buf.clear();
                buf.extend_from_slice(b"set key:");
                buf.extend_from_slice(key.to_string().as_bytes());
                buf.extend_from_slice(b" 0 0 ");
                buf.extend_from_slice(value_size.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.resize(buf.len() + value_size, b'x');
                buf.extend_from_slice(b"\r\n");
                buf.to_vec()
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

        // Check for "END\r\n" (key not found) - 5 bytes
        if data.len() >= 5 && &data[0..5] == b"END\r\n" {
            let seq = self.next_recv_seq(conn_id);
            return Ok((5, Some((conn_id, seq))));
        }

        // Check for "STORED\r\n" (successful set) - 8 bytes
        if data.len() >= 8 && &data[0..8] == b"STORED\r\n" {
            let seq = self.next_recv_seq(conn_id);
            return Ok((8, Some((conn_id, seq))));
        }

        // Check for GET response: VALUE key flags length\r\n<data>\r\nEND\r\n
        // We need to find the third newline
        let mut newline_count = 0;
        let mut pos = 0;

        for (i, &byte) in data.iter().enumerate() {
            if byte == b'\n' {
                newline_count += 1;
                if newline_count == 3 {
                    pos = i + 1;
                    break;
                }
            }
        }

        if newline_count == 3 {
            // Found complete GET response
            let seq = self.next_recv_seq(conn_id);
            return Ok((pos, Some((conn_id, seq))));
        }

        // Incomplete response
        Ok((0, None))
    }

    fn name(&self) -> &'static str {
        "memcached-ascii"
    }

    fn reset(&mut self) {
        self.conn_send_seq.clear();
        self.conn_recv_seq.clear();
        if let Some(ref mut key_gen) = self.key_gen {
            key_gen.reset();
        }
    }
}
