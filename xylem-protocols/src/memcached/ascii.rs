//! Memcached ASCII protocol
//!
//! Implements the Memcached text protocol with GET and SET commands.

use super::{next_conn_seq, InsertPhaseState};
use crate::Protocol;
use anyhow::Result;
use std::collections::HashMap;
use zeropool::BufferPool;

pub use super::MemcachedOp;

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
    key_prefix: String,
    /// Insert phase state
    insert_phase: Option<InsertPhaseState>,
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
            key_prefix: "key:".to_string(),
            insert_phase: None,
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
            key_prefix: "key:".to_string(),
            insert_phase: None,
        }
    }

    /// Set the prefix used by all generated keys, including warmup requests.
    pub fn with_key_prefix(mut self, prefix: String) -> Self {
        self.key_prefix = prefix;
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
    ///
    /// Format: set key:N 0 0 <value_len>\r\n<value>\r\n
    fn encode_set(&self, key: u64, value_size: usize) -> Vec<u8> {
        let mut buf = self.pool.get(256 + value_size);
        buf.clear();
        buf.extend_from_slice(b"set ");
        buf.extend_from_slice(self.key_prefix.as_bytes());
        buf.extend_from_slice(key.to_string().as_bytes());
        buf.extend_from_slice(b" 0 0 ");
        buf.extend_from_slice(value_size.to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.resize(buf.len() + value_size, b'x');
        buf.extend_from_slice(b"\r\n");
        buf.to_vec()
    }
}

impl Default for MemcachedAsciiProtocol {
    fn default() -> Self {
        Self::new(MemcachedOp::Get)
    }
}

impl Protocol for MemcachedAsciiProtocol {
    type RequestId = (usize, u64);

    fn next_request(&mut self, conn_id: usize) -> crate::Request<Self::RequestId> {
        // Phase 1: Insert phase (warmup) - populate data before measurement
        if let Some(ref mut insert_state) = self.insert_phase {
            if let Some(key) = insert_state.next_key() {
                let value_size = insert_state.value_size;
                let seq = next_conn_seq(&mut self.conn_send_seq, conn_id);
                let request_data = self.encode_set(key, value_size);
                return crate::Request::warmup(request_data, (conn_id, seq));
            }
        }

        // Phase 2: Normal measurement phase
        let key = self.key_gen.as_mut().map(|g| g.next_key()).unwrap_or(0);
        let seq = next_conn_seq(&mut self.conn_send_seq, conn_id);
        let request_data = match self.operation {
            MemcachedOp::Get => {
                // Format: get key:N\r\n
                let mut buf = self.pool.get(64);
                buf.clear();
                buf.extend_from_slice(b"get ");
                buf.extend_from_slice(self.key_prefix.as_bytes());
                buf.extend_from_slice(key.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.to_vec()
            }
            MemcachedOp::Set => self.encode_set(key, self.value_size),
        };
        crate::Request::measurement(request_data, (conn_id, seq))
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)> {
        let mut pos = 0;
        loop {
            let Some(line_end) = data[pos..].windows(2).position(|w| w == b"\r\n") else {
                return Ok((0, None));
            };
            let line = &data[pos..pos + line_end];
            pos += line_end + 2;
            if line == b"END" || (pos == line_end + 2 && line == b"STORED") {
                let seq = next_conn_seq(&mut self.conn_recv_seq, conn_id);
                return Ok((pos, Some((conn_id, seq))));
            }
            if line == b"ERROR"
                || line.starts_with(b"CLIENT_ERROR ")
                || line.starts_with(b"SERVER_ERROR ")
            {
                anyhow::bail!("Memcached error: {}", String::from_utf8_lossy(line));
            }
            let header = std::str::from_utf8(line)?;
            let fields: Vec<_> = header.split_whitespace().collect();
            anyhow::ensure!(
                fields.len() == 4 || fields.len() == 5,
                "Invalid Memcached VALUE header"
            );
            anyhow::ensure!(fields[0] == "VALUE", "Unexpected Memcached response: {header}");
            let length: usize = fields[3].parse()?;
            let end = pos
                .checked_add(length)
                .and_then(|n| n.checked_add(2))
                .ok_or_else(|| anyhow::anyhow!("Memcached value length overflow"))?;
            if data.len() < end {
                return Ok((0, None));
            }
            anyhow::ensure!(&data[end - 2..end] == b"\r\n", "Missing Memcached value terminator");
            pos = end;
        }
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
