//! Memcached ASCII protocol
//!
//! Implements the Memcached text protocol: get (also of several keys), set,
//! add, replace, delete, incr, decr and touch.

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

pub struct MemcachedAsciiProtocol {
    /// Operations, keys and value sizes of the measurement phase
    workload: Workload,
    /// Per-connection sequence numbers for send
    conn_send_seq: HashMap<usize, u64>,
    /// Per-connection sequence numbers for receive
    conn_recv_seq: HashMap<usize, u64>,
    key_prefix: String,
    /// Insert phase state
    insert_phase: Option<InsertPhaseState>,
    /// UDP framing and reassembly; `None` over byte streams
    udp: Option<UdpFraming>,
}

impl MemcachedAsciiProtocol {
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

    fn push_key(&self, buf: &mut Vec<u8>, key: u64) {
        buf.extend_from_slice(self.key_prefix.as_bytes());
        buf.extend_from_slice(key.to_string().as_bytes());
    }

    /// Encode a storage command for a specific key and value size
    ///
    /// Format: <command> key:N 0 0 <value_len>\r\n<value>\r\n
    fn encode_store(&self, command: &[u8], key: u64, value_size: usize) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256 + value_size);
        buf.extend_from_slice(command);
        buf.push(b' ');
        self.push_key(&mut buf, key);
        buf.extend_from_slice(b" 0 0 ");
        buf.extend_from_slice(value_size.to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.resize(buf.len() + value_size, b'x');
        buf.extend_from_slice(b"\r\n");
        buf
    }

    /// Encode a command line: <command> <keys...> [<argument>]\r\n
    fn encode_line(&self, command: &[u8], keys: &[u64], argument: Option<u64>) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16 + keys.len() * (self.key_prefix.len() + 21));
        buf.extend_from_slice(command);
        for &key in keys {
            buf.push(b' ');
            self.push_key(&mut buf, key);
        }
        if let Some(argument) = argument {
            buf.push(b' ');
            buf.extend_from_slice(argument.to_string().as_bytes());
        }
        buf.extend_from_slice(b"\r\n");
        buf
    }

    fn encode(&self, NextOp { op, keys, value_size }: &NextOp) -> Vec<u8> {
        match *op {
            MemcachedOp::Get | MemcachedOp::MultiGet { .. } => self.encode_line(b"get", keys, None),
            MemcachedOp::Set => self.encode_store(b"set", keys[0], *value_size),
            MemcachedOp::Add => self.encode_store(b"add", keys[0], *value_size),
            MemcachedOp::Replace => self.encode_store(b"replace", keys[0], *value_size),
            MemcachedOp::Delete => self.encode_line(b"delete", keys, None),
            MemcachedOp::Incr { delta } => self.encode_line(b"incr", keys, Some(delta)),
            MemcachedOp::Decr { delta } => self.encode_line(b"decr", keys, Some(delta)),
            MemcachedOp::Touch { exptime } => {
                self.encode_line(b"touch", keys, Some(u64::from(exptime)))
            }
        }
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
                let request_data = self.encode_store(b"set", key, value_size);
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
        let Some((len, error)) = response_end(data)? else {
            return Ok(Parsed::Incomplete);
        };
        let seq = next_conn_seq(&mut self.conn_recv_seq, conn_id);
        Ok(Parsed::Complete {
            consumed: len,
            id: (conn_id, seq),
            error: error.map(error_reply),
        })
    }

    fn preload_pending(&self, _conn_id: usize) -> bool {
        // The connections share the insert phase
        self.is_in_insert_phase()
    }

    fn name(&self) -> &'static str {
        "memcached-ascii"
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
/// A response is one line of a storage, delete, touch, incr or decr
/// command, an error line, or the values of a get followed by `END`.
fn response_end(data: &[u8]) -> Result<Option<ResponseEnd<'_>>> {
    let mut pos = 0;
    loop {
        let Some(line_end) = data[pos..].windows(2).position(|w| w == b"\r\n") else {
            return Ok(None);
        };
        let line = &data[pos..pos + line_end];
        let first = pos == 0;
        pos += line_end + 2;
        if line == b"END" || (first && is_single_line_reply(line)) {
            return Ok(Some((pos, None)));
        }
        // An error line ends the response, after any values before it
        if let Some(kind) = error_kind(line) {
            return Ok(Some((pos, Some((kind, line)))));
        }
        let header = std::str::from_utf8(line)?;
        let fields: Vec<_> = header.split_whitespace().collect();
        anyhow::ensure!(fields.len() == 4 || fields.len() == 5, "Invalid Memcached VALUE header");
        anyhow::ensure!(fields[0] == "VALUE", "Unexpected Memcached response: {header}");
        let length: usize = fields[3].parse()?;
        let end = pos
            .checked_add(length)
            .and_then(|n| n.checked_add(2))
            .ok_or_else(|| anyhow::anyhow!("Memcached value length overflow"))?;
        if data.len() < end {
            return Ok(None);
        }
        anyhow::ensure!(&data[end - 2..end] == b"\r\n", "Missing Memcached value terminator");
        pos = end;
    }
}

/// Whether `line` is a whole reply on its own: the result of a storage
/// command, delete, touch, or the new value of an incr or decr
fn is_single_line_reply(line: &[u8]) -> bool {
    matches!(
        line,
        b"STORED" | b"NOT_STORED" | b"EXISTS" | b"NOT_FOUND" | b"DELETED" | b"TOUCHED"
    ) || (!line.is_empty() && line.len() <= 20 && line.iter().all(u8::is_ascii_digit))
}

/// The keyword of an error line, `None` for any other line
fn error_kind(line: &[u8]) -> Option<&'static str> {
    if line == b"ERROR" {
        Some("ERROR")
    } else if line.starts_with(b"CLIENT_ERROR ") {
        Some("CLIENT_ERROR")
    } else if line.starts_with(b"SERVER_ERROR ") {
        Some("SERVER_ERROR")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::{FixedCommandSelector, KeyGeneration};
    use crate::ErrorReply;

    fn protocol(op: MemcachedOp) -> MemcachedAsciiProtocol {
        MemcachedAsciiProtocol::with_workload(op, KeyGeneration::sequential(7), 3)
    }

    #[test]
    fn every_operation_encodes_its_command() {
        for (op, expected) in [
            (MemcachedOp::Get, "get key:7\r\n"),
            (MemcachedOp::Set, "set key:7 0 0 3\r\nxxx\r\n"),
            (MemcachedOp::Add, "add key:7 0 0 3\r\nxxx\r\n"),
            (MemcachedOp::Replace, "replace key:7 0 0 3\r\nxxx\r\n"),
            (MemcachedOp::Delete, "delete key:7\r\n"),
            (MemcachedOp::Incr { delta: 5 }, "incr key:7 5\r\n"),
            (MemcachedOp::Decr { delta: 1 }, "decr key:7 1\r\n"),
            (MemcachedOp::Touch { exptime: 60 }, "touch key:7 60\r\n"),
            (MemcachedOp::MultiGet { count: 3 }, "get key:7 key:8 key:9\r\n"),
        ] {
            let request = protocol(op).next_request(0);
            assert_eq!(String::from_utf8(request.data).unwrap(), expected, "{op:?}");
        }
    }

    #[test]
    fn a_mix_sends_each_operation_with_value_sizes_from_the_generator() {
        let mut protocol = protocol(MemcachedOp::Get)
            .with_operations(Box::new(FixedCommandSelector::new(MemcachedOp::Add)))
            .with_value_size_generator(Box::new(crate::workload::FixedSize::new(5)));
        assert_eq!(protocol.next_request(0).data, b"add key:7 0 0 5\r\nxxxxx\r\n");
    }

    /// Every reply ends where it should, with a miss or a failed condition
    /// a result and the error lines error replies
    #[test]
    fn replies_of_every_operation_are_framed() {
        for reply in [
            "STORED\r\n",
            "NOT_STORED\r\n",
            "EXISTS\r\n",
            "NOT_FOUND\r\n",
            "DELETED\r\n",
            "TOUCHED\r\n",
            "0\r\n",
            "18446744073709551615\r\n",
            "END\r\n",
            "VALUE key:1 0 2\r\nab\r\nEND\r\n",
            "VALUE key:1 0 2\r\nab\r\nVALUE key:3 0 0\r\n\r\nEND\r\n",
        ] {
            let mut protocol = protocol(MemcachedOp::Get);
            let data = reply.as_bytes();
            assert_eq!(protocol.parse_response(0, data).unwrap(), Parsed::ok(data.len(), (0, 0)));
            // A reply followed by the next one ends at its own end
            let mut two = data.to_vec();
            two.extend_from_slice(b"END\r\n");
            assert_eq!(protocol.parse_response(0, &two).unwrap(), Parsed::ok(data.len(), (0, 1)));
            // Every proper prefix is incomplete
            for len in 0..data.len() {
                assert_eq!(protocol.parse_response(0, &data[..len]).unwrap(), Parsed::Incomplete);
            }
        }
        for (reply, kind) in [
            ("ERROR\r\n", "ERROR"),
            (
                "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n",
                "CLIENT_ERROR",
            ),
            ("SERVER_ERROR out of memory storing object\r\n", "SERVER_ERROR"),
            ("VALUE key:1 0 1\r\na\r\nSERVER_ERROR busy\r\n", "SERVER_ERROR"),
        ] {
            let mut protocol = protocol(MemcachedOp::Get);
            let data = reply.as_bytes();
            let line = reply.trim_end().rsplit("\r\n").next().unwrap();
            assert_eq!(
                protocol.parse_response(0, data).unwrap(),
                Parsed::error(data.len(), (0, 0), ErrorReply::new(kind, line))
            );
        }
    }

    #[test]
    fn single_line_replies_only_start_a_reply() {
        let mut protocol = protocol(MemcachedOp::Get);
        // A number or STORED after a value is not a get reply's end
        for reply in ["VALUE key:1 0 1\r\na\r\n5\r\n", "VALUE key:1 0 1\r\na\r\nSTORED\r\n"] {
            assert!(protocol.parse_response(0, reply.as_bytes()).is_err(), "{reply}");
        }
        for reply in ["NOT_A_REPLY\r\n", "12a\r\n", "-1\r\n"] {
            assert!(protocol.parse_response(0, reply.as_bytes()).is_err(), "{reply}");
        }
    }
}
