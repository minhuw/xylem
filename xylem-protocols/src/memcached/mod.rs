//! Memcached protocol implementations
//!
//! The ASCII and binary codecs share the operation enum, the insert-phase
//! state, per-connection sequence numbering, and the UDP framing defined here.
//!
//! Over TCP, responses arrive in request order and match requests by their
//! per-connection sequence numbers. Over UDP (`with_udp_framing`), every
//! datagram carries memcached's frame header (see [`udp`]), and responses
//! match requests by its request id, so loss and reordering are tolerated.

use crate::Parsed;
use std::collections::HashMap;

pub mod ascii;
pub mod binary;
mod config;
pub mod udp;

pub use ascii::MemcachedAsciiProtocol;
pub use binary::MemcachedBinaryProtocol;
pub use udp::MAX_UDP_VALUE_SIZE;

/// Operation sent in the measurement phase, shared by the ASCII and binary codecs
#[derive(Debug, Clone, Copy)]
pub enum MemcachedOp {
    Get,
    Set,
}

/// Insert phase state for data population before measurement
#[derive(Debug)]
pub struct InsertPhaseState {
    /// Total number of keys to insert
    key_count: u64,
    /// Number of keys inserted so far
    inserted: u64,
    /// Value size for inserts
    value_size: usize,
    /// Key generator for inserts (clone of workload generator to preserve distribution/seed)
    key_gen: Option<crate::workload::KeyGeneration>,
}

impl InsertPhaseState {
    /// Create new insert phase state
    pub fn new(
        key_count: u64,
        value_size: usize,
        key_gen: Option<crate::workload::KeyGeneration>,
    ) -> Self {
        Self {
            key_count,
            inserted: 0,
            value_size,
            key_gen,
        }
    }

    /// Check if insert phase is complete
    pub fn is_complete(&self) -> bool {
        self.inserted >= self.key_count
    }

    /// Get next key to insert, returns None if insert phase is complete
    pub fn next_key(&mut self) -> Option<u64> {
        if self.inserted >= self.key_count {
            None
        } else {
            self.inserted += 1;
            let key = if let Some(ref mut key_gen) = self.key_gen {
                key_gen.next_key()
            } else {
                self.inserted - 1 // Fallback to sequential if no generator is configured
            };
            Some(key)
        }
    }
}

/// Return the next sequence number for `conn_id` and advance it; numbering starts at 0
fn next_conn_seq(conn_seq: &mut HashMap<usize, u64>, conn_id: usize) -> u64 {
    let seq = conn_seq.entry(conn_id).or_insert(0);
    let result = *seq;
    *seq += 1;
    result
}

/// Frame a request body for UDP with its sequence number as the request id
fn frame_udp_request(seq: u64, body: Vec<u8>, framing: Option<&udp::UdpFraming>) -> Vec<u8> {
    match framing {
        Some(_) => udp::frame_request(seq as u16, &body),
        None => body,
    }
}

/// Parse one response datagram for a codec whose complete responses are
/// `response_len` bytes long (`Ok(None)` while incomplete)
///
/// Consumes the whole datagram. Returns the request ID once the datagram
/// completes a response; a reassembled response must be exactly one
/// complete response of the codec.
fn parse_udp_response<'a>(
    framing: &mut udp::UdpFraming,
    conn_send_seq: &HashMap<usize, u64>,
    conn_id: usize,
    datagram: &'a [u8],
    response_len: impl FnOnce(&[u8]) -> anyhow::Result<Option<usize>>,
) -> anyhow::Result<Parsed<'a, (usize, u64)>> {
    let sent = conn_send_seq.get(&conn_id).copied().unwrap_or(0);
    let Some((seq, response)) = framing.receive(conn_id, sent, datagram)? else {
        return Ok(Parsed::Absorbed(datagram.len()));
    };
    let len = response_len(&response)?;
    anyhow::ensure!(
        len == Some(response.len()),
        "Memcached UDP response of {} bytes is not one complete response",
        response.len()
    );
    Ok(Parsed::ok(datagram.len(), (conn_id, seq)))
}
