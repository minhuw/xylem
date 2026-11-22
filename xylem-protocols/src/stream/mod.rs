//! Bulk byte stream ("sink") protocol (`protocol = "stream"`).
//!
//! Supports two unidirectional bulk-transfer modes over stream transports:
//! - [`StreamDirection::Send`]: client generates deterministic one-way byte
//!   chunks (no server responses expected) so the server can sink and
//!   optionally verify every byte from stream offset 0.
//! - [`StreamDirection::Recv`]: client receives an unsolicited server byte
//!   stream and optionally verifies every byte from stream offset 0 using the
//!   same deterministic byte pattern, reporting any mismatch via
//!   [`Protocol::verification_report`].
//!
//! ## Deterministic Byte Pattern
//!
//! Every 8-byte word `w` covering stream byte offsets `[w*8 .. w*8+8)` in
//! little-endian order is:
//! ```text
//! splitmix64(seed ^ (w * 0x9e3779b97f4a7c15))
//! ```
//! Wire stream offsets advance monotonically from byte 0 across warm-up and
//! measurement phases so the peer never needs phase synchronization to verify
//! the stream.

use crate::configs::{StreamConfig, StreamDirection};
use crate::{ErrorReply, Parsed, Protocol, Request};
use anyhow::Result;
use std::collections::HashMap;
use xylem_common::{VerificationMismatch, VerificationReport};

/// SplitMix64 mixing step used for deterministic stream payload generation.
#[inline]
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Deterministic 64-bit word `word_index` covering stream bytes
/// `[word_index * 8 .. word_index * 8 + 8)` in little-endian byte order.
#[inline]
pub fn stream_pattern_word(seed: u64, word_index: u64) -> u64 {
    splitmix64(seed ^ word_index.wrapping_mul(0x9e37_79b9_7f4a_7c15))
}

/// Fill `out` with the deterministic byte pattern starting at 0-based stream
/// byte offset `start_offset`.
pub fn fill_stream_pattern(seed: u64, start_offset: u64, out: &mut [u8]) {
    if out.is_empty() {
        return;
    }
    let mut pos = 0usize;
    let mut off = start_offset;

    // Unaligned head up to the next 8-byte word boundary
    let rem_head = (off & 7) as usize;
    if rem_head != 0 {
        let word = stream_pattern_word(seed, off >> 3).to_le_bytes();
        let take = (8 - rem_head).min(out.len());
        out[..take].copy_from_slice(&word[rem_head..rem_head + take]);
        pos += take;
        off = off.wrapping_add(take as u64);
    }

    let mut word_idx = off >> 3;

    // 4x-unrolled 32-byte body
    while pos + 32 <= out.len() {
        let w0 = stream_pattern_word(seed, word_idx).to_le_bytes();
        let w1 = stream_pattern_word(seed, word_idx.wrapping_add(1)).to_le_bytes();
        let w2 = stream_pattern_word(seed, word_idx.wrapping_add(2)).to_le_bytes();
        let w3 = stream_pattern_word(seed, word_idx.wrapping_add(3)).to_le_bytes();
        out[pos..pos + 8].copy_from_slice(&w0);
        out[pos + 8..pos + 16].copy_from_slice(&w1);
        out[pos + 16..pos + 24].copy_from_slice(&w2);
        out[pos + 24..pos + 32].copy_from_slice(&w3);
        word_idx = word_idx.wrapping_add(4);
        pos += 32;
    }

    // Remaining full 8-byte words
    while pos + 8 <= out.len() {
        let w = stream_pattern_word(seed, word_idx).to_le_bytes();
        out[pos..pos + 8].copy_from_slice(&w);
        word_idx = word_idx.wrapping_add(1);
        pos += 8;
    }

    // Trailing partial word (< 8 bytes)
    if pos < out.len() {
        let w = stream_pattern_word(seed, word_idx).to_le_bytes();
        let tail = out.len() - pos;
        out[pos..].copy_from_slice(&w[..tail]);
    }
}

/// Find the first mismatched byte within a single 8-byte word.
#[inline]
fn first_word_mismatch(word_start_offset: u64, expected: u64, actual: u64) -> (u64, u8, u8) {
    let exp = expected.to_le_bytes();
    let act = actual.to_le_bytes();
    for i in 0..8 {
        if exp[i] != act[i] {
            return (word_start_offset.wrapping_add(i as u64), exp[i], act[i]);
        }
    }
    unreachable!("first_word_mismatch called with equal words");
}

/// Verify `data` against the deterministic byte pattern starting at 0-based
/// stream byte offset `start_offset` without allocating.
///
/// Returns `Some((stream_byte_offset, expected_byte, actual_byte))` for the
/// first mismatched byte, or `None` if every byte matches.
pub fn check_stream_pattern(seed: u64, start_offset: u64, data: &[u8]) -> Option<(u64, u8, u8)> {
    if data.is_empty() {
        return None;
    }
    let mut pos = 0usize;
    let mut off = start_offset;

    // Unaligned head up to the next 8-byte word boundary
    let rem_head = (off & 7) as usize;
    if rem_head != 0 {
        let word = stream_pattern_word(seed, off >> 3).to_le_bytes();
        let take = (8 - rem_head).min(data.len());
        for i in 0..take {
            let expected = word[rem_head + i];
            let actual = data[i];
            if expected != actual {
                return Some((off.wrapping_add(i as u64), expected, actual));
            }
        }
        pos += take;
        off = off.wrapping_add(take as u64);
    }

    let mut word_idx = off >> 3;

    // 4x-unrolled 32-byte body
    while pos + 32 <= data.len() {
        let e0 = stream_pattern_word(seed, word_idx);
        let e1 = stream_pattern_word(seed, word_idx.wrapping_add(1));
        let e2 = stream_pattern_word(seed, word_idx.wrapping_add(2));
        let e3 = stream_pattern_word(seed, word_idx.wrapping_add(3));
        let a0 = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        let a1 = u64::from_le_bytes(data[pos + 8..pos + 16].try_into().unwrap());
        let a2 = u64::from_le_bytes(data[pos + 16..pos + 24].try_into().unwrap());
        let a3 = u64::from_le_bytes(data[pos + 24..pos + 32].try_into().unwrap());
        if ((e0 ^ a0) | (e1 ^ a1) | (e2 ^ a2) | (e3 ^ a3)) != 0 {
            if e0 != a0 {
                return Some(first_word_mismatch(word_idx << 3, e0, a0));
            }
            if e1 != a1 {
                return Some(first_word_mismatch((word_idx.wrapping_add(1)) << 3, e1, a1));
            }
            if e2 != a2 {
                return Some(first_word_mismatch((word_idx.wrapping_add(2)) << 3, e2, a2));
            }
            return Some(first_word_mismatch((word_idx.wrapping_add(3)) << 3, e3, a3));
        }
        word_idx = word_idx.wrapping_add(4);
        pos += 32;
    }

    // Remaining full 8-byte words
    while pos + 8 <= data.len() {
        let expected = stream_pattern_word(seed, word_idx);
        let actual = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        if expected != actual {
            return Some(first_word_mismatch(word_idx << 3, expected, actual));
        }
        word_idx = word_idx.wrapping_add(1);
        pos += 8;
    }

    // Trailing partial word (< 8 bytes)
    if pos < data.len() {
        let word = stream_pattern_word(seed, word_idx).to_le_bytes();
        let tail = data.len() - pos;
        let base_off = word_idx << 3;
        for i in 0..tail {
            let expected = word[i];
            let actual = data[pos + i];
            if expected != actual {
                return Some((base_off.wrapping_add(i as u64), expected, actual));
            }
        }
    }

    None
}

/// Bulk byte stream protocol implementation.
pub struct StreamProtocol {
    direction: StreamDirection,
    chunk_size: usize,
    byte_limit: Option<u64>,
    seed: u64,
    verify: bool,
    in_measurement: bool,
    measured_bytes: u64,
    send_offsets: HashMap<usize, u64>,
    recv_offsets: HashMap<usize, u64>,
    next_seqs: HashMap<usize, u64>,
    report: VerificationReport,
}

impl StreamProtocol {
    /// Construct a `StreamProtocol` from a validated [`StreamConfig`].
    pub fn from_config(config: &StreamConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            direction: config.direction,
            chunk_size: config.chunk_size,
            byte_limit: config.bytes,
            seed: config.seed,
            verify: config.verify,
            in_measurement: false,
            measured_bytes: 0,
            send_offsets: HashMap::new(),
            recv_offsets: HashMap::new(),
            next_seqs: HashMap::new(),
            report: VerificationReport::default(),
        })
    }

    fn next_seq(&mut self, conn_id: usize) -> u64 {
        let seq = self.next_seqs.entry(conn_id).or_insert(0);
        let current = *seq;
        *seq += 1;
        current
    }
}

impl Protocol for StreamProtocol {
    type RequestId = (usize, u64);

    fn next_request(&mut self, conn_id: usize) -> Request<Self::RequestId> {
        debug_assert_eq!(
            self.direction,
            StreamDirection::Send,
            "next_request called on receive-only StreamProtocol"
        );
        let len = if self.in_measurement {
            match self.byte_limit {
                Some(limit) => {
                    let remaining = limit.saturating_sub(self.measured_bytes);
                    (self.chunk_size as u64).min(remaining) as usize
                }
                None => self.chunk_size,
            }
        } else {
            self.chunk_size
        };

        let offset = {
            let entry = self.send_offsets.entry(conn_id).or_insert(0);
            let off = *entry;
            *entry = off.wrapping_add(len as u64);
            off
        };
        if self.in_measurement {
            self.measured_bytes = self.measured_bytes.saturating_add(len as u64);
        }

        let mut buf = vec![0u8; len];
        fill_stream_pattern(self.seed, offset, &mut buf);
        let seq = self.next_seq(conn_id);
        if self.in_measurement {
            Request::one_way(buf, (conn_id, seq))
        } else {
            Request::one_way_warmup(buf, (conn_id, seq))
        }
    }

    fn parse_response<'a>(
        &mut self,
        conn_id: usize,
        data: &'a [u8],
    ) -> Result<Parsed<'a, Self::RequestId>> {
        if data.is_empty() {
            return Ok(Parsed::Incomplete);
        }
        let offset = {
            let entry = self.recv_offsets.entry(conn_id).or_insert(0);
            let off = *entry;
            *entry = off.wrapping_add(data.len() as u64);
            off
        };
        if self.in_measurement {
            self.measured_bytes = self.measured_bytes.saturating_add(data.len() as u64);
        }

        let seq = self.next_seq(conn_id);
        let error = if self.direction == StreamDirection::Recv && self.verify {
            match check_stream_pattern(self.seed, offset, data) {
                Some((bad_offset, expected, actual)) => {
                    self.report.failures += 1;
                    if self.report.first_mismatch.is_none() {
                        self.report.first_mismatch = Some(VerificationMismatch {
                            kind: "ValueMismatch".to_string(),
                            key: None,
                            offset: bad_offset,
                            expected: Some(expected),
                            actual: Some(actual),
                            expected_len: None,
                            actual_len: None,
                            detail: format!(
                                "stream byte mismatch at offset {bad_offset}: expected 0x{expected:02x}, got 0x{actual:02x}"
                            ),
                        });
                    }
                    Some(ErrorReply::new("ValueMismatch", "Stream byte mismatch"))
                }
                None => {
                    self.report.verified += data.len() as u64;
                    None
                }
            }
        } else {
            None
        };

        Ok(Parsed::Complete {
            consumed: data.len(),
            id: (conn_id, seq),
            error,
        })
    }

    fn can_send(&self, _conn_id: usize) -> bool {
        match self.direction {
            StreamDirection::Send => {
                !(self.in_measurement && self.byte_limit.is_some_and(|l| self.measured_bytes >= l))
            }
            StreamDirection::Recv => false,
        }
    }

    fn accepts_unsolicited_responses(&self) -> bool {
        self.direction == StreamDirection::Recv
    }

    fn is_done(&self) -> bool {
        self.in_measurement && self.byte_limit.is_some_and(|l| self.measured_bytes >= l)
    }

    fn on_measurement_start(&mut self) {
        self.in_measurement = true;
        self.measured_bytes = 0;
    }

    fn verification_report(&self) -> Option<VerificationReport> {
        (self.direction == StreamDirection::Recv && self.verify).then(|| self.report.clone())
    }

    fn name(&self) -> &'static str {
        "stream"
    }

    fn reset(&mut self) {
        self.in_measurement = false;
        self.measured_bytes = 0;
        self.send_offsets.clear();
        self.recv_offsets.clear();
        self.next_seqs.clear();
        self.report = VerificationReport::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_byte(seed: u64, offset: u64) -> u8 {
        stream_pattern_word(seed, offset / 8).to_le_bytes()[(offset % 8) as usize]
    }

    #[test]
    fn pattern_reference_vectors_and_boundaries() {
        // Fixed reference vector for seed = 0, word 0:
        // splitmix64(0) == 0xe220a8397b1dcdaf
        assert_eq!(stream_pattern_word(0, 0), 0xe220_a839_7b1d_cdaf);
        // Fixed reference vector for seed = 0, word 1:
        assert_eq!(stream_pattern_word(0, 1), splitmix64(0x9e37_79b9_7f4a_7c15));

        let seed = 0x1234_5678_9abc_def0;
        for start in [0u64, 1, 3, 7, 8, 13, 31, 32, 33, 63, 64, 65] {
            for len in [0usize, 1, 2, 7, 8, 9, 31, 32, 33, 63, 64, 65, 100, 65536] {
                let mut buf = vec![0xAAu8; len];
                fill_stream_pattern(seed, start, &mut buf);
                for (i, &b) in buf.iter().enumerate() {
                    let expected = reference_byte(seed, start + i as u64);
                    assert_eq!(b, expected, "mismatch at start={start} len={len} i={i}");
                }
                assert_eq!(check_stream_pattern(seed, start, &buf), None);
            }
        }
    }

    #[test]
    fn check_pattern_catches_single_bit_flip_at_exact_offset() {
        let seed = 42;
        for start in [0u64, 3, 7, 13, 32] {
            let mut buf = vec![0u8; 97];
            fill_stream_pattern(seed, start, &mut buf);
            // Flip single bit in head, unrolled 32B body (each of 4 words), 8B loop, and tail
            for flip_idx in [0usize, 1, 4, 5, 12, 20, 28, 36, 44, 52, 60, 72, 85, 95, 96] {
                let expected = buf[flip_idx];
                buf[flip_idx] ^= 0x01;
                let actual = buf[flip_idx];
                let mismatch = check_stream_pattern(seed, start, &buf);
                assert_eq!(
                    mismatch,
                    Some((start + flip_idx as u64, expected, actual)),
                    "start={start} flip_idx={flip_idx}"
                );
                buf[flip_idx] = expected;
            }
        }
    }

    #[test]
    fn send_mode_warmup_continuity_and_exact_byte_quota() {
        let config = StreamConfig {
            direction: StreamDirection::Send,
            chunk_size: 1000,
            bytes: Some(2500),
            seed: 99,
            verify: true,
        };
        let mut proto = StreamProtocol::from_config(&config).unwrap();
        assert!(proto.can_send(0));
        assert!(!proto.is_done());
        assert!(!proto.accepts_unsolicited_responses());
        assert!(proto.verification_report().is_none());

        // Warm-up: stream 3 chunks = 3000 bytes (must not count toward byte_limit)
        let mut wire = Vec::new();
        for _ in 0..3 {
            let req = proto.next_request(0);
            assert!(req.metadata.is_warmup);
            assert!(!req.metadata.expects_response);
            assert_eq!(req.data.len(), 1000);
            wire.extend_from_slice(&req.data);
        }
        assert!(proto.can_send(0));
        assert!(!proto.is_done());

        // Transition to measurement: wire offset continues from 3000, quota is exact 2500
        proto.on_measurement_start();
        let mut measured = 0usize;
        let mut chunks = Vec::new();
        while proto.can_send(0) {
            let req = proto.next_request(0);
            assert!(!req.metadata.is_warmup);
            assert!(!req.metadata.expects_response);
            chunks.push(req.data.len());
            measured += req.data.len();
            wire.extend_from_slice(&req.data);
        }
        assert_eq!(chunks, vec![1000, 1000, 500]);
        assert_eq!(measured, 2500);
        assert!(proto.is_done());
        assert!(!proto.can_send(0));

        // Entire wire stream from offset 0 through warmup + measurement must be continuous
        assert_eq!(wire.len(), 5500);
        assert_eq!(check_stream_pattern(99, 0, &wire), None);
    }

    #[test]
    fn recv_mode_verify_pass_and_corruption_offset() {
        let config = StreamConfig {
            direction: StreamDirection::Recv,
            chunk_size: 4096,
            bytes: Some(5000),
            seed: 7,
            verify: true,
        };
        let mut proto = StreamProtocol::from_config(&config).unwrap();
        assert!(!proto.can_send(0));
        assert!(proto.accepts_unsolicited_responses());
        assert!(!proto.is_done());

        // Generate 8000 bytes of valid wire data
        let mut wire = vec![0u8; 8000];
        fill_stream_pattern(7, 0, &mut wire);

        // Warm-up receives first 3000 bytes (does not trigger is_done)
        let parsed = proto.parse_response(0, &wire[..3000]).unwrap();
        assert!(matches!(parsed, Parsed::Complete { consumed: 3000, error: None, .. }));
        assert!(!proto.is_done());

        // Measurement receives remaining 5000 bytes in two chunks
        proto.on_measurement_start();
        let parsed = proto.parse_response(0, &wire[3000..5500]).unwrap();
        assert!(matches!(parsed, Parsed::Complete { consumed: 2500, error: None, .. }));
        assert!(!proto.is_done());

        let parsed = proto.parse_response(0, &wire[5500..8000]).unwrap();
        assert!(matches!(parsed, Parsed::Complete { consumed: 2500, error: None, .. }));
        assert!(proto.is_done());

        let report = proto.verification_report().unwrap();
        assert_eq!(report.verified, 8000);
        assert_eq!(report.failures, 0);
        assert!(report.first_mismatch.is_none());

        // Now test corruption detection with exact offset
        proto.reset();
        proto.on_measurement_start();
        let mut corrupt = wire[..5000].to_vec();
        let expected_byte = corrupt[1234];
        corrupt[1234] ^= 0x80;
        let actual_byte = corrupt[1234];
        let parsed = proto.parse_response(0, &corrupt[..1000]).unwrap();
        assert!(matches!(parsed, Parsed::Complete { consumed: 1000, error: None, .. }));
        let parsed = proto.parse_response(0, &corrupt[1000..5000]).unwrap();
        match parsed {
            Parsed::Complete { consumed, error: Some(err), .. } => {
                assert_eq!(consumed, 4000);
                assert_eq!(err.kind, "ValueMismatch");
            }
            other => panic!("expected ValueMismatch error, got {other:?}"),
        }
        let report = proto.verification_report().unwrap();
        assert_eq!(report.failures, 1);
        let mismatch = report.first_mismatch.unwrap();
        assert_eq!(mismatch.offset, 1234);
        assert_eq!(mismatch.expected, Some(expected_byte));
        assert_eq!(mismatch.actual, Some(actual_byte));
    }

    #[test]
    fn config_validation_rejects_zero_chunk_or_zero_bytes() {
        let bad_chunk = StreamConfig { chunk_size: 0, ..Default::default() };
        assert!(bad_chunk.validate().is_err());

        let bad_bytes = StreamConfig { bytes: Some(0), ..Default::default() };
        assert!(bad_bytes.validate().is_err());

        assert!(StreamConfig::default().validate().is_ok());
    }
}
