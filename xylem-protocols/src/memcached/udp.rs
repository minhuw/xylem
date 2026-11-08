//! Memcached UDP framing
//!
//! Over UDP, memcached prefixes every datagram with the 8-byte frame header
//! of protocol.txt ("UDP protocol"), all fields big-endian u16:
//!
//! ```text
//! [request id][sequence number][total datagrams][reserved = 0]
//! ```
//!
//! A request is one datagram (memcached rejects multi-datagram requests); its
//! request id is the low 16 bits of the connection's request sequence number.
//! A response may span several datagrams, at most 1400 bytes each, which share
//! the request id and are numbered `0..total`. [`UdpFraming`] reassembles them
//! per connection in sequence-number order, whatever order they arrive in.

use anyhow::{ensure, Result};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

/// Size of the frame header
pub const HEADER_LEN: usize = 8;

/// Largest value a memcached request over UDP may carry
///
/// The whole request must fit one IPv4 UDP datagram (65,507 bytes), with
/// the frame header and up to 512 bytes of command line.
pub const MAX_UDP_VALUE_SIZE: usize = 65_507 - HEADER_LEN - 512;

/// Most incomplete responses kept per connection; the oldest is dropped
/// first, since a response that lost a datagram never completes
const MAX_PARTIAL: usize = 256;

/// Most completed responses remembered per connection to drop their duplicates
const COMPLETED_MEMORY: usize = 1024;

/// Prefix `body` with the frame header of a single-datagram request
pub fn frame_request(request_id: u16, body: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HEADER_LEN + body.len());
    buf.extend_from_slice(&request_id.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes()); // sequence number
    buf.extend_from_slice(&1u16.to_be_bytes()); // total datagrams
    buf.extend_from_slice(&0u16.to_be_bytes()); // reserved
    buf.extend_from_slice(body);
    buf
}

/// Reassembly state of every connection
#[derive(Debug, Default)]
pub struct UdpFraming {
    connections: HashMap<usize, ConnectionFrames>,
}

#[derive(Debug, Default)]
struct ConnectionFrames {
    /// Incomplete responses by request sequence number
    partial: BTreeMap<u64, Partial>,
    /// Recently completed request sequence numbers, and their order
    completed: HashSet<u64>,
    completed_order: VecDeque<u64>,
}

#[derive(Debug)]
struct Partial {
    /// Datagram payloads by sequence number
    parts: Vec<Option<Vec<u8>>>,
    /// Number of `Some` entries in `parts`
    received: usize,
}

impl ConnectionFrames {
    fn mark_completed(&mut self, seq: u64) {
        if self.completed_order.len() == COMPLETED_MEMORY {
            if let Some(oldest) = self.completed_order.pop_front() {
                self.completed.remove(&oldest);
            }
        }
        self.completed_order.push_back(seq);
        self.completed.insert(seq);
    }
}

/// Request sequence number of the request with frame id `request_id`, given
/// that `sent` requests were sent on the connection; `None` if none had it
///
/// It is the latest sent request whose low 16 bits are `request_id`.
fn request_seq(request_id: u16, sent: u64) -> Option<u64> {
    let last = sent.checked_sub(1)?;
    let back = (last as u16).wrapping_sub(request_id) as u64;
    last.checked_sub(back)
}

impl UdpFraming {
    /// Take in one response datagram on `conn_id`, after `sent` requests
    ///
    /// Returns the request sequence number and the reassembled response once
    /// its last datagram arrives, and `None` for a datagram that completes
    /// nothing: a fragment, a duplicate, or one for an unknown request id.
    /// A malformed frame header is an error.
    pub fn receive<'a>(
        &mut self,
        conn_id: usize,
        sent: u64,
        datagram: &'a [u8],
    ) -> Result<Option<(u64, Cow<'a, [u8]>)>> {
        ensure!(
            datagram.len() >= HEADER_LEN,
            "Memcached UDP datagram of {} bytes has no frame header",
            datagram.len()
        );
        let field = |i: usize| u16::from_be_bytes([datagram[2 * i], datagram[2 * i + 1]]);
        let (request_id, number, total) = (field(0), field(1) as usize, field(2) as usize);
        ensure!(
            number < total,
            "Invalid memcached UDP frame header: datagram {number} of {total}"
        );
        let payload = &datagram[HEADER_LEN..];

        let Some(seq) = request_seq(request_id, sent) else {
            return Ok(None);
        };
        let frames = self.connections.entry(conn_id).or_default();
        if frames.completed.contains(&seq) {
            return Ok(None);
        }
        if total == 1 {
            frames.mark_completed(seq);
            return Ok(Some((seq, Cow::Borrowed(payload))));
        }

        let partial = frames
            .partial
            .entry(seq)
            .or_insert_with(|| Partial { parts: vec![None; total], received: 0 });
        ensure!(
            partial.parts.len() == total,
            "Memcached UDP response {request_id} changed its datagram count from {} to {total}",
            partial.parts.len()
        );
        if partial.parts[number].is_some() {
            return Ok(None);
        }
        partial.parts[number] = Some(payload.to_vec());
        partial.received += 1;
        if partial.received < total {
            if frames.partial.len() > MAX_PARTIAL {
                frames.partial.pop_first();
            }
            return Ok(None);
        }

        let partial = frames.partial.remove(&seq).expect("the partial was just updated");
        frames.mark_completed(seq);
        let response = partial.parts.into_iter().flatten().flatten().collect();
        Ok(Some((seq, Cow::Owned(response))))
    }

    /// Forget every connection's state
    pub fn reset(&mut self) {
        self.connections.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn datagram(request_id: u16, number: u16, total: u16, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        for field in [request_id, number, total, 0] {
            buf.extend_from_slice(&field.to_be_bytes());
        }
        buf.extend_from_slice(payload);
        buf
    }

    fn completed(result: Option<(u64, Cow<'_, [u8]>)>) -> Option<(u64, Vec<u8>)> {
        result.map(|(seq, payload)| (seq, payload.into_owned()))
    }

    #[test]
    fn request_frame_header_matches_protocol_txt() {
        let frame = frame_request(0x1234, b"get k\r\n");
        assert_eq!(&frame[..HEADER_LEN], &[0x12, 0x34, 0, 0, 0, 1, 0, 0]);
        assert_eq!(&frame[HEADER_LEN..], b"get k\r\n");
    }

    #[test]
    fn request_ids_map_back_to_the_latest_sequence_number() {
        assert_eq!(request_seq(0, 0), None);
        assert_eq!(request_seq(0, 1), Some(0));
        assert_eq!(request_seq(1, 1), None, "not sent yet");
        assert_eq!(request_seq(3, 10), Some(3));
        // Across the 16-bit wrap: ids 65535 and 0 are requests 65535 and 65536
        assert_eq!(request_seq(0xffff, 65_537), Some(65_535));
        assert_eq!(request_seq(0, 65_537), Some(65_536));
        assert_eq!(request_seq(1, 65_537), Some(1));
    }

    #[test]
    fn single_datagram_response_completes_at_once() {
        let mut framing = UdpFraming::default();
        let data = datagram(2, 0, 1, b"END\r\n");
        let result = framing.receive(0, 3, &data).unwrap();
        assert!(matches!(result, Some((2, Cow::Borrowed(b"END\r\n")))));
        // A duplicate is dropped
        assert!(framing.receive(0, 3, &data).unwrap().is_none());
    }

    #[test]
    fn multi_datagram_response_reassembles_out_of_order_and_drops_duplicates() {
        let mut framing = UdpFraming::default();
        let parts = [b"VALUE".as_slice(), b" k 0 1\r\n", b"x\r\nEND\r\n"];
        let receive = |framing: &mut UdpFraming, number: u16| {
            let data = datagram(7, number, 3, parts[number as usize]);
            completed(framing.receive(4, 8, &data).unwrap())
        };
        assert_eq!(receive(&mut framing, 2), None);
        assert_eq!(receive(&mut framing, 0), None);
        assert_eq!(receive(&mut framing, 0), None, "duplicate fragment");
        assert_eq!(receive(&mut framing, 1), Some((7, parts.concat())));
        // Every later copy of a completed response is dropped
        for number in 0..3 {
            assert_eq!(receive(&mut framing, number), None);
        }
        assert!(framing.connections[&4].partial.is_empty());
    }

    #[test]
    fn interleaved_responses_and_connections_stay_apart() {
        let mut framing = UdpFraming::default();
        let a0 = datagram(0, 0, 2, b"a0");
        let b0 = datagram(1, 0, 2, b"b0");
        let a1 = datagram(0, 1, 2, b"a1");
        let b1 = datagram(1, 1, 2, b"b1");
        assert!(framing.receive(0, 2, &a0).unwrap().is_none());
        assert!(framing.receive(0, 2, &b0).unwrap().is_none());
        // The same id on another connection is another response
        assert!(framing.receive(1, 2, &a1).unwrap().is_none());
        assert_eq!(completed(framing.receive(0, 2, &b1).unwrap()), Some((1, b"b0b1".to_vec())));
        assert_eq!(completed(framing.receive(0, 2, &a1).unwrap()), Some((0, b"a0a1".to_vec())));
    }

    #[test]
    fn unknown_request_ids_are_dropped() {
        let mut framing = UdpFraming::default();
        // Nothing sent yet, or an id ahead of every sent request
        assert!(framing.receive(0, 0, &datagram(0, 0, 1, b"END\r\n")).unwrap().is_none());
        assert!(framing.receive(0, 5, &datagram(9, 0, 1, b"END\r\n")).unwrap().is_none());
        assert!(framing.connections.get(&0).is_none_or(|c| c.completed.is_empty()));
    }

    #[test]
    fn malformed_headers_are_errors() {
        let mut framing = UdpFraming::default();
        assert!(framing.receive(0, 1, &[0; 7]).is_err());
        assert!(framing.receive(0, 1, &datagram(0, 0, 0, b"")).is_err());
        assert!(framing.receive(0, 1, &datagram(0, 2, 2, b"")).is_err());
        assert!(framing.receive(0, 1, &datagram(0, 0, 2, b"x")).unwrap().is_none());
        let error = framing.receive(0, 1, &datagram(0, 1, 3, b"y")).unwrap_err();
        assert!(error.to_string().contains("changed its datagram count"), "{error}");
    }

    #[test]
    fn incomplete_responses_are_bounded() {
        let mut framing = UdpFraming::default();
        let sent = MAX_PARTIAL as u64 + 10;
        for id in 0..sent as u16 {
            assert!(framing.receive(0, sent, &datagram(id, 0, 2, b"x")).unwrap().is_none());
        }
        let partial = &framing.connections[&0].partial;
        assert_eq!(partial.len(), MAX_PARTIAL);
        assert_eq!(partial.keys().next(), Some(&10), "the oldest are dropped");
        // A dropped response's last datagram restarts it instead of completing it
        assert!(framing.receive(0, sent, &datagram(0, 1, 2, b"y")).unwrap().is_none());
        let last = sent as u16 - 1;
        assert_eq!(
            completed(framing.receive(0, sent, &datagram(last, 1, 2, b"y")).unwrap()),
            Some((sent - 1, b"xy".to_vec()))
        );
    }

    #[test]
    fn completed_memory_is_bounded() {
        let mut frames = ConnectionFrames::default();
        for seq in 0..COMPLETED_MEMORY as u64 + 1 {
            frames.mark_completed(seq);
        }
        assert_eq!(frames.completed.len(), COMPLETED_MEMORY);
        assert!(!frames.completed.contains(&0));
    }

    mod codecs {
        use super::datagram;
        use crate::memcached::{MemcachedAsciiProtocol, MemcachedBinaryProtocol, MemcachedOp};
        use crate::workload::KeyGeneration;
        use crate::{Parsed, Protocol};

        fn ascii(op: MemcachedOp) -> MemcachedAsciiProtocol {
            MemcachedAsciiProtocol::with_workload(op, KeyGeneration::sequential(0), 4)
                .with_udp_framing()
        }

        #[test]
        fn ascii_requests_carry_their_sequence_number_as_request_id() {
            let mut protocol = ascii(MemcachedOp::Get);
            for seq in 0..3u64 {
                let request = protocol.next_request(5);
                assert_eq!(request.request_id, (5, seq));
                let mut expected = datagram(seq as u16, 0, 1, b"get key:");
                expected.extend_from_slice(format!("{seq}\r\n").as_bytes());
                assert_eq!(request.data, expected);
            }
            let mut protocol = ascii(MemcachedOp::Set);
            let request = protocol.next_request(0);
            assert_eq!(&request.data[..8], &[0, 0, 0, 0, 0, 1, 0, 0]);
            assert_eq!(&request.data[8..], b"set key:0 0 0 4\r\nxxxx\r\n");
        }

        #[test]
        fn ascii_responses_match_by_request_id_in_any_order() {
            let mut protocol = ascii(MemcachedOp::Get);
            for _ in 0..3 {
                protocol.next_request(0);
            }
            // Response 2 overtakes 1, which spans three datagrams sent out of order
            let r2 = datagram(2, 0, 1, b"END\r\n");
            assert_eq!(protocol.parse_response(0, &r2).unwrap(), Parsed::ok(r2.len(), (0, 2)));
            let value = b"VALUE key:1 0 4\r\nxxxx\r\nEND\r\n";
            let parts = [&value[..6], &value[6..20], &value[20..]];
            for number in [1, 2] {
                let data = datagram(1, number, 3, parts[number as usize]);
                assert_eq!(
                    protocol.parse_response(0, &data).unwrap(),
                    Parsed::Absorbed(data.len())
                );
            }
            let data = datagram(1, 0, 3, parts[0]);
            assert_eq!(protocol.parse_response(0, &data).unwrap(), Parsed::ok(data.len(), (0, 1)));
            // Duplicates and unknown ids are absorbed without a request ID
            assert_eq!(protocol.parse_response(0, &r2).unwrap(), Parsed::Absorbed(r2.len()));
            let unknown = datagram(9, 0, 1, b"END\r\n");
            assert_eq!(
                protocol.parse_response(0, &unknown).unwrap(),
                Parsed::Absorbed(unknown.len())
            );
            let stored = datagram(0, 0, 1, b"STORED\r\n");
            assert_eq!(
                protocol.parse_response(0, &stored).unwrap(),
                Parsed::ok(stored.len(), (0, 0))
            );
        }

        #[test]
        fn ascii_reassembled_response_must_be_complete_and_valid() {
            let mut protocol = ascii(MemcachedOp::Get);
            protocol.next_request(0);
            protocol.next_request(0);
            let truncated = datagram(0, 0, 1, b"VALUE key:0 0 4\r\nxx");
            assert!(protocol.parse_response(0, &truncated).is_err());
            let error = datagram(1, 0, 1, b"SERVER_ERROR out of memory\r\n");
            let message = protocol.parse_response(0, &error).unwrap_err().to_string();
            assert!(message.contains("SERVER_ERROR"), "{message}");
            protocol.reset();
            assert_eq!(protocol.next_request(0).request_id, (0, 0));
        }

        #[test]
        fn binary_requests_and_responses_use_the_frame_header() {
            let mut protocol = MemcachedBinaryProtocol::with_workload(
                MemcachedOp::Get,
                KeyGeneration::sequential(0),
                4,
            )
            .with_udp_framing();
            let request = protocol.next_request(0);
            assert_eq!(&request.data[..8], &[0, 0, 0, 0, 0, 1, 0, 0]);
            assert_eq!(request.data[8], 0x80, "binary request magic follows the header");
            protocol.next_request(0);

            // A GETK hit for request 1, split over two datagrams
            let mut response = vec![0x81, 0x0c, 0, 5, 4, 0, 0, 0, 0, 0, 0, 13];
            response.extend_from_slice(&[0; 12]);
            response.extend_from_slice(&[0; 4]); // flags
            response.extend_from_slice(b"key:1abcd");
            let first = datagram(1, 0, 2, &response[..20]);
            let second = datagram(1, 1, 2, &response[20..]);
            assert_eq!(
                protocol.parse_response(0, &second).unwrap(),
                Parsed::Absorbed(second.len())
            );
            assert_eq!(
                protocol.parse_response(0, &first).unwrap(),
                Parsed::ok(first.len(), (0, 1))
            );
            // A response shorter than its header's body length is an error
            let short = datagram(0, 0, 1, &response[..30]);
            assert!(protocol.parse_response(0, &short).is_err());
        }
    }
}
