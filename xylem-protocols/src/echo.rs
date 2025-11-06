//! Echo protocol implementation

use crate::Protocol;
use anyhow::Result;

pub struct EchoProtocol {
    payload_size: usize,
    /// Per-connection sequence numbers
    conn_seq: std::collections::HashMap<usize, (u64, u64)>, // (send_seq, recv_seq)
}

impl EchoProtocol {
    pub fn new(payload_size: usize) -> Self {
        Self {
            payload_size,
            conn_seq: std::collections::HashMap::new(),
        }
    }

    fn next_send_seq(&mut self, conn_id: usize) -> u64 {
        let (send_seq, _recv_seq) = self.conn_seq.entry(conn_id).or_insert((0, 0));
        let result = *send_seq;
        *send_seq += 1;
        result
    }

    fn next_recv_seq(&mut self, conn_id: usize) -> u64 {
        let (_send_seq, recv_seq) = self.conn_seq.entry(conn_id).or_insert((0, 0));
        let result = *recv_seq;
        *recv_seq += 1;
        result
    }
}

impl Protocol for EchoProtocol {
    type RequestId = (usize, u64); // (conn_id, sequence)

    fn generate_request(
        &mut self,
        conn_id: usize,
        _key: u64,
        _value_size: usize,
    ) -> (Vec<u8>, Self::RequestId) {
        let seq = self.next_send_seq(conn_id);
        (vec![0u8; self.payload_size], (conn_id, seq))
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)> {
        if data.len() >= self.payload_size {
            // Echo protocol doesn't embed IDs, use FIFO per connection
            let seq = self.next_recv_seq(conn_id);
            Ok((self.payload_size, Some((conn_id, seq))))
        } else {
            // Incomplete response
            Ok((0, None))
        }
    }

    fn name(&self) -> &'static str {
        "echo"
    }

    fn reset(&mut self) {
        self.conn_seq.clear();
    }
}
