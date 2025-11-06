//! Synthetic protocol implementation

use crate::Protocol;
use anyhow::Result;

pub struct SyntheticProtocol {
    service_time_ns: u64,
    /// Per-connection sequence numbers for send
    conn_send_seq: std::collections::HashMap<usize, u64>,
    /// Per-connection sequence numbers for receive
    conn_recv_seq: std::collections::HashMap<usize, u64>,
}

impl SyntheticProtocol {
    pub fn new(service_time_ns: u64) -> Self {
        Self {
            service_time_ns,
            conn_send_seq: std::collections::HashMap::new(),
            conn_recv_seq: std::collections::HashMap::new(),
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

impl Protocol for SyntheticProtocol {
    type RequestId = (usize, u64);

    fn generate_request(
        &mut self,
        conn_id: usize,
        _key: u64,
        _value_size: usize,
    ) -> (Vec<u8>, Self::RequestId) {
        let seq = self.next_send_seq(conn_id);
        (format!("{}\n", self.service_time_ns).into_bytes(), (conn_id, seq))
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)> {
        if !data.is_empty() {
            let seq = self.next_recv_seq(conn_id);
            Ok((data.len(), Some((conn_id, seq))))
        } else {
            Ok((0, None))
        }
    }

    fn name(&self) -> &'static str {
        "synthetic"
    }

    fn reset(&mut self) {
        self.conn_send_seq.clear();
        self.conn_recv_seq.clear();
    }
}
