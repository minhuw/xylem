//! Masstree protocol implementation

use crate::Protocol;
use anyhow::Result;

pub struct MasstreeProtocol {
    // TODO: Add fields
}

impl MasstreeProtocol {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for MasstreeProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl Protocol for MasstreeProtocol {
    type RequestId = (usize, u64);

    fn generate_request(
        &mut self,
        conn_id: usize,
        _key: u64,
        _value_size: usize,
    ) -> (Vec<u8>, Self::RequestId) {
        // TODO: Implement
        (Vec::new(), (conn_id, 0))
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)> {
        // TODO: Implement
        Ok((data.len(), Some((conn_id, 0))))
    }

    fn name(&self) -> &'static str {
        "masstree"
    }

    fn reset(&mut self) {
        // TODO: Implement
    }
}
