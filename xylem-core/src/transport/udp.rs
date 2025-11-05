//! UDP transport implementation

use super::Transport;
use crate::{request::Timestamp, Error, Result};
use async_trait::async_trait;
use std::net::SocketAddr;

pub struct UdpTransport {
    _target: Option<SocketAddr>,
}

impl UdpTransport {
    pub fn new() -> Self {
        Self { _target: None }
    }
}

impl Default for UdpTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for UdpTransport {
    async fn connect(&mut self, target: &SocketAddr) -> Result<()> {
        self._target = Some(*target);
        // TODO: Implement UDP socket setup
        Ok(())
    }

    async fn send(&mut self, _data: &[u8]) -> Result<Timestamp> {
        if self._target.is_none() {
            return Err(Error::Connection("Not connected".to_string()));
        }
        // TODO: Implement send
        Ok(Timestamp::now())
    }

    async fn recv(&mut self) -> Result<(Vec<u8>, Timestamp)> {
        // TODO: Implement recv
        Ok((Vec::new(), Timestamp::now()))
    }

    async fn close(&mut self) -> Result<()> {
        self._target = None;
        Ok(())
    }
}
