//! UDP transport implementation (stub - TODO)

use super::Transport;
use crate::{Error, Result, Timestamp};
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

impl Transport for UdpTransport {
    fn connect(&mut self, target: &SocketAddr) -> Result<()> {
        self._target = Some(*target);
        // TODO: Implement UDP socket setup with mio
        Err(Error::Other("UDP transport not yet implemented".to_string()))
    }

    fn send(&mut self, _data: &[u8]) -> Result<Timestamp> {
        Err(Error::Other("UDP transport not yet implemented".to_string()))
    }

    fn recv(&mut self) -> Result<(Vec<u8>, Timestamp)> {
        Err(Error::Other("UDP transport not yet implemented".to_string()))
    }

    fn poll_readable(&mut self) -> Result<bool> {
        Ok(false)
    }

    fn close(&mut self) -> Result<()> {
        self._target = None;
        Ok(())
    }
}
