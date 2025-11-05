//! TCP transport implementation

use super::Transport;
use crate::{request::Timestamp, Error, Result};
use async_trait::async_trait;
use std::net::SocketAddr;
use tokio::net::TcpStream;

pub struct TcpTransport {
    stream: Option<TcpStream>,
}

impl TcpTransport {
    pub fn new() -> Self {
        Self { stream: None }
    }
}

impl Default for TcpTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn connect(&mut self, target: &SocketAddr) -> Result<()> {
        let stream = TcpStream::connect(target).await?;
        stream.set_nodelay(true)?;
        self.stream = Some(stream);
        Ok(())
    }

    async fn send(&mut self, _data: &[u8]) -> Result<Timestamp> {
        if self.stream.is_none() {
            return Err(Error::Connection("Not connected".to_string()));
        }
        // TODO: Implement send
        Ok(Timestamp::now())
    }

    async fn recv(&mut self) -> Result<(Vec<u8>, Timestamp)> {
        if self.stream.is_none() {
            return Err(Error::Connection("Not connected".to_string()));
        }
        // TODO: Implement recv
        Ok((Vec::new(), Timestamp::now()))
    }

    async fn close(&mut self) -> Result<()> {
        self.stream = None;
        Ok(())
    }
}
