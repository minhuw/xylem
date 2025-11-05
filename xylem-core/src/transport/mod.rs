//! Transport layer implementations

use crate::{request::Timestamp, Result};
use async_trait::async_trait;
use std::net::SocketAddr;

/// Transport protocol trait
#[async_trait]
pub trait Transport: Send + Sync {
    /// Connect to a target
    async fn connect(&mut self, target: &SocketAddr) -> Result<()>;

    /// Send data and return timestamp
    async fn send(&mut self, data: &[u8]) -> Result<Timestamp>;

    /// Receive data and return timestamp
    async fn recv(&mut self) -> Result<(Vec<u8>, Timestamp)>;

    /// Close the connection
    async fn close(&mut self) -> Result<()>;
}

pub mod tcp;
pub mod tls;
pub mod udp;
