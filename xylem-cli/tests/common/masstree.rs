//! Masstree test fixture
//!
//! Each guard starts its own compose project on host ports Docker picks and
//! tears it down on drop.

use super::compose::{addr, ComposeSpec, ComposeStack};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

/// Guard for Masstree in a compose project of its own, torn down on drop
pub struct MasstreeGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    port: u16,
    address: String,
}

impl MasstreeGuard {
    /// Start Masstree and wait until it answers
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Masstree",
            compose_file: "tests/masstree/docker-compose.test.yml",
            build_images: true,
            is_ready: |stack| {
                stack.published_port("masstree", 2117, "tcp").is_some_and(|port| {
                    TcpStream::connect_timeout(&addr(port), Duration::from_millis(500)).is_ok()
                })
            },
        })?;
        let port = stack
            .published_port("masstree", 2117, "tcp")
            .ok_or("Masstree port not published")?;
        Ok(Self {
            _stack: stack,
            port,
            address: addr(port).to_string(),
        })
    }

    /// Host port Docker published for the TCP service
    pub fn get_port(&self) -> u16 {
        self.port
    }

    /// `127.0.0.1:<get_port()>`
    pub fn addr(&self) -> SocketAddr {
        addr(self.port)
    }

    /// [`Self::addr`] as a string
    pub fn get_addr(&self) -> &str {
        &self.address
    }
}
