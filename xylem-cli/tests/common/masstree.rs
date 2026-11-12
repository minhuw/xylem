//! Masstree test fixture
//!
//! Each guard starts its own compose project and tears it down on drop; the
//! server is reached at its container's address.

use super::compose::{ComposeSpec, ComposeStack, ImageBuild};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

/// Guard for Masstree in a compose project of its own, torn down on drop
pub struct MasstreeGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    addr: SocketAddr,
    address: String,
}

impl MasstreeGuard {
    /// Start Masstree and wait until it answers
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Masstree",
            compose_file: "tests/masstree/docker-compose.test.yml",
            image: Some(ImageBuild {
                tag: "xylem-test-masstree:latest",
                context: "tests/masstree",
            }),
            is_ready: |stack| {
                stack.container_addr("masstree", 2117).is_some_and(|addr| {
                    TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok()
                })
            },
        })?;
        let addr = stack
            .container_addr("masstree", 2117)
            .ok_or("Masstree has no container address")?;
        Ok(Self {
            _stack: stack,
            addr,
            address: addr.to_string(),
        })
    }

    /// Address of the TCP service
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// [`Self::addr`] as a string
    pub fn get_addr(&self) -> &str {
        &self.address
    }
}
