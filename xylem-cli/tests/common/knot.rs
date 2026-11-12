//! Knot DNS test fixture
//!
//! Each guard starts its own compose project and tears it down on drop; the
//! server is reached at its container's address. Knot serves the zones of
//! `tests/knot` on TCP and UDP.

use super::compose::{answers_at, ComposeSpec, ComposeStack};
use std::net::SocketAddr;

/// A query for www.example.test A with its 2-byte TCP length
const WWW_QUERY: &[u8] = b"\x00\x22\x12\x34\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\
\x03www\x07example\x04test\x00\x00\x01\x00\x01";

/// The address of www.example.test, as its A record carries it
const WWW_ADDRESS: &[u8] = &[192, 0, 2, 80];

/// Guard for Knot DNS in a compose project of its own, torn down on drop
pub struct KnotGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    addr: SocketAddr,
}

impl KnotGuard {
    /// Start Knot and wait until it answers over TCP
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Knot DNS",
            compose_file: "tests/knot/docker-compose.test.yml",
            build_images: false,
            is_ready: |stack| {
                stack
                    .container_addr("knot", 53)
                    .is_some_and(|addr| answers_at(addr, WWW_QUERY, WWW_ADDRESS))
            },
        })?;
        let addr = stack.container_addr("knot", 53).ok_or("Knot has no container address")?;
        Ok(Self { _stack: stack, addr })
    }

    /// Address of DNS over TCP, and over UDP on the same port
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}
