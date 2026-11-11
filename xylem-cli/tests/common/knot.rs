//! Knot DNS test fixture
//!
//! Each guard starts its own compose project on host ports Docker picks and
//! tears it down on drop. Knot serves the zones of `tests/knot` on TCP and UDP.

use super::compose::{answers, ComposeSpec, ComposeStack};

/// A query for www.example.test A with its 2-byte TCP length
const WWW_QUERY: &[u8] = b"\x00\x22\x12\x34\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\
\x03www\x07example\x04test\x00\x00\x01\x00\x01";

/// The address of www.example.test, as its A record carries it
const WWW_ADDRESS: &[u8] = &[192, 0, 2, 80];

/// Guard for Knot DNS in a compose project of its own, torn down on drop
pub struct KnotGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    port: u16,
    udp_port: u16,
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
                    .published_port("knot", 53, "tcp")
                    .is_some_and(|port| answers(port, WWW_QUERY, WWW_ADDRESS))
                    && stack.published_port("knot", 53, "udp").is_some()
            },
        })?;
        let port = stack.published_port("knot", 53, "tcp").ok_or("Knot TCP port not published")?;
        let udp_port =
            stack.published_port("knot", 53, "udp").ok_or("Knot UDP port not published")?;
        Ok(Self { _stack: stack, port, udp_port })
    }

    /// Host port Docker published for DNS over TCP
    pub fn get_port(&self) -> u16 {
        self.port
    }

    /// Host port Docker published for DNS over UDP
    pub fn get_udp_port(&self) -> u16 {
        self.udp_port
    }
}
