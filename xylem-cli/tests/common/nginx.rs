//! Nginx test fixture
//!
//! Each guard starts its own compose project on host ports Docker picks and
//! tears it down on drop.

use super::compose::{addr, answers, ComposeSpec, ComposeStack};
use std::net::SocketAddr;
/// Guard for Nginx in a compose project of its own, torn down on drop
pub struct NginxGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    port: u16,
}

impl NginxGuard {
    /// Start Nginx and wait until it answers
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Nginx",
            compose_file: "tests/nginx/docker-compose.test.yml",
            build_images: false,
            is_ready: |stack| {
                stack.published_port("nginx", 80, "tcp").is_some_and(Self::is_nginx_ready)
            },
        })?;
        let port = stack
            .published_port("nginx", 80, "tcp")
            .ok_or("Nginx host port not published")?;
        Ok(Self { _stack: stack, port })
    }

    /// Whether Nginx on `127.0.0.1:<port>` answers a GET with 200
    pub(super) fn is_nginx_ready(port: u16) -> bool {
        answers(port, b"GET / HTTP/1.0\r\n\r\n", b"200")
    }

    /// Host port Docker published for the TCP service
    pub fn get_port(&self) -> u16 {
        self.port
    }

    /// `127.0.0.1:<get_port()>`
    pub fn addr(&self) -> SocketAddr {
        addr(self.port)
    }
}
