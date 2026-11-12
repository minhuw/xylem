//! Nginx test fixture
//!
//! Each guard starts its own compose project and tears it down on drop; the
//! server is reached at its container's address.

use super::compose::{answers_at, ComposeSpec, ComposeStack};
use std::net::SocketAddr;
/// Guard for Nginx in a compose project of its own, torn down on drop
pub struct NginxGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    addr: SocketAddr,
}

impl NginxGuard {
    /// Start Nginx and wait until it answers
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Nginx",
            compose_file: "tests/nginx/docker-compose.test.yml",
            image: None,
            is_ready: |stack| stack.container_addr("nginx", 80).is_some_and(Self::is_nginx_ready),
        })?;
        let addr = stack.container_addr("nginx", 80).ok_or("Nginx has no container address")?;
        Ok(Self { _stack: stack, addr })
    }

    /// Whether Nginx on `addr` answers a GET with 200
    pub(super) fn is_nginx_ready(addr: SocketAddr) -> bool {
        answers_at(addr, b"GET / HTTP/1.0\r\n\r\n", b"200")
    }

    /// Address of the HTTP service
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}
