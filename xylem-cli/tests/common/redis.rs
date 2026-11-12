//! Redis test fixture
//!
//! Each guard starts its own compose project and tears it down on drop; the
//! server is reached at its container's address.

use super::compose::{answers_at, ComposeSpec, ComposeStack};
use std::net::SocketAddr;

/// Guard for Redis in a compose project of its own, torn down on drop
pub struct RedisGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    addr: SocketAddr,
}

impl RedisGuard {
    /// Start Redis and wait until it answers
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Redis",
            compose_file: "tests/redis/docker-compose.test.yml",
            image: None,
            is_ready: |stack| {
                stack
                    .container_addr("redis", 6379)
                    .is_some_and(|addr| answers_at(addr, b"PING\r\n", b"+PONG"))
            },
        })?;
        let addr = stack.container_addr("redis", 6379).ok_or("Redis has no container address")?;
        Ok(Self { _stack: stack, addr })
    }

    /// Address of the TCP service
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}
