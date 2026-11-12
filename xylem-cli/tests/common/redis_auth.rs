//! Password-protected Redis test fixture
//!
//! Each guard starts its own compose project and tears it down on drop; the
//! server is reached at its container's address.

use super::compose::{answers_at, ComposeSpec, ComposeStack};
use std::net::SocketAddr;

/// Guard for password-protected Redis in a compose project of its own, torn down on drop
pub struct RedisAuthGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    addr: SocketAddr,
}

impl RedisAuthGuard {
    /// Start password-protected Redis and wait until it answers
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Redis (password)",
            compose_file: "tests/redis-auth/docker-compose.test.yml",
            image: None,
            is_ready: |stack| {
                stack.container_addr("redis-auth", 6379).is_some_and(|addr| {
                    answers_at(addr, b"AUTH testpassword\r\nPING\r\n", b"+PONG")
                })
            },
        })?;
        let addr = stack
            .container_addr("redis-auth", 6379)
            .ok_or("Redis has no container address")?;
        Ok(Self { _stack: stack, addr })
    }

    /// Address of the TCP service
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Password for authentication
    pub fn get_password(&self) -> &str {
        "testpassword"
    }
}
