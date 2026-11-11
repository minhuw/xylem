//! Password-protected Redis test fixture
//!
//! Each guard starts its own compose project on host ports Docker picks and
//! tears it down on drop.

use super::compose::{addr, answers, ComposeSpec, ComposeStack};
use std::net::SocketAddr;

/// Guard for password-protected Redis in a compose project of its own, torn down on drop
pub struct RedisAuthGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    port: u16,
}

impl RedisAuthGuard {
    /// Start password-protected Redis and wait until it answers
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Redis (password)",
            compose_file: "tests/redis-auth/docker-compose.test.yml",
            build_images: false,
            is_ready: |stack| {
                stack
                    .published_port("redis-auth", 6379, "tcp")
                    .is_some_and(|port| answers(port, b"AUTH testpassword\r\nPING\r\n", b"+PONG"))
            },
        })?;
        let port = stack
            .published_port("redis-auth", 6379, "tcp")
            .ok_or("Redis host port not published")?;
        Ok(Self { _stack: stack, port })
    }

    /// Host port Docker published for the TCP service
    pub fn get_port(&self) -> u16 {
        self.port
    }

    /// `127.0.0.1:<get_port()>`
    pub fn addr(&self) -> SocketAddr {
        addr(self.port)
    }

    /// Password for authentication
    pub fn get_password(&self) -> &str {
        "testpassword"
    }
}
