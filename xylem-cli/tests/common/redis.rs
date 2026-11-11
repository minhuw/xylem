//! Redis test fixture
//!
//! Each guard starts its own compose project on host ports Docker picks and
//! tears it down on drop.

use super::compose::{addr, answers, ComposeSpec, ComposeStack};
use std::net::SocketAddr;

/// Guard for Redis in a compose project of its own, torn down on drop
pub struct RedisGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    port: u16,
}

impl RedisGuard {
    /// Start Redis and wait until it answers
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Redis",
            compose_file: "tests/redis/docker-compose.test.yml",
            build_images: false,
            is_ready: |stack| {
                stack
                    .published_port("redis", 6379, "tcp")
                    .is_some_and(|port| answers(port, b"PING\r\n", b"+PONG"))
            },
        })?;
        let port = stack
            .published_port("redis", 6379, "tcp")
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
}
