//! Multi-protocol test fixture
//!
//! Each guard starts its own compose project on host ports Docker picks and
//! tears it down on drop.

use super::compose::{addr, answers, ComposeSpec, ComposeStack};
use super::nginx::NginxGuard;
use std::net::SocketAddr;

/// Host ports Docker published for the Redis, Memcached and Nginx services
struct Ports {
    redis: u16,
    memcached: u16,
    http: u16,
}

impl Ports {
    fn published(stack: &ComposeStack) -> Option<Self> {
        Some(Self {
            redis: stack.published_port("redis", 6379, "tcp")?,
            memcached: stack.published_port("memcached", 11211, "tcp")?,
            http: stack.published_port("nginx", 80, "tcp")?,
        })
    }
}

/// Guard for Redis, Memcached and Nginx in a compose project of their own,
/// torn down on drop
pub struct MultiProtocolGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    /// Host ports Docker published for the services
    ports: Ports,
}

impl MultiProtocolGuard {
    /// Start Redis, Memcached and Nginx and wait until all three answer
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Multi-protocol services",
            compose_file: "tests/multi-protocol/docker-compose.test.yml",
            build_images: false,
            is_ready: |stack| {
                Ports::published(stack).is_some_and(|p| {
                    answers(p.redis, b"PING\r\n", b"+PONG")
                        && answers(p.memcached, b"stats\r\n", b"STAT")
                        && NginxGuard::is_nginx_ready(p.http)
                })
            },
        })?;
        let ports = Ports::published(&stack).ok_or("Multi-protocol host ports not published")?;
        Ok(Self { _stack: stack, ports })
    }

    /// Host port of Redis
    pub fn redis_port(&self) -> u16 {
        self.ports.redis
    }

    /// Host port of Memcached
    pub fn memcached_port(&self) -> u16 {
        self.ports.memcached
    }

    /// Host port of Nginx
    pub fn http_port(&self) -> u16 {
        self.ports.http
    }

    /// `127.0.0.1:<redis_port()>`
    pub fn redis_addr(&self) -> SocketAddr {
        addr(self.ports.redis)
    }

    /// `127.0.0.1:<memcached_port()>`
    pub fn memcached_addr(&self) -> SocketAddr {
        addr(self.ports.memcached)
    }

    /// `127.0.0.1:<http_port()>`
    pub fn http_addr(&self) -> SocketAddr {
        addr(self.ports.http)
    }
}
