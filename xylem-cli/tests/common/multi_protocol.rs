//! Multi-protocol test fixture
//!
//! Each guard starts its own compose project and tears it down on drop; the
//! server is reached at its container's address.

use super::compose::{answers_at, ComposeSpec, ComposeStack};
use super::nginx::NginxGuard;
use std::net::SocketAddr;

/// Addresses of the Redis, Memcached and Nginx services
struct Addrs {
    redis: SocketAddr,
    memcached: SocketAddr,
    http: SocketAddr,
}

impl Addrs {
    fn of(stack: &ComposeStack) -> Option<Self> {
        Some(Self {
            redis: stack.container_addr("redis", 6379)?,
            memcached: stack.container_addr("memcached", 11211)?,
            http: stack.container_addr("nginx", 80)?,
        })
    }
}

/// Guard for Redis, Memcached and Nginx in a compose project of their own,
/// torn down on drop
pub struct MultiProtocolGuard {
    /// Held for its drop, which tears the project down
    _stack: ComposeStack,
    addrs: Addrs,
}

impl MultiProtocolGuard {
    /// Start Redis, Memcached and Nginx and wait until all three answer
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Multi-protocol services",
            compose_file: "tests/multi-protocol/docker-compose.test.yml",
            image: None,
            is_ready: |stack| {
                Addrs::of(stack).is_some_and(|a| {
                    answers_at(a.redis, b"PING\r\n", b"+PONG")
                        && answers_at(a.memcached, b"stats\r\n", b"STAT")
                        && NginxGuard::is_nginx_ready(a.http)
                })
            },
        })?;
        let addrs =
            Addrs::of(&stack).ok_or("Multi-protocol services have no container addresses")?;
        Ok(Self { _stack: stack, addrs })
    }

    /// Address of Redis
    pub fn redis_addr(&self) -> SocketAddr {
        self.addrs.redis
    }

    /// Address of Memcached
    pub fn memcached_addr(&self) -> SocketAddr {
        self.addrs.memcached
    }

    /// Address of Nginx
    pub fn http_addr(&self) -> SocketAddr {
        self.addrs.http
    }
}
