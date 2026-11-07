//! Multi-protocol test utilities
//!
//! Provides helpers for managing multiple protocol services (Redis, Memcached, Nginx) in integration tests.
//! Uses the shared docker-compose fixture for lifecycle management.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use super::compose::{published_port, ComposeSpec, ComposeStack};
use super::nginx::NginxGuard;

/// Compose file of the multi-protocol stack, relative to the workspace root
const COMPOSE_FILE: &str = "tests/multi-protocol/docker-compose.yml";

/// Host ports Docker published for the Redis, Memcached and Nginx services
#[derive(Clone, Copy)]
struct Ports {
    redis: u16,
    memcached: u16,
    http: u16,
}

impl Ports {
    /// Ports of this stack's services, if all three are running
    fn published() -> Option<Self> {
        Some(Self {
            redis: published_port(COMPOSE_FILE, "redis", 6379)?,
            memcached: published_port(COMPOSE_FILE, "memcached", 11211)?,
            http: published_port(COMPOSE_FILE, "nginx", 80)?,
        })
    }
}

/// Guard for multi-protocol services that ensures cleanup
///
/// This will start Redis, Memcached, and Nginx using Docker Compose and clean them up on drop.
pub struct MultiProtocolGuard {
    /// Held for its drop, which stops the services if this guard started them
    _stack: ComposeStack,
    /// Host ports Docker published for the services
    ports: Ports,
}

impl MultiProtocolGuard {
    /// Start multi-protocol services using Docker Compose
    ///
    /// This will:
    /// 1. Check if Docker and docker-compose are available
    /// 2. Start Redis, Memcached and Nginx via docker-compose, on host ports
    ///    Docker picks, so they never clash with the single-service stacks
    /// 3. Wait for all services to be ready
    ///
    /// If this stack's three services already answer, the guard uses them and
    /// leaves them running on drop.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Docker or docker-compose is not available
    /// - Services fail to start
    /// - Services don't become ready within 30 seconds
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Multi-protocol services",
            compose_file: COMPOSE_FILE,
            build_images: false,
            is_ready: Self::are_services_ready,
        })?;
        let ports = Ports::published().ok_or("Multi-protocol host ports not published")?;
        Ok(Self { _stack: stack, ports })
    }

    /// Check if all services are ready
    fn are_services_ready() -> bool {
        Ports::published().is_some_and(|ports| {
            answers(ports.redis, b"PING\r\n", "+PONG")
                && answers(ports.memcached, b"stats\r\n", "STAT")
                && NginxGuard::is_nginx_ready(ports.http)
        })
    }

    /// Get the Redis port
    pub fn redis_port(&self) -> u16 {
        self.ports.redis
    }

    /// Get the Memcached port
    pub fn memcached_port(&self) -> u16 {
        self.ports.memcached
    }

    /// Get the HTTP port
    pub fn http_port(&self) -> u16 {
        self.ports.http
    }
}

/// Whether the server on `port` of 127.0.0.1 answers `request` with a reply
/// that starts with `expected`
fn answers(port: u16, request: &[u8], expected: &str) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
    if stream.write_all(request).is_err() {
        return false;
    }
    let mut buf = [0u8; 1024];
    match stream.read(&mut buf) {
        Ok(n) if n > 0 => buf[..n].starts_with(expected.as_bytes()),
        _ => false,
    }
}

// Guard lifecycle tests moved to tests/guard_lifecycle.rs to avoid
// duplicate container starts when multiple test binaries run in parallel
