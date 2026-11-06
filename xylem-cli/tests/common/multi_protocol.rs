//! Multi-protocol test utilities
//!
//! Provides helpers for managing multiple protocol services (Redis, Memcached, Nginx) in integration tests.
//! Uses the shared docker-compose fixture for lifecycle management.

use super::compose::{ComposeSpec, ComposeStack};
use super::memcached::MemcachedGuard;
use super::nginx::NginxGuard;
use super::redis::RedisGuard;

/// Guard for multi-protocol services that ensures cleanup
///
/// This will start Redis, Memcached, and Nginx using Docker Compose and clean them up on drop.
pub struct MultiProtocolGuard {
    /// Held for its drop, which stops the services if this guard started them
    _stack: ComposeStack,
}

impl MultiProtocolGuard {
    /// Start multi-protocol services using Docker Compose
    ///
    /// This will:
    /// 1. Check if Docker and docker-compose are available
    /// 2. Start Redis (6379), Memcached (11211), Nginx (8080) via docker-compose
    /// 3. Wait for all services to be ready
    ///
    /// If all three services already answer, the guard uses them and leaves them
    /// running on drop.
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
            compose_file: "tests/multi-protocol/docker-compose.yml",
            build_images: false,
            is_ready: Self::are_services_ready,
        })?;
        Ok(Self { _stack: stack })
    }

    /// Check if all services are ready
    ///
    /// The services listen on the same ports as the single-service stacks, so their
    /// probes apply unchanged.
    fn are_services_ready() -> bool {
        RedisGuard::is_redis_ready()
            && MemcachedGuard::is_memcached_ready()
            && NginxGuard::is_nginx_ready()
    }

    /// Get the Redis port
    pub fn redis_port(&self) -> u16 {
        6379
    }

    /// Get the Memcached port
    pub fn memcached_port(&self) -> u16 {
        11211
    }

    /// Get the HTTP port
    pub fn http_port(&self) -> u16 {
        8080
    }
}

// Guard lifecycle tests moved to tests/guard_lifecycle.rs to avoid
// duplicate container starts when multiple test binaries run in parallel
