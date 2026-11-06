//! Redis test utilities
//!
//! Provides helpers for managing Redis instances in integration tests.
//! Uses the shared docker-compose fixture for Redis lifecycle management.

use std::process::Command;

use super::compose::{ComposeSpec, ComposeStack};

/// Guard for Redis that ensures cleanup
///
/// This will start a Redis instance using Docker Compose and clean it up on drop.
pub struct RedisGuard {
    /// Held for its drop, which stops Redis if this guard started it
    _stack: ComposeStack,
}

impl RedisGuard {
    /// Start a Redis instance using Docker Compose
    ///
    /// This will:
    /// 1. Check if Docker and docker-compose are available
    /// 2. Start Redis (port 6379) via docker-compose
    /// 3. Wait for Redis to be ready
    ///
    /// If Redis is already running, the guard uses it and leaves it running on drop.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Docker or docker-compose is not available
    /// - Redis fails to start
    /// - Redis doesn't become ready within 30 seconds
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Redis",
            compose_file: "tests/redis/docker-compose.yml",
            build_images: false,
            is_ready: Self::is_redis_ready,
        })?;
        Ok(Self { _stack: stack })
    }

    /// Check if Redis is ready
    pub(super) fn is_redis_ready() -> bool {
        let output = Command::new("redis-cli").args(["-p", "6379", "PING"]).output();

        match output {
            Ok(out) => {
                let response = String::from_utf8_lossy(&out.stdout);
                response.contains("PONG")
            }
            Err(_) => false,
        }
    }

    /// Get the port Redis is listening on
    pub fn get_port(&self) -> u16 {
        6379
    }
}

// Guard lifecycle tests moved to tests/guard_lifecycle.rs to avoid
// duplicate container starts when multiple test binaries run in parallel
