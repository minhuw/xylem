//! Masstree test utilities
//!
//! Provides helpers for managing Masstree instances in integration tests.
//! Uses the shared docker-compose fixture for Masstree lifecycle management.

use std::time::Duration;

use super::compose::{ComposeSpec, ComposeStack};

/// Guard for Masstree that ensures cleanup
///
/// This will start a Masstree instance using Docker Compose and clean it up on drop.
pub struct MasstreeGuard {
    /// Held for its drop, which stops Masstree if this guard started it
    _stack: ComposeStack,
}

impl MasstreeGuard {
    /// Start a Masstree instance using Docker Compose
    ///
    /// This will:
    /// 1. Check if Docker and docker-compose are available
    /// 2. Build the Masstree image (may take several minutes on first run)
    /// 3. Start Masstree (port 2117) via docker-compose
    /// 4. Wait for Masstree to be ready
    ///
    /// If Masstree is already running, the guard uses it and leaves it running on drop.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Docker or docker-compose is not available
    /// - Masstree image fails to build
    /// - Masstree fails to start
    /// - Masstree doesn't become ready within 30 seconds after container starts
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Masstree",
            compose_file: "tests/masstree/docker-compose.yml",
            build_images: true,
            is_ready: Self::is_masstree_ready,
        })?;
        Ok(Self { _stack: stack })
    }

    /// Check if Masstree is ready by attempting a TCP connection
    fn is_masstree_ready() -> bool {
        use std::net::TcpStream;

        // Try to connect to the Masstree port
        TcpStream::connect_timeout(&"127.0.0.1:2117".parse().unwrap(), Duration::from_millis(500))
            .is_ok()
    }

    /// Get the port Masstree is listening on
    pub fn get_port(&self) -> u16 {
        2117
    }

    /// Get the address string for Masstree
    pub fn get_addr(&self) -> &'static str {
        "127.0.0.1:2117"
    }
}
