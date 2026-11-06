//! Memcached test utilities
//!
//! Provides helpers for managing Memcached instances in integration tests.
//! Uses the shared docker-compose fixture for Memcached lifecycle management.

use super::compose::{ComposeSpec, ComposeStack};

/// Guard for Memcached that ensures cleanup
///
/// This will start a Memcached instance using Docker Compose and clean it up on drop.
pub struct MemcachedGuard {
    /// Held for its drop, which stops Memcached if this guard started it
    _stack: ComposeStack,
}

impl MemcachedGuard {
    /// Start a Memcached instance using Docker Compose
    ///
    /// This will:
    /// 1. Check if Docker and docker-compose are available
    /// 2. Start Memcached (port 11211) via docker-compose
    /// 3. Wait for Memcached to be ready
    ///
    /// If Memcached is already running, the guard uses it and leaves it running on drop.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Docker or docker-compose is not available
    /// - Memcached fails to start
    /// - Memcached doesn't become ready within 30 seconds
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Memcached",
            compose_file: "tests/memcached/docker-compose.yml",
            build_images: false,
            is_ready: Self::is_memcached_ready,
        })?;
        Ok(Self { _stack: stack })
    }

    /// Check if Memcached is ready
    pub(super) fn is_memcached_ready() -> bool {
        use std::io::{Read, Write};
        use std::net::TcpStream;

        let stream = TcpStream::connect("127.0.0.1:11211");
        match stream {
            Ok(mut stream) => {
                // Send stats command
                if stream.write_all(b"stats\r\n").is_err() {
                    return false;
                }

                // Try to read response
                let mut buf = [0u8; 1024];
                match stream.read(&mut buf) {
                    Ok(n) if n > 0 => {
                        let response = String::from_utf8_lossy(&buf[..n]);
                        response.contains("STAT")
                    }
                    _ => false,
                }
            }
            Err(_) => false,
        }
    }

    /// Get the port Memcached is listening on
    pub fn get_port(&self) -> u16 {
        11211
    }
}

// Guard lifecycle tests moved to tests/guard_lifecycle.rs to avoid
// duplicate container starts when multiple test binaries run in parallel
