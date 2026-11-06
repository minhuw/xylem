//! Nginx test utilities
//!
//! Provides helpers for managing Nginx instances in integration tests.
//! Uses the shared docker-compose fixture for Nginx lifecycle management.

use super::compose::{ComposeSpec, ComposeStack};

/// Guard for Nginx that ensures cleanup
///
/// This will start an Nginx instance using Docker Compose and clean it up on drop.
pub struct NginxGuard {
    /// Held for its drop, which stops Nginx if this guard started it
    _stack: ComposeStack,
}

impl NginxGuard {
    /// Start an Nginx instance using Docker Compose
    ///
    /// This will:
    /// 1. Check if Docker and docker-compose are available
    /// 2. Start Nginx (port 8080) via docker-compose
    /// 3. Wait for Nginx to be ready
    ///
    /// If Nginx is already running, the guard uses it and leaves it running on drop.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Docker or docker-compose is not available
    /// - Nginx fails to start
    /// - Nginx doesn't become ready within 30 seconds
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Nginx",
            compose_file: "tests/nginx/docker-compose.yml",
            build_images: false,
            is_ready: Self::is_nginx_ready,
        })?;
        Ok(Self { _stack: stack })
    }

    /// Check if Nginx is ready
    pub(super) fn is_nginx_ready() -> bool {
        use std::io::{Read, Write};
        use std::net::TcpStream;

        let stream = TcpStream::connect("127.0.0.1:8080");
        match stream {
            Ok(mut stream) => {
                // Send simple HTTP GET request
                let request = b"GET / HTTP/1.0\r\n\r\n";
                if stream.write_all(request).is_err() {
                    return false;
                }

                // Try to read response
                let mut buf = [0u8; 1024];
                match stream.read(&mut buf) {
                    Ok(n) if n > 0 => {
                        let response = String::from_utf8_lossy(&buf[..n]);
                        response.contains("HTTP/") && response.contains("200")
                    }
                    _ => false,
                }
            }
            Err(_) => false,
        }
    }

    /// Get the port Nginx is listening on
    pub fn get_port(&self) -> u16 {
        8080
    }
}

// Guard lifecycle tests moved to tests/guard_lifecycle.rs to avoid
// duplicate container starts when multiple test binaries run in parallel
