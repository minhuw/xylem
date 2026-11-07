//! Nginx test utilities
//!
//! Provides helpers for managing Nginx instances in integration tests.
//! Uses the shared docker-compose fixture for Nginx lifecycle management.

use super::compose::{published_port, ComposeSpec, ComposeStack};

/// Compose file of the Nginx stack, relative to the workspace root
const COMPOSE_FILE: &str = "tests/nginx/docker-compose.yml";

/// Guard for Nginx that ensures cleanup
///
/// This will start an Nginx instance using Docker Compose and clean it up on drop.
pub struct NginxGuard {
    /// Held for its drop, which stops Nginx if this guard started it
    _stack: ComposeStack,
    /// Host port Docker published for Nginx
    port: u16,
}

impl NginxGuard {
    /// Start an Nginx instance using Docker Compose
    ///
    /// This will:
    /// 1. Check if Docker and docker-compose are available
    /// 2. Start Nginx via docker-compose, on a host port Docker picks
    /// 3. Wait for Nginx to be ready
    ///
    /// If this stack's Nginx is already running, the guard uses it and leaves
    /// it running on drop.
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
            compose_file: COMPOSE_FILE,
            build_images: false,
            is_ready: || Self::published_port().is_some_and(Self::is_nginx_ready),
        })?;
        let port = Self::published_port().ok_or("Nginx host port not published")?;
        Ok(Self { _stack: stack, port })
    }

    /// Host port of this stack's Nginx, if it is running
    fn published_port() -> Option<u16> {
        published_port(COMPOSE_FILE, "nginx", 80)
    }

    /// Check if Nginx answers on `port` of 127.0.0.1
    pub(super) fn is_nginx_ready(port: u16) -> bool {
        use std::io::{Read, Write};
        use std::net::TcpStream;

        let stream = TcpStream::connect(("127.0.0.1", port));
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
        self.port
    }
}

// Guard lifecycle tests moved to tests/guard_lifecycle.rs to avoid
// duplicate container starts when multiple test binaries run in parallel
