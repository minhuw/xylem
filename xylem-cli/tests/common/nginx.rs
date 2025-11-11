//! Nginx test utilities
//!
//! Provides helpers for managing Nginx instances in integration tests.
//! Uses docker-compose for Nginx lifecycle management.

use std::process::Command;
use std::thread;
use std::time::Duration;

/// Guard for Nginx that ensures cleanup
///
/// This will start an Nginx instance using Docker Compose and clean it up on drop.
pub struct NginxGuard {
    started: bool,
    compose_file: std::path::PathBuf,
}

impl NginxGuard {
    /// Start an Nginx instance using Docker Compose
    ///
    /// This will:
    /// 1. Check if Docker and docker-compose are available
    /// 2. Start Nginx (port 8080) via docker-compose
    /// 3. Wait for Nginx to be ready
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Docker or docker-compose is not available
    /// - Nginx fails to start
    /// - Nginx doesn't become ready within 30 seconds
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // Check if Docker is available
        let docker_check = Command::new("docker").arg("--version").output()?;
        if !docker_check.status.success() {
            return Err("Docker is not available".into());
        }

        // Check if docker-compose is available (try both docker-compose and docker compose)
        let compose_cmd = if Command::new("docker-compose")
            .arg("version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            vec!["docker-compose"]
        } else if Command::new("docker")
            .args(["compose", "version"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            vec!["docker", "compose"]
        } else {
            return Err(
                "docker-compose not available (tried 'docker-compose' and 'docker compose')".into(),
            );
        };

        // Get path to docker-compose.yml
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let workspace_root =
            std::path::Path::new(manifest_dir).parent().ok_or("No parent directory")?;
        let compose_file = workspace_root.join("tests/nginx/docker-compose.yml");

        if !compose_file.exists() {
            return Err(format!("docker-compose.yml not found at {:?}", compose_file).into());
        }

        // Start Nginx using docker-compose up -d
        let mut cmd = Command::new(compose_cmd[0]);
        if compose_cmd.len() > 1 {
            cmd.args(&compose_cmd[1..]);
        }
        let status = cmd
            .args(["-f", compose_file.to_str().unwrap(), "up", "-d"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()?;

        if !status.success() {
            return Err("Failed to start Nginx with docker-compose".into());
        }

        thread::sleep(Duration::from_secs(2));

        // Verify Nginx is ready
        for _ in 0..15 {
            if Self::is_nginx_ready() {
                return Ok(Self { started: true, compose_file });
            }
            thread::sleep(Duration::from_secs(2));
        }

        Err("Nginx failed to become ready within 30 seconds".into())
    }

    /// Check if Nginx is ready
    fn is_nginx_ready() -> bool {
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

impl Drop for NginxGuard {
    fn drop(&mut self) {
        if self.started {
            // Determine compose command (docker-compose or docker compose)
            let compose_cmd = if Command::new("docker-compose")
                .arg("version")
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
            {
                vec!["docker-compose"]
            } else {
                vec!["docker", "compose"]
            };

            // Stop and remove containers using docker-compose down -v
            let mut cmd = Command::new(compose_cmd[0]);
            if compose_cmd.len() > 1 {
                cmd.args(&compose_cmd[1..]);
            }

            let _ = cmd
                .args(["-f", self.compose_file.to_str().unwrap(), "down", "-v"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
    }
}

// Guard lifecycle tests moved to tests/guard_lifecycle.rs to avoid
// duplicate container starts when multiple test binaries run in parallel
