//! Multi-protocol test utilities
//!
//! Provides helpers for managing multiple protocol services (Redis, Memcached, Nginx) in integration tests.
//! Uses docker-compose for lifecycle management.

use std::process::Command;
use std::thread;
use std::time::Duration;

/// Guard for multi-protocol services that ensures cleanup
///
/// This will start Redis, Memcached, and Nginx using Docker Compose and clean them up on drop.
pub struct MultiProtocolGuard {
    started: bool,
    compose_file: std::path::PathBuf,
}

impl MultiProtocolGuard {
    /// Start multi-protocol services using Docker Compose
    ///
    /// This will:
    /// 1. Check if Docker and docker-compose are available
    /// 2. Start Redis (6379), Memcached (11211), Nginx (8080) via docker-compose
    /// 3. Wait for all services to be ready
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Docker or docker-compose is not available
    /// - Services fail to start
    /// - Services don't become ready within 30 seconds
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
        let compose_file = workspace_root.join("tests/multi-protocol/docker-compose.yml");

        if !compose_file.exists() {
            return Err(format!("docker-compose.yml not found at {:?}", compose_file).into());
        }

        // Start services using docker-compose up -d
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
            return Err("Failed to start services with docker-compose".into());
        }

        thread::sleep(Duration::from_secs(3));

        // Verify all services are ready
        for _ in 0..15 {
            if Self::are_services_ready() {
                return Ok(Self { started: true, compose_file });
            }
            thread::sleep(Duration::from_secs(2));
        }

        Err("Services failed to become ready within 30 seconds".into())
    }

    /// Check if all services are ready
    fn are_services_ready() -> bool {
        Self::is_redis_ready() && Self::is_memcached_ready() && Self::is_nginx_ready()
    }

    /// Check if Redis is ready
    fn is_redis_ready() -> bool {
        let output = Command::new("redis-cli").args(["-p", "6379", "PING"]).output();

        match output {
            Ok(out) => {
                let response = String::from_utf8_lossy(&out.stdout);
                response.contains("PONG")
            }
            Err(_) => false,
        }
    }

    /// Check if Memcached is ready
    fn is_memcached_ready() -> bool {
        use std::io::{Read, Write};
        use std::net::TcpStream;

        let stream = TcpStream::connect("127.0.0.1:11211");
        match stream {
            Ok(mut stream) => {
                if stream.write_all(b"stats\r\n").is_err() {
                    return false;
                }

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

    /// Check if Nginx is ready
    fn is_nginx_ready() -> bool {
        use std::io::{Read, Write};
        use std::net::TcpStream;

        let stream = TcpStream::connect("127.0.0.1:8080");
        match stream {
            Ok(mut stream) => {
                let request = b"GET / HTTP/1.0\r\n\r\n";
                if stream.write_all(request).is_err() {
                    return false;
                }

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

impl Drop for MultiProtocolGuard {
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
