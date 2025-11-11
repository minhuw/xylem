//! Memcached test utilities
//!
//! Provides helpers for managing Memcached instances in integration tests.
//! Uses docker-compose for Memcached lifecycle management.

use std::process::Command;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

// Global lock to prevent concurrent Memcached container start/stop
static MEMCACHED_LOCK: Mutex<()> = Mutex::new(());

/// Guard for Memcached that ensures cleanup
///
/// This will start a Memcached instance using Docker Compose and clean it up on drop.
pub struct MemcachedGuard {
    started: bool,
    compose_file: std::path::PathBuf,
}

impl MemcachedGuard {
    /// Start a Memcached instance using Docker Compose
    ///
    /// This will:
    /// 1. Check if Docker and docker-compose are available
    /// 2. Start Memcached (port 11211) via docker-compose
    /// 3. Wait for Memcached to be ready
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Docker or docker-compose is not available
    /// - Memcached fails to start
    /// - Memcached doesn't become ready within 30 seconds
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // Acquire lock to prevent concurrent start attempts
        let _lock = MEMCACHED_LOCK.lock().unwrap();

        // Get path to docker-compose.yml first
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let workspace_root =
            std::path::Path::new(manifest_dir).parent().ok_or("No parent directory")?;
        let compose_file = workspace_root.join("tests/memcached/docker-compose.yml");

        if !compose_file.exists() {
            return Err(format!("docker-compose.yml not found at {:?}", compose_file).into());
        }

        // Check if Memcached is already running
        if Self::is_memcached_ready() {
            return Ok(Self { started: false, compose_file });
        }

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

        // Start Memcached using docker-compose up -d
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
            return Err("Failed to start Memcached with docker-compose".into());
        }

        thread::sleep(Duration::from_secs(2));

        // Verify Memcached is ready
        for _ in 0..15 {
            if Self::is_memcached_ready() {
                return Ok(Self { started: true, compose_file });
            }
            thread::sleep(Duration::from_secs(2));
        }

        Err("Memcached failed to become ready within 30 seconds".into())
    }

    /// Check if Memcached is ready
    fn is_memcached_ready() -> bool {
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

impl Drop for MemcachedGuard {
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
