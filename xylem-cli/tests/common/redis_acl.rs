//! Redis with ACL authentication test utilities
//!
//! Provides helpers for managing ACL-enabled Redis instances in integration tests.
//! Uses docker-compose for Redis lifecycle management.

use std::process::Command;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

// Global lock to prevent concurrent Redis container start/stop
static REDIS_ACL_LOCK: Mutex<()> = Mutex::new(());

/// Guard for ACL-enabled Redis that ensures cleanup
///
/// This will start a Redis instance with ACL authentication (username+password)
/// using Docker Compose and clean it up on drop.
pub struct RedisAclGuard {
    started: bool,
    compose_file: std::path::PathBuf,
}

impl RedisAclGuard {
    /// Start an ACL-enabled Redis instance using Docker Compose
    ///
    /// This will:
    /// 1. Check if Docker and docker-compose are available
    /// 2. Start Redis (port 6381) with ACL file via docker-compose
    /// 3. Wait for Redis to be ready
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Docker or docker-compose is not available
    /// - Redis fails to start
    /// - Redis doesn't become ready within 30 seconds
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // Acquire lock to prevent concurrent start attempts
        let _lock = REDIS_ACL_LOCK.lock().unwrap();

        // Get path to docker-compose.yml first
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let workspace_root =
            std::path::Path::new(manifest_dir).parent().ok_or("No parent directory")?;
        let compose_file = workspace_root.join("tests/redis-acl/docker-compose.yml");

        if !compose_file.exists() {
            return Err(format!("docker-compose.yml not found at {:?}", compose_file).into());
        }

        // Check if Redis is already running
        if Self::is_redis_ready() {
            // Redis is already running, just return a guard that won't stop it
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

        // Start Redis using docker-compose up -d
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
            return Err("Failed to start Redis with docker-compose".into());
        }

        thread::sleep(Duration::from_secs(2));

        // Verify Redis is ready
        for _ in 0..15 {
            if Self::is_redis_ready() {
                return Ok(Self { started: true, compose_file });
            }
            thread::sleep(Duration::from_secs(2));
        }

        Err("Redis failed to become ready within 30 seconds".into())
    }

    /// Check if Redis is ready (with ACL authentication)
    fn is_redis_ready() -> bool {
        let output = Command::new("redis-cli")
            .args(["-p", "6381", "--user", "testuser", "-a", "testpassword", "PING"])
            .output();

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
        6381
    }

    /// Get the username for authentication
    pub fn get_username(&self) -> &str {
        "testuser"
    }

    /// Get the password for authentication
    pub fn get_password(&self) -> &str {
        "testpassword"
    }
}

impl Drop for RedisAclGuard {
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
