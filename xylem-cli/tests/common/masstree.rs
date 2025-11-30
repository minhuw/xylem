//! Masstree test utilities
//!
//! Provides helpers for managing Masstree instances in integration tests.
//! Uses docker-compose for Masstree lifecycle management.

use std::process::Command;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

// Global lock to prevent concurrent Masstree container start/stop
static MASSTREE_LOCK: Mutex<()> = Mutex::new(());

/// Guard for Masstree that ensures cleanup
///
/// This will start a Masstree instance using Docker Compose and clean it up on drop.
pub struct MasstreeGuard {
    started: bool,
    compose_file: std::path::PathBuf,
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
    /// # Errors
    ///
    /// Returns error if:
    /// - Docker or docker-compose is not available
    /// - Masstree image fails to build
    /// - Masstree fails to start
    /// - Masstree doesn't become ready within 30 seconds after container starts
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // Acquire lock to prevent concurrent start attempts
        let _lock = MASSTREE_LOCK.lock().unwrap();

        // Get path to docker-compose.yml first
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let workspace_root =
            std::path::Path::new(manifest_dir).parent().ok_or("No parent directory")?;
        let compose_file = workspace_root.join("tests/masstree/docker-compose.yml");

        if !compose_file.exists() {
            return Err(format!("docker-compose.yml not found at {:?}", compose_file).into());
        }

        // Check if Masstree is already running
        if Self::is_masstree_ready() {
            // Masstree is already running, just return a guard that won't stop it
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

        // Step 1: Build the image separately (this can take several minutes on first run)
        // The build has no timeout - it completes when done
        eprintln!("Building Masstree Docker image (this may take a few minutes on first run)...");
        let mut build_cmd = Command::new(compose_cmd[0]);
        if compose_cmd.len() > 1 {
            build_cmd.args(&compose_cmd[1..]);
        }
        let build_status = build_cmd
            .args(["-f", compose_file.to_str().unwrap(), "build"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()?;

        if !build_status.success() {
            return Err("Failed to build Masstree Docker image".into());
        }
        eprintln!("Masstree Docker image built successfully");

        // Step 2: Start the container (fast, image is already built)
        let mut start_cmd = Command::new(compose_cmd[0]);
        if compose_cmd.len() > 1 {
            start_cmd.args(&compose_cmd[1..]);
        }
        let start_status = start_cmd
            .args(["-f", compose_file.to_str().unwrap(), "up", "-d"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()?;

        if !start_status.success() {
            return Err("Failed to start Masstree container".into());
        }

        thread::sleep(Duration::from_secs(2));

        // Step 3: Wait for Masstree to be ready (30 seconds is enough once container is running)
        for _ in 0..15 {
            if Self::is_masstree_ready() {
                return Ok(Self { started: true, compose_file });
            }
            thread::sleep(Duration::from_secs(2));
        }

        Err("Masstree failed to become ready within 30 seconds after container start".into())
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

impl Drop for MasstreeGuard {
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
