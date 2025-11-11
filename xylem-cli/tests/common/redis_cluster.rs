//! Redis Cluster test utilities
//!
//! Provides helpers for managing Redis Cluster instances in integration tests.
//! Uses docker-compose directly from Rust for cluster lifecycle management.

use std::process::Command;
use std::thread;
use std::time::Duration;

/// Guard for Redis Cluster that ensures cleanup
///
/// This will start a 3-node Redis Cluster using Docker Compose and clean it up on drop.
/// Pure Rust implementation - no bash scripts needed!
pub struct RedisClusterGuard {
    started: bool,
    compose_file: std::path::PathBuf,
}

impl RedisClusterGuard {
    /// Start a Redis Cluster using Docker Compose
    ///
    /// This will:
    /// 1. Check if Docker and docker-compose are available
    /// 2. Start 3 Redis nodes (ports 7000, 7001, 7002) via docker-compose
    /// 3. Wait for cluster to initialize
    /// 4. Verify cluster is ready
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Docker or docker-compose is not available
    /// - Cluster fails to start
    /// - Cluster doesn't become ready within 30 seconds
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
        let compose_file = workspace_root.join("tests/redis-cluster/docker-compose.yml");

        if !compose_file.exists() {
            return Err(format!("docker-compose.yml not found at {:?}", compose_file).into());
        }

        // Start cluster using docker-compose up -d
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
            return Err("Failed to start Redis Cluster with docker-compose".into());
        }

        thread::sleep(Duration::from_secs(10));

        // Verify cluster is ready
        for _ in 0..30 {
            if Self::is_cluster_ready() {
                return Ok(Self { started: true, compose_file });
            }
            thread::sleep(Duration::from_secs(1));
        }

        Err("Cluster failed to become ready within 30 seconds".into())
    }

    /// Check if the cluster is ready by querying each node
    fn is_cluster_ready() -> bool {
        for port in [7000, 7001, 7002] {
            let output = Command::new("redis-cli")
                .args(["-p", &port.to_string(), "cluster", "info"])
                .output();

            match output {
                Ok(out) => {
                    let info = String::from_utf8_lossy(&out.stdout);
                    if !info.contains("cluster_state:ok") {
                        return false;
                    }
                }
                Err(_) => return false,
            }
        }
        true
    }

    /// Get the ports of the cluster nodes
    pub fn get_node_ports(&self) -> Vec<u16> {
        vec![7000, 7001, 7002]
    }

    /// Get the seed node address (first node)
    pub fn get_seed_node(&self) -> String {
        "127.0.0.1:7000".to_string()
    }

    /// Get cluster slot assignments
    ///
    /// Returns a map of (start_slot, end_slot, port)
    pub fn get_slot_assignments(&self) -> Vec<(u16, u16, u16)> {
        vec![(0, 5460, 7000), (5461, 10922, 7001), (10923, 16383, 7002)]
    }
}

impl Drop for RedisClusterGuard {
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

/// Trigger a slot migration for testing
///
/// This will put a slot into migration state:
/// - Source node: MIGRATING state
/// - Target node: IMPORTING state
///
/// # Arguments
///
/// * `from_port` - Source node port
/// * `to_port` - Target node port
/// * `slot` - Slot number to migrate
///
/// # Example
///
/// ```ignore
/// migrate_slot(7000, 7001, 100)?;
/// // Slot 100 is now in migration from node 7000 to node 7001
/// ```
pub fn migrate_slot(
    from_port: u16,
    to_port: u16,
    slot: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    // Get node IDs
    let from_id = get_node_id(from_port)?;
    let to_id = get_node_id(to_port)?;

    // Mark slot as importing on target node
    Command::new("redis-cli")
        .args([
            "-p",
            &to_port.to_string(),
            "cluster",
            "setslot",
            &slot.to_string(),
            "importing",
            &from_id,
        ])
        .status()?;

    // Mark slot as migrating on source node
    Command::new("redis-cli")
        .args([
            "-p",
            &from_port.to_string(),
            "cluster",
            "setslot",
            &slot.to_string(),
            "migrating",
            &to_id,
        ])
        .status()?;

    Ok(())
}

/// Get the cluster node ID for a given port
fn get_node_id(port: u16) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "cluster", "myid"])
        .output()?;

    if !output.status.success() {
        return Err(format!("Failed to get node ID for port {}", port).into());
    }

    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

// Guard lifecycle tests moved to tests/guard_lifecycle.rs to avoid
// duplicate container starts when multiple test binaries run in parallel
