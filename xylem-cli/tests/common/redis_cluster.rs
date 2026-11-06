//! Redis Cluster test utilities
//!
//! Provides helpers for managing Redis Cluster instances in integration tests.
//! Uses the shared docker-compose fixture for cluster lifecycle management.

use std::process::Command;

use super::compose::{ComposeSpec, ComposeStack};

/// Guard for Redis Cluster that ensures cleanup
///
/// This will start a 3-node Redis Cluster using Docker Compose and clean it up on drop.
/// Pure Rust implementation - no bash scripts needed!
pub struct RedisClusterGuard {
    /// Held for its drop, which stops the cluster if this guard started it
    _stack: ComposeStack,
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
    /// If the cluster is already running and ready, the guard uses it and leaves it
    /// running on drop.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Docker or docker-compose is not available
    /// - Cluster fails to start
    /// - Cluster doesn't become ready within 30 seconds
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let stack = ComposeStack::start(&ComposeSpec {
            name: "Redis Cluster",
            compose_file: "tests/redis-cluster/docker-compose.yml",
            build_images: false,
            is_ready: Self::is_cluster_ready,
        })?;
        Ok(Self { _stack: stack })
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
}

// Guard lifecycle tests moved to tests/guard_lifecycle.rs to avoid
// duplicate container starts when multiple test binaries run in parallel
