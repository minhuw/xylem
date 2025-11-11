//! Common test utilities for integration tests
//!
//! This module provides helpers for managing test servers and avoiding port conflicts.

#![allow(dead_code)]

// Docker-based service guards
pub mod memcached;
pub mod multi_protocol;
pub mod nginx;
pub mod redis;
pub mod redis_cluster;

use std::net::{TcpListener, UdpSocket};
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use xylem_core::stats::GroupStatsCollector;

/// Get an available port from the OS
pub fn get_available_port() -> u16 {
    // Bind to port 0 to let OS assign an available port
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to port 0");
    let port = listener.local_addr().expect("Failed to get local addr").port();
    drop(listener); // Close immediately so port becomes available
    thread::sleep(Duration::from_millis(10)); // Small delay to ensure port is released
    port
}

/// Guard for Echo server that ensures cleanup
pub struct EchoServerGuard {
    process: Option<Child>,
    port: u16,
}

impl EchoServerGuard {
    pub fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for EchoServerGuard {
    fn drop(&mut self) {
        if let Some(mut process) = self.process.take() {
            let _ = process.kill();
            let _ = process.wait();
        }
    }
}

/// Start xylem-echo-server on an available port
pub fn start_echo_server() -> Result<EchoServerGuard, Box<dyn std::error::Error>> {
    let port = get_available_port();

    // Build echo server if needed
    Command::new("cargo")
        .args(["build", "--release", "--package", "xylem-echo-server"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()?;

    // Get the workspace root directory (where Cargo.toml is)
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let workspace_root =
        std::path::Path::new(manifest_dir).parent().ok_or("No parent directory")?;
    let server_path = workspace_root.join("target/release/xylem-echo-server");

    let mut process = Command::new(&server_path)
        .arg("--port")
        .arg(port.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    // Wait for server to be ready
    for _ in 0..50 {
        thread::sleep(Duration::from_millis(100));

        if std::net::TcpStream::connect_timeout(
            &format!("127.0.0.1:{}", port).parse().unwrap(),
            Duration::from_millis(100),
        )
        .is_ok()
        {
            return Ok(EchoServerGuard { process: Some(process), port });
        }

        // Check if process died
        if let Ok(Some(_)) = process.try_wait() {
            return Err("Echo server died immediately".into());
        }
    }

    let _ = process.kill();
    Err("Echo server failed to start within timeout".into())
}

/// Check if a port is available
pub fn is_port_available(port: u16) -> bool {
    TcpListener::bind(format!("127.0.0.1:{}", port)).is_ok()
}

/// Wait for a port to become available
pub fn wait_for_port(port: u16, timeout: Duration) -> bool {
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        if std::net::TcpStream::connect_timeout(
            &format!("127.0.0.1:{}", port).parse().unwrap(),
            Duration::from_millis(100),
        )
        .is_ok()
        {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }

    false
}

/// Start a simple UDP echo server for testing (returns handle and port)
pub fn start_udp_echo_server() -> (thread::JoinHandle<()>, u16) {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind UDP socket");
    let port = socket.local_addr().expect("Failed to get local addr").port();

    let handle = thread::spawn(move || {
        socket.set_read_timeout(Some(Duration::from_secs(1))).ok();
        let mut buf = vec![0u8; 65535];

        // Run for 30 seconds
        for _ in 0..30 {
            match socket.recv_from(&mut buf) {
                Ok((n, src)) => {
                    socket.send_to(&buf[..n], src).ok();
                }
                Err(_) => continue,
            }
        }
    });

    (handle, port)
}

/// Create a GroupStatsCollector for testing with a single group (group_id=0)
///
/// This is a helper for tests that were written for single-group Workers
/// and need to be migrated to GroupStatsCollector.
pub fn create_test_stats() -> GroupStatsCollector {
    let mut stats = GroupStatsCollector::new();
    // Register group 0 with unlimited sampling
    stats.register_group_legacy(0, 1_000_000, 1.0);
    stats
}
