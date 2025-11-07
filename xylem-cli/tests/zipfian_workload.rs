//! Integration test for Zipfian key distribution workload

use std::collections::HashMap;
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use xylem_core::workload::generator::KeyGeneration;

/// Start the xylem-echo-server in the background
fn start_echo_server() -> Child {
    Command::new("cargo")
        .args(["run", "--release", "--package", "xylem-echo-server"])
        .spawn()
        .expect("Failed to start xylem-echo-server")
}

#[test]
#[ignore] // Run with: cargo test zipfian_workload -- --ignored
#[allow(clippy::zombie_processes)] // Test cleanup handles server termination
fn test_zipfian_workload_distribution() {
    // Start echo server
    let mut server = start_echo_server();
    thread::sleep(Duration::from_secs(2)); // Wait for server to start

    // Test Zipfian distribution properties
    let mut keygen = KeyGeneration::zipfian(1000, 0.99).expect("Failed to create Zipfian");

    // Collect 10000 samples
    let mut key_counts: HashMap<u64, u32> = HashMap::new();
    for _ in 0..10000 {
        let key = keygen.next_key();
        *key_counts.entry(key).or_insert(0) += 1;
    }

    // Verify hot-key concentration
    // Top 10% of keys (0-99) should get significantly more requests
    let top_10_percent: u32 = (0..100).map(|k| key_counts.get(&k).copied().unwrap_or(0)).sum();

    let bottom_10_percent: u32 =
        (900..1000).map(|k| key_counts.get(&k).copied().unwrap_or(0)).sum();

    // With theta=0.99, top 10% should get at least 3x more requests than bottom 10%
    assert!(
        top_10_percent > bottom_10_percent * 3,
        "Zipfian distribution not skewed enough: top={}  bottom={}",
        top_10_percent,
        bottom_10_percent
    );

    // Most frequent key should be 0
    let most_frequent_key =
        key_counts.iter().max_by_key(|(_, count)| *count).map(|(key, _)| *key).unwrap();

    assert_eq!(most_frequent_key, 0, "Most frequent key should be 0, got {}", most_frequent_key);

    // Cleanup
    server.kill().expect("Failed to kill server");
}

#[test]
#[ignore] // Run with: cargo test zipfian_cli -- --ignored
#[allow(clippy::zombie_processes)] // Test cleanup handles server termination
fn test_zipfian_cli_integration() {
    // Start echo server
    let mut server = start_echo_server();
    thread::sleep(Duration::from_secs(2)); // Wait for server to start

    // Run xylem with zipfian distribution
    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--package",
            "xylem-cli",
            "--",
            "--target",
            "127.0.0.1:9999",
            "--protocol",
            "xylem-echo",
            "--key-dist",
            "zipfian",
            "--duration",
            "2s",
            "--rate",
            "1000",
            "--threads",
            "1",
        ])
        .output()
        .expect("Failed to run xylem");

    // Verify it completed successfully
    assert!(
        output.status.success(),
        "Xylem failed with zipfian distribution: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify output contains expected metrics
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Xylem Latency Measurement Results"));
    assert!(stdout.contains("p50"));
    assert!(stdout.contains("p95"));
    assert!(stdout.contains("p99"));

    // Cleanup
    server.kill().expect("Failed to kill server");
}

#[test]
fn cleanup() {
    // Cleanup any lingering servers
    let _ = Command::new("pkill").args(["-f", "xylem-echo-server"]).output();
}
