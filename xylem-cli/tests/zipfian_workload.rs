//! Integration test for Zipfian key distribution workload

mod common;

use std::collections::HashMap;
use std::process::Command;
use std::thread;
use std::time::Duration;
use xylem_core::workload::generator::KeyGeneration;

#[test]
#[ignore] // Run with: cargo test zipfian_workload -- --ignored
fn test_zipfian_workload_distribution() {
    // Test Zipfian distribution properties without server
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
}

#[test]
#[ignore] // Run with: cargo test zipfian_cli -- --ignored
fn test_zipfian_cli_integration() {
    // Start echo server on a random port
    let server = common::start_echo_server().expect("Failed to start echo server");
    let port = server.port();

    println!("Started echo server on port {}", port);

    // Give server a moment to fully initialize
    thread::sleep(Duration::from_millis(200));

    // Run xylem with zipfian distribution
    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--package",
            "xylem-cli",
            "--",
            "--target",
            &format!("127.0.0.1:{}", port),
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

    // Server is automatically cleaned up when `server` is dropped
    drop(server);
}
