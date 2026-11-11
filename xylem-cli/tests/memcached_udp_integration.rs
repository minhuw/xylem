//! memcached over UDP through the real CLI
//!
//! Uses the UDP port published by the Docker Compose memcached fixture.
//! Each test populates keys in the insert phase and then runs GETs, so the
//! responses carry values: 4 KiB values span three datagrams, 32 KiB ones 24.

use std::path::Path;
use std::process::Command;

mod common;

/// Run a one-second memcached profile over UDP and return its JSON results
fn run_udp(port: u16, protocol: &str, value_size: usize, prefix: &str) -> serde_json::Value {
    let dir = tempfile::tempdir().unwrap();
    let profile = dir.path().join("profile.toml");
    let results = dir.path().join("results.json");
    let config = serde_json::json!({
        "experiment": { "name": "memcached-udp", "duration": "1s", "seed": 1 },
        "traffic_groups": [{
            "name": "udp", "threads": [0], "connections_per_thread": 2,
            "max_pending_per_connection": 4, "target": format!("127.0.0.1:{port}"),
            "transport": "udp", "protocol": protocol,
            "traffic_policy": { "type": "closed-loop" },
            "protocol_config": {
                "operation": "GET",
                "keys": {
                    "strategy": "round-robin", "max": 100, "value_size": value_size,
                    "prefix": prefix
                },
                "insert_phase": { "key_count": 100 }
            }
        }],
        "output": { "format": "json", "file": results }
    });
    std::fs::write(&profile, toml::to_string(&config).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile)
        .env("RUST_LOG", "warn")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "xylem failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    read_json(&results)
}

fn read_json(path: &Path) -> serde_json::Value {
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn assert_udp_run(json: &serde_json::Value, min_bytes_per_response: f64) {
    let requests = &json["global"]["requests"];
    let count = |key: &str| requests[key].as_u64().unwrap();
    assert_eq!(json["target"]["transport"], "udp");
    assert!(count("completed") > 100, "{requests}");
    assert_eq!(count("sent"), count("completed") + count("timed_out") + count("failed"));
    assert_eq!(count("failed"), 0, "{requests}");
    // Loopback loses few datagrams, if any
    assert!(count("timed_out") * 100 <= count("sent"), "{requests}");
    let mbps = json["global"]["throughput_mbps"].as_f64().unwrap();
    let rps = json["global"]["throughput_rps"].as_f64().unwrap();
    let bytes_per_response = mbps * 1e6 / 8.0 / rps;
    assert!(bytes_per_response >= min_bytes_per_response, "{bytes_per_response}");
}

#[test]
fn memcached_ascii_udp_reassembles_multi_datagram_values() {
    let memcached = common::memcached::MemcachedGuard::new().expect("Failed to start Memcached");
    assert_udp_run(
        &run_udp(memcached.get_udp_port(), "memcached-ascii", 4096, "udp-ascii-4k:"),
        4096.0,
    );
    assert_udp_run(
        &run_udp(memcached.get_udp_port(), "memcached-ascii", 32768, "udp-ascii-32k:"),
        32768.0,
    );
}

#[test]
fn memcached_binary_udp_reassembles_multi_datagram_values() {
    let memcached = common::memcached::MemcachedGuard::new().expect("Failed to start Memcached");
    assert_udp_run(
        &run_udp(memcached.get_udp_port(), "memcached-binary", 4096, "udp-bin-4k:"),
        4096.0,
    );
    assert_udp_run(
        &run_udp(memcached.get_udp_port(), "memcached-binary", 32768, "udp-bin-32k:"),
        32768.0,
    );
}
