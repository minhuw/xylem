//! Integration tests for Redis Cluster support
//!
//! These tests require a real Redis Cluster running via Docker Compose.
//! Run with: cargo test --test redis_cluster_integration -- --ignored

mod common;

use common::redis_cluster::RedisClusterGuard;
use std::process::Command;

#[test]
fn test_cluster_startup_and_connectivity() {
    let cluster = RedisClusterGuard::new().expect("Failed to start cluster");

    // Verify all nodes are accessible
    for port in cluster.get_node_ports() {
        let output = Command::new("redis-cli")
            .args(["-p", &port.to_string(), "ping"])
            .output()
            .expect("Failed to ping node");

        let response = String::from_utf8_lossy(&output.stdout);
        assert!(response.contains("PONG"), "Node on port {} is not responding", port);
    }
}

#[test]
fn test_cluster_topology_discovery() {
    let _cluster = RedisClusterGuard::new().expect("Failed to start cluster");

    // Query cluster slots from the first node
    let output = Command::new("redis-cli")
        .args(["-p", "7000", "cluster", "slots"])
        .output()
        .expect("Failed to get cluster slots");

    let response = String::from_utf8_lossy(&output.stdout);

    // Verify we have 3 slot ranges by checking for the expected start slots
    // The output format from redis-cli is line-based with values:
    // start_slot
    // end_slot
    // host
    // port
    // node_id
    // (repeat for each range)

    // Look for the three expected start slots
    let has_range_1 = response.lines().any(|line| line.trim() == "0");
    let has_range_2 = response.lines().any(|line| line.trim() == "5461");
    let has_range_3 = response.lines().any(|line| line.trim() == "10923");

    assert!(
        has_range_1 && has_range_2 && has_range_3,
        "Expected 3 slot ranges (0, 5461, 10923), got output:\n{}",
        response
    );
}

#[test]
fn test_cluster_key_routing() {
    let _cluster = RedisClusterGuard::new().expect("Failed to start cluster");

    // Test that we can SET and GET keys that route to different nodes
    let test_cases = vec![
        ("user:1000", 7000),   // Should route to node 1 (slot 1649)
        ("session:abc", 7002), // Should route to node 3 (slot 14788)
        ("cache:123", 7000),   // Should route to node 1 (slot 3923)
    ];

    for (key, _expected_port) in test_cases {
        // SET the key via cluster-aware client
        let set_output = Command::new("redis-cli")
            .args(["-c", "-p", "7000", "set", key, "test_value"])
            .output()
            .expect("Failed to SET key");

        assert!(set_output.status.success(), "Failed to SET key {}", key);

        // GET the key to verify it was stored
        let get_output = Command::new("redis-cli")
            .args(["-c", "-p", "7000", "get", key])
            .output()
            .expect("Failed to GET key");

        let response = String::from_utf8_lossy(&get_output.stdout);
        assert!(response.contains("test_value"), "Failed to GET key {}", key);

        // Clean up
        Command::new("redis-cli").args(["-c", "-p", "7000", "del", key]).output().ok();
    }
}

#[test]
fn test_cluster_hash_tag_routing() {
    let _cluster = RedisClusterGuard::new().expect("Failed to start cluster");

    // Keys with the same hash tag should route to the same node
    let keys_with_same_tag =
        vec!["{user:1000}.profile", "{user:1000}.settings", "{user:1000}.preferences"];

    // SET all keys
    for key in &keys_with_same_tag {
        let set_output = Command::new("redis-cli")
            .args(["-c", "-p", "7000", "set", key, "test_value"])
            .output()
            .expect("Failed to SET key");

        assert!(set_output.status.success(), "Failed to SET key {}", key);
    }

    // Use MGET to retrieve all keys at once (only works if they're on the same node)
    let mget_output = Command::new("redis-cli")
        .args(["-c", "-p", "7000", "mget"])
        .args(&keys_with_same_tag)
        .output()
        .expect("Failed to MGET keys");

    let response = String::from_utf8_lossy(&mget_output.stdout);

    // Should get 3 values back
    let value_count = response.matches("test_value").count();
    assert_eq!(value_count, 3, "MGET should return 3 values for hash-tagged keys");

    // Clean up
    for key in &keys_with_same_tag {
        Command::new("redis-cli").args(["-c", "-p", "7000", "del", key]).output().ok();
    }
}

#[test]
fn test_moved_redirect_response() {
    let _cluster = RedisClusterGuard::new().expect("Failed to start cluster");

    // Directly query a node for a key that it doesn't own (without -c flag)
    // This should trigger a MOVED redirect

    // First, find a key that routes to node 2 or 3 (not node 1)
    // Key "session:abc" routes to slot 14788 which is on node 3 (port 7002)

    // Try to GET from node 1 (port 7000) without cluster mode
    let output = Command::new("redis-cli")
        .args(["-p", "7000", "get", "session:abc"])
        .output()
        .expect("Failed to GET key");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should get a MOVED redirect error
    // Format: "MOVED 14788 172.28.0.103:7002" or "MOVED 14788 127.0.0.1:7002"
    // redis-cli outputs MOVED redirects to stdout
    let combined = format!("{}{}", stdout, stderr);
    assert!(
        combined.contains("MOVED") || combined.contains("moved"),
        "Expected MOVED redirect, got stdout: '{}', stderr: '{}'",
        stdout,
        stderr
    );
}

#[test]
fn test_slot_migration_ask_redirect() {
    let _cluster = RedisClusterGuard::new().expect("Failed to start cluster");

    // This test would trigger a slot migration and verify ASK redirects
    // For now, this is a placeholder as it requires more complex setup

    // Steps would be:
    // 1. Use common::redis_cluster::migrate_slot(7000, 7001, 100)
    // 2. SET a key in slot 100
    // 3. Query without cluster mode, expect ASK redirect
    // 4. Complete the migration
    // 5. Clean up

    println!("Slot migration test not yet implemented - requires redis-cli cluster commands");
}

#[test]
fn test_cluster_info_command() {
    let cluster = RedisClusterGuard::new().expect("Failed to start cluster");

    for port in cluster.get_node_ports() {
        let output = Command::new("redis-cli")
            .args(["-p", &port.to_string(), "cluster", "info"])
            .output()
            .expect("Failed to get cluster info");

        let info = String::from_utf8_lossy(&output.stdout);

        // Verify cluster is in ok state
        assert!(info.contains("cluster_state:ok"), "Cluster state is not ok on port {}", port);

        // Verify we have 3 known nodes
        assert!(
            info.contains("cluster_known_nodes:3"),
            "Expected 3 known nodes on port {}",
            port
        );

        // Verify slot coverage
        assert!(
            info.contains("cluster_slots_assigned:16384"),
            "Not all slots assigned on port {}",
            port
        );
    }
}

#[test]
fn test_cluster_nodes_command() {
    let _cluster = RedisClusterGuard::new().expect("Failed to start cluster");

    let output = Command::new("redis-cli")
        .args(["-p", "7000", "cluster", "nodes"])
        .output()
        .expect("Failed to get cluster nodes");

    let nodes = String::from_utf8_lossy(&output.stdout);

    // Should have 3 nodes listed
    let line_count = nodes.lines().filter(|l| !l.trim().is_empty()).count();
    assert_eq!(line_count, 3, "Expected 3 nodes in cluster");

    // Each node should be marked as master
    let master_count = nodes.matches("master").count();
    assert_eq!(master_count, 3, "Expected 3 master nodes");

    // Verify all nodes are connected
    for line in nodes.lines() {
        if line.trim().is_empty() {
            continue;
        }
        assert!(line.contains("connected"), "Node not connected: {}", line);
    }
}
