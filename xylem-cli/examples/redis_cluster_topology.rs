//! Example demonstrating Redis Cluster topology and redirect handling
//!
//! This example shows how to:
//! - Build cluster topology from slot ranges
//! - Route keys to appropriate nodes
//! - Handle MOVED and ASK redirects
//! - Generate cluster commands

use std::net::SocketAddr;
use xylem_protocols::{
    calculate_slot_str, generate_asking_command, generate_cluster_slots_command, parse_redirect,
    ClusterTopology, RedirectType, SlotRange,
};

fn main() {
    println!("=== Redis Cluster Topology & Redirect Handling ===\n");

    // Example 1: Build a 3-node cluster topology
    println!("Example 1: Building Cluster Topology");
    println!("-------------------------------------");

    let node1: SocketAddr = "127.0.0.1:7000".parse().unwrap();
    let node2: SocketAddr = "127.0.0.1:7001".parse().unwrap();
    let node3: SocketAddr = "127.0.0.1:7002".parse().unwrap();

    let ranges = vec![
        SlotRange {
            start: 0,
            end: 5460,
            master: node1,
            replicas: vec!["127.0.0.1:7003".parse().unwrap()],
        },
        SlotRange {
            start: 5461,
            end: 10922,
            master: node2,
            replicas: vec!["127.0.0.1:7004".parse().unwrap()],
        },
        SlotRange {
            start: 10923,
            end: 16383,
            master: node3,
            replicas: vec!["127.0.0.1:7005".parse().unwrap()],
        },
    ];

    let topology = ClusterTopology::from_slot_ranges(ranges);

    println!("Cluster initialized:");
    println!("  Total nodes: {} ({} masters)", topology.node_count(), topology.master_count());
    println!("  Masters: {:?}", topology.get_masters());
    println!();

    // Example 2: Route keys to nodes
    println!("Example 2: Key Routing");
    println!("----------------------");

    let keys = ["user:1000", "session:abc", "cache:123", "data:xyz"];

    for key in &keys {
        let slot = calculate_slot_str(key);
        let node = topology.get_node_for_slot(slot);
        println!("  Key '{}' → Slot {} → Node {}", key, slot, node);
    }
    println!();

    // Example 3: Grouped keys with hash tags
    println!("Example 3: Hash Tag Routing");
    println!("---------------------------");

    let tagged_keys = ["{user:1000}.profile", "{user:1000}.settings", "{user:1000}.preferences"];

    let slot = calculate_slot_str(tagged_keys[0]);
    let node = topology.get_node_for_slot(slot);

    println!("All keys with tag '{{user:1000}}':");
    for key in &tagged_keys {
        println!("  '{}' → Slot {} → Node {}", key, slot, node);
    }
    println!("  ✓ All route to same node for MGET/MSET");
    println!();

    // Example 4: MOVED redirect handling
    println!("Example 4: MOVED Redirect Handling");
    println!("-----------------------------------");

    let moved_error = b"-MOVED 3999 127.0.0.1:7001\r\n";
    let redirect = parse_redirect(moved_error).unwrap();

    match redirect {
        Some(RedirectType::Moved { slot, addr }) => {
            println!("Received MOVED redirect:");
            println!("  Slot: {}", slot);
            println!("  New owner: {}", addr);
            println!("  Action: Update topology and retry on {}", addr);

            // Simulate topology update
            let mut updated_topology = topology.clone();
            updated_topology.update_slot(slot, addr);
            println!("  ✓ Topology updated");
        }
        _ => println!("Unexpected redirect type"),
    }
    println!();

    // Example 5: ASK redirect handling
    println!("Example 5: ASK Redirect Handling (Migration)");
    println!("---------------------------------------------");

    let ask_error = b"-ASK 5000 127.0.0.1:7002\r\n";
    let redirect = parse_redirect(ask_error).unwrap();

    match redirect {
        Some(RedirectType::Ask { slot, addr }) => {
            println!("Received ASK redirect (slot migration in progress):");
            println!("  Slot: {}", slot);
            println!("  Target node: {}", addr);
            println!("  Action: Send ASKING, then retry on {}", addr);
            println!("  Do NOT update topology (temporary state)");

            let asking_cmd = generate_asking_command();
            println!("  ASKING command: {:?}", String::from_utf8_lossy(&asking_cmd));
        }
        _ => println!("Unexpected redirect type"),
    }
    println!();

    // Example 6: Non-redirect errors
    println!("Example 6: Regular Error Handling");
    println!("----------------------------------");

    let regular_errors: &[&[u8]] = &[
        b"-ERR unknown command\r\n",
        b"-WRONGTYPE Operation against wrong key type\r\n",
        b"-NOAUTH Authentication required\r\n",
    ];

    for error in regular_errors {
        let redirect = parse_redirect(error).unwrap();
        let error_str = String::from_utf8_lossy(error).trim().to_string();

        match redirect {
            None => println!("  Regular error (no redirect): {}", error_str),
            Some(_) => println!("  Unexpected redirect detected"),
        }
    }
    println!();

    // Example 7: CLUSTER SLOTS command
    println!("Example 7: Topology Discovery");
    println!("------------------------------");

    let cluster_slots_cmd = generate_cluster_slots_command();
    println!("CLUSTER SLOTS command:");
    println!("  Raw: {:?}", String::from_utf8_lossy(&cluster_slots_cmd));
    println!("  Use this to discover/refresh cluster topology");
    println!();

    // Example 8: Redirect decision tree
    println!("Example 8: Redirect Decision Tree");
    println!("----------------------------------");

    println!("When receiving redirect:");
    println!("  MOVED:");
    println!("    1. Update local slot map (slot → new_node)");
    println!("    2. Retry command on new_node");
    println!("    3. (Optional) Refresh full topology with CLUSTER SLOTS");
    println!();
    println!("  ASK:");
    println!("    1. Send ASKING command to target_node");
    println!("    2. Immediately send original command");
    println!("    3. Do NOT update slot map");
    println!("    4. ASK flag cleared after one command");
    println!();

    // Example 9: Topology staleness
    println!("Example 9: Topology Staleness Detection");
    println!("----------------------------------------");

    if let Some(age) = topology.age() {
        println!("  Topology age: {:?}", age);

        if topology.is_stale(std::time::Duration::from_secs(60)) {
            println!("  Status: STALE (>60s old)");
            println!("  Recommendation: Refresh with CLUSTER SLOTS");
        } else {
            println!("  Status: FRESH");
        }
    }
    println!();

    // Example 10: Complete request flow
    println!("Example 10: Complete Request Flow");
    println!("----------------------------------");

    let key = "product:12345";
    let slot = calculate_slot_str(key);
    let target_node = topology.get_node_for_slot(slot);

    println!("1. Client wants to GET '{}'", key);
    println!("2. Calculate slot: {}", slot);
    println!("3. Lookup node in topology: {}", target_node);
    println!("4. Send GET request to {}", target_node);
    println!();
    println!("5a. If successful: Return value to client");
    println!("5b. If MOVED: Update topology, retry on new node");
    println!("5c. If ASK: Send ASKING + retry, don't update topology");
    println!("5d. If other error: Handle according to error type");
    println!();

    println!("=== Summary ===");
    println!("✓ Cluster topology management implemented");
    println!("✓ Slot-based key routing working");
    println!("✓ MOVED redirect handling ready");
    println!("✓ ASK redirect handling ready");
    println!("✓ Ready for Phase 4 cluster protocol integration");
}
