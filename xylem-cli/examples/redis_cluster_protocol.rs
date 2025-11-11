//! Example demonstrating RedisClusterProtocol with automatic routing
//!
//! This example shows how to use RedisClusterProtocol to automatically
//! route requests to the correct cluster node based on hash slots.

use xylem_protocols::{
    calculate_slot, ClusterTopology, FixedCommandSelector, Protocol, RedisClusterProtocol, RedisOp,
    SlotRange,
};

fn main() {
    println!("=== Redis Cluster Protocol Example ===\n");

    // Example 1: Create protocol
    println!("Example 1: Creating RedisClusterProtocol");
    println!("-----------------------------------------");

    let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
    let mut protocol = RedisClusterProtocol::new(selector);
    println!("✓ Created cluster protocol: {}", protocol.name());
    println!();

    // Example 2: Register connections
    println!("Example 2: Registering Node Connections");
    println!("-----------------------------------------");

    let node1 = "127.0.0.1:7000".parse().unwrap();
    let node2 = "127.0.0.1:7001".parse().unwrap();
    let node3 = "127.0.0.1:7002".parse().unwrap();

    protocol.register_connection(node1, 0);
    protocol.register_connection(node2, 1);
    protocol.register_connection(node3, 2);

    println!("✓ Registered connections:");
    println!("  - Node {} → conn_id 0", node1);
    println!("  - Node {} → conn_id 1", node2);
    println!("  - Node {} → conn_id 2", node3);
    println!();

    // Example 3: Set up topology
    println!("Example 3: Setting Up Cluster Topology");
    println!("----------------------------------------");

    let ranges = vec![
        SlotRange {
            start: 0,
            end: 5460,
            master: node1,
            replicas: vec![],
        },
        SlotRange {
            start: 5461,
            end: 10922,
            master: node2,
            replicas: vec![],
        },
        SlotRange {
            start: 10923,
            end: 16383,
            master: node3,
            replicas: vec![],
        },
    ];

    let topology = ClusterTopology::from_slot_ranges(ranges);
    protocol.update_topology(topology);

    println!("✓ Configured topology:");
    println!("  - Slots 0-5460     → {}", node1);
    println!("  - Slots 5461-10922 → {}", node2);
    println!("  - Slots 10923-16383 → {}", node3);
    println!();

    // Example 4: Generate requests with automatic routing
    println!("Example 4: Automatic Request Routing");
    println!("-------------------------------------");

    let test_keys = [1000, 5000, 10000, 15000];

    for key in &test_keys {
        let key_str = format!("key:{}", key);
        let slot = calculate_slot(key_str.as_bytes());

        // Determine expected node
        let expected_node = if slot <= 5460 {
            node1
        } else if slot <= 10922 {
            node2
        } else {
            node3
        };

        // Generate request
        let (request_data, req_id) = protocol.generate_request(0, *key, 100);
        let (_orig_conn, req_slot, (target_conn, _seq)) = req_id;

        // Verify routing
        let actual_node = if target_conn == 0 {
            node1
        } else if target_conn == 1 {
            node2
        } else {
            node3
        };

        println!(
            "  Key '{}' → Slot {} → Node {} ({})",
            key_str,
            slot,
            actual_node,
            if actual_node == expected_node {
                "✓"
            } else {
                "✗"
            }
        );

        assert_eq!(slot, req_slot, "Slot mismatch in request ID");
        assert_eq!(actual_node, expected_node, "Request routed to wrong node");
        assert!(!request_data.is_empty(), "Request data is empty");
    }
    println!();

    // Example 5: Hash tags for multi-key operations
    println!("Example 5: Hash Tags for Multi-Key Operations");
    println!("-----------------------------------------------");

    // Note: The Protocol trait generates keys as "key:{n}", so we demonstrate
    // hash tag slots here, but actual routing still uses the numeric key
    let hash_tag_keys = ["{user:1000}.profile", "{user:1000}.settings", "{user:1000}.preferences"];

    let first_slot = calculate_slot(hash_tag_keys[0].as_bytes());

    println!("  Hash tag example: keys with same tag route to same slot");
    for key_template in &hash_tag_keys {
        let slot = calculate_slot(key_template.as_bytes());

        let expected_node_idx = if slot <= 5460 {
            0
        } else if slot <= 10922 {
            1
        } else {
            2
        };

        let node = if expected_node_idx == 0 {
            node1
        } else if expected_node_idx == 1 {
            node2
        } else {
            node3
        };

        println!("  Key '{}' → Slot {} → Node {}", key_template, slot, node);

        assert_eq!(slot, first_slot, "Hash tag keys should have same slot");
    }

    println!("\n  ✓ All hash-tagged keys route to same slot ({}) and node", first_slot);
    println!("  → This enables multi-key operations like MGET/MSET");
    println!();

    // Note about Protocol trait limitation
    println!("  Note: Current Protocol trait generates keys as 'key:{{n}}'");
    println!("        Custom key formats would require protocol extension");
    println!();

    // Example 6: Redirect statistics
    println!("Example 6: Monitoring Redirect Statistics");
    println!("-------------------------------------------");

    let stats = protocol.redirect_stats();
    println!("  MOVED redirects: {}", stats.moved_count);
    println!("  ASK redirects:   {}", stats.ask_count);
    println!("  Loops prevented: {}", stats.redirect_loops_prevented);
    println!();

    if let Some(age) = protocol.topology_age() {
        println!("  Topology age: {:?}", age);
    } else {
        println!("  Topology age: Never updated");
    }
    println!();

    // Example 7: Key distribution
    println!("Example 7: Key Distribution Across Nodes");
    println!("------------------------------------------");

    let mut node_counts = [0, 0, 0];
    let sample_size = 1000;

    for i in 0..sample_size {
        let key_str = format!("key:{}", i);
        let slot = calculate_slot(key_str.as_bytes());

        let node_idx = if slot <= 5460 {
            0
        } else if slot <= 10922 {
            1
        } else {
            2
        };

        node_counts[node_idx] += 1;
    }

    println!("  Distribution of {} keys:", sample_size);
    println!(
        "  - Node {} (0-5460):     {} keys ({:.1}%)",
        node1,
        node_counts[0],
        (node_counts[0] as f64 / sample_size as f64) * 100.0
    );
    println!(
        "  - Node {} (5461-10922):  {} keys ({:.1}%)",
        node2,
        node_counts[1],
        (node_counts[1] as f64 / sample_size as f64) * 100.0
    );
    println!(
        "  - Node {} (10923-16383): {} keys ({:.1}%)",
        node3,
        node_counts[2],
        (node_counts[2] as f64 / sample_size as f64) * 100.0
    );
    println!();

    println!("=== Example Complete ===");
    println!("\nKey Takeaways:");
    println!("  1. RedisClusterProtocol automatically routes requests by slot");
    println!("  2. Hash tags ensure multi-key operations target same node");
    println!("  3. Topology can be updated dynamically on MOVED redirects");
    println!("  4. Statistics help monitor cluster behavior");
}
