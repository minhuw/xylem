//! Example demonstrating Redis Cluster slot calculation
//!
//! This example shows how to calculate hash slots for Redis keys,
//! including support for hash tags that allow multi-key operations.

use xylem_protocols::{calculate_slot_str, extract_hash_tag, has_hash_tag, CLUSTER_SLOTS};

fn main() {
    println!("=== Redis Cluster Hash Slot Calculator ===\n");

    println!("Total slots in Redis Cluster: {}\n", CLUSTER_SLOTS);

    // Example 1: Basic key slot calculation
    println!("Example 1: Basic Keys");
    println!("---------------------");
    let keys = ["user:1000", "user:1001", "session:abc", "cache:123"];
    for key in &keys {
        let slot = calculate_slot_str(key);
        println!("  Key '{}' → Slot {}", key, slot);
    }
    println!();

    // Example 2: Hash tags for grouping keys
    println!("Example 2: Hash Tags (Grouped Keys)");
    println!("------------------------------------");
    let tagged_keys = ["{user:1000}.profile", "{user:1000}.settings", "{user:1000}.preferences"];

    println!("Keys with tag '{{user:1000}}':");
    for key in &tagged_keys {
        let slot = calculate_slot_str(key);
        let tag = extract_hash_tag(key.as_bytes());
        println!(
            "  Key '{}' → Slot {} (tag: {:?})",
            key,
            slot,
            tag.map(|t| String::from_utf8_lossy(t).to_string())
        );
    }

    // Verify they all map to the same slot
    let slots: Vec<_> = tagged_keys.iter().map(|k| calculate_slot_str(k)).collect();
    if slots.windows(2).all(|w| w[0] == w[1]) {
        println!("  ✓ All keys map to the same slot: {}", slots[0]);
    }
    println!();

    // Example 3: Hash tag detection
    println!("Example 3: Hash Tag Detection");
    println!("------------------------------");
    let test_keys = [
        ("regular_key", false),
        ("{tagged}key", true),
        ("key{tagged}", true),
        ("{}empty_tag", false),
        ("{no_closing", false),
    ];

    for (key, expected_tag) in &test_keys {
        let has_tag = has_hash_tag(key.as_bytes());
        let status = if has_tag == *expected_tag {
            "✓"
        } else {
            "✗"
        };
        println!("  {} Key '{}': has_tag={}", status, key, has_tag);
    }
    println!();

    // Example 4: Different users likely map to different slots
    println!("Example 4: Key Distribution");
    println!("----------------------------");
    let users = ["user:1000", "user:2000", "user:3000"];
    for user in &users {
        let slot = calculate_slot_str(user);
        let percentage = (slot as f64 / CLUSTER_SLOTS as f64) * 100.0;
        println!("  Key '{}' → Slot {} ({:.2}% through keyspace)", user, slot, percentage);
    }
    println!();

    // Example 5: Multi-key operations with hash tags
    println!("Example 5: Multi-Key Operations");
    println!("--------------------------------");
    println!("To execute MGET on multiple keys in Redis Cluster,");
    println!("they must be in the same slot. Use hash tags:\n");

    let session_keys = ["{session:abc}.data", "{session:abc}.metadata", "{session:abc}.timestamp"];

    let slot = calculate_slot_str(session_keys[0]);
    println!("  All keys in slot {}", slot);
    println!("  Command: MGET {}", session_keys.join(" "));
    println!("  ✓ This will work in Redis Cluster!\n");

    // Example without hash tags (won't work)
    let bad_keys = ["session:abc.data", "session:xyz.data", "session:def.data"];
    println!("  Without hash tags:");
    for key in &bad_keys {
        let slot = calculate_slot_str(key);
        println!("    '{}' → Slot {}", key, slot);
    }
    println!("  ✗ MGET would fail - keys in different slots!\n");

    // Example 6: Slot distribution statistics
    println!("Example 6: Distribution Statistics");
    println!("-----------------------------------");
    let mut slot_counts = [0usize; 16];

    for i in 0..10000 {
        let key = format!("key:{}", i);
        let slot = calculate_slot_str(&key);
        let bucket = (slot as usize * 16) / CLUSTER_SLOTS as usize;
        slot_counts[bucket] += 1;
    }

    println!("  10,000 keys distributed across 16 buckets:");
    for (i, count) in slot_counts.iter().enumerate() {
        let start = (i * CLUSTER_SLOTS as usize) / 16;
        let end = ((i + 1) * CLUSTER_SLOTS as usize) / 16 - 1;
        let bar = "#".repeat(*count / 50);
        println!("  Slots {:5}-{:5}: {:4} keys {}", start, end, count, bar);
    }
    println!("\n  Distribution appears uniform ✓");
}
