//! Helper functions for Redis Cluster operations
//!
//! Utilities for distributing hash slots across cluster nodes.

/// Calculate slot range for a node in an evenly-distributed cluster
///
/// Distributes 16,384 slots evenly across nodes, with remainder slots
/// going to the first nodes (similar to Redis Cluster's default distribution).
///
/// # Arguments
///
/// * `node_index` - Zero-based index of the node (0 to node_count-1)
/// * `node_count` - Total number of nodes in the cluster
///
/// # Returns
///
/// `Some((start, end))` if valid, `None` if invalid parameters
///
/// # Example
///
/// ```text
/// // 3-node cluster: 16384 / 3 = 5461 slots per node, 1 remainder
/// // Node 0 gets the extra slot
/// assert_eq!(calculate_slot_range(0, 3), Some((0, 5461)));
/// assert_eq!(calculate_slot_range(1, 3), Some((5462, 10922)));
/// assert_eq!(calculate_slot_range(2, 3), Some((10923, 16383)));
/// ```
pub fn calculate_slot_range(node_index: usize, node_count: usize) -> Option<(u16, u16)> {
    if node_count == 0 || node_index >= node_count {
        return None;
    }

    let total_slots: usize = 16384;
    let slots_per_node = total_slots / node_count;
    let remainder = total_slots % node_count;

    // Distribute remainder slots: first `remainder` nodes get one extra slot
    let start = if node_index < remainder {
        node_index * (slots_per_node + 1)
    } else {
        node_index * slots_per_node + remainder
    } as u16;

    let end = if node_index < remainder {
        start + (slots_per_node + 1) as u16 - 1
    } else {
        start + slots_per_node as u16 - 1
    };

    Some((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_slot_range_3_nodes() {
        // 16384 / 3 = 5461 slots per node, with 1 remainder slot
        // Node 0 gets the extra slot: 5462 slots (0-5461)
        // Node 1 gets 5461 slots (5462-10922)
        // Node 2 gets 5461 slots (10923-16383)
        assert_eq!(calculate_slot_range(0, 3), Some((0, 5461)));
        assert_eq!(calculate_slot_range(1, 3), Some((5462, 10922)));
        assert_eq!(calculate_slot_range(2, 3), Some((10923, 16383)));

        // Verify total coverage
        let (s0, e0) = calculate_slot_range(0, 3).unwrap();
        let (s1, e1) = calculate_slot_range(1, 3).unwrap();
        let (s2, e2) = calculate_slot_range(2, 3).unwrap();
        assert_eq!(s0, 0);
        assert_eq!(e2, 16383);
        assert_eq!(e0 + 1, s1);
        assert_eq!(e1 + 1, s2);

        // Verify total slots = 16384
        let total = (e0 - s0 + 1) + (e1 - s1 + 1) + (e2 - s2 + 1);
        assert_eq!(total, 16384);
    }

    #[test]
    fn test_calculate_slot_range_invalid() {
        assert_eq!(calculate_slot_range(3, 3), None); // node_index >= node_count
        assert_eq!(calculate_slot_range(0, 0), None); // node_count == 0
    }
}
