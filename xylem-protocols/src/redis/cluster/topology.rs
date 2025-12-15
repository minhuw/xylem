//! Redis Cluster topology management
//!
//! This module handles the mapping of hash slots to cluster nodes.
//! Redis Cluster has 16,384 slots distributed across nodes.

use std::collections::HashMap;
use std::net::SocketAddr;

/// Redis Cluster node information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNode {
    /// Node address (ip:port)
    pub addr: SocketAddr,
    /// Node ID (40-char hex string, optional)
    pub node_id: Option<String>,
    /// Is this node a master?
    pub is_master: bool,
}

/// Slot range assignment
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotRange {
    /// Start slot (inclusive)
    pub start: u16,
    /// End slot (inclusive)
    pub end: u16,
    /// Master node serving this range
    pub master: SocketAddr,
    /// Replica nodes (optional)
    pub replicas: Vec<SocketAddr>,
}

/// Redis Cluster topology
///
/// Maintains the mapping from hash slots (0-16383) to cluster nodes.
/// This is the core data structure for cluster-aware routing.
#[derive(Debug, Clone)]
pub struct ClusterTopology {
    /// Map from slot number to master node address
    /// Array of 16384 entries for O(1) lookup
    slot_map: Vec<SocketAddr>,

    /// All known nodes in the cluster
    nodes: HashMap<SocketAddr, ClusterNode>,

    /// Slot range assignments (for debugging/display)
    ranges: Vec<SlotRange>,

    /// Timestamp of last topology update (for staleness detection)
    last_updated: Option<std::time::Instant>,
}

impl ClusterTopology {
    /// Create a new empty topology
    ///
    /// The topology is uninitialized and must be populated via
    /// `from_slot_ranges()` or discovery.
    pub fn new() -> Self {
        // Use a placeholder address that will be replaced on discovery
        let placeholder: SocketAddr = "0.0.0.0:0".parse().unwrap();

        Self {
            slot_map: vec![placeholder; 16384],
            nodes: HashMap::new(),
            ranges: Vec::new(),
            last_updated: None,
        }
    }

    /// Build topology from CLUSTER SLOTS response
    ///
    /// # Arguments
    ///
    /// * `ranges` - Slot ranges returned from CLUSTER SLOTS command
    ///
    /// # Panics
    ///
    /// Panics if ranges overlap (indicates configuration error)
    ///
    /// # Example
    ///
    /// ```
    /// use xylem_protocols::redis::cluster::{SlotRange, ClusterTopology};
    /// use std::net::SocketAddr;
    ///
    /// let master1: SocketAddr = "127.0.0.1:7000".parse().unwrap();
    /// let master2: SocketAddr = "127.0.0.1:7001".parse().unwrap();
    ///
    /// let ranges = vec![
    ///     SlotRange {
    ///         start: 0,
    ///         end: 8191,
    ///         master: master1,
    ///         replicas: vec![],
    ///     },
    ///     SlotRange {
    ///         start: 8192,
    ///         end: 16383,
    ///         master: master2,
    ///         replicas: vec![],
    ///     },
    /// ];
    ///
    /// let topology = ClusterTopology::from_slot_ranges(ranges);
    /// assert_eq!(topology.get_node_for_slot(0), master1);
    /// assert_eq!(topology.get_node_for_slot(10000), master2);
    /// ```
    pub fn from_slot_ranges(ranges: Vec<SlotRange>) -> Self {
        let mut topology = Self::new();

        // Track which slots have been assigned to detect overlaps
        let mut slot_assigned = vec![false; 16384];

        // Build slot map from ranges
        for range in &ranges {
            // Validate slot range bounds
            assert!(range.start < 16384, "Invalid start slot {} in range", range.start);
            assert!(range.end < 16384, "Invalid end slot {} in range", range.end);
            assert!(
                range.start <= range.end,
                "Invalid range: start {} > end {}",
                range.start,
                range.end
            );

            // Check for overlaps
            for slot in range.start..=range.end {
                assert!(
                    !slot_assigned[slot as usize],
                    "Overlapping slot ranges detected: slot {} assigned multiple times",
                    slot
                );
                slot_assigned[slot as usize] = true;
                topology.slot_map[slot as usize] = range.master;
            }

            // Register master node
            topology.nodes.entry(range.master).or_insert_with(|| ClusterNode {
                addr: range.master,
                node_id: None,
                is_master: true,
            });

            // Register replica nodes
            for replica in &range.replicas {
                topology.nodes.entry(*replica).or_insert_with(|| ClusterNode {
                    addr: *replica,
                    node_id: None,
                    is_master: false,
                });
            }
        }

        topology.ranges = ranges;
        topology.last_updated = Some(std::time::Instant::now());
        topology
    }

    /// Get the master node address for a given slot
    ///
    /// # Arguments
    ///
    /// * `slot` - Hash slot number (0-16383)
    ///
    /// # Returns
    ///
    /// The socket address of the master node responsible for this slot
    ///
    /// # Panics
    ///
    /// Panics if slot >= 16384
    pub fn get_node_for_slot(&self, slot: u16) -> SocketAddr {
        assert!(slot < 16384, "Invalid slot {}: must be < 16384", slot);
        self.slot_map[slot as usize]
    }

    /// Update a single slot mapping (used after MOVED redirect)
    ///
    /// When receiving a MOVED redirect, update the topology to reflect
    /// the new slot owner. This is a partial update - for complete refresh,
    /// use `from_slot_ranges()`.
    ///
    /// # Arguments
    ///
    /// * `slot` - Hash slot number (0-16383)
    /// * `addr` - New owner address
    ///
    /// # Panics
    ///
    /// Panics if slot >= 16384
    pub fn update_slot(&mut self, slot: u16, addr: SocketAddr) {
        assert!(slot < 16384, "Invalid slot {}: must be < 16384", slot);
        self.slot_map[slot as usize] = addr;

        // Register node if not known
        self.nodes.entry(addr).or_insert_with(|| ClusterNode {
            addr,
            node_id: None,
            is_master: true,
        });

        self.last_updated = Some(std::time::Instant::now());
    }

    /// Get all master node addresses
    ///
    /// Returns a vector of all master nodes in the cluster.
    /// Useful for establishing initial connections.
    pub fn get_masters(&self) -> Vec<SocketAddr> {
        self.nodes
            .iter()
            .filter(|(_, node)| node.is_master)
            .map(|(addr, _)| *addr)
            .collect()
    }

    /// Get all node addresses (masters + replicas)
    pub fn get_all_nodes(&self) -> Vec<SocketAddr> {
        self.nodes.keys().copied().collect()
    }

    /// Check if topology is initialized
    ///
    /// Returns true if the topology has been populated with at least one node.
    pub fn is_initialized(&self) -> bool {
        !self.nodes.is_empty()
    }

    /// Get the number of nodes in the cluster
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get the number of master nodes
    pub fn master_count(&self) -> usize {
        self.nodes.values().filter(|n| n.is_master).count()
    }

    /// Get slot ranges for debugging/display
    pub fn get_ranges(&self) -> &[SlotRange] {
        &self.ranges
    }

    /// Get time since last update
    pub fn age(&self) -> Option<std::time::Duration> {
        self.last_updated.map(|t| t.elapsed())
    }

    /// Check if topology is stale (older than threshold)
    pub fn is_stale(&self, threshold: std::time::Duration) -> bool {
        self.age().map(|age| age > threshold).unwrap_or(true)
    }
}

impl Default for ClusterTopology {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topology_creation() {
        let topology = ClusterTopology::new();
        assert!(!topology.is_initialized());
        assert_eq!(topology.node_count(), 0);
        assert_eq!(topology.master_count(), 0);
    }

    #[test]
    fn test_topology_from_ranges() {
        let master1: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let master2: SocketAddr = "127.0.0.1:7001".parse().unwrap();

        let ranges = vec![
            SlotRange {
                start: 0,
                end: 5460,
                master: master1,
                replicas: vec![],
            },
            SlotRange {
                start: 5461,
                end: 16383,
                master: master2,
                replicas: vec![],
            },
        ];

        let topology = ClusterTopology::from_slot_ranges(ranges);

        assert!(topology.is_initialized());
        assert_eq!(topology.node_count(), 2);
        assert_eq!(topology.master_count(), 2);
        assert_eq!(topology.get_node_for_slot(0), master1);
        assert_eq!(topology.get_node_for_slot(5460), master1);
        assert_eq!(topology.get_node_for_slot(5461), master2);
        assert_eq!(topology.get_node_for_slot(16383), master2);
    }

    #[test]
    fn test_topology_with_replicas() {
        let master1: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let master2: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        let replica1: SocketAddr = "127.0.0.1:7003".parse().unwrap();
        let replica2: SocketAddr = "127.0.0.1:7004".parse().unwrap();

        let ranges = vec![
            SlotRange {
                start: 0,
                end: 8191,
                master: master1,
                replicas: vec![replica1],
            },
            SlotRange {
                start: 8192,
                end: 16383,
                master: master2,
                replicas: vec![replica2],
            },
        ];

        let topology = ClusterTopology::from_slot_ranges(ranges);

        assert_eq!(topology.node_count(), 4); // 2 masters + 2 replicas
        assert_eq!(topology.master_count(), 2);

        let masters = topology.get_masters();
        assert_eq!(masters.len(), 2);
        assert!(masters.contains(&master1));
        assert!(masters.contains(&master2));

        let all_nodes = topology.get_all_nodes();
        assert_eq!(all_nodes.len(), 4);
    }

    #[test]
    fn test_topology_update_slot() {
        let master1: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let master2: SocketAddr = "127.0.0.1:7001".parse().unwrap();

        let mut topology = ClusterTopology::new();
        topology.update_slot(100, master1);

        assert_eq!(topology.get_node_for_slot(100), master1);

        // Simulate slot migration
        topology.update_slot(100, master2);
        assert_eq!(topology.get_node_for_slot(100), master2);

        assert_eq!(topology.node_count(), 2); // Both nodes registered
    }

    #[test]
    fn test_get_masters() {
        let master1: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let master2: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        let replica1: SocketAddr = "127.0.0.1:7003".parse().unwrap();

        let ranges = vec![
            SlotRange {
                start: 0,
                end: 8191,
                master: master1,
                replicas: vec![replica1],
            },
            SlotRange {
                start: 8192,
                end: 16383,
                master: master2,
                replicas: vec![],
            },
        ];

        let topology = ClusterTopology::from_slot_ranges(ranges);
        let masters = topology.get_masters();

        assert_eq!(masters.len(), 2);
        assert!(masters.contains(&master1));
        assert!(masters.contains(&master2));
        assert!(!masters.contains(&replica1));
    }

    #[test]
    fn test_three_node_cluster() {
        // Typical 3-node cluster with even slot distribution
        let master1: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let master2: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        let master3: SocketAddr = "127.0.0.1:7002".parse().unwrap();

        let ranges = vec![
            SlotRange {
                start: 0,
                end: 5460,
                master: master1,
                replicas: vec![],
            },
            SlotRange {
                start: 5461,
                end: 10922,
                master: master2,
                replicas: vec![],
            },
            SlotRange {
                start: 10923,
                end: 16383,
                master: master3,
                replicas: vec![],
            },
        ];

        let topology = ClusterTopology::from_slot_ranges(ranges);

        assert_eq!(topology.master_count(), 3);

        // Verify slot distribution
        assert_eq!(topology.get_node_for_slot(0), master1);
        assert_eq!(topology.get_node_for_slot(5460), master1);
        assert_eq!(topology.get_node_for_slot(5461), master2);
        assert_eq!(topology.get_node_for_slot(10922), master2);
        assert_eq!(topology.get_node_for_slot(10923), master3);
        assert_eq!(topology.get_node_for_slot(16383), master3);
    }

    #[test]
    fn test_topology_staleness() {
        let master: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let ranges = vec![SlotRange {
            start: 0,
            end: 16383,
            master,
            replicas: vec![],
        }];

        let topology = ClusterTopology::from_slot_ranges(ranges);

        // Should not be stale immediately
        assert!(!topology.is_stale(std::time::Duration::from_secs(60)));

        // Should be stale if threshold is 0
        assert!(topology.is_stale(std::time::Duration::from_secs(0)));
    }

    #[test]
    fn test_get_ranges() {
        let master: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let ranges = vec![SlotRange {
            start: 0,
            end: 16383,
            master,
            replicas: vec![],
        }];

        let topology = ClusterTopology::from_slot_ranges(ranges.clone());
        let retrieved = topology.get_ranges();

        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0], ranges[0]);
    }

    #[test]
    #[should_panic(expected = "Overlapping slot ranges detected")]
    fn test_overlapping_slots_rejected() {
        // Test that overlapping ranges are now rejected (fixed bug)
        let master1: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let master2: SocketAddr = "127.0.0.1:7001".parse().unwrap();

        let ranges = vec![
            SlotRange {
                start: 0,
                end: 100,
                master: master1,
                replicas: vec![],
            },
            SlotRange {
                start: 50,
                end: 150,
                master: master2,
                replicas: vec![],
            },
        ];

        // This should panic due to overlapping ranges
        let _topology = ClusterTopology::from_slot_ranges(ranges);
    }

    #[test]
    fn test_empty_ranges() {
        let topology = ClusterTopology::from_slot_ranges(vec![]);
        assert!(!topology.is_initialized());
        assert_eq!(topology.node_count(), 0);
    }

    #[test]
    fn test_single_slot_range() {
        let master: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let ranges = vec![SlotRange {
            start: 100,
            end: 100, // Single slot
            master,
            replicas: vec![],
        }];

        let topology = ClusterTopology::from_slot_ranges(ranges);
        assert_eq!(topology.get_node_for_slot(100), master);
    }

    #[test]
    fn test_age_tracking() {
        let master: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let ranges = vec![SlotRange {
            start: 0,
            end: 16383,
            master,
            replicas: vec![],
        }];

        let topology = ClusterTopology::from_slot_ranges(ranges);

        // Age should be very small (just created)
        let age = topology.age().unwrap();
        assert!(age < std::time::Duration::from_secs(1));
    }
}
