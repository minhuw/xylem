//! Redis Cluster protocol implementation with automatic routing
//!
//! This module provides `RedisClusterProtocol` which wraps `RedisProtocol`
//! and adds cluster-aware routing based on hash slots.

use super::redirect::{parse_redirect, RedirectType};
use super::topology::ClusterTopology;
use crate::redis::command_selector::CommandSelector;
use crate::redis::slot::calculate_slot;
use crate::redis::{RedisOp, RedisProtocol};
use crate::Protocol;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

/// Statistics for cluster redirects
#[derive(Debug, Default, Clone)]
pub struct RedirectStats {
    /// Number of MOVED redirects encountered
    pub moved_count: u64,
    /// Number of ASK redirects encountered
    pub ask_count: u64,
    /// Number of redirect loops prevented
    pub redirect_loops_prevented: u64,
}

/// Request ID for cluster protocol
///
/// Tracks: (original_conn_id, slot, (target_conn_id, sequence))
pub type ClusterRequestId = (usize, u16, (usize, u64));

/// Metadata for tracking requests (needed for retries)
#[derive(Debug, Clone, Copy)]
struct RequestMetadata {
    key: u64,
    value_size: usize,
    slot: u16,
    original_conn_id: usize,
}

/// Redis Cluster protocol with automatic slot-based routing
///
/// This protocol wraps `RedisProtocol` and adds cluster-aware routing:
/// - Calculates slot from key
/// - Routes requests to correct node
/// - Detects MOVED/ASK redirects
/// - Updates topology on MOVED
/// - Automatically retries redirects (Phase 7)
///
/// # Example
///
/// ```text
/// use xylem_protocols::redis::cluster::RedisClusterProtocol;
///
/// let mut protocol = RedisClusterProtocol::new(command_selector)?;
/// protocol.register_connection("127.0.0.1:7000".parse()?, 0);
/// protocol.register_connection("127.0.0.1:7001".parse()?, 1);
/// protocol.register_connection("127.0.0.1:7002".parse()?, 2);
///
/// // Requests automatically route to correct node
/// let (request, id) = protocol.generate_request(0, 1000, 100);
/// ```
pub struct RedisClusterProtocol {
    /// Underlying single-node Redis protocol
    base_protocol: RedisProtocol,

    /// Cluster topology (slot -> node mapping)
    topology: ClusterTopology,

    /// Maximum number of redirects before giving up (default 5)
    max_redirects: usize,

    /// Redirect statistics for monitoring
    redirect_stats: RedirectStats,

    /// Map connection ID to node address
    conn_to_node: HashMap<usize, SocketAddr>,

    /// Map node address to connection ID
    node_to_conn: HashMap<SocketAddr, usize>,

    /// Default node for requests if topology is empty
    default_node: Option<SocketAddr>,

    /// Track request metadata for retry support (Phase 7)
    /// Maps (conn_id, sequence) to request metadata
    request_metadata: HashMap<(usize, u64), RequestMetadata>,

    /// Track retry attempts per request to prevent loops
    /// Maps (conn_id, sequence) to retry count
    retry_counts: HashMap<(usize, u64), usize>,
}

impl RedisClusterProtocol {
    /// Create a new Redis Cluster protocol
    ///
    /// # Arguments
    ///
    /// * `command_selector` - Selector for Redis commands
    ///
    /// # Example
    ///
    /// ```text
    /// use xylem_protocols::redis::cluster::RedisClusterProtocol;
    /// use xylem_protocols::FixedCommandSelector;
    /// use xylem_protocols::RedisOp;
    ///
    /// let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
    /// let protocol = RedisClusterProtocol::new(selector);
    /// ```
    pub fn new(command_selector: Box<dyn CommandSelector<RedisOp>>) -> Self {
        Self {
            base_protocol: RedisProtocol::new(command_selector),
            topology: ClusterTopology::new(),
            max_redirects: 5,
            redirect_stats: RedirectStats::default(),
            conn_to_node: HashMap::new(),
            node_to_conn: HashMap::new(),
            default_node: None,
            request_metadata: HashMap::new(),
            retry_counts: HashMap::new(),
        }
    }

    /// Register a connection for a specific cluster node
    ///
    /// This establishes the mapping between connection IDs and cluster nodes.
    ///
    /// # Arguments
    ///
    /// * `node` - Node address (IP:port)
    /// * `conn_id` - Connection identifier
    ///
    /// # Example
    ///
    /// ```text
    /// protocol.register_connection("127.0.0.1:7000".parse()?, 0);
    /// protocol.register_connection("127.0.0.1:7001".parse()?, 1);
    /// ```
    pub fn register_connection(&mut self, node: SocketAddr, conn_id: usize) {
        self.conn_to_node.insert(conn_id, node);
        self.node_to_conn.insert(node, conn_id);

        // Set first registered node as default
        if self.default_node.is_none() {
            self.default_node = Some(node);
        }
    }

    /// Clear all connection registrations
    ///
    /// Used when re-wiring connections after pool creation.
    pub fn clear_connections(&mut self) {
        self.conn_to_node.clear();
        self.node_to_conn.clear();
        self.default_node = None;
    }

    /// Re-register connections from actual pool mappings
    ///
    /// Call this after ConnectionPool creation to wire up the actual connection IDs.
    ///
    /// # Arguments
    ///
    /// * `connections` - Iterator of (connection_id, target_address) pairs
    pub fn register_connections<I>(&mut self, connections: I)
    where
        I: IntoIterator<Item = (usize, SocketAddr)>,
    {
        self.clear_connections();
        for (conn_id, addr) in connections {
            self.register_connection(addr, conn_id);
        }
    }

    /// Update cluster topology with new slot ranges
    ///
    /// # Arguments
    ///
    /// * `topology` - New topology to use
    ///
    /// # Example
    ///
    /// ```text
    /// use xylem_protocols::ClusterTopology;
    ///
    /// let topology = ClusterTopology::from_slot_ranges(ranges);
    /// protocol.update_topology(topology);
    /// ```
    pub fn update_topology(&mut self, topology: ClusterTopology) {
        self.topology = topology;
    }

    /// Get redirect statistics
    ///
    /// Returns counts of MOVED and ASK redirects encountered.
    pub fn redirect_stats(&self) -> &RedirectStats {
        &self.redirect_stats
    }

    /// Get age of current topology
    ///
    /// Returns `Some(duration)` if topology has been updated, `None` if never updated.
    pub fn topology_age(&self) -> Option<Duration> {
        self.topology.age()
    }

    /// Get the target node for a given key
    ///
    /// Calculates the slot and looks up the responsible node.
    fn get_target_node(&self, key: &[u8]) -> Result<SocketAddr> {
        // Calculate slot
        let slot = calculate_slot(key);

        // Look up node in topology
        let node = self.topology.get_node_for_slot(slot);

        // Check if we have a connection to this node
        if !self.node_to_conn.contains_key(&node) {
            // Topology says to use this node, but we don't have a connection
            // This can happen if:
            // 1. Topology is stale
            // 2. Node just joined cluster
            // 3. We haven't connected to this node yet

            // Try default node as fallback
            if let Some(default) = self.default_node {
                return Ok(default);
            }

            return Err(anyhow!(
                "No connection available for node {} (slot {}). Known connections: {:?}",
                node,
                slot,
                self.node_to_conn.keys().collect::<Vec<_>>()
            ));
        }

        Ok(node)
    }

    /// Get connection ID for a node
    fn get_conn_id_for_node(&self, node: SocketAddr) -> Result<usize> {
        self.node_to_conn
            .get(&node)
            .copied()
            .ok_or_else(|| anyhow!("No connection for node {}", node))
    }

    /// Update redirect statistics based on redirect type
    fn update_redirect_stats(&mut self, redirect: &RedirectType) {
        match redirect {
            RedirectType::Moved { slot, addr } => {
                self.redirect_stats.moved_count += 1;
                self.topology.update_slot(*slot, *addr);
            }
            RedirectType::Ask { .. } => {
                self.redirect_stats.ask_count += 1;
            }
        }
    }

    /// Build a retry request from redirect information
    fn build_retry_request(
        &mut self,
        redirect: &RedirectType,
        req_id: (usize, u64),
        consumed: usize,
        retry_count: usize,
    ) -> Result<crate::RetryRequest<ClusterRequestId>> {
        // Get request metadata
        let metadata = self
            .request_metadata
            .get(&req_id)
            .copied()
            .ok_or_else(|| anyhow!("Missing metadata for request {:?}", req_id))?;

        let cluster_req_id = (metadata.original_conn_id, metadata.slot, req_id);

        match redirect {
            RedirectType::Moved { slot, addr } => {
                self.redirect_stats.moved_count += 1;
                self.topology.update_slot(*slot, *addr);

                let target_conn_id = self.node_to_conn.get(addr).copied();

                Ok(crate::RetryRequest {
                    bytes_consumed: consumed,
                    original_request_id: cluster_req_id,
                    key: metadata.key,
                    value_size: metadata.value_size,
                    target_conn_id,
                    prepare_commands: vec![],
                    attempt: retry_count,
                })
            }
            RedirectType::Ask { slot: _, addr } => {
                self.redirect_stats.ask_count += 1;

                let target_conn_id = self.node_to_conn.get(addr).copied();
                let asking_cmd = super::redirect::generate_asking_command();

                Ok(crate::RetryRequest {
                    bytes_consumed: consumed,
                    original_request_id: cluster_req_id,
                    key: metadata.key,
                    value_size: metadata.value_size,
                    target_conn_id,
                    prepare_commands: vec![asking_cmd],
                    attempt: retry_count,
                })
            }
        }
    }

    /// Handle a redirect by updating stats and topology
    fn handle_redirect(
        &mut self,
        redirect: RedirectType,
    ) -> Result<(usize, Option<ClusterRequestId>)> {
        match redirect {
            RedirectType::Moved { slot, addr } => {
                self.redirect_stats.moved_count += 1;

                // Update topology: this slot now belongs to the new node
                self.topology.update_slot(slot, addr);

                Err(anyhow!("MOVED redirect: slot {} moved to {} (topology updated)", slot, addr))
            }
            RedirectType::Ask { slot, addr } => {
                self.redirect_stats.ask_count += 1;

                // ASK is temporary - don't update topology
                Err(anyhow!(
                    "ASK redirect: slot {} temporarily at {} (no topology update)",
                    slot,
                    addr
                ))
            }
        }
    }
}

impl Protocol for RedisClusterProtocol {
    type RequestId = ClusterRequestId;

    fn next_request(&mut self, conn_id: usize) -> (Vec<u8>, Self::RequestId) {
        // Delegate to base protocol's key generation then route
        let key = self.base_protocol.key_gen_mut().map(|g| g.next_key()).unwrap_or(0);
        let value_size = self.base_protocol.value_size();
        self.generate_request(conn_id, key, value_size)
    }

    fn generate_request(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
    ) -> (Vec<u8>, Self::RequestId) {
        // Format key as "key:{n}"
        let key_str = format!("key:{}", key);
        let key_bytes = key_str.as_bytes();

        // Calculate slot for this key
        let slot = calculate_slot(key_bytes);

        // Determine target node
        let target_node = match self.get_target_node(key_bytes) {
            Ok(node) => node,
            Err(e) => {
                // If we can't determine target, use the provided conn_id
                // This will likely result in a redirect, which is fine
                eprintln!("Warning: {}, using provided conn_id {}", e, conn_id);
                match self.conn_to_node.get(&conn_id) {
                    Some(&node) => node,
                    None => {
                        // Last resort: use default node or first available
                        self.default_node
                            .or_else(|| self.conn_to_node.values().next().copied())
                            .unwrap_or_else(|| {
                                eprintln!("No nodes available, using dummy address");
                                "127.0.0.1:6379".parse().unwrap()
                            })
                    }
                }
            }
        };

        // Get connection ID for target node
        let target_conn_id = match self.get_conn_id_for_node(target_node) {
            Ok(id) => id,
            Err(_) => {
                // Fallback to provided conn_id
                conn_id
            }
        };

        // Generate request using base protocol
        let (request_data, base_req_id) =
            self.base_protocol.generate_request(target_conn_id, key, value_size);

        // Store request metadata for potential retry (Phase 7)
        let metadata = RequestMetadata {
            key,
            value_size,
            slot,
            original_conn_id: conn_id,
        };
        self.request_metadata.insert(base_req_id, metadata);

        // Create cluster request ID: (original_conn_id, slot, base_req_id)
        let cluster_req_id = (conn_id, slot, base_req_id);

        (request_data, cluster_req_id)
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)> {
        // Parse using base protocol
        let result = self.base_protocol.parse_response(conn_id, data);

        match result {
            Ok((consumed, Some((target_conn, seq)))) => {
                // Success - construct cluster request ID
                // We need the original conn_id and slot, but we don't have them here
                // For now, use conn_id as original and slot 0 as placeholder
                // This is a limitation of the current design - Phase 5 will track this properly
                let cluster_req_id = (conn_id, 0, (target_conn, seq));
                Ok((consumed, Some(cluster_req_id)))
            }
            Ok((consumed, None)) => {
                // Incomplete response
                Ok((consumed, None))
            }
            Err(e) => {
                // Check if this is a redirect error
                let error_msg = e.to_string();

                // Try to parse as redirect
                if error_msg.contains("MOVED") || error_msg.contains("ASK") {
                    // Extract redirect from error message
                    // The error contains the raw bytes, try to parse
                    if let Ok(Some(redirect)) = parse_redirect(data) {
                        return self.handle_redirect(redirect);
                    }
                }

                // Not a redirect, pass through the error
                Err(e)
            }
        }
    }

    fn parse_response_extended(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<crate::ParseResult<Self::RequestId>> {
        // Parse using base protocol
        let result = self.base_protocol.parse_response(conn_id, data);

        match result {
            Ok((consumed, Some((target_conn, seq)))) => {
                // Success - construct cluster request ID and clean up metadata
                let req_id = (target_conn, seq);
                let metadata = self.request_metadata.remove(&req_id);

                let cluster_req_id = if let Some(meta) = metadata {
                    (meta.original_conn_id, meta.slot, (target_conn, seq))
                } else {
                    // Fallback if metadata missing
                    (conn_id, 0, (target_conn, seq))
                };

                // Clean up retry count for completed request
                self.retry_counts.remove(&req_id);

                Ok(crate::ParseResult::Complete {
                    bytes_consumed: consumed,
                    request_id: cluster_req_id,
                })
            }
            Ok((_, None)) => {
                // Incomplete response
                Ok(crate::ParseResult::Incomplete)
            }
            Err(e) => {
                // Check if this is a redirect error
                let error_msg = e.to_string();

                // Try to parse as redirect
                let is_redirect = error_msg.contains("MOVED") || error_msg.contains("ASK");
                if !is_redirect {
                    return Err(e);
                }

                let Some(redirect) = parse_redirect(data)? else {
                    return Err(e);
                };

                // Try parsing again to get the request ID
                let Ok((consumed, Some((target_conn, seq)))) =
                    self.base_protocol.parse_response(conn_id, data)
                else {
                    // Couldn't parse request ID - update stats and return error
                    self.update_redirect_stats(&redirect);
                    return Err(e);
                };

                let req_id = (target_conn, seq);

                // Get or create retry count
                let retry_count = self.retry_counts.entry(req_id).or_insert(0);

                // Check retry budget
                if *retry_count >= self.max_redirects {
                    self.redirect_stats.redirect_loops_prevented += 1;
                    self.retry_counts.remove(&req_id);
                    self.request_metadata.remove(&req_id);
                    return Err(anyhow::anyhow!(
                        "Max redirect limit ({}) exceeded for request",
                        self.max_redirects
                    ));
                }

                // Increment retry count
                *retry_count += 1;
                let current_retry = *retry_count - 1;

                // Build retry request using helper
                match self.build_retry_request(&redirect, req_id, consumed, current_retry) {
                    Ok(retry_req) => Ok(crate::ParseResult::Retry(retry_req)),
                    Err(_) => {
                        // Couldn't build retry - update stats and return error
                        self.update_redirect_stats(&redirect);
                        Err(e)
                    }
                }
            }
        }
    }

    fn name(&self) -> &'static str {
        "redis-cluster"
    }

    fn reset(&mut self) {
        self.base_protocol.reset();
        self.redirect_stats = RedirectStats::default();
        self.request_metadata.clear();
        self.retry_counts.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis::cluster::topology::SlotRange;
    use crate::redis::command_selector::FixedCommandSelector;
    use crate::redis::RedisOp;

    fn create_test_protocol() -> RedisClusterProtocol {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        RedisClusterProtocol::new(selector)
    }

    fn create_3_node_topology() -> ClusterTopology {
        let ranges = vec![
            SlotRange {
                start: 0,
                end: 5460,
                master: "127.0.0.1:7000".parse().unwrap(),
                replicas: vec![],
            },
            SlotRange {
                start: 5461,
                end: 10922,
                master: "127.0.0.1:7001".parse().unwrap(),
                replicas: vec![],
            },
            SlotRange {
                start: 10923,
                end: 16383,
                master: "127.0.0.1:7002".parse().unwrap(),
                replicas: vec![],
            },
        ];
        ClusterTopology::from_slot_ranges(ranges)
    }

    #[test]
    fn test_protocol_creation() {
        let protocol = create_test_protocol();
        assert_eq!(protocol.name(), "redis-cluster");
        assert_eq!(protocol.redirect_stats().moved_count, 0);
        assert_eq!(protocol.redirect_stats().ask_count, 0);
    }

    #[test]
    fn test_connection_registration() {
        let mut protocol = create_test_protocol();

        let node1: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let node2: SocketAddr = "127.0.0.1:7001".parse().unwrap();

        protocol.register_connection(node1, 0);
        protocol.register_connection(node2, 1);

        assert_eq!(protocol.conn_to_node.len(), 2);
        assert_eq!(protocol.node_to_conn.len(), 2);
        assert_eq!(protocol.conn_to_node[&0], node1);
        assert_eq!(protocol.node_to_conn[&node1], 0);
    }

    #[test]
    fn test_topology_update() {
        let mut protocol = create_test_protocol();
        let topology = create_3_node_topology();

        protocol.update_topology(topology);

        assert!(protocol.topology_age().is_some());
    }

    #[test]
    fn test_request_generation() {
        let mut protocol = create_test_protocol();

        // Register connections
        protocol.register_connection("127.0.0.1:7000".parse().unwrap(), 0);

        // Generate request
        let (request_data, req_id) = protocol.generate_request(0, 1000, 100);

        // Verify request data is not empty
        assert!(!request_data.is_empty());

        // Verify request ID structure
        let (orig_conn, slot, (_target_conn, _seq)) = req_id;
        assert_eq!(orig_conn, 0);
        // slot should be calculated from "key:1000"
        assert!(slot < 16384);
    }

    #[test]
    fn test_request_routing_with_topology() {
        let mut protocol = create_test_protocol();
        let topology = create_3_node_topology();
        protocol.update_topology(topology);

        // Register all connections
        protocol.register_connection("127.0.0.1:7000".parse().unwrap(), 0);
        protocol.register_connection("127.0.0.1:7001".parse().unwrap(), 1);
        protocol.register_connection("127.0.0.1:7002".parse().unwrap(), 2);

        // Generate request and verify slot calculation
        let (_, req_id1) = protocol.generate_request(99, 1000, 100);
        let (_, slot1, (target_conn1, _)) = req_id1;
        assert_eq!(slot1, calculate_slot(b"key:1000"));

        // Verify it routes to the correct node based on topology
        // Slot 1649 is in range 0-5460, so should route to node 7000 (conn 0)
        if slot1 <= 5460 {
            assert_eq!(target_conn1, 0);
        } else if slot1 <= 10922 {
            assert_eq!(target_conn1, 1);
        } else {
            assert_eq!(target_conn1, 2);
        }

        // Different key should potentially route to different node
        let (_, req_id2) = protocol.generate_request(99, 9999, 100);
        let (_, slot2, _) = req_id2;
        assert_eq!(slot2, calculate_slot(b"key:9999"));

        // Verify slot2 is also routed correctly
        assert!(slot2 < 16384);
    }

    #[test]
    fn test_redirect_stats() {
        let protocol = create_test_protocol();
        let stats = protocol.redirect_stats();

        assert_eq!(stats.moved_count, 0);
        assert_eq!(stats.ask_count, 0);
        assert_eq!(stats.redirect_loops_prevented, 0);
    }

    #[test]
    fn test_reset() {
        let mut protocol = create_test_protocol();

        // Generate a request to change state
        protocol.register_connection("127.0.0.1:7000".parse().unwrap(), 0);
        protocol.generate_request(0, 1000, 100);

        // Reset
        protocol.reset();

        // Stats should be reset
        assert_eq!(protocol.redirect_stats().moved_count, 0);
    }

    #[test]
    fn test_default_node_fallback() {
        let mut protocol = create_test_protocol();

        // Register only one connection
        let node: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        protocol.register_connection(node, 0);

        // Don't set topology - should use default node
        let (request, req_id) = protocol.generate_request(0, 1000, 100);

        // Should still generate valid request
        assert!(!request.is_empty());

        // Should use the registered connection
        let (_, _, (target_conn, _)) = req_id;
        assert_eq!(target_conn, 0);
    }
}
