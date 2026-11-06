//! Redis Cluster protocol implementation with automatic routing
//!
//! This module provides `RedisClusterProtocol` which wraps `RedisProtocol`
//! and adds cluster-aware routing based on hash slots.

use super::redirect::RedirectType;
use super::topology::ClusterTopology;
use crate::redis::command_selector::CommandSelector;
use crate::redis::slot::calculate_slot;
use crate::redis::{RedisOp, RedisProtocol, ReplyKind};
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
#[derive(Debug, Clone)]
struct RequestMetadata {
    data: Vec<u8>,
    needs_asking: bool,
    slot: u16,
    original_conn_id: usize,
    is_warmup: bool,
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
/// let request = protocol.generate_request_with_key(0, 1000, 100);
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
    asking_replies: std::collections::HashSet<(usize, u64)>,
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
        Self::from_base_protocol(RedisProtocol::new(command_selector))
    }

    /// Wrap a configured Redis protocol with cluster routing.
    pub fn from_base_protocol(base_protocol: RedisProtocol) -> Self {
        Self {
            base_protocol,
            topology: ClusterTopology::new(),
            max_redirects: 5,
            redirect_stats: RedirectStats::default(),
            node_to_conn: HashMap::new(),
            default_node: None,
            request_metadata: HashMap::new(),
            retry_counts: HashMap::new(),
            asking_replies: std::collections::HashSet::new(),
        }
    }

    /// Create with embedded workload generator
    pub fn with_workload(
        command_selector: Box<dyn CommandSelector<RedisOp>>,
        key_gen: crate::workload::KeyGeneration,
        value_size: usize,
    ) -> Self {
        Self::with_workload_and_options(
            command_selector,
            key_gen,
            value_size,
            "key:".to_string(),
            false,
            None,
        )
    }

    /// Create with embedded workload generator and custom options
    pub fn with_workload_and_options(
        command_selector: Box<dyn CommandSelector<RedisOp>>,
        key_gen: crate::workload::KeyGeneration,
        value_size: usize,
        key_prefix: String,
        random_data: bool,
        seed: Option<u64>,
    ) -> Self {
        Self {
            base_protocol: RedisProtocol::with_workload_and_options(
                command_selector,
                key_gen,
                value_size,
                key_prefix,
                random_data,
                seed,
            ),
            topology: ClusterTopology::new(),
            max_redirects: 5,
            redirect_stats: RedirectStats::default(),
            node_to_conn: HashMap::new(),
            default_node: None,
            request_metadata: HashMap::new(),
            retry_counts: HashMap::new(),
            asking_replies: std::collections::HashSet::new(),
        }
    }

    /// Set up insert phase for data population before measurement
    pub fn with_insert_phase(mut self, key_count: u64, value_size: usize) -> Self {
        self.base_protocol = self.base_protocol.with_insert_phase(key_count, value_size);
        self
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
    ///
    /// This function handles both stats updates and topology changes.
    fn build_retry_request(
        &mut self,
        redirect: &RedirectType,
        req_id: (usize, u64),
        consumed: usize,
        retry_count: usize,
    ) -> Result<crate::RetryRequest<ClusterRequestId>> {
        let metadata = self
            .request_metadata
            .get_mut(&req_id)
            .ok_or_else(|| anyhow!("Missing metadata for request {:?}", req_id))?;
        metadata.needs_asking = matches!(redirect, RedirectType::Ask { .. });
        let original_request_id = (metadata.original_conn_id, metadata.slot, req_id);
        let is_warmup = metadata.is_warmup;
        let (addr, prepare_commands) = match redirect {
            RedirectType::Moved { addr, .. } => (addr, vec![]),
            RedirectType::Ask { addr, .. } => {
                (addr, vec![super::redirect::generate_asking_command()])
            }
        };
        let target_conn_id = Some(self.get_conn_id_for_node(*addr)?);
        self.update_redirect_stats(redirect);
        Ok(crate::RetryRequest {
            bytes_consumed: consumed,
            original_request_id,
            is_warmup,
            target_conn_id,
            prepare_commands,
            attempt: retry_count,
        })
    }
}

impl Protocol for RedisClusterProtocol {
    type RequestId = ClusterRequestId;

    fn next_request(&mut self, conn_id: usize) -> crate::Request<Self::RequestId> {
        self.next_request_impl(conn_id, true)
    }

    fn regenerate_request(
        &mut self,
        conn_id: usize,
        original_request_id: Self::RequestId,
    ) -> crate::Request<Self::RequestId> {
        let (orig_conn, slot, base_req_id) = original_request_id;
        let retry_count = self.retry_counts.remove(&base_req_id);
        // Retry callers must supply an outstanding request returned by this protocol.
        let meta = self
            .request_metadata
            .remove(&base_req_id)
            .expect("Cannot retry a Redis request without its original payload");
        let new_base_req_id = (conn_id, self.base_protocol.next_send_seq(conn_id));
        let request = crate::Request::new(
            meta.data.clone(),
            (orig_conn, slot, new_base_req_id),
            crate::RequestMeta { is_warmup: meta.is_warmup },
        );
        if meta.needs_asking {
            self.asking_replies.insert(new_base_req_id);
        }
        self.request_metadata.insert(new_base_req_id, meta);
        if let Some(count) = retry_count {
            self.retry_counts.insert(new_base_req_id, count);
        }
        request
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)> {
        // Delegate to internal implementation
        self.parse_response_impl(conn_id, data)
    }

    fn parse_response_extended(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<crate::ParseResult<Self::RequestId>> {
        // Delegate to internal implementation
        self.parse_response_extended_impl(conn_id, data)
    }

    fn name(&self) -> &'static str {
        "redis-cluster"
    }

    fn reset(&mut self) {
        self.base_protocol.reset();
        self.redirect_stats = RedirectStats::default();
        self.request_metadata.clear();
        self.retry_counts.clear();
        self.asking_replies.clear();
    }
}

impl RedisClusterProtocol {
    /// Generate for a connection chosen by a worker that sends on that exact
    /// socket. MOVED/ASK responses still resolve to registered retry targets.
    /// The routing helpers and Protocol::next_request retain slot-based routing.
    pub fn next_request_on_connection(
        &mut self,
        conn_id: usize,
    ) -> crate::Request<ClusterRequestId> {
        self.next_request_impl(conn_id, false)
    }

    fn next_request_impl(
        &mut self,
        conn_id: usize,
        route_by_slot: bool,
    ) -> crate::Request<ClusterRequestId> {
        // Phase 1: Insert phase (warmup) - populate data before measurement
        if let Some(ref mut insert_state) = self.base_protocol.insert_phase {
            if let Some(key) = insert_state.next_key() {
                let value_size = insert_state.value_size;
                let mut request = self.generate_request_with_key_and_operation_impl(
                    conn_id,
                    key,
                    value_size,
                    &RedisOp::Set,
                    route_by_slot,
                );
                self.mark_request_warmup(request.request_id);
                request.metadata.is_warmup = true;
                return request;
            }
        }

        // Phase 2: Normal measurement phase
        let value_size = self.base_protocol.value_size();

        let request = if self.base_protocol.command_selector.has_per_command_keys() {
            let operation = self.base_protocol.command_selector.next_command();
            let key = self
                .base_protocol
                .command_selector
                .generate_key_for_command(&operation)
                .expect("Per-command key generation should be available");

            let value_size = self.base_protocol.size_for_operation(&operation, value_size);
            self.generate_request_with_key_and_operation_impl(
                conn_id,
                key,
                value_size,
                &operation,
                route_by_slot,
            )
        } else {
            let key = self.base_protocol.key_gen_mut().map(|g| g.next_key()).unwrap_or(0);
            self.generate_request_with_key_impl(conn_id, key, value_size, route_by_slot)
        };

        request
    }

    fn build_cluster_request<F>(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
        route_by_slot: bool,
        build_request: F,
    ) -> (Vec<u8>, ClusterRequestId)
    where
        F: FnOnce(&mut RedisProtocol, usize, u64, usize) -> (Vec<u8>, (usize, u64)),
    {
        let key_str = self.base_protocol.format_key(key);
        let key_bytes = key_str.as_bytes();

        // Calculate slot for this key
        let slot = calculate_slot(key_bytes);

        // Generic workers send on the socket they selected. Routing-aware
        // callers may instead use the target encoded in the request ID.
        let target_conn_id = if route_by_slot {
            self.get_target_node(key_bytes)
                .and_then(|node| self.get_conn_id_for_node(node))
                .unwrap_or(conn_id)
        } else {
            conn_id
        };

        // Generate request using provided builder
        let (request_data, base_req_id) =
            build_request(&mut self.base_protocol, target_conn_id, key, value_size);

        // Store request metadata for potential retry (Phase 7)
        let metadata = RequestMetadata {
            data: request_data.clone(),
            needs_asking: false,
            slot,
            original_conn_id: conn_id,
            is_warmup: false,
        };
        self.request_metadata.insert(base_req_id, metadata);

        // Create cluster request ID: (original_conn_id, slot, base_req_id)
        let cluster_req_id = (conn_id, slot, base_req_id);

        (request_data, cluster_req_id)
    }

    fn mark_request_warmup(&mut self, cluster_req_id: ClusterRequestId) {
        let base_req_id = cluster_req_id.2;
        if let Some(meta) = self.request_metadata.get_mut(&base_req_id) {
            meta.is_warmup = true;
        }
    }

    /// Generate a request with specific key and value size
    ///
    /// This is used for testing and for routing requests with known keys.
    pub fn generate_request_with_key(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
    ) -> crate::Request<ClusterRequestId> {
        self.generate_request_with_key_impl(conn_id, key, value_size, true)
    }

    fn generate_request_with_key_impl(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
        route_by_slot: bool,
    ) -> crate::Request<ClusterRequestId> {
        let request = self.build_cluster_request(
            conn_id,
            key,
            value_size,
            route_by_slot,
            |protocol, target_conn_id, key, value_size| {
                let req = protocol.generate_request_with_key(target_conn_id, key, value_size);
                (req.data, req.request_id)
            },
        );
        crate::Request::measurement(request.0, request.1)
    }

    fn generate_request_with_key_and_operation_impl(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
        operation: &RedisOp,
        route_by_slot: bool,
    ) -> crate::Request<ClusterRequestId> {
        let request = self.build_cluster_request(
            conn_id,
            key,
            value_size,
            route_by_slot,
            |protocol, target_conn_id, key, value_size| {
                let req =
                    protocol.generate_request_for_op(target_conn_id, key, value_size, operation);
                (req.data, req.request_id)
            },
        );
        crate::Request::measurement(request.0, request.1)
    }

    fn parse_response_impl(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<ClusterRequestId>)> {
        match self.parse_response_extended_impl(conn_id, data)? {
            crate::ParseResult::Complete { bytes_consumed, request_id } => {
                Ok((bytes_consumed, Some(request_id)))
            }
            crate::ParseResult::Incomplete => Ok((0, None)),
            crate::ParseResult::Retry(retry) => {
                self.request_metadata.remove(&retry.original_request_id.2);
                self.retry_counts.remove(&retry.original_request_id.2);
                Err(anyhow!("Redis cluster redirect requires parse_response_extended"))
            }
        }
    }

    fn parse_response_extended_impl(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<crate::ParseResult<ClusterRequestId>> {
        // ASKING's reply has no request ID. Consume it together with the retry's
        // response so the public ParseResult needs no new variant.
        let expected = (conn_id, *self.base_protocol.conn_recv_seq.get(&conn_id).unwrap_or(&0));
        let prefix = if self.asking_replies.contains(&expected) {
            let size = RedisProtocol::find_resp_message_end(data)?;
            if size == 0 {
                return Ok(crate::ParseResult::Incomplete);
            }
            anyhow::ensure!(&data[..size] == b"+OK\r\n", "ASKING failed");
            size
        } else {
            0
        };
        let Some(mut reply) = self.base_protocol.parse_framed_reply(conn_id, &data[prefix..])?
        else {
            return Ok(crate::ParseResult::Incomplete);
        };
        reply.consumed += prefix;
        self.asking_replies.remove(&expected);
        let req_id = reply.id;
        match reply.kind {
            ReplyKind::Success => {
                let metadata = self.request_metadata.remove(&req_id);
                self.retry_counts.remove(&req_id);
                let request_id = metadata
                    .map(|meta| (meta.original_conn_id, meta.slot, req_id))
                    .unwrap_or((conn_id, 0, req_id));
                Ok(crate::ParseResult::Complete {
                    bytes_consumed: reply.consumed,
                    request_id,
                })
            }
            ReplyKind::Redirect(redirect) => {
                let retry_count = self.retry_counts.entry(req_id).or_insert(0);
                if *retry_count >= self.max_redirects {
                    self.redirect_stats.redirect_loops_prevented += 1;
                    self.retry_counts.remove(&req_id);
                    self.request_metadata.remove(&req_id);
                    return Err(anyhow!(
                        "Max redirect limit ({}) exceeded for request",
                        self.max_redirects
                    ));
                }
                let attempt = *retry_count;
                *retry_count += 1;
                match self.build_retry_request(&redirect, req_id, reply.consumed, attempt) {
                    Ok(retry) => Ok(crate::ParseResult::Retry(retry)),
                    Err(error) => {
                        self.request_metadata.remove(&req_id);
                        self.retry_counts.remove(&req_id);
                        self.update_redirect_stats(&redirect);
                        Err(error)
                    }
                }
            }
            ReplyKind::Error(error) => {
                self.request_metadata.remove(&req_id);
                self.retry_counts.remove(&req_id);
                Err(anyhow!("Redis error: {}", error))
            }
        }
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
    fn retry_to_unregistered_target_releases_metadata() {
        for response in [b"-MOVED 1 127.0.0.1:7999\r\n".as_slice(), b"-ASK 1 127.0.0.1:7999\r\n"] {
            let mut protocol = create_test_protocol();
            protocol.register_connection("127.0.0.1:7000".parse().unwrap(), 0);
            protocol.next_request(0);
            let error = protocol.parse_response_extended(0, response).unwrap_err();
            assert!(error.to_string().contains("127.0.0.1:7999"));
            assert_eq!(
                protocol.redirect_stats.moved_count,
                u64::from(response.starts_with(b"-MOVED"))
            );
            assert_eq!(protocol.redirect_stats.ask_count, u64::from(response.starts_with(b"-ASK")));
            assert!(protocol.request_metadata.is_empty());
            assert!(protocol.retry_counts.is_empty());
            assert!(protocol.asking_replies.is_empty());
        }
    }

    #[test]
    fn retry_metadata_is_released_on_completion_error_and_budget_exhaustion() {
        for response in [b"+OK\r\n".as_slice(), b"-ERR failed\r\n", b"-MOVED 1 127.0.0.1:7000\r\n"]
        {
            let mut protocol = create_test_protocol();
            protocol.register_connection("127.0.0.1:7000".parse().unwrap(), 0);
            protocol.max_redirects = 1;
            let original = protocol.next_request(0);
            assert!(matches!(
                protocol.parse_response_extended(0, b"-ASK 1 127.0.0.1:7000\r\n").unwrap(),
                crate::ParseResult::Retry(_)
            ));
            let retry = protocol.regenerate_request(0, original.request_id);
            assert!(!protocol.request_metadata.contains_key(&original.request_id.2));
            let result =
                protocol.parse_response_extended(0, &[b"+OK\r\n".as_slice(), response].concat());
            if response == b"+OK\r\n" {
                assert!(
                    matches!(result.unwrap(), crate::ParseResult::Complete { request_id, .. } if request_id == retry.request_id)
                );
            } else {
                assert!(result.is_err());
            }
            assert!(protocol.request_metadata.is_empty());
            assert!(protocol.retry_counts.is_empty());
            assert!(protocol.asking_replies.is_empty());
        }
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

        assert_eq!(protocol.node_to_conn.len(), 2);
        assert_eq!(protocol.node_to_conn[&node1], 0);
        assert_eq!(protocol.node_to_conn[&node2], 1);
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
        let request = protocol.generate_request_with_key(0, 1000, 100);

        // Verify request data is not empty
        assert!(!request.data.is_empty());

        // Verify request ID structure
        let (orig_conn, slot, (_target_conn, _seq)) = request.request_id;
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
        let request = protocol.generate_request_with_key(99, 1000, 100);
        let (_, slot1, (target_conn1, _)) = request.request_id;
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
        let request2 = protocol.generate_request_with_key(99, 9999, 100);
        let (_, slot2, _) = request2.request_id;
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
        protocol.generate_request_with_key(0, 1000, 100);

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
        let request = protocol.generate_request_with_key(0, 1000, 100);

        // Should still generate valid request
        assert!(!request.data.is_empty());

        // Should use the registered connection
        let (_, _, (target_conn, _)) = request.request_id;
        assert_eq!(target_conn, 0);
    }
}
