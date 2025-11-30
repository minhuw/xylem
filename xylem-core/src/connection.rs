//! Connection management with pipelining support
//!
//! This module implements Lancet-style connection pooling, allowing multiple
//! outstanding requests per connection for higher throughput.
//!
//! Uses `ConnectionGroup` internally for efficient I/O multiplexing - a single
//! poll syscall checks all connections in the pool.

use crate::scheduler::{Policy, ReadyHeap};
use crate::Result;
use std::collections::HashMap;
use std::hash::Hash;
use std::net::SocketAddr;
use std::time::Duration;
use xylem_transport::{ConnectionGroup, GroupConnection, Timestamp, TransportFactory};

/// Maximum receive buffer size per connection
/// Increased to support large values (e.g., YCSB workloads with 10KB+ values)
/// With RESP protocol overhead, 64KB supports ~50KB values
const MAX_PAYLOAD: usize = 65536; // 64KB

/// Traffic group configuration: (group_id, target, conn_count, max_pending_per_conn, policy_scheduler)
pub type TrafficGroupConfig =
    (usize, SocketAddr, usize, usize, Box<dyn crate::scheduler::PolicyScheduler>);

/// Pending request tracking
#[derive(Debug)]
struct PendingRequest {
    /// Timestamp when request was sent
    send_ts: Timestamp,
}

/// Per-connection application state (no transport - I/O goes through ConnectionGroup)
pub struct ConnectionState<ReqId: Eq + Hash + Clone> {
    /// Connection ID in the ConnectionGroup
    connection_id: usize,
    /// Target address
    target: SocketAddr,
    /// Maximum pending requests allowed
    max_pending_requests: usize,
    /// Map of request ID to pending request timestamps
    pending_map: HashMap<ReqId, PendingRequest>,
    /// Receive buffer for partial responses
    recv_buffer: Vec<u8>,
    /// Current position in receive buffer
    buffer_pos: usize,
    /// Whether connection is closed
    closed: bool,
    /// Traffic policy for this connection
    policy: Box<dyn Policy>,
    /// Traffic group ID this connection belongs to
    group_id: usize,
    /// Timestamp (in nanoseconds) when this connection was last used for sending
    last_active: u64,
}

impl<ReqId: Eq + Hash + Clone + std::fmt::Debug> ConnectionState<ReqId> {
    /// Create new connection state
    fn new(
        connection_id: usize,
        target: SocketAddr,
        max_pending_requests: usize,
        policy: Box<dyn Policy>,
        group_id: usize,
    ) -> Self {
        Self {
            connection_id,
            target,
            max_pending_requests,
            pending_map: HashMap::with_capacity(max_pending_requests),
            recv_buffer: vec![0u8; MAX_PAYLOAD],
            buffer_pos: 0,
            closed: false,
            policy,
            group_id,
            last_active: 0,
        }
    }

    /// Check if this connection can accept more requests
    pub fn can_send(&self) -> bool {
        !self.closed && self.pending_map.len() < self.max_pending_requests
    }

    /// Get connection ID
    pub fn connection_id(&self) -> usize {
        self.connection_id
    }

    /// Get target address
    pub fn target(&self) -> SocketAddr {
        self.target
    }

    /// Get pending request count
    pub fn pending_count(&self) -> usize {
        self.pending_map.len()
    }

    /// Check if connection is closed
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Get the traffic group ID
    pub fn group_id(&self) -> usize {
        self.group_id
    }

    /// Get the time (in nanoseconds) when this connection should send its next request
    pub fn next_send_time(&mut self, current_time_ns: u64) -> Option<u64> {
        self.policy.next_send_time(current_time_ns)
    }

    /// Notify the policy that a request was sent
    pub fn on_request_sent(&mut self, sent_time_ns: u64) {
        self.policy.on_request_sent(sent_time_ns);
    }

    /// Reset the policy state
    pub fn reset_policy(&mut self) {
        self.policy.reset();
    }

    /// Get the policy name (for debugging)
    pub fn policy_name(&self) -> &'static str {
        self.policy.name()
    }

    /// Record a sent request
    fn record_send(&mut self, req_id: ReqId, send_ts: Timestamp) {
        self.pending_map.insert(req_id, PendingRequest { send_ts });
    }

    /// Process received data and extract latencies
    fn process_recv<F>(
        &mut self,
        data: &[u8],
        recv_ts: Timestamp,
        mut process_fn: F,
    ) -> Result<Vec<(ReqId, Duration)>>
    where
        F: FnMut(&[u8]) -> Result<(usize, Option<ReqId>)>,
    {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        // Append to buffer
        let data_len = data.len();
        if self.buffer_pos + data_len > MAX_PAYLOAD {
            return Err(crate::Error::Protocol(
                "Response exceeds maximum payload size".to_string(),
            ));
        }

        self.recv_buffer[self.buffer_pos..self.buffer_pos + data_len].copy_from_slice(data);
        self.buffer_pos += data_len;

        // Process complete responses in buffer
        let mut latencies = Vec::new();
        loop {
            let (consumed, req_id_opt) = process_fn(&self.recv_buffer[..self.buffer_pos])?;

            if consumed == 0 {
                break;
            }

            let req_id = req_id_opt.ok_or_else(|| {
                crate::Error::Protocol("Protocol returned None for request ID".to_string())
            })?;

            if let Some(pending) = self.pending_map.remove(&req_id) {
                let latency = recv_ts.duration_since(&pending.send_ts);
                latencies.push((req_id, latency));
            } else {
                return Err(crate::Error::Protocol(format!(
                    "Received response for unknown request ID: {req_id:?}"
                )));
            }

            // Remove consumed bytes from buffer
            if consumed == self.buffer_pos {
                self.buffer_pos = 0;
                break;
            } else if consumed < self.buffer_pos {
                let remaining = self.buffer_pos - consumed;
                self.recv_buffer.copy_within(consumed..self.buffer_pos, 0);
                self.buffer_pos = remaining;
            } else {
                return Err(crate::Error::Protocol(
                    "Response parser consumed more bytes than available".to_string(),
                ));
            }
        }

        Ok(latencies)
    }
}

/// Connection pool managing multiple connections with per-connection policies
///
/// Uses a single `ConnectionGroup` internally for efficient I/O multiplexing.
/// One `poll()` syscall checks all connections instead of O(N) syscalls.
///
/// Connection scheduling uses a min-heap (`TemporalScheduler`) for O(log N)
/// `pick_connection()` instead of O(N) iteration.
pub struct ConnectionPool<G: ConnectionGroup, ReqId: Eq + Hash + Clone> {
    /// Connection group for I/O (single multiplexer for all connections)
    group: G,
    /// Per-connection application state
    connection_states: HashMap<usize, ConnectionState<ReqId>>,
    /// Ordered list of connection IDs for iteration
    connection_ids: Vec<usize>,
    /// Min-heap for O(log N) lookup of next ready connection
    ready_heap: ReadyHeap,
}

impl<G: ConnectionGroup, ReqId: Eq + Hash + Clone + std::fmt::Debug> ConnectionPool<G, ReqId> {
    /// Create a new connection pool with per-connection policies for a single group
    ///
    /// # Parameters
    /// - `factory`: Transport factory to create the connection group
    /// - `target`: Target server address
    /// - `conn_count`: Number of connections to create
    /// - `max_pending_per_conn`: Maximum pending requests per connection
    /// - `policy_scheduler`: Scheduler that assigns policies to connections
    /// - `group_id`: Traffic group ID for all connections in this pool
    pub fn new<F: TransportFactory<Group = G>>(
        factory: &F,
        target: SocketAddr,
        connection_count: usize,
        max_pending_per_connection: usize,
        mut policy_scheduler: Box<dyn crate::scheduler::PolicyScheduler>,
        group_id: usize,
    ) -> Result<Self> {
        let mut group = factory.create_group()?;
        let mut connection_states = HashMap::with_capacity(connection_count);
        let mut connection_ids = Vec::with_capacity(connection_count);
        let mut ready_heap = ReadyHeap::new();
        let current_time_ns = crate::timing::time_ns();

        for idx in 0..connection_count {
            let connection_id = group.add_connection(&target)?;
            let mut policy = policy_scheduler.assign_policy(idx);

            // Get initial next_send_time and add to heap
            let next_send_time = policy.next_send_time(current_time_ns);
            ready_heap.push(connection_id, next_send_time);

            let state = ConnectionState::new(
                connection_id,
                target,
                max_pending_per_connection,
                policy,
                group_id,
            );
            connection_states.insert(connection_id, state);
            connection_ids.push(connection_id);
        }

        Ok(Self {
            group,
            connection_states,
            connection_ids,
            ready_heap,
        })
    }

    /// Create a new connection pool with connections from multiple traffic groups
    pub fn new_multi_group<F: TransportFactory<Group = G>>(
        factory: &F,
        target: SocketAddr,
        groups: Vec<(usize, usize, usize, Box<dyn crate::scheduler::PolicyScheduler>)>,
    ) -> Result<Self> {
        let groups_with_targets: Vec<_> = groups
            .into_iter()
            .map(|(group_id, connection_count, max_pending, scheduler)| {
                (group_id, target, connection_count, max_pending, scheduler)
            })
            .collect();

        Self::new_multi_group_with_targets(factory, groups_with_targets)
    }

    /// Create a new connection pool with multiple traffic groups, each with its own target
    pub fn new_multi_group_with_targets<F: TransportFactory<Group = G>>(
        factory: &F,
        groups: Vec<TrafficGroupConfig>,
    ) -> Result<Self> {
        let total_connections: usize = groups.iter().map(|(_, _, count, _, _)| count).sum();
        let mut group = factory.create_group()?;
        let mut connection_states = HashMap::with_capacity(total_connections);
        let mut connection_ids = Vec::with_capacity(total_connections);
        let mut ready_heap = ReadyHeap::new();
        let current_time_ns = crate::timing::time_ns();
        let mut idx = 0;

        for (
            group_id,
            target,
            connection_count,
            max_pending_per_connection,
            mut policy_scheduler,
        ) in groups
        {
            for _ in 0..connection_count {
                let connection_id = group.add_connection(&target)?;
                let mut policy = policy_scheduler.assign_policy(idx);

                // Get initial next_send_time and add to heap
                let next_send_time = policy.next_send_time(current_time_ns);
                ready_heap.push(connection_id, next_send_time);

                let state = ConnectionState::new(
                    connection_id,
                    target,
                    max_pending_per_connection,
                    policy,
                    group_id,
                );
                connection_states.insert(connection_id, state);
                connection_ids.push(connection_id);
                idx += 1;
            }
        }

        Ok(Self {
            group,
            connection_states,
            connection_ids,
            ready_heap,
        })
    }

    /// Poll all connections for readability (single syscall)
    ///
    /// Returns connection IDs that have data ready to read.
    pub fn poll(&mut self, timeout: Option<Duration>) -> Result<Vec<usize>> {
        let ready = self.group.poll(timeout)?;
        // Filter to only include connections we're tracking
        Ok(ready.into_iter().filter(|id| self.connection_states.contains_key(id)).collect())
    }

    /// Send data on a connection
    pub fn send(&mut self, connection_id: usize, data: &[u8], req_id: ReqId) -> Result<()> {
        let state = self
            .connection_states
            .get_mut(&connection_id)
            .ok_or_else(|| crate::Error::Connection("Connection not found".to_string()))?;

        if !state.can_send() {
            return Err(crate::Error::Connection(
                "Connection cannot accept more requests".to_string(),
            ));
        }

        let connection = self
            .group
            .get_mut(connection_id)
            .ok_or_else(|| crate::Error::Connection("Connection not found in group".to_string()))?;

        let send_ts = connection.send(data)?;
        state.record_send(req_id, send_ts);

        Ok(())
    }

    /// Send data without tracking (e.g., preparation commands like ASKING)
    pub fn send_untracked(&mut self, connection_id: usize, data: &[u8]) -> Result<()> {
        let state = self
            .connection_states
            .get(&connection_id)
            .ok_or_else(|| crate::Error::Connection("Connection not found".to_string()))?;

        if state.is_closed() {
            return Err(crate::Error::Connection("Connection is closed".to_string()));
        }

        let connection = self
            .group
            .get_mut(connection_id)
            .ok_or_else(|| crate::Error::Connection("Connection not found in group".to_string()))?;

        let _ = connection.send(data)?;
        Ok(())
    }

    /// Receive and process responses for a connection
    pub fn recv_responses<F>(
        &mut self,
        connection_id: usize,
        process_fn: F,
    ) -> Result<Vec<(ReqId, Duration)>>
    where
        F: FnMut(&[u8]) -> Result<(usize, Option<ReqId>)>,
    {
        let connection = self
            .group
            .get_mut(connection_id)
            .ok_or_else(|| crate::Error::Connection("Connection not found in group".to_string()))?;

        let (data, recv_ts) = connection.recv()?;

        let state = self
            .connection_states
            .get_mut(&connection_id)
            .ok_or_else(|| crate::Error::Connection("Connection state not found".to_string()))?;

        state.process_recv(&data, recv_ts, process_fn)
    }

    /// Pick the next ready connection (O(log N) via min-heap)
    ///
    /// Pops connections from the heap until finding one that can send.
    /// Stale entries (connections that can't send due to full pipeline) are discarded.
    pub fn pick_connection(&mut self) -> Option<usize> {
        let current_time_ns = crate::timing::time_ns();

        // Pop from heap until we find a connection that can actually send
        while let Some(connection_id) = self.ready_heap.pop_ready(current_time_ns) {
            if let Some(state) = self.connection_states.get_mut(&connection_id) {
                if state.can_send() {
                    state.last_active = current_time_ns;
                    return Some(connection_id);
                }
                // Connection can't send (pipeline full or closed) - don't re-add, it will
                // be re-added when a response is received and pipeline has capacity
            }
        }

        None
    }

    /// Re-add a connection to the heap after it becomes sendable again
    ///
    /// Called after receiving a response to re-enable the connection for scheduling.
    pub fn reschedule_connection(&mut self, connection_id: usize) {
        let current_time_ns = crate::timing::time_ns();
        if let Some(state) = self.connection_states.get_mut(&connection_id) {
            if state.can_send() {
                let next_send_time = state.next_send_time(current_time_ns);
                self.ready_heap.push(connection_id, next_send_time);
            }
        }
    }

    /// Get connection state by ID
    pub fn get_state(&self, connection_id: usize) -> Option<&ConnectionState<ReqId>> {
        self.connection_states.get(&connection_id)
    }

    /// Get mutable connection state by ID
    pub fn get_state_mut(&mut self, connection_id: usize) -> Option<&mut ConnectionState<ReqId>> {
        self.connection_states.get_mut(&connection_id)
    }

    /// Iterate over all connection IDs
    pub fn connection_ids(&self) -> &[usize] {
        &self.connection_ids
    }

    /// Get connection count
    pub fn len(&self) -> usize {
        self.connection_states.len()
    }

    /// Check if pool is empty
    pub fn is_empty(&self) -> bool {
        self.connection_states.is_empty()
    }

    /// Close a specific connection
    ///
    /// This marks the connection as closed, clears pending requests, and removes
    /// it from the connection pool to prevent drain_iteration from hanging.
    pub fn close(&mut self, connection_id: usize) -> Result<()> {
        if let Some(state) = self.connection_states.get_mut(&connection_id) {
            state.closed = true;
            state.pending_map.clear();
        }
        // Remove from connection_ids to prevent drain_iteration from waiting on it
        self.connection_ids.retain(|&id| id != connection_id);
        self.group.close(connection_id)?;
        Ok(())
    }

    /// Close all connections
    pub fn close_all(&mut self) -> Result<()> {
        for state in self.connection_states.values_mut() {
            state.closed = true;
            state.pending_map.clear();
        }
        self.connection_ids.clear();
        self.group.close_all()?;
        Ok(())
    }
}
