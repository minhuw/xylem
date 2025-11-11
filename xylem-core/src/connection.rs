//! Connection management with pipelining support
//!
//! This module implements Lancet-style connection pooling, allowing multiple
//! outstanding requests per connection for higher throughput.

use crate::scheduler::Policy;
use crate::Result;
use std::collections::HashMap;
use std::hash::Hash;
use std::net::SocketAddr;
use xylem_transport::{Timestamp, Transport};

/// Maximum receive buffer size per connection
const MAX_PAYLOAD: usize = 16384;

/// Traffic group configuration: (group_id, target, conn_count, max_pending_per_conn, policy_scheduler)
pub type TrafficGroupConfig =
    (usize, SocketAddr, usize, usize, Box<dyn crate::scheduler::PolicyScheduler>);

/// Pending request tracking
#[derive(Debug)]
struct PendingRequest {
    /// Timestamp when request was sent
    send_ts: Timestamp,
}

/// A single TCP connection with pipelining support
pub struct Connection<T: Transport, ReqId: Eq + Hash + Clone> {
    /// Transport layer (TCP socket)
    transport: T,
    /// Connection index
    idx: usize,
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
    /// Traffic policy for this connection (per-connection model)
    ///
    /// Each connection has its own independent traffic policy that determines
    /// when it should send its next request. The scheduler will pick whichever
    /// connection's next request is due soonest.
    policy: Box<dyn Policy>,
    /// Traffic group ID this connection belongs to
    group_id: usize,
    /// Timestamp (in nanoseconds) when this connection was last used for sending (for round-robin fairness)
    last_active: u64,
}

impl<T: Transport, ReqId: Eq + Hash + Clone + std::fmt::Debug> Connection<T, ReqId> {
    /// Create a new connection with a traffic policy
    ///
    /// # Parameters
    /// - `transport`: Transport implementation (TCP, UDP, etc.)
    /// - `idx`: Connection index in the pool
    /// - `target`: Target server address
    /// - `max_pending_requests`: Maximum number of pending requests allowed
    /// - `policy`: Traffic policy that controls when this connection should send
    /// - `group_id`: Traffic group ID this connection belongs to
    pub fn new(
        mut transport: T,
        idx: usize,
        target: SocketAddr,
        max_pending_requests: usize,
        policy: Box<dyn Policy>,
        group_id: usize,
    ) -> Result<Self> {
        transport.connect(&target)?;

        Ok(Self {
            transport,
            idx,
            target,
            max_pending_requests,
            pending_map: HashMap::with_capacity(max_pending_requests),
            recv_buffer: vec![0u8; MAX_PAYLOAD],
            buffer_pos: 0,
            closed: false,
            policy,
            group_id,
            last_active: 0,
        })
    }

    /// Check if this connection can accept more requests
    pub fn can_send(&self) -> bool {
        !self.closed && self.pending_map.len() < self.max_pending_requests
    }

    /// Send a request on this connection with a request ID
    pub fn send(&mut self, data: &[u8], req_id: ReqId) -> Result<()>
    where
        ReqId: Clone,
    {
        if !self.can_send() {
            return Err(crate::Error::Connection(
                "Connection cannot accept more requests".to_string(),
            ));
        }

        let send_ts = self.transport.send(data)?;
        self.pending_map.insert(req_id, PendingRequest { send_ts });

        Ok(())
    }

    /// Send data without tracking (e.g., preparation commands like ASKING)
    ///
    /// This sends data on the connection without creating a pending request.
    /// Use this for protocol-level commands that don't expect tracked responses.
    pub fn send_untracked(&mut self, data: &[u8]) -> Result<()> {
        if self.closed {
            return Err(crate::Error::Connection("Connection is closed".to_string()));
        }

        let _ = self.transport.send(data)?;
        Ok(())
    }

    /// Poll if this connection has data ready to read
    pub fn poll_readable(&mut self) -> Result<bool> {
        Ok(self.transport.poll_readable()?)
    }

    /// Receive and process responses, returning (request_id, latency) pairs
    ///
    /// Processes all complete responses in the receive buffer and matches them
    /// with pending requests using the request ID returned by the protocol parser.
    pub fn recv_responses<F>(
        &mut self,
        mut process_fn: F,
    ) -> Result<Vec<(ReqId, std::time::Duration)>>
    where
        F: FnMut(&[u8]) -> Result<(usize, Option<ReqId>)>,
    {
        // Read data into buffer
        let (data, recv_ts) = self.transport.recv()?;
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

        self.recv_buffer[self.buffer_pos..self.buffer_pos + data_len].copy_from_slice(&data);
        self.buffer_pos += data_len;

        // Process complete responses in buffer
        let mut latencies = Vec::new();
        loop {
            // Try to parse response from buffer
            let (consumed, req_id_opt) = process_fn(&self.recv_buffer[..self.buffer_pos])?;

            if consumed == 0 {
                // No complete response yet
                break;
            }

            // Protocol must return a request ID
            let req_id = req_id_opt.ok_or_else(|| {
                crate::Error::Protocol("Protocol returned None for request ID".to_string())
            })?;

            // Look up the corresponding pending request and calculate latency
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
                // Consumed entire buffer
                self.buffer_pos = 0;
                break;
            } else if consumed < self.buffer_pos {
                // Partial consumption - move remaining data to front
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

    /// Get connection index
    pub fn idx(&self) -> usize {
        self.idx
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

    /// Close the connection
    pub fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(self.transport.close()?)
    }

    /// Get the time (in nanoseconds) when this connection should send its next request
    ///
    /// Returns `None` if the connection is ready to send immediately (closed-loop policy).
    pub fn next_send_time(&mut self, current_time_ns: u64) -> Option<u64> {
        self.policy.next_send_time(current_time_ns)
    }

    /// Notify the policy that a request was sent on this connection
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

    /// Get the traffic group ID
    pub fn group_id(&self) -> usize {
        self.group_id
    }
}

/// Connection pool managing multiple connections with per-connection policies
///
/// Supports connections from multiple traffic groups within a single pool.
/// Each connection tracks its own group_id independently.
pub struct ConnectionPool<T: Transport, ReqId: Eq + Hash + Clone> {
    /// All connections in the pool (may belong to different groups)
    connections: Vec<Connection<T, ReqId>>,
}

impl<T: Transport, ReqId: Eq + Hash + Clone + std::fmt::Debug> ConnectionPool<T, ReqId> {
    /// Create a new connection pool with per-connection policies for a single group
    ///
    /// Each connection gets its own independent traffic policy from the PolicyScheduler.
    /// The temporal scheduler picks whichever connection's next request is due soonest.
    ///
    /// # Parameters
    /// - `transport_factory`: Function to create a new transport instance
    /// - `target`: Target server address
    /// - `conn_count`: Number of connections to create
    /// - `max_pending_per_conn`: Maximum pending requests per connection
    /// - `policy_scheduler`: Scheduler that assigns policies to connections
    /// - `group_id`: Traffic group ID for all connections in this pool
    ///
    /// # Example
    ///
    /// ```no_run
    /// use xylem_core::connection::ConnectionPool;
    /// use xylem_core::scheduler::UniformPolicyScheduler;
    /// use xylem_transport::TcpTransport;
    /// use std::net::SocketAddr;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let target: SocketAddr = "127.0.0.1:6379".parse()?;
    /// // All connections use Poisson arrivals at 1M req/s
    /// let policy_scheduler = UniformPolicyScheduler::poisson(1_000_000.0)?;
    /// let pool: ConnectionPool<TcpTransport, (usize, u64)> = ConnectionPool::new(
    ///     TcpTransport::new,
    ///     target,
    ///     100,  // 100 connections
    ///     10,   // max 10 pending per connection
    ///     Box::new(policy_scheduler),
    ///     0    // group_id
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(
        transport_factory: impl Fn() -> T,
        target: SocketAddr,
        conn_count: usize,
        max_pending_per_conn: usize,
        mut policy_scheduler: Box<dyn crate::scheduler::PolicyScheduler>,
        group_id: usize,
    ) -> Result<Self> {
        let mut connections = Vec::with_capacity(conn_count);

        for idx in 0..conn_count {
            let transport = transport_factory();
            let policy = policy_scheduler.assign_policy(idx);
            let conn =
                Connection::new(transport, idx, target, max_pending_per_conn, policy, group_id)?;
            connections.push(conn);
        }

        Ok(Self { connections })
    }

    /// Create a new connection pool with connections from multiple traffic groups
    ///
    /// This allows multiple traffic groups to share the same worker thread, with connections
    /// from different groups coexisting in the same pool. Each connection independently tracks
    /// its group_id and policy.
    ///
    /// # Parameters
    /// - `transport_factory`: Function to create a new transport instance
    /// - `target`: Target server address
    /// - `groups`: Vector of (group_id, conn_count, max_pending, policy_scheduler) tuples
    ///
    /// # Example
    ///
    /// ```no_run
    /// use xylem_core::connection::ConnectionPool;
    /// use xylem_core::scheduler::UniformPolicyScheduler;
    /// use xylem_transport::TcpTransport;
    /// use std::net::SocketAddr;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let target: SocketAddr = "127.0.0.1:6379".parse()?;
    ///
    /// // Group 0: Latency measurements (low rate, high sampling)
    /// let latency_scheduler: Box<dyn xylem_core::scheduler::PolicyScheduler> =
    ///     Box::new(UniformPolicyScheduler::poisson(100.0)?);
    ///
    /// // Group 1: Throughput measurements (closed-loop, low sampling)
    /// let throughput_scheduler: Box<dyn xylem_core::scheduler::PolicyScheduler> =
    ///     Box::new(UniformPolicyScheduler::closed_loop());
    ///
    /// let groups: Vec<(usize, usize, usize, Box<dyn xylem_core::scheduler::PolicyScheduler>)> = vec![
    ///     (0, 10, 1, latency_scheduler),    // group_id=0, 10 conns, max_pending=1
    ///     (1, 50, 10, throughput_scheduler), // group_id=1, 50 conns, max_pending=10
    /// ];
    ///
    /// let pool: ConnectionPool<TcpTransport, (usize, u64)> =
    ///     ConnectionPool::new_multi_group(TcpTransport::new, target, groups)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new_multi_group(
        transport_factory: impl Fn() -> T,
        target: SocketAddr,
        groups: Vec<(usize, usize, usize, Box<dyn crate::scheduler::PolicyScheduler>)>,
    ) -> Result<Self> {
        // Convert to new format with per-group targets (all using same target for backward compat)
        let groups_with_targets: Vec<_> = groups
            .into_iter()
            .map(|(group_id, conn_count, max_pending, scheduler)| {
                (group_id, target, conn_count, max_pending, scheduler)
            })
            .collect();

        Self::new_multi_group_with_targets(transport_factory, groups_with_targets)
    }

    /// Create a new connection pool with multiple traffic groups, each with its own target
    ///
    /// # Parameters
    /// - `transport_factory`: Function to create a new transport instance
    /// - `groups`: Vec of (group_id, target, conn_count, max_pending_per_conn, policy_scheduler)
    pub fn new_multi_group_with_targets(
        transport_factory: impl Fn() -> T,
        groups: Vec<TrafficGroupConfig>,
    ) -> Result<Self> {
        let total_conns: usize = groups.iter().map(|(_, _, count, _, _)| count).sum();
        let mut connections = Vec::with_capacity(total_conns);
        let mut conn_idx = 0;

        for (group_id, target, conn_count, max_pending_per_conn, mut policy_scheduler) in groups {
            for _ in 0..conn_count {
                let transport = transport_factory();
                let policy = policy_scheduler.assign_policy(conn_idx);
                let conn = Connection::new(
                    transport,
                    conn_idx,
                    target,
                    max_pending_per_conn,
                    policy,
                    group_id,
                )?;
                connections.push(conn);
                conn_idx += 1;
            }
        }

        Ok(Self { connections })
    }

    /// Pick the next ready connection using temporal scheduling with round-robin fairness
    ///
    /// Queries each connection for its next send time and picks the one that's ready soonest.
    /// For closed-loop connections (None), uses least-recently-used to ensure fairness.
    ///
    /// # Returns
    /// - `Some(conn)`: The connection that's ready to send
    /// - `None`: No connection is ready yet
    pub fn pick_connection(&mut self) -> Option<&mut Connection<T, ReqId>> {
        let current_time_ns = crate::timing::time_ns();

        // Collect candidate connections with their ready times
        let mut timed_conns = Vec::new();
        let mut closed_loop_conns = Vec::new();

        for conn in &mut self.connections {
            if !conn.can_send() {
                continue;
            }

            match conn.next_send_time(current_time_ns) {
                None => closed_loop_conns.push(conn.idx()),
                Some(ready_time) if ready_time <= current_time_ns => {
                    timed_conns.push((ready_time, conn.idx()));
                }
                Some(_) => {} // Not ready yet
            }
        }

        // Priority: closed-loop connections (least recently used), then earliest timed connection
        let chosen_idx = if !closed_loop_conns.is_empty() {
            // Pick the least recently used connection for fairness
            closed_loop_conns
                .iter()
                .min_by_key(|&&idx| self.connections[idx].last_active)
                .copied()
        } else if !timed_conns.is_empty() {
            // Pick connection with earliest ready time
            timed_conns.sort_by_key(|(time, _)| *time);
            Some(timed_conns[0].1)
        } else {
            None
        };

        // Update last_active timestamp for the chosen connection
        if let Some(idx) = chosen_idx {
            self.connections[idx].last_active = current_time_ns;
            Some(&mut self.connections[idx])
        } else {
            None
        }
    }

    /// Get all connections for polling
    pub fn connections_mut(&mut self) -> &mut [Connection<T, ReqId>] {
        &mut self.connections
    }

    /// Get connection count
    pub fn len(&self) -> usize {
        self.connections.len()
    }

    /// Check if pool is empty
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    /// Close all connections
    pub fn close_all(&mut self) -> Result<()> {
        for conn in &mut self.connections {
            conn.close()?;
        }
        Ok(())
    }
}
