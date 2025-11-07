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
    pub fn new(
        mut transport: T,
        idx: usize,
        target: SocketAddr,
        max_pending_requests: usize,
        policy: Box<dyn Policy>,
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
}

/// Connection pool managing multiple connections with policy scheduler
pub struct ConnectionPool<T: Transport, ReqId: Eq + Hash + Clone> {
    /// All connections in the pool
    connections: Vec<Connection<T, ReqId>>,
    /// Unified scheduler for timing and connection selection (TODO: Remove after migration)
    scheduler: crate::scheduler::UnifiedScheduler,
    /// Maximum pending requests per connection
    max_pending_per_conn: usize,
}

impl<T: Transport, ReqId: Eq + Hash + Clone + std::fmt::Debug> ConnectionPool<T, ReqId> {
    /// Create a new connection pool with a unified scheduler (old API)
    ///
    /// TODO: This is deprecated. Use `with_policy_scheduler()` instead for the new per-connection model.
    pub fn new(
        transport_factory: impl Fn() -> T,
        target: SocketAddr,
        conn_count: usize,
        max_pending_per_conn: usize,
        scheduler: crate::scheduler::UnifiedScheduler,
    ) -> Result<Self> {
        let mut connections = Vec::with_capacity(conn_count);

        // For backwards compatibility, use ClosedLoopPolicy for all connections
        for idx in 0..conn_count {
            let transport = transport_factory();
            let policy = Box::new(crate::scheduler::ClosedLoopPolicy::new());
            let conn = Connection::new(transport, idx, target, max_pending_per_conn, policy)?;
            connections.push(conn);
        }

        Ok(Self {
            connections,
            scheduler,
            max_pending_per_conn,
        })
    }

    /// Create a new connection pool with a policy scheduler (new API)
    ///
    /// This is the new per-connection traffic model where each connection has its own
    /// independent policy that determines when it should send requests.
    ///
    /// # Parameters
    /// - `transport_factory`: Function to create a new transport instance
    /// - `target`: Target server address
    /// - `conn_count`: Number of connections to create
    /// - `max_pending_per_conn`: Maximum pending requests per connection
    /// - `policy_scheduler`: Scheduler that assigns policies to connections
    pub fn with_policy_scheduler(
        transport_factory: impl Fn() -> T,
        target: SocketAddr,
        conn_count: usize,
        max_pending_per_conn: usize,
        mut policy_scheduler: Box<dyn crate::scheduler::PolicyScheduler>,
    ) -> Result<Self> {
        let mut connections = Vec::with_capacity(conn_count);

        for idx in 0..conn_count {
            let transport = transport_factory();
            let policy = policy_scheduler.assign_policy(idx);
            let conn = Connection::new(transport, idx, target, max_pending_per_conn, policy)?;
            connections.push(conn);
        }

        // Create a temporary UnifiedScheduler for backwards compatibility
        // TODO: Remove this after full migration to TemporalScheduler
        let timing = Box::new(crate::scheduler::ClosedLoopTiming::new());
        let selector = Box::new(crate::scheduler::RoundRobinSelector::new());
        let scheduler = crate::scheduler::UnifiedScheduler::new(timing, selector);

        Ok(Self {
            connections,
            scheduler,
            max_pending_per_conn,
        })
    }

    /// Create a new connection pool with closed-loop + round-robin scheduler
    pub fn with_round_robin(
        transport_factory: impl Fn() -> T,
        target: SocketAddr,
        conn_count: usize,
        max_pending_per_conn: usize,
    ) -> Result<Self> {
        let timing = Box::new(crate::scheduler::ClosedLoopTiming::new());
        let selector = Box::new(crate::scheduler::RoundRobinSelector::new());
        let scheduler = crate::scheduler::UnifiedScheduler::new(timing, selector);

        Self::new(transport_factory, target, conn_count, max_pending_per_conn, scheduler)
    }

    /// Pick a connection using the new temporal scheduler (per-connection policies)
    ///
    /// This uses the true per-connection traffic model where each connection's policy
    /// determines when it should send. The scheduler picks the connection whose next
    /// request is due soonest.
    pub fn pick_connection_temporal(&mut self) -> Option<&mut Connection<T, ReqId>> {
        let current_time_ns = crate::timing::time_ns();
        let mut temporal_scheduler = crate::scheduler::TemporalScheduler::new();

        // Build heap: query each connection for its next send time
        for conn in &mut self.connections {
            if conn.can_send() {
                let next_time = conn.next_send_time(current_time_ns);
                temporal_scheduler.update_connection(conn.idx(), next_time);
            }
        }

        // Pick the connection that is ready soonest
        if let Some(conn_idx) = temporal_scheduler.pick_ready_connection(current_time_ns) {
            Some(&mut self.connections[conn_idx])
        } else {
            None
        }
    }

    /// Pick a connection that can accept more requests using the scheduler (old API)
    ///
    /// # Parameters
    /// - `key`: The key being requested (used by affinity-based schedulers)
    pub fn pick_connection(&mut self, key: u64) -> Option<&mut Connection<T, ReqId>> {
        // Build connection states for scheduler
        let conn_states: Vec<crate::scheduler::ConnState> = self
            .connections
            .iter()
            .map(|conn| crate::scheduler::ConnState {
                idx: conn.idx(),
                pending_count: conn.pending_count(),
                closed: conn.is_closed(),
                avg_latency: None, // TODO: Track if needed for latency-aware scheduling
                ready: conn.pending_count() == 0, // Ready if no pending requests (for closed-loop)
            })
            .collect();

        // Use unified scheduler to select connection
        let idx = self.scheduler.select_connection(
            key,
            &conn_states,
            self.max_pending_per_conn,
            crate::timing::time_ns(),
        )?;

        Some(&mut self.connections[idx])
    }

    /// Notify scheduler that a request was sent
    pub fn on_request_sent(&mut self, conn_idx: usize, key: u64) {
        self.scheduler.on_request_sent(conn_idx, key, crate::timing::time_ns());
    }

    /// Notify scheduler that a response was received
    pub fn on_response_received(
        &mut self,
        conn_idx: usize,
        key: u64,
        latency: std::time::Duration,
    ) {
        self.scheduler.on_response_received(conn_idx, key, latency);
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
