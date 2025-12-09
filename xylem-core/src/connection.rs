//! Connection management with pipelining support
//!
//! This module implements Lancet-style connection pooling, allowing multiple
//! outstanding requests per connection for higher throughput.
//!
//! Uses `ConnectionGroup` internally for efficient I/O multiplexing - a single
//! poll syscall checks all connections in the pool.

use crate::scheduler::{Policy, ReadyHeap};
use crate::Result;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::net::SocketAddr;
use std::time::Duration;
use xylem_transport::{ConnectionGroup, GroupConnection, Timestamp, TransportFactory};

/// Wrapping less-than-or-equal comparison for u32 counters
///
/// Returns true if `a <= b` accounting for wrapping.
/// Uses signed arithmetic: if (a - b) as i32 <= 0, then a <= b in wrapping sense.
/// This works correctly when the difference is less than 2^31 (~2 billion).
///
/// # Assumptions
///
/// The outstanding data between oldest pending request and newest TX timestamp
/// must be less than the wrap threshold. This is safe in practice:
/// - **TCP**: Counter tracks bytes. With typical max_pending of 64-256 requests
///   and 64KB max request size, that's at most 16MB outstanding per connection.
///   Would need ~32,000 connections each with 64KB max pipeline to hit 2GB.
/// - **UDP**: Counter tracks packets. With typical max_pending of 64-256,
///   we're far below the ~2 billion packet threshold.
#[inline]
fn wrapping_le(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) <= 0
}

/// Maximum receive buffer size per connection
/// Increased to support large values (e.g., YCSB workloads with 10KB+ values)
/// With RESP protocol overhead, 64KB supports ~50KB values
const MAX_PAYLOAD: usize = 65536; // 64KB

/// Traffic group configuration: (group_id, target, conn_count, max_pending_per_conn, policy_scheduler)
pub type TrafficGroupConfig =
    (usize, SocketAddr, usize, usize, Box<dyn crate::scheduler::PolicyScheduler>);

/// Unix traffic group configuration: (group_id, path, conn_count, max_pending_per_conn, policy_scheduler)
/// Uses String paths instead of SocketAddr for Unix domain sockets
pub type UnixTrafficGroupConfig =
    (usize, String, usize, usize, Box<dyn crate::scheduler::PolicyScheduler>);

/// Timeout for stale TX queue entries (1 minute in nanoseconds)
/// Entries older than this are cleaned up to prevent memory leaks from lost packets
const TX_QUEUE_STALE_TIMEOUT_NS: u64 = 60_000_000_000;

/// Pending request tracking
#[derive(Debug)]
struct PendingRequest {
    /// Software timestamp when request was sent
    send_ts: Timestamp,
    /// Hardware TX timestamp (filled in later when received from error queue)
    tx_hw_ts: Option<Timestamp>,
    /// Whether this is a warmup request (stats should not be collected)
    is_warmup: bool,
}

/// Per-connection application state (no transport - I/O goes through ConnectionGroup)
///
/// # Pending Request Tracking
///
/// Uses two data structures for efficient request tracking:
///
/// 1. `pending_map: HashMap<ReqId, PendingRequest>` - Primary storage, O(1) lookup by request ID
///    Used when responses arrive (protocol provides req_id)
///
/// 2. `tx_pending_queue: VecDeque<(u32, ReqId, u64)>` - Queue for TX timestamp correlation
///    Requests are pushed to back (tx_end is monotonically increasing),
///    TX timestamps are matched from front (kernel delivers in order).
///    The u64 is a monotonic timestamp (nanoseconds) for stale entry cleanup.
///
/// Flow:
/// - send() -> push_back to tx_pending_queue, insert into pending_map
/// - poll_tx_timestamps() -> pop from front while tx_end <= offset, set pending_map[req_id].tx_hw_ts
/// - recv() -> lookup pending_map by req_id, clean up queue if needed, compute latency
///
/// Queue cleanup: When a response arrives before its TX timestamp (or for transports without
/// HW timestamps like Unix sockets), the queue entry is cleaned up by popping from front.
///
/// # Scheduling Invariant
///
/// A connection is on the ready heap if and only if it can accept more requests:
/// - `on_heap == true` ⟺ `pending_count < max_pending`
///
/// The heap stores ALL connections that can send, including rate-limited connections
/// with future send times. This allows efficient timeout calculation by peeking at the
/// earliest ready time (needed for epoll_wait timeout).
pub struct ConnectionState<ReqId: Eq + Hash + Clone> {
    /// Connection ID in the ConnectionGroup
    connection_id: usize,
    /// Target address
    target: SocketAddr,
    /// Maximum pending requests allowed
    max_pending_requests: usize,
    /// Primary storage: ReqId -> PendingRequest for O(1) response lookup
    pending_map: HashMap<ReqId, PendingRequest>,
    /// Queue of (tx_end, ReqId, enqueue_time_ns) for TX timestamp correlation - push back, pop front
    /// The enqueue_time_ns is used for stale entry cleanup (entries older than 1 minute are removed)
    tx_pending_queue: VecDeque<(u32, ReqId, u64)>,
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
    /// Whether this connection is currently in the ready heap
    /// Invariant: on_heap == true ⟺ connection can accept more requests
    on_heap: bool,
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
            tx_pending_queue: VecDeque::with_capacity(max_pending_requests),
            recv_buffer: vec![0u8; MAX_PAYLOAD],
            buffer_pos: 0,
            closed: false,
            policy,
            group_id,
            last_active: 0,
            on_heap: false,
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

    /// Get maximum pending requests allowed
    pub fn max_pending(&self) -> usize {
        self.max_pending_requests
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

    /// Check if this connection is currently on the ready heap
    pub fn is_on_heap(&self) -> bool {
        self.on_heap
    }

    /// Mark this connection as being on the ready heap
    pub fn set_on_heap(&mut self, on_heap: bool) {
        self.on_heap = on_heap;
    }

    /// Record a sent request with TX byte range for hardware timestamp correlation
    ///
    /// `tx_start` is accepted for API symmetry with `GroupConnection::send()` but unused;
    /// only `tx_end` is needed to determine when a TX timestamp covers this request.
    fn record_send(
        &mut self,
        req_id: ReqId,
        send_ts: Timestamp,
        _tx_start: u32,
        tx_end: u32,
        is_warmup: bool,
    ) {
        let now_ns = crate::timing::time_ns();
        // Push to queue for TX timestamp correlation (tx_end increases monotonically)
        self.tx_pending_queue.push_back((tx_end, req_id.clone(), now_ns));
        self.pending_map
            .insert(req_id, PendingRequest { send_ts, tx_hw_ts: None, is_warmup });
    }

    /// Clean up queue entry for a completed request that didn't receive its TX timestamp
    ///
    /// Uses linear search to find and remove the entry, handling out-of-order responses.
    /// Also cleans up any stale entries for already-completed requests along the way.
    fn cleanup_tx_queue_entry(&mut self, req_id: &ReqId) {
        // First, try to pop stale entries from the front (common case optimization)
        while let Some((_, front_id, _)) = self.tx_pending_queue.front() {
            if front_id == req_id {
                self.tx_pending_queue.pop_front();
                return;
            }
            if !self.pending_map.contains_key(front_id) {
                // Stale entry, remove it
                self.tx_pending_queue.pop_front();
            } else {
                // Found a still-pending request, stop front cleanup
                break;
            }
        }

        // If not found at front, search and remove from middle of queue (out-of-order case)
        if let Some(pos) = self.tx_pending_queue.iter().position(|(_, id, _)| id == req_id) {
            self.tx_pending_queue.remove(pos);
        }
    }

    /// Clean up stale entries from the TX pending queue
    ///
    /// Removes entries that are older than `TX_QUEUE_STALE_TIMEOUT_NS` (1 minute).
    /// This prevents memory growth when TX timestamps are never received (e.g., packet loss,
    /// NIC doesn't support HW timestamps, or kernel drops error queue messages).
    ///
    /// Returns the number of stale entries removed.
    pub fn cleanup_stale_tx_queue_entries(&mut self) -> usize {
        let now_ns = crate::timing::time_ns();
        let mut removed = 0;

        // Pop stale entries from front
        // Queue is ordered by tx_end, and since sends are sequential, enqueue_time
        // increases monotonically. Thus if front is not stale, none are.
        while let Some(&(_, _, enqueue_time)) = self.tx_pending_queue.front() {
            if now_ns.saturating_sub(enqueue_time) > TX_QUEUE_STALE_TIMEOUT_NS {
                self.tx_pending_queue.pop_front();
                removed += 1;
            } else {
                break;
            }
        }

        removed
    }

    /// Update pending requests with hardware TX timestamp
    ///
    /// A TX timestamp with `offset` indicates all bytes/packets up to `offset` have been transmitted.
    /// We pop from front of queue all requests with `tx_end <= offset` and set their TX timestamp.
    ///
    /// Uses wrapping comparison to handle u32 counter overflow correctly.
    ///
    /// Returns the number of pending requests that were updated with the TX timestamp.
    /// A single TX timestamp may cover multiple small requests.
    pub fn set_tx_timestamp(&mut self, offset: u32, tx_ts: Timestamp) -> usize {
        let mut updated_count = 0;

        // Pop from front while tx_end <= offset (queue is ordered by tx_end)
        // Use wrapping comparison to handle counter overflow after ~4GB (TCP) or ~4B packets (UDP)
        while let Some(&(tx_end, ref req_id, _)) = self.tx_pending_queue.front() {
            if wrapping_le(tx_end, offset) {
                // Update pending_map if request still exists (might have been completed already
                // if response arrived before TX timestamp)
                if let Some(pending) = self.pending_map.get_mut(req_id) {
                    pending.tx_hw_ts = Some(tx_ts);
                    updated_count += 1;
                }
                self.tx_pending_queue.pop_front();
            } else {
                break;
            }
        }

        updated_count
    }

    /// Process received data and extract latencies
    ///
    /// Returns a vector of (request_id, latency, is_warmup, bytes_consumed) tuples for each completed response.
    fn process_recv<F>(
        &mut self,
        data: &[u8],
        recv_ts: Timestamp,
        mut process_fn: F,
    ) -> Result<Vec<(ReqId, Duration, bool, usize)>>
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
                // If TX timestamp wasn't received yet, clean up queue entry to prevent leak
                // This happens when response arrives before TX timestamp (fast network/loopback)
                // or when HW timestamps are not supported (Unix sockets)
                if pending.tx_hw_ts.is_none() {
                    self.cleanup_tx_queue_entry(&req_id);
                }
                // Use hardware TX timestamp if available, otherwise software timestamp
                let send_ts = pending.tx_hw_ts.unwrap_or(pending.send_ts);
                let latency = recv_ts.duration_since(&send_ts);
                // Report the bytes consumed for THIS response (not cumulative)
                latencies.push((req_id, latency, pending.is_warmup, consumed));
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

            let mut state = ConnectionState::new(
                connection_id,
                target,
                max_pending_per_connection,
                policy,
                group_id,
            );

            state.set_on_heap(true);
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

                let mut state = ConnectionState::new(
                    connection_id,
                    target,
                    max_pending_per_connection,
                    policy,
                    group_id,
                );

                state.set_on_heap(true);
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
    ///
    /// # Arguments
    /// * `connection_id` - The connection to send on
    /// * `data` - The request data
    /// * `req_id` - The request ID for tracking
    /// * `is_warmup` - Whether this is a warmup request (stats will not be collected for response)
    pub fn send(
        &mut self,
        connection_id: usize,
        data: &[u8],
        req_id: ReqId,
        is_warmup: bool,
    ) -> Result<()> {
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

        let (send_ts, tx_start, tx_end) = connection.send(data)?;

        let state = self.connection_states.get_mut(&connection_id).unwrap();
        state.record_send(req_id.clone(), send_ts, tx_start, tx_end, is_warmup);

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
    ///
    /// Returns a vector of (request_id, latency, is_warmup, bytes_consumed) tuples for each completed response.
    /// The `is_warmup` flag indicates whether the original request was a warmup request
    /// and stats should not be collected.
    pub fn recv_responses<F>(
        &mut self,
        connection_id: usize,
        process_fn: F,
    ) -> Result<Vec<(ReqId, Duration, bool, usize)>>
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

    /// Poll for and process TX timestamps from the transport layer
    ///
    /// TX timestamps arrive asynchronously from the kernel's error queue.
    /// This method retrieves them and updates the corresponding pending requests
    /// so that latency calculations can use hardware TX timestamps.
    ///
    /// Returns the number of pending requests updated with TX timestamps.
    /// Note: A single TX timestamp may update multiple requests if they were
    /// batched together in a single kernel acknowledgment.
    pub fn poll_tx_timestamps(&mut self) -> Result<usize> {
        let tx_timestamps = self.group.poll_tx_timestamps()?;
        let mut count = 0;

        for tx_info in tx_timestamps {
            if let Some(state) = self.connection_states.get_mut(&tx_info.conn_id) {
                count += state.set_tx_timestamp(tx_info.tx_offset, tx_info.timestamp);
            }
        }

        Ok(count)
    }

    /// Clean up stale TX queue entries across all connections
    ///
    /// Removes TX queue entries older than 1 minute to prevent memory leaks
    /// when TX timestamps are never received (packet loss, unsupported NIC, etc.).
    ///
    /// This should be called periodically (e.g., once per second or per poll cycle).
    /// Returns the total number of stale entries removed across all connections.
    pub fn cleanup_stale_tx_queues(&mut self) -> usize {
        let mut total_removed = 0;
        for state in self.connection_states.values_mut() {
            total_removed += state.cleanup_stale_tx_queue_entries();
        }
        total_removed
    }

    /// Pick the next ready connection (O(log N) via min-heap)
    ///
    /// Pops the next ready connection from the heap and marks it as off-heap.
    ///
    /// Note: With the new scheduling invariant, all connections on the heap
    /// should be sendable. This method still checks `can_send()` defensively
    /// but should never encounter a connection that can't send.
    pub fn pick_connection(&mut self) -> Option<usize> {
        let current_time_ns = crate::timing::time_ns();

        if let Some(connection_id) = self.ready_heap.pop_ready(current_time_ns) {
            if let Some(state) = self.connection_states.get_mut(&connection_id) {
                if state.can_send() {
                    state.set_on_heap(false);
                    state.last_active = current_time_ns;
                    return Some(connection_id);
                } else {
                    eprintln!(
                        "[ERROR] INVARIANT VIOLATION: Connection {} popped from heap but can't send (pending={}, max={}, closed={})",
                        connection_id,
                        state.pending_count(),
                        state.max_pending_requests,
                        state.is_closed()
                    );
                }
            } else {
                eprintln!(
                    "[ERROR] Connection {} popped from heap but state not found!",
                    connection_id
                );
            }
        }

        None
    }

    /// Add a connection to the ready heap if not already on it
    ///
    /// Maintains the invariant: on_heap == true ⟺ connection can send (pipeline not full AND protocol ready).
    ///
    /// The heap stores ALL connections that can send, including rate-limited connections
    /// with future send times. This allows us to peek at the earliest ready time for
    /// efficient epoll_wait timeout calculation.
    ///
    /// This method should be called:
    /// 1. After sending a request (if pipeline still has capacity AND protocol is ready)
    /// 2. After receiving a response (if pipeline transitioned from full to available AND protocol is ready)
    ///
    /// # Parameters
    /// - `connection_id`: Connection to potentially schedule
    /// - `protocol`: Protocol instance to check readiness
    ///
    /// # Returns
    /// - `true` if connection was added to heap
    /// - `false` if connection was already on heap or cannot be scheduled (pipeline full or protocol not ready)
    pub fn try_schedule_connection<P>(&mut self, connection_id: usize, protocol: &P) -> bool
    where
        P: crate::threading::worker::Protocol,
    {
        let current_time_ns = crate::timing::time_ns();

        if let Some(state) = self.connection_states.get_mut(&connection_id) {
            // Don't add if already on heap (maintain invariant: no duplicates)
            if state.is_on_heap() {
                return false;
            }

            // Can't send (pipeline full or closed)
            if !state.can_send() {
                return false;
            }

            // Check protocol readiness (e.g., handshake not done)
            if !protocol.can_send(connection_id) {
                return false;
            }

            // Add to heap regardless of next_send_time
            // - Closed-loop (None): ready immediately
            // - Rate-limited (Some(future)): will be popped when time arrives
            let next_send_time = state.next_send_time(current_time_ns);
            state.set_on_heap(true);
            self.ready_heap.push(connection_id, next_send_time);
            return true;
        }
        false
    }

    /// Get the next time when a connection will be ready to send
    ///
    /// Returns `Some(time_ns)` for the earliest time a connection will be ready,
    /// or `None` if a connection is ready now (closed-loop) or heap is empty.
    ///
    /// Useful for determining how long to sleep in the worker loop.
    pub fn peek_next_ready_time(&self) -> Option<u64> {
        self.ready_heap.peek_next_time()
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

    /// Get the number of connections in the pool
    pub fn connection_count(&self) -> usize {
        self.connection_ids.len()
    }

    /// Get mapping of connection ID to target address
    ///
    /// Useful for protocols that need to know which connections go to which targets
    /// (e.g., Redis Cluster routing).
    pub fn connection_targets(&self) -> Vec<(usize, SocketAddr)> {
        self.connection_ids
            .iter()
            .filter_map(|&id| self.connection_states.get(&id).map(|s| (id, s.target())))
            .collect()
    }

    /// Get mapping of connection ID to target address for a specific traffic group
    ///
    /// Filters connections to only those belonging to the specified group_id.
    /// Essential for Redis Cluster to avoid routing to connections from other groups.
    pub fn connection_targets_for_group(&self, group_id: usize) -> Vec<(usize, SocketAddr)> {
        self.connection_ids
            .iter()
            .filter_map(|&id| {
                self.connection_states.get(&id).and_then(|s| {
                    if s.group_id() == group_id {
                        Some((id, s.target()))
                    } else {
                        None
                    }
                })
            })
            .collect()
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

/// Specialized implementation for Unix domain sockets
impl<ReqId: Eq + Hash + Clone + std::fmt::Debug>
    ConnectionPool<xylem_transport::UnixConnectionGroup, ReqId>
{
    /// Create a new connection pool for Unix domain sockets with path-based targets
    ///
    /// This method uses string paths instead of SocketAddr for Unix socket targets.
    pub fn new_multi_group_unix(
        factory: &xylem_transport::UnixTransportFactory,
        groups: Vec<UnixTrafficGroupConfig>,
    ) -> Result<Self> {
        let total_connections: usize = groups.iter().map(|(_, _, count, _, _)| count).sum();
        let mut group = factory.create_group()?;
        let mut connection_states = HashMap::with_capacity(total_connections);
        let mut connection_ids = Vec::with_capacity(total_connections);
        let mut ready_heap = ReadyHeap::new();
        let current_time_ns = crate::timing::time_ns();
        let mut idx = 0;

        // Placeholder address for Unix sockets (not used for actual connections)
        let placeholder_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();

        for (group_id, path, connection_count, max_pending_per_connection, mut policy_scheduler) in
            groups
        {
            for _ in 0..connection_count {
                let connection_id = group.add_connection_path(&path)?;
                let mut policy = policy_scheduler.assign_policy(idx);

                // Get initial next_send_time and add to heap
                let next_send_time = policy.next_send_time(current_time_ns);
                ready_heap.push(connection_id, next_send_time);

                let mut state = ConnectionState::new(
                    connection_id,
                    placeholder_addr, // Unix sockets don't have a SocketAddr
                    max_pending_per_connection,
                    policy,
                    group_id,
                );

                state.set_on_heap(true);
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrapping_le() {
        // Normal cases
        assert!(wrapping_le(0, 0));
        assert!(wrapping_le(0, 1));
        assert!(wrapping_le(100, 200));
        assert!(!wrapping_le(200, 100));

        // Near max value
        assert!(wrapping_le(u32::MAX - 1, u32::MAX));
        assert!(wrapping_le(u32::MAX, u32::MAX));
        assert!(!wrapping_le(u32::MAX, u32::MAX - 1));

        // Wrap-around cases (critical for byte/packet counter overflow)
        // After wrapping: 0xFFFFFFFF -> 0x00000000 -> 0x00000001
        assert!(wrapping_le(u32::MAX, 0)); // MAX is "before" 0 after wrap
        assert!(wrapping_le(u32::MAX, 1)); // MAX is "before" 1 after wrap
        assert!(wrapping_le(u32::MAX - 10, 5)); // Near-MAX is "before" small values after wrap

        // Realistic scenario: tx_end wraps around, kernel reports wrapped offset
        let tx_end = u32::MAX - 100; // Request sent just before wrap
        let offset = 50u32; // Kernel reports offset after wrap
        assert!(wrapping_le(tx_end, offset)); // Should match

        // Large gap within valid range (< 2^31)
        assert!(wrapping_le(0, i32::MAX as u32));
        assert!(wrapping_le(1000, 1000 + (i32::MAX as u32 - 1)));
    }
}
