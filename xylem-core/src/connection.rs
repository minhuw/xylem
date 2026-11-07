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
use xylem_common::Protocol;
use xylem_transport::{
    ConnectionGroup, KernelTimestamp, RecvOutcome, Timestamp, TransportFactory, TxRange,
    TxTimestampInfo,
};

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

/// Maximum size of an individual response or incomplete frame
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

/// Traffic group configuration with either target type, as used by the pool builder:
/// (group_id, target, conn_count, max_pending_per_conn, policy_scheduler)
type GroupConfig<Target> =
    (usize, Target, usize, usize, Box<dyn crate::scheduler::PolicyScheduler>);

/// Timeout for stale TX queue entries (1 minute in nanoseconds)
/// Entries older than this are cleaned up to prevent memory leaks from lost packets
const TX_QUEUE_STALE_TIMEOUT_NS: u64 = 60_000_000_000;

/// Most timed-out request IDs a connection remembers to recognise their late
/// responses; the oldest are forgotten first
const TIMED_OUT_MEMORY: usize = 4096;

/// Pending request tracking
#[derive(Debug)]
struct PendingRequest {
    /// Cycles read when the send call completed
    send_ts: Timestamp,
    /// Time from the intended send time to the send; zero without a schedule
    send_lag: Duration,
    is_warmup: bool,
    /// Kernel TX timestamp of the send, once reported
    tx_kernel_ts: Option<KernelTimestamp>,
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
///    Holds `(TxRange::end, req_id, enqueue_time_ns)` for sends whose transport reports
///    TX timestamps (`SendReceipt::tx` is `Some`); other sends never enter it.
///    Requests are pushed to back (ends increase monotonically, wrapping),
///    TX timestamps are matched from front (kernel delivers in order).
///    The u64 is a monotonic timestamp (nanoseconds) for stale entry cleanup.
///
/// Flow (see `xylem_transport`'s "TX Timestamp Correlation" for the contract):
/// - send() -> push_back to tx_pending_queue if `tx` is Some, insert into pending_map
/// - poll_tx_timestamps() -> a cumulative stamp pops from front while end <= tx_end; an
///   exact stamp removes only the entry with end == tx_end; set pending_map[req_id].tx_kernel_ts
/// - recv() -> lookup pending_map by req_id, clean up queue if needed, compute latency
///
/// Queue cleanup: When a response arrives before its TX timestamp, or its stamp was lost,
/// the queue entry is removed when the response completes the request.
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
    /// Receive buffer: unparsed bytes in `recv_buffer[..recv_len]`, spare
    /// initialised space after them that the transport reads into
    recv_buffer: Vec<u8>,
    /// Number of unparsed bytes at the front of `recv_buffer`
    recv_len: usize,
    /// Whether connection is closed
    closed: bool,
    /// Traffic policy for this connection
    policy: Box<dyn Policy>,
    /// Traffic group ID this connection belongs to
    group_id: usize,
    /// Whether this connection is currently in the ready heap
    /// Invariant: on_heap == true ⟺ connection can accept more requests
    on_heap: bool,
    /// How long a request may stay pending before it times out; `None` waits forever
    request_timeout: Option<Duration>,
    /// `(deadline_ns, req_id)` of sends with a request timeout, in send order.
    /// Entries of completed requests are skipped and pruned lazily.
    deadlines: VecDeque<(u64, ReqId)>,
    /// Timed-out requests whose late responses are still recognised, mapped
    /// to whether each was a warmup request
    timed_out_ids: HashMap<ReqId, bool>,
    /// `timed_out_ids` keys in timeout order, for forgetting the oldest
    timed_out_order: VecDeque<ReqId>,
    /// Late responses to timed-out measurement requests, not yet taken by the worker
    late_responses: u64,
    /// Datagram bytes the protocol absorbed without completing a request;
    /// they are attributed to the next completed response
    absorbed_bytes: usize,
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
            recv_buffer: Vec::new(),
            recv_len: 0,
            closed: false,
            policy,
            group_id,
            on_heap: false,
            request_timeout: None,
            deadlines: VecDeque::new(),
            timed_out_ids: HashMap::new(),
            timed_out_order: VecDeque::new(),
            late_responses: 0,
            absorbed_bytes: 0,
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

    /// Get the next send or policy-refresh wakeup time (in nanoseconds)
    pub fn next_send_time(&mut self, current_time_ns: u64) -> Option<u64> {
        self.policy.next_send_time(current_time_ns)
    }

    /// Intended send time of the next request; `None` without a schedule
    pub fn intended_send_time(&self) -> Option<u64> {
        self.policy.intended_send_time()
    }

    /// Number of pending measurement (non-warmup) requests
    pub fn measured_pending_count(&self) -> usize {
        self.pending_map.values().filter(|pending| !pending.is_warmup).count()
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

    /// Record a sent request, with its TX range when the transport reports TX timestamps
    ///
    /// Only `tx.end` is needed to determine which TX timestamp covers this request.
    fn record_send(
        &mut self,
        req_id: ReqId,
        send_ts: Timestamp,
        send_lag: Duration,
        tx: Option<TxRange>,
        is_warmup: bool,
    ) {
        if let Some(tx) = tx {
            // Queue for TX timestamp correlation (ends increase monotonically)
            let now_ns = crate::timing::time_ns();
            self.tx_pending_queue.push_back((tx.end, req_id.clone(), now_ns));
        }
        if let Some(timeout) = self.request_timeout {
            // One timeout per connection, so deadlines increase in send order
            let timeout_ns = timeout.as_nanos().min(u64::MAX as u128) as u64;
            let deadline_ns = send_ts.clock_ns().saturating_add(timeout_ns);
            self.deadlines.push_back((deadline_ns, req_id.clone()));
        }
        self.pending_map.insert(
            req_id,
            PendingRequest {
                send_ts,
                send_lag,
                is_warmup,
                tx_kernel_ts: None,
            },
        );
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
        self.cleanup_stale_tx_queue_entries_at(crate::timing::time_ns())
    }

    fn cleanup_stale_tx_queue_entries_at(&mut self, now_ns: u64) -> usize {
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

    /// Update pending requests with a kernel TX timestamp
    ///
    /// `tx_end` is the exclusive end of the TX counter range the stamp covers.
    /// - `cumulative` (byte streams): every queued request with `end <= tx_end`
    ///   was transmitted by then; pop them all from the front and set their
    ///   TX timestamp. A single TX timestamp may cover multiple small requests.
    /// - Otherwise (datagrams): only the request with `end == tx_end` gets
    ///   it. Earlier entries stay queued, so a stamp that is lost or arrives
    ///   out of order is never given to another request.
    ///
    /// Uses wrapping comparison to handle u32 counter overflow correctly.
    ///
    /// Returns the number of pending requests that were updated with the TX timestamp.
    pub fn set_tx_timestamp(
        &mut self,
        tx_end: u32,
        cumulative: bool,
        tx_ts: KernelTimestamp,
    ) -> usize {
        if !cumulative {
            let Some(pos) = self.tx_pending_queue.iter().position(|&(end, _, _)| end == tx_end)
            else {
                return 0;
            };
            let (_, req_id, _) = self.tx_pending_queue.remove(pos).expect("position is in range");
            return match self.pending_map.get_mut(&req_id) {
                Some(pending) => {
                    pending.tx_kernel_ts = Some(tx_ts);
                    1
                }
                None => 0,
            };
        }

        let mut updated_count = 0;

        // Pop from front while end <= tx_end (queue is ordered by end)
        // Use wrapping comparison to handle counter overflow after ~4GB (TCP) or ~4B packets (UDP)
        while let Some(&(end, ref req_id, _)) = self.tx_pending_queue.front() {
            if wrapping_le(end, tx_end) {
                // Update pending_map if request still exists (might have been completed already
                // if response arrived before TX timestamp)
                if let Some(pending) = self.pending_map.get_mut(req_id) {
                    pending.tx_kernel_ts = Some(tx_ts);
                    updated_count += 1;
                }
                self.tx_pending_queue.pop_front();
            } else {
                break;
            }
        }

        updated_count
    }

    /// Deadline of the oldest pending request with a timeout, pruning the
    /// entries of completed requests in front of it
    fn next_deadline(&mut self) -> Option<u64> {
        while let Some((deadline_ns, req_id)) = self.deadlines.front() {
            if self.pending_map.contains_key(req_id) {
                return Some(*deadline_ns);
            }
            self.deadlines.pop_front();
        }
        None
    }

    /// Time out every pending request whose deadline is at or before `now_ns`
    ///
    /// Each one leaves the pipeline, and its ID is remembered so a late
    /// response is dropped. Returns `(expired, expired measurement requests)`.
    fn expire_requests_at(&mut self, now_ns: u64) -> (usize, u64) {
        let mut expired = 0;
        let mut measured = 0;
        while let Some(deadline_ns) = self.next_deadline() {
            if deadline_ns > now_ns {
                break;
            }
            let (_, req_id) = self.deadlines.pop_front().expect("next_deadline found an entry");
            let pending = self.pending_map.remove(&req_id).expect("next_deadline checked it");
            if pending.tx_kernel_ts.is_none() {
                self.cleanup_tx_queue_entry(&req_id);
            }
            expired += 1;
            if !pending.is_warmup {
                measured += 1;
            }
            self.remember_timed_out(req_id, pending.is_warmup);
        }
        (expired, measured)
    }

    fn remember_timed_out(&mut self, req_id: ReqId, is_warmup: bool) {
        if self.timed_out_order.len() == TIMED_OUT_MEMORY {
            if let Some(oldest) = self.timed_out_order.pop_front() {
                self.timed_out_ids.remove(&oldest);
            }
        }
        self.timed_out_order.push_back(req_id.clone());
        self.timed_out_ids.insert(req_id, is_warmup);
    }

    /// Complete the pending request `req_id`, returning its latency and
    /// whether it was a warmup request
    ///
    /// A response to a timed-out request is dropped (`Ok(None)`) and, for a
    /// measurement request, counted as late. Any other unknown ID is an error
    /// on a byte stream; on a datagram connection it is a duplicate or stray
    /// datagram and is dropped.
    fn complete(
        &mut self,
        req_id: &ReqId,
        recv_ts: Timestamp,
        datagram: bool,
    ) -> Result<Option<(Duration, bool)>> {
        let Some(pending) = self.pending_map.remove(req_id) else {
            if let Some(is_warmup) = self.timed_out_ids.remove(req_id) {
                if !is_warmup {
                    self.late_responses += 1;
                }
                return Ok(None);
            }
            if datagram {
                tracing::debug!(?req_id, "Dropping a datagram response for no pending request");
                return Ok(None);
            }
            return Err(crate::Error::Protocol(format!(
                "Received response for unknown request ID: {req_id:?}"
            )));
        };
        // The kernel TX timestamp joins the send's own cycles, so the
        // latency falls back to cycles unless the receive has a kernel
        // timestamp from the same clock
        let send_ts = match pending.tx_kernel_ts {
            Some(kernel) => pending.send_ts.with_kernel(kernel),
            None => {
                self.cleanup_tx_queue_entry(req_id);
                pending.send_ts
            }
        };
        let latency = pending.send_lag + recv_ts.duration_since(&send_ts);
        Ok(Some((latency, pending.is_warmup)))
    }

    /// Process `data` as if the transport had just read it into the spare tail
    #[cfg(test)]
    fn process_recv<F>(
        &mut self,
        data: &[u8],
        recv_ts: Timestamp,
        process_fn: F,
    ) -> Result<Vec<(ReqId, Duration, bool, usize)>>
    where
        F: FnMut(&[u8]) -> Result<(usize, Option<ReqId>)>,
    {
        let needed = self.recv_len + data.len();
        if self.recv_buffer.len() < needed {
            self.recv_buffer.resize(needed, 0);
        }
        self.recv_buffer[self.recv_len..needed].copy_from_slice(data);
        self.process_received(data.len(), recv_ts, process_fn)
    }

    /// Unparsed bytes buffered from earlier reads
    #[cfg(test)]
    fn buffered(&self) -> &[u8] {
        &self.recv_buffer[..self.recv_len]
    }

    /// Spare tail of the receive buffer for the transport to read into
    ///
    /// Always offers at least `MAX_PAYLOAD` bytes, so a datagram transport
    /// never truncates a response. Grows the buffer only when needed.
    fn recv_spare(&mut self) -> &mut [u8] {
        let needed = self.recv_len + MAX_PAYLOAD;
        if self.recv_buffer.len() < needed {
            self.recv_buffer.resize(needed, 0);
        }
        &mut self.recv_buffer[self.recv_len..]
    }

    /// Process `len` bytes the transport read into `recv_spare()`
    ///
    /// Returns (request_id, latency, is_warmup, bytes_consumed) for each
    /// completed response. The latency is the request's send lag plus its
    /// service latency, from the send to the receive timestamp.
    fn process_received<F>(
        &mut self,
        len: usize,
        recv_ts: Timestamp,
        mut process_fn: F,
    ) -> Result<Vec<(ReqId, Duration, bool, usize)>>
    where
        F: FnMut(&[u8]) -> Result<(usize, Option<ReqId>)>,
    {
        if len == 0 {
            return Ok(Vec::new());
        }

        // A read can finish a near-limit frame and contain subsequent frames.
        // Limit individual frames, not the sum of buffered and newly read bytes.
        self.recv_len += len;
        let mut responses = Vec::new();
        let mut cursor = 0;
        while cursor < self.recv_len {
            let available = self.recv_len - cursor;
            let (consumed, req_id) = process_fn(&self.recv_buffer[cursor..self.recv_len])?;
            if consumed > available {
                return Err(crate::Error::Protocol(
                    "Response parser consumed more bytes than available".to_string(),
                ));
            }
            if consumed > MAX_PAYLOAD || (consumed == 0 && available > MAX_PAYLOAD) {
                return Err(crate::Error::Protocol(
                    "Response exceeds maximum payload size".to_string(),
                ));
            }
            if consumed == 0 {
                break;
            }
            let req_id = req_id.ok_or_else(|| {
                crate::Error::Protocol(format!(
                    "Protocol consumed {consumed} bytes but returned no request ID"
                ))
            })?;
            if let Some((latency, is_warmup)) = self.complete(&req_id, recv_ts, false)? {
                responses.push((req_id, latency, is_warmup, consumed));
            }
            cursor += consumed;
        }
        // Compact the remaining partial frame once, regardless of response count.
        self.recv_buffer.copy_within(cursor..self.recv_len, 0);
        self.recv_len -= cursor;

        Ok(responses)
    }

    /// Process one datagram of `len` bytes the transport read into `recv_spare()`
    ///
    /// The datagram is parsed on its own: nothing of it is kept for the next
    /// one. The parser may absorb bytes without completing a request (a
    /// fragment, duplicate, or stray), but must consume the whole datagram.
    /// Returns the completed responses like [`Self::process_received`]; each
    /// one's byte count includes the absorbed bytes before it.
    fn process_datagram<F>(
        &mut self,
        len: usize,
        recv_ts: Timestamp,
        mut process_fn: F,
    ) -> Result<Vec<(ReqId, Duration, bool, usize)>>
    where
        F: FnMut(&[u8]) -> Result<(usize, Option<ReqId>)>,
    {
        debug_assert_eq!(self.recv_len, 0, "datagrams leave no unparsed bytes");
        let mut responses = Vec::new();
        let mut cursor = 0;
        while cursor < len {
            let available = len - cursor;
            let (consumed, req_id) = process_fn(&self.recv_buffer[cursor..len])?;
            if consumed > available {
                return Err(crate::Error::Protocol(
                    "Response parser consumed more bytes than available".to_string(),
                ));
            }
            if consumed == 0 {
                return Err(crate::Error::Protocol(format!(
                    "Protocol left {available} bytes of a {len}-byte datagram unparsed"
                )));
            }
            cursor += consumed;
            self.absorbed_bytes += consumed;
            let Some(req_id) = req_id else {
                continue;
            };
            if let Some((latency, is_warmup)) = self.complete(&req_id, recv_ts, true)? {
                let bytes = std::mem::take(&mut self.absorbed_bytes);
                responses.push((req_id, latency, is_warmup, bytes));
            }
        }
        Ok(responses)
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
    /// Reused buffer for TX timestamps drained from the group
    tx_timestamps: Vec<TxTimestampInfo>,
    /// Whether the group carries datagrams, which are parsed one at a time
    datagram: bool,
    /// No pending request times out before this (a `timing::time_ns` value);
    /// `None` when none can. May be earlier than the true next deadline.
    next_deadline_ns: Option<u64>,
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
        policy_scheduler: Box<dyn crate::scheduler::PolicyScheduler>,
        group_id: usize,
    ) -> Result<Self> {
        // With a single group, the pool-wide policy index is the per-group index
        let groups = vec![(
            group_id,
            target,
            connection_count,
            max_pending_per_connection,
            policy_scheduler,
        )];

        Self::new_multi_group_with_targets(factory, groups)
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
        Self::build(factory.create_group()?, groups, |group, target| {
            let connection_id = group.add_connection(target)?;
            Ok((connection_id, *target))
        })
    }

    /// Build a pool by adding every group's connections to `group`
    ///
    /// `add_connection` opens one connection to a group's target and returns its
    /// connection ID and the address recorded in its state. Policies are assigned
    /// with a pool-wide index that counts connections across groups in order.
    fn build<T>(
        mut group: G,
        groups: Vec<GroupConfig<T>>,
        mut add_connection: impl FnMut(&mut G, &T) -> Result<(usize, SocketAddr)>,
    ) -> Result<Self> {
        let total_connections: usize = groups.iter().map(|(_, _, count, _, _)| count).sum();
        let mut connection_states = HashMap::with_capacity(total_connections);
        let mut connection_ids = Vec::with_capacity(total_connections);
        let mut ready_heap = ReadyHeap::new();
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
                let (connection_id, address) = add_connection(&mut group, &target)?;
                let mut policy = policy_scheduler.assign_policy(idx);

                // The first call starts the connection's intended schedule once
                // it is connected. Then add it to the heap.
                let next_send_time = policy.next_send_time(crate::timing::time_ns());
                ready_heap.push(connection_id, next_send_time);

                let mut state = ConnectionState::new(
                    connection_id,
                    address,
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
            datagram: group.is_datagram(),
            group,
            connection_states,
            connection_ids,
            ready_heap,
            tx_timestamps: Vec::new(),
            next_deadline_ns: None,
        })
    }

    /// Whether the connections carry datagrams (see [`ConnectionGroup::is_datagram`])
    pub fn is_datagram(&self) -> bool {
        self.datagram
    }

    /// Set how long each request of traffic group `group_id` may stay pending
    ///
    /// A request still pending at its deadline times out: it leaves the
    /// pipeline, gets no latency, and a late response to it is dropped.
    /// `None` (the default) waits forever. Applies to requests sent from now on.
    pub fn set_request_timeout(&mut self, group_id: usize, timeout: Option<Duration>) {
        for state in self.connection_states.values_mut() {
            if state.group_id == group_id {
                state.request_timeout = timeout;
            }
        }
    }

    /// Earliest time a pending request may time out (a `timing::time_ns`
    /// value), or `None` if none can. It may be early, never late.
    pub fn next_request_deadline(&self) -> Option<u64> {
        self.next_deadline_ns
    }

    /// Time out the requests whose deadline passed by `now_ns`
    ///
    /// Calls `on_expired(connection_id, group_id, expired, expired_measured)`
    /// for every connection that lost requests. Cheap until the earliest
    /// deadline passes.
    pub fn expire_requests_at(
        &mut self,
        now_ns: u64,
        mut on_expired: impl FnMut(usize, usize, usize, u64),
    ) {
        if self.next_deadline_ns.is_none_or(|deadline| deadline > now_ns) {
            return;
        }
        let mut next = None;
        for &connection_id in &self.connection_ids {
            let Some(state) = self.connection_states.get_mut(&connection_id) else {
                continue;
            };
            let (expired, measured) = state.expire_requests_at(now_ns);
            if expired > 0 {
                on_expired(connection_id, state.group_id, expired, measured);
            }
            next = match (next, state.next_deadline()) {
                (Some(a), Some(b)) => Some(u64::min(a, b)),
                (a, b) => a.or(b),
            };
        }
        self.next_deadline_ns = next;
    }

    /// Take the count of late responses to timed-out measurement requests
    /// received on `connection_id` since the last call
    pub fn take_late_responses(&mut self, connection_id: usize) -> u64 {
        self.connection_states
            .get_mut(&connection_id)
            .map_or(0, |state| std::mem::take(&mut state.late_responses))
    }

    /// Poll all connections for readability (single syscall)
    ///
    /// Clears `ready`, waits up to `timeout` (zero means do not wait), and
    /// fills it with the IDs of tracked connections that are ready to read.
    pub fn poll(&mut self, timeout: Duration, ready: &mut Vec<usize>) -> Result<()> {
        self.group.poll(timeout, ready)?;
        // Filter to only include connections we're tracking
        ready.retain(|id| self.connection_states.contains_key(id));
        Ok(())
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
        self.send_scheduled(connection_id, data, req_id, is_warmup, None).map(|_| ())
    }

    /// Send a request that was intended to go out at `intended_ns`
    ///
    /// The send lag is the time from `intended_ns` to the cycles the transport
    /// read when the send completed, on the [`crate::timing::time_ns`] clock.
    /// It is added to the request's latency. Returns the lag, or `None` for a
    /// request without an intended time (closed loop), whose lag is zero.
    pub fn send_scheduled(
        &mut self,
        connection_id: usize,
        data: &[u8],
        req_id: ReqId,
        is_warmup: bool,
        intended_ns: Option<u64>,
    ) -> Result<Option<Duration>> {
        let state = self
            .connection_states
            .get_mut(&connection_id)
            .ok_or_else(|| crate::Error::Connection("Connection not found".to_string()))?;

        if !state.can_send() {
            return Err(crate::Error::Connection(
                "Connection cannot accept more requests".to_string(),
            ));
        }

        let receipt = self.group.send(connection_id, data)?;

        let send_lag = intended_ns.map(|intended| {
            Duration::from_nanos(receipt.timestamp.clock_ns().saturating_sub(intended))
        });

        let state = self.connection_states.get_mut(&connection_id).unwrap();
        state.record_send(
            req_id,
            receipt.timestamp,
            send_lag.unwrap_or_default(),
            receipt.tx,
            is_warmup,
        );
        if state.request_timeout.is_some() {
            if let Some(&(deadline_ns, _)) = state.deadlines.back() {
                self.next_deadline_ns =
                    Some(self.next_deadline_ns.map_or(deadline_ns, |next| next.min(deadline_ns)));
            }
        }

        Ok(send_lag)
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
        let state = self
            .connection_states
            .get_mut(&connection_id)
            .ok_or_else(|| crate::Error::Connection("Connection state not found".to_string()))?;

        // The transport reads straight into the state's buffer, after any
        // partial frame left by earlier reads.
        let responses = match self.group.recv(connection_id, state.recv_spare())? {
            RecvOutcome::Data { len, timestamp } if self.datagram => {
                state.process_datagram(len, timestamp, process_fn)?
            }
            RecvOutcome::Data { len, timestamp } => {
                state.process_received(len, timestamp, process_fn)?
            }
            RecvOutcome::WouldBlock => return Ok(Vec::new()),
            RecvOutcome::Eof => {
                return Err(crate::Error::Connection("Connection closed by peer".to_string()))
            }
        };
        for &(_, latency, is_warmup, _) in &responses {
            if !is_warmup {
                state.policy.on_response(latency);
            }
        }
        Ok(responses)
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
        self.group.poll_tx_timestamps(&mut self.tx_timestamps)?;
        let mut count = 0;

        for tx_info in &self.tx_timestamps {
            if let Some(state) = self.connection_states.get_mut(&tx_info.conn_id) {
                count +=
                    state.set_tx_timestamp(tx_info.tx_end, tx_info.cumulative, tx_info.timestamp);
            }
        }

        Ok(count)
    }

    /// Clean up stale TX queue entries across all connections
    ///
    /// Removes TX queue entries older than 1 minute to prevent memory leaks
    /// when TX timestamps are never received (packet loss, unsupported NIC, etc.).
    ///
    /// This should be called periodically (e.g., once per second).
    /// Returns the total number of stale entries removed across all connections.
    pub fn cleanup_stale_tx_queues(&mut self) -> usize {
        self.cleanup_stale_tx_queues_at(crate::timing::time_ns())
    }

    pub(crate) fn cleanup_stale_tx_queues_at(&mut self, now_ns: u64) -> usize {
        let mut total_removed = 0;
        for state in self.connection_states.values_mut() {
            total_removed += state.cleanup_stale_tx_queue_entries_at(now_ns);
        }
        total_removed
    }

    /// Pick the next ready connection (O(log N) via min-heap)
    ///
    /// Pops the next ready connection from the heap and marks it as off-heap.
    ///
    /// Policy-refresh entries are rescheduled if they do not yet permit a send.
    /// Closed connections and full pipelines are skipped defensively.
    pub fn pick_connection(&mut self) -> Option<usize> {
        self.pick_connection_at(crate::timing::time_ns())
    }

    pub(crate) fn pick_connection_at(&mut self, current_time_ns: u64) -> Option<usize> {
        while let Some(connection_id) = self.ready_heap.pop_ready(current_time_ns) {
            let Some(state) = self.connection_states.get_mut(&connection_id) else {
                continue;
            };
            state.set_on_heap(false);
            if !state.can_send() {
                continue;
            }
            if state.policy.ready_to_send(current_time_ns) {
                return Some(connection_id);
            }
            // Dynamic policies wake at their refresh cadence even when paused or
            // waiting for a much later send. Recalculate only those popped entries.
            let next_wakeup = state.next_send_time(current_time_ns);
            state.set_on_heap(true);
            self.ready_heap.push(connection_id, next_wakeup);
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
        P: Protocol,
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

    /// Get the next time when a connection is scheduled to send or refresh its policy
    ///
    /// Returns `Some(time_ns)` for the earliest send or policy-refresh wakeup,
    /// or `None` if a connection is ready now (closed-loop) or heap is empty.
    ///
    /// Useful for determining how long to sleep in the worker loop.
    pub fn peek_next_ready_time(&self) -> Option<u64> {
        self.ready_heap.peek_next_time()
    }

    pub(crate) fn has_ready(&self, current_time_ns: u64) -> bool {
        self.ready_heap.has_ready(current_time_ns)
    }

    /// Count each connection's intended sends from its next intended time up
    /// to `end_ns`, walking at most `cap` per connection
    ///
    /// Returns `(group_id, connection_id, count, capped)` for every connection
    /// with a nonzero count, including closed ones. Consumes the schedules, so
    /// call it only once the connections stop sending.
    pub fn count_unsent(&mut self, end_ns: u64, cap: u64) -> Vec<(usize, usize, u64, bool)> {
        let mut unsent: Vec<_> = self
            .connection_states
            .values_mut()
            .filter_map(|state| {
                let (count, capped) = state.policy.count_intended_before(end_ns, cap);
                (count > 0).then_some((state.group_id, state.connection_id, count, capped))
            })
            .collect();
        unsent.sort_unstable_by_key(|&(group_id, connection_id, _, _)| (group_id, connection_id));
        unsent
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
    pub fn connection_targets(&self) -> Vec<(usize, SocketAddr)> {
        self.connection_ids
            .iter()
            .filter_map(|&id| self.connection_states.get(&id).map(|s| (id, s.target())))
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
        // Placeholder address for Unix sockets (not used for actual connections)
        let placeholder_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();

        Self::build(factory.create_group()?, groups, |group, path| {
            let connection_id = group.add_connection_path(path)?;
            // Unix sockets don't have a SocketAddr
            Ok((connection_id, placeholder_addr))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> ConnectionState<u64> {
        ConnectionState::new(
            0,
            "127.0.0.1:1".parse().unwrap(),
            1024,
            Box::new(crate::scheduler::ClosedLoopPolicy),
            0,
        )
    }

    fn tx(start: u32, end: u32) -> Option<TxRange> {
        Some(TxRange { start, end })
    }

    // Test frames encode their total size and request ID in an eight-byte header.
    fn frame(id: u32, size: usize) -> Vec<u8> {
        let mut data = vec![0; size];
        data[..4].copy_from_slice(&(size as u32).to_le_bytes());
        data[4..8].copy_from_slice(&id.to_le_bytes());
        data
    }

    fn parse_frame(data: &[u8]) -> Result<(usize, Option<u64>)> {
        if data.len() < 8 {
            return Ok((0, None));
        }
        let size = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
        let id = u32::from_le_bytes(data[4..8].try_into().unwrap()) as u64;
        Ok(if data.len() < size {
            (0, None)
        } else {
            (size, Some(id))
        })
    }

    #[test]
    fn fragmented_limit_frame_followed_by_coalesced_frame() {
        let mut state = state();
        let ts = Timestamp::now();
        state.record_send(1, ts, Duration::ZERO, tx(0, 1), false);
        state.record_send(2, ts, Duration::ZERO, tx(1, 2), false);
        let first = frame(1, MAX_PAYLOAD);
        assert!(state
            .process_recv(&first[..MAX_PAYLOAD - 4], ts, parse_frame)
            .unwrap()
            .is_empty());
        let tail = [&first[MAX_PAYLOAD - 4..], &frame(2, 16)].concat();
        let responses = state.process_recv(&tail, ts, parse_frame).unwrap();
        assert_eq!(
            responses.iter().map(|r| (r.0, r.3)).collect::<Vec<_>>(),
            vec![(1, MAX_PAYLOAD), (2, 16)]
        );
        assert!(state.buffered().is_empty());
        assert_eq!(state.pending_count(), 0);
    }

    #[test]
    fn rejects_oversized_complete_and_incomplete_frames() {
        for complete in [false, true] {
            let mut state = state();
            let ts = Timestamp::now();
            state.record_send(1, ts, Duration::ZERO, tx(0, 1), false);
            let data = frame(1, MAX_PAYLOAD + 2);
            let len = if complete { data.len() } else { data.len() - 1 };
            let error = state.process_recv(&data[..len], ts, parse_frame).unwrap_err();
            assert!(error.to_string().contains("maximum payload"));
            assert_eq!(state.pending_count(), 1);
        }
    }

    #[test]
    fn coalesced_small_frames_preserve_partial_tail() {
        let mut state = state();
        let ts = Timestamp::now();
        let mut data = Vec::new();
        for id in 0..100 {
            state.record_send(id, ts, Duration::ZERO, tx(0, id as u32), false);
            data.extend(frame(id as u32, 8));
        }
        let tail = frame(100, 12);
        state.record_send(100, ts, Duration::ZERO, tx(0, 100), false);
        data.extend_from_slice(&tail[..6]);
        assert_eq!(state.process_recv(&data, ts, parse_frame).unwrap().len(), 100);
        assert_eq!(state.buffered(), &tail[..6]);
        assert_eq!(state.process_recv(&tail[6..], ts, parse_frame).unwrap()[0].0, 100);
        assert!(state.buffered().is_empty());
    }

    #[test]
    fn invalid_parser_consumption_does_not_complete_request() {
        let mut state = state();
        let ts = Timestamp::now();
        state.record_send(1, ts, Duration::ZERO, tx(0, 1), false);
        assert!(state.process_recv(&[0], ts, |_| Ok((2, Some(1)))).is_err());
        assert_eq!(state.pending_count(), 1);
    }

    #[test]
    fn consumed_bytes_without_request_id_is_an_error() {
        let mut state = state();
        let ts = Timestamp::now();
        state.record_send(1, ts, Duration::ZERO, tx(0, 1), false);
        let error = state.process_recv(b"hello", ts, |_| Ok((5, None))).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Protocol error: Protocol consumed 5 bytes but returned no request ID"
        );
        assert_eq!(state.pending_count(), 1);

        // Consuming nothing is an incomplete response, with or without an ID
        for request_id in [None, Some(1)] {
            let mut state = self::state();
            state.record_send(1, ts, Duration::ZERO, tx(0, 1), false);
            let responses = state.process_recv(b"hel", ts, |_| Ok((0, request_id))).unwrap();
            assert!(responses.is_empty());
            assert_eq!(state.buffered(), b"hel");
            assert_eq!(state.pending_count(), 1);
        }
    }

    #[test]
    fn receive_returns_completion_tuples() {
        let factory = test_support::FakeFactory::default();
        let mut pool = ConnectionPool::new(
            &factory,
            "127.0.0.1:1".parse().unwrap(),
            1,
            1,
            Box::new(crate::scheduler::UniformPolicyScheduler::closed_loop()),
            0,
        )
        .unwrap();
        pool.send(0, &[0], 7u64, true).unwrap();
        factory.0.lock().unwrap().responses.push_back(Ok(vec![0]));
        assert_eq!(
            pool.recv_responses(0, |_| Ok((1, Some(7)))).unwrap(),
            vec![(7, Duration::ZERO, true, 1)]
        );
        assert_eq!(pool.get_state(0).unwrap().pending_count(), 0);
    }

    #[test]
    fn pool_receive_reuses_buffer_and_maps_outcomes() {
        let factory = test_support::FakeFactory::default();
        let mut pool = ConnectionPool::new(
            &factory,
            "127.0.0.1:1".parse().unwrap(),
            1,
            2,
            Box::new(crate::scheduler::UniformPolicyScheduler::closed_loop()),
            0,
        )
        .unwrap();
        pool.send(0, &[0], 1u64, false).unwrap();
        pool.send(0, &[0], 2u64, false).unwrap();
        let data = [frame(1, 16), frame(2, 12)].concat();
        let io = factory.0.clone();

        // A frame split across reads stays buffered between them
        io.lock().unwrap().responses.push_back(Ok(data[..10].to_vec()));
        let responses = pool.recv_responses(0, parse_frame).unwrap();
        assert!(responses.is_empty());
        assert_eq!(pool.get_state(0).unwrap().buffered(), &data[..10]);

        io.lock().unwrap().responses.push_back(Ok(data[10..].to_vec()));
        let responses = pool.recv_responses(0, parse_frame).unwrap();
        assert_eq!(responses.iter().map(|r| r.0).collect::<Vec<_>>(), vec![1, 2]);
        let state = pool.get_state(0).unwrap();
        assert!(state.buffered().is_empty());
        assert!(state.recv_buffer.len() >= MAX_PAYLOAD);
        let buffer = (state.recv_buffer.as_ptr(), state.recv_buffer.len());

        // Once large enough, later reads neither grow nor reallocate it
        pool.send(0, &[0], 3u64, false).unwrap();
        io.lock().unwrap().responses.push_back(Ok(frame(3, 8)));
        assert_eq!(pool.recv_responses(0, parse_frame).unwrap()[0].0, 3);
        let state = pool.get_state(0).unwrap();
        assert_eq!((state.recv_buffer.as_ptr(), state.recv_buffer.len()), buffer);

        // Nothing to read is not an error; EOF is a connection error
        assert!(pool.recv_responses(0, parse_frame).unwrap().is_empty());
        io.lock().unwrap().eof = true;
        assert!(matches!(pool.recv_responses(0, parse_frame), Err(crate::Error::Connection(_))));
        assert!(matches!(pool.send(9, &[0], 4, false), Err(crate::Error::Connection(_))));
    }

    #[test]
    fn kernel_tx_timestamp_keeps_send_cycles() {
        use xylem_transport::KernelClock;
        let kernel = |nanos, clock| KernelTimestamp { nanos, clock };
        // TX has a software kernel stamp: (RX clock, whether the kernel delta is used)
        for (rx_clock, kernel_used) in [
            (None, false),
            (Some(KernelClock::NicHardware), false),
            (Some(KernelClock::Realtime), true),
        ] {
            let mut state = state();
            let send_ts = Timestamp::now();
            state.record_send(
                1,
                send_ts,
                Duration::ZERO,
                Some(TxRange { start: 0, end: 8 }),
                false,
            );
            std::thread::sleep(Duration::from_millis(10));
            // The TX stamp is decoded long after the send; its decode time
            // must not stand in for the send time
            assert_eq!(
                state.set_tx_timestamp(8, true, kernel(1_000_000, KernelClock::Realtime)),
                1
            );
            let recv_ts = Timestamp::now();
            let recv_ts = match rx_clock {
                Some(clock) => recv_ts.with_kernel(kernel(1_500_000, clock)),
                None => recv_ts,
            };
            let latency = state.process_recv(&frame(1, 8), recv_ts, parse_frame).unwrap()[0].1;
            if kernel_used {
                assert_eq!(latency, Duration::from_micros(500));
            } else {
                assert_eq!(latency, recv_ts.duration_since(&send_ts));
                assert!(latency >= Duration::from_millis(5), "{rx_clock:?}: {latency:?}");
            }
        }
    }

    fn stamped(state: &ConnectionState<u64>, id: u64) -> Option<u64> {
        state.pending_map[&id].tx_kernel_ts.map(|k| k.nanos)
    }

    fn sw(nanos: u64) -> KernelTimestamp {
        KernelTimestamp {
            nanos,
            clock: xylem_transport::KernelClock::Realtime,
        }
    }

    /// Two 10-byte TCP writes: the kernel reports IDs 9 and 19, which the
    /// transport normalises to tx_end 10 and 20. Each request gets its own
    /// stamp; before the fix, request 1 waited for and took request 2's.
    #[test]
    fn cumulative_tx_stamp_maps_to_its_own_request() {
        let mut state = state();
        let ts = Timestamp::now();
        state.record_send(1, ts, Duration::ZERO, tx(0, 10), false);
        state.record_send(2, ts, Duration::ZERO, tx(10, 20), false);
        assert_eq!(state.set_tx_timestamp(10, true, sw(100)), 1);
        assert_eq!((stamped(&state, 1), stamped(&state, 2)), (Some(100), None));
        assert_eq!(state.set_tx_timestamp(20, true, sw(200)), 1);
        assert_eq!(stamped(&state, 2), Some(200));
        assert!(state.tx_pending_queue.is_empty());

        // A stamp for coalesced writes covers every request it ends past
        state.record_send(3, ts, Duration::ZERO, tx(20, 25), false);
        state.record_send(4, ts, Duration::ZERO, tx(25, 30), false);
        state.record_send(5, ts, Duration::ZERO, tx(30, 40), false);
        assert_eq!(state.set_tx_timestamp(30, true, sw(300)), 2);
        assert_eq!(
            (stamped(&state, 3), stamped(&state, 4), stamped(&state, 5)),
            (Some(300), Some(300), None)
        );

        // Across the u32 wrap
        let mut state = self::state();
        state.record_send(1, ts, Duration::ZERO, tx(u32::MAX - 4, 5), false);
        state.record_send(2, ts, Duration::ZERO, tx(5, 15), false);
        assert_eq!(state.set_tx_timestamp(5, true, sw(1)), 1);
        assert_eq!((stamped(&state, 1), stamped(&state, 2)), (Some(1), None));
    }

    /// UDP datagrams 0, 1, 2 give tx_end 1, 2, 3. A missing or reordered
    /// stamp goes to its own datagram or nowhere.
    #[test]
    fn exact_tx_stamp_never_goes_to_another_datagram() {
        let mut state = state();
        let ts = Timestamp::now();
        for id in 0..4u64 {
            state.record_send(id, ts, Duration::ZERO, tx(id as u32, id as u32 + 1), false);
        }
        // Datagram 0's stamp is lost; 2 arrives before 1
        assert_eq!(state.set_tx_timestamp(3, false, sw(30)), 1);
        assert_eq!(state.set_tx_timestamp(2, false, sw(20)), 1);
        assert_eq!(
            (0..4).map(|id| stamped(&state, id)).collect::<Vec<_>>(),
            vec![None, Some(20), Some(30), None]
        );
        // A duplicate or unknown stamp matches nothing
        assert_eq!(state.set_tx_timestamp(3, false, sw(31)), 0);
        assert_eq!(state.set_tx_timestamp(9, false, sw(90)), 0);
        assert_eq!(stamped(&state, 2), Some(30));

        // Completing the unstamped request removes its queue entry
        state.process_recv(&frame(0, 8), ts, parse_frame).unwrap();
        assert_eq!(state.tx_pending_queue.iter().map(|e| e.1).collect::<Vec<_>>(), vec![3]);
    }

    #[test]
    fn sends_without_tx_range_skip_correlation() {
        let mut state = state();
        let ts = Timestamp::now();
        state.record_send(1, ts, Duration::ZERO, None, false);
        assert!(state.tx_pending_queue.is_empty());
        assert_eq!(state.set_tx_timestamp(1, true, sw(1)), 0);
        assert_eq!(state.process_recv(&frame(1, 8), ts, parse_frame).unwrap().len(), 1);
    }

    #[test]
    fn stale_tx_cleanup_preserves_expiry_boundary() {
        let mut state = state();
        state.tx_pending_queue.extend([(1, 1, 10), (2, 2, 11), (3, 3, 12)]);
        assert_eq!(state.cleanup_stale_tx_queue_entries_at(TX_QUEUE_STALE_TIMEOUT_NS + 11), 1);
        assert_eq!(state.tx_pending_queue.front().unwrap().1, 2);
        assert_eq!(state.cleanup_stale_tx_queue_entries_at(TX_QUEUE_STALE_TIMEOUT_NS + 13), 2);
    }

    fn pattern_pool(baseline: f64) -> ConnectionPool<test_support::FakeGroup, u64> {
        use crate::scheduler::{PatternAwarePolicy, UniformPolicyScheduler};
        use crate::workload::patterns::SpikePattern;
        let factory = test_support::FakeFactory::default();
        let mut pool = ConnectionPool::new(
            &factory,
            "127.0.0.1:1".parse().unwrap(),
            1,
            1,
            Box::new(UniformPolicyScheduler::closed_loop()),
            0,
        )
        .unwrap();
        pool.connection_states.get_mut(&0).unwrap().policy = Box::new(PatternAwarePolicy::new(
            std::sync::Arc::new(SpikePattern::new(
                baseline,
                1000.0,
                Duration::from_millis(20),
                Duration::from_millis(20),
            )),
            100,
        ));
        pool.ready_heap.clear();
        let next = pool.connection_states.get_mut(&0).unwrap().next_send_time(100);
        pool.ready_heap.push(0, next);
        pool
    }

    /// Send on connection 0 as the worker does and put it back on the heap
    fn send_and_reschedule(pool: &mut ConnectionPool<test_support::FakeGroup, u64>, now: u64) {
        let state = pool.connection_states.get_mut(&0).unwrap();
        state.on_request_sent(now);
        let next = state.next_send_time(now);
        state.set_on_heap(true);
        pool.ready_heap.push(0, next);
    }

    #[test]
    fn paused_pattern_refreshes_without_sending_and_resumes() {
        let mut pool = pattern_pool(0.0);
        assert_eq!(pool.pick_connection_at(100), None);
        assert_eq!(pool.pick_connection_at(10_000_100), None);
        assert_eq!(pool.peek_next_ready_time(), Some(20_000_100));
        assert_eq!(pool.pick_connection_at(20_000_100), Some(0));
        let state = pool.connection_states.get_mut(&0).unwrap();
        assert_eq!(state.intended_send_time(), Some(20_000_100));
        send_and_reschedule(&mut pool, 20_000_100);
        // Requests intended during the spike are still sent after it ends,
        // each with its own intended time, but the idle phase adds none.
        let mut intended = Vec::new();
        while pool.pick_connection_at(45_000_100).is_some() {
            intended.push(pool.connection_states[&0].intended_send_time().unwrap());
            send_and_reschedule(&mut pool, 45_000_100);
        }
        assert_eq!(intended, (21..40).map(|ms| ms * 1_000_000 + 100).collect::<Vec<_>>());
        assert_eq!(pool.peek_next_ready_time(), Some(55_000_100));
        assert_eq!(pool.pick_connection_at(55_000_100), None);
    }

    #[test]
    fn queued_low_rate_pattern_observes_short_spike() {
        let mut pool = pattern_pool(0.1); // Ten seconds between baseline requests.
        assert_eq!(pool.pick_connection_at(100), Some(0));
        send_and_reschedule(&mut pool, 100);
        assert_eq!(pool.pick_connection_at(10_000_100), None);
        // The low rate accumulated 0.002 requests before the spike, so the
        // spike's rate completes the next one 0.998 ms into it.
        assert_eq!(pool.pick_connection_at(20_000_100), None);
        assert_eq!(pool.pick_connection_at(21_000_100), Some(0));
        assert_eq!(pool.connection_states[&0].intended_send_time(), Some(20_998_100));
    }

    #[test]
    fn latency_adds_send_lag_to_the_service_latency() {
        use xylem_transport::KernelClock;
        let kernel = |nanos| KernelTimestamp { nanos, clock: KernelClock::Realtime };
        let lag = Duration::from_millis(3);
        let mut state = state();
        let send_ts = Timestamp::now();
        state.record_send(1, send_ts, lag, tx(0, 8), false);
        state.record_send(2, send_ts, lag, tx(8, 16), false);
        // A kernel TX stamp still measures the service part: 500 µs
        assert_eq!(state.set_tx_timestamp(8, true, kernel(1_000_000)), 1);
        let recv_ts = Timestamp::now().with_kernel(kernel(1_500_000));
        let responses = state
            .process_recv(&[frame(1, 8), frame(2, 8)].concat(), recv_ts, parse_frame)
            .unwrap();
        assert_eq!(responses[0].1, lag + Duration::from_micros(500));
        // Without one, the service part is measured in cycles
        assert_eq!(responses[1].1, lag + recv_ts.duration_since(&send_ts));
    }

    #[test]
    fn scheduled_send_measures_lag_from_the_intended_time() {
        let factory = test_support::FakeFactory::default();
        factory.0.lock().unwrap().live_timestamps = true;
        let mut pool = ConnectionPool::new(
            &factory,
            "127.0.0.1:1".parse().unwrap(),
            1,
            4,
            Box::new(crate::scheduler::UniformPolicyScheduler::closed_loop()),
            0,
        )
        .unwrap();
        let intended = crate::timing::time_ns().saturating_sub(5_000_000);
        let lag = pool.send_scheduled(0, &[0], 1u64, false, Some(intended)).unwrap().unwrap();
        assert!(lag >= Duration::from_millis(5) && lag < Duration::from_secs(1), "{lag:?}");
        // A request without an intended time has no lag
        assert_eq!(pool.send_scheduled(0, &[0], 2u64, true, None).unwrap(), None);
        assert_eq!(pool.get_state(0).unwrap().measured_pending_count(), 1);
        assert_eq!(pool.get_state(0).unwrap().pending_count(), 2);
    }

    fn fake_pool(
        datagram: bool,
        max_pending: usize,
    ) -> (test_support::FakeFactory, ConnectionPool<test_support::FakeGroup, u64>) {
        let factory = test_support::FakeFactory::default();
        factory.0.lock().unwrap().datagram = datagram;
        let pool = ConnectionPool::new(
            &factory,
            "127.0.0.1:1".parse().unwrap(),
            1,
            max_pending,
            Box::new(crate::scheduler::UniformPolicyScheduler::closed_loop()),
            0,
        )
        .unwrap();
        (factory, pool)
    }

    /// Test datagram parser: an eight-byte frame completes its ID; a datagram
    /// starting with `F` is a four-byte fragment that completes nothing.
    fn parse_datagram(data: &[u8]) -> Result<(usize, Option<u64>)> {
        if data[0] == b'F' {
            return Ok((4, None));
        }
        parse_frame(data)
    }

    #[test]
    fn datagrams_are_parsed_one_at_a_time() {
        let (factory, mut pool) = fake_pool(true, 4);
        assert!(pool.is_datagram());
        for id in 1..=3 {
            pool.send(0, &[0], id, false).unwrap();
        }
        let io = factory.0.clone();

        // Fragments are absorbed; their bytes count with the response they precede
        io.lock()
            .unwrap()
            .responses
            .extend([Ok(b"Fab1".to_vec()), Ok(b"Fab2".to_vec())]);
        assert!(pool.recv_responses(0, parse_datagram).unwrap().is_empty());
        assert!(pool.recv_responses(0, parse_datagram).unwrap().is_empty());
        assert!(pool.get_state(0).unwrap().buffered().is_empty());
        io.lock().unwrap().responses.push_back(Ok(frame(1, 8)));
        let responses = pool.recv_responses(0, parse_datagram).unwrap();
        assert_eq!(responses.iter().map(|r| (r.0, r.3)).collect::<Vec<_>>(), vec![(1, 16)]);

        // One datagram may hold several frames, and a fragment after them
        let data = [frame(2, 8), frame(3, 12), b"Fxyz".to_vec()].concat();
        io.lock().unwrap().responses.push_back(Ok(data));
        let responses = pool.recv_responses(0, parse_datagram).unwrap();
        assert_eq!(responses.iter().map(|r| (r.0, r.3)).collect::<Vec<_>>(), vec![(2, 8), (3, 12)]);
        assert_eq!(pool.get_state(0).unwrap().pending_count(), 0);

        // A datagram is never joined with the next: a partial frame is an error
        pool.send(0, &[0], 4, false).unwrap();
        io.lock().unwrap().responses.push_back(Ok(frame(4, 16)[..10].to_vec()));
        let error = pool.recv_responses(0, parse_datagram).unwrap_err();
        assert!(error.to_string().contains("10 bytes of a 10-byte datagram unparsed"), "{error}");
        assert!(pool.get_state(0).unwrap().buffered().is_empty());
        assert_eq!(pool.get_state(0).unwrap().pending_count(), 1);

        // A duplicate datagram for a completed request is dropped, not an error
        io.lock().unwrap().responses.push_back(Ok(frame(3, 12)));
        assert!(pool.recv_responses(0, parse_datagram).unwrap().is_empty());
        assert_eq!(pool.take_late_responses(0), 0);
    }

    #[test]
    fn stream_pool_keeps_joining_reads() {
        let (factory, mut pool) = fake_pool(false, 1);
        assert!(!pool.is_datagram());
        pool.send(0, &[0], 1, false).unwrap();
        let data = frame(1, 16);
        factory.0.lock().unwrap().responses.push_back(Ok(data[..10].to_vec()));
        assert!(pool.recv_responses(0, parse_frame).unwrap().is_empty());
        factory.0.lock().unwrap().responses.push_back(Ok(data[10..].to_vec()));
        assert_eq!(pool.recv_responses(0, parse_frame).unwrap()[0].0, 1);
        // Unknown IDs and fragments stay errors on a byte stream
        factory.0.lock().unwrap().responses.push_back(Ok(frame(9, 8)));
        assert!(pool.recv_responses(0, parse_frame).is_err());
    }

    #[test]
    fn timed_out_request_frees_its_slot_and_its_late_response_is_dropped() {
        for datagram in [true, false] {
            let (factory, mut pool) = fake_pool(datagram, 2);
            let timeout = Duration::from_millis(10);
            pool.set_request_timeout(0, Some(timeout));
            let sent_ns = factory.0.lock().unwrap().timestamp.clock_ns();
            assert_eq!(pool.next_request_deadline(), None);
            pool.send(0, &[0], 1, false).unwrap();
            pool.send(0, &[0], 2, true).unwrap();
            assert!(!pool.get_state(0).unwrap().can_send());
            let deadline = sent_ns + 10_000_000;
            assert_eq!(pool.next_request_deadline(), Some(deadline));

            let mut expired = Vec::new();
            pool.expire_requests_at(deadline - 1, |c, g, n, m| expired.push((c, g, n, m)));
            assert!(expired.is_empty());
            pool.expire_requests_at(deadline, |c, g, n, m| expired.push((c, g, n, m)));
            // Both expired, one of them a measurement request
            assert_eq!(expired, vec![(0, 0, 2, 1)]);
            let state = pool.get_state(0).unwrap();
            assert_eq!(state.pending_count(), 0);
            assert!(state.can_send());
            assert!(state.tx_pending_queue.is_empty());
            assert_eq!(pool.next_request_deadline(), None);

            // Late responses complete nothing; only the measured one counts
            let io = factory.0.clone();
            io.lock().unwrap().responses.push_back(Ok([frame(2, 8), frame(1, 8)].concat()));
            assert!(pool.recv_responses(0, parse_frame).unwrap().is_empty());
            assert_eq!(pool.take_late_responses(0), 1);
            assert_eq!(pool.take_late_responses(0), 0);
            // A second copy is no longer recognised: dropped on datagrams only
            io.lock().unwrap().responses.push_back(Ok(frame(1, 8)));
            assert_eq!(pool.recv_responses(0, parse_frame).is_ok(), datagram);
        }
    }

    #[test]
    fn completed_requests_do_not_time_out() {
        let (factory, mut pool) = fake_pool(true, 4);
        pool.set_request_timeout(0, Some(Duration::from_millis(10)));
        let sent_ns = factory.0.lock().unwrap().timestamp.clock_ns();
        for id in 1..=3 {
            pool.send(0, &[0], id, false).unwrap();
        }
        factory.0.lock().unwrap().responses.push_back(Ok(frame(2, 8)));
        assert_eq!(pool.recv_responses(0, parse_frame).unwrap().len(), 1);
        let mut expired = Vec::new();
        pool.expire_requests_at(sent_ns + 20_000_000, |c, g, n, m| expired.push((c, g, n, m)));
        assert_eq!(expired, vec![(0, 0, 2, 2)]);
        // Request 2 completed before its deadline and is not remembered
        let state = pool.get_state(0).unwrap();
        assert_eq!(state.timed_out_order, VecDeque::from([1, 3]));
        assert!(state.deadlines.is_empty());

        // Other groups and connections without a timeout never expire
        let (_, mut pool) = fake_pool(true, 4);
        pool.set_request_timeout(1, Some(Duration::from_millis(10)));
        pool.send(0, &[0], 1, false).unwrap();
        assert_eq!(pool.next_request_deadline(), None);
        pool.expire_requests_at(u64::MAX, |_, _, _, _| panic!("no timeout"));
        assert_eq!(pool.get_state(0).unwrap().pending_count(), 1);
    }

    #[test]
    fn timed_out_ids_are_forgotten_oldest_first() {
        let mut state = state();
        for id in 0..TIMED_OUT_MEMORY as u64 + 2 {
            state.remember_timed_out(id, false);
        }
        assert_eq!(state.timed_out_ids.len(), TIMED_OUT_MEMORY);
        assert!(!state.timed_out_ids.contains_key(&1));
        assert!(state.timed_out_ids.contains_key(&2));
    }

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

/// Deterministic transport shared by execution tests; no sockets or timing waits.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::sync::{Arc, Mutex};

    pub struct FakeIo {
        pub responses: VecDeque<xylem_transport::Result<Vec<u8>>>,
        pub sent: Vec<Vec<u8>>,
        pub fail_send: bool,
        pub timeouts: Vec<Duration>,
        pub tx_polls: usize,
        pub closed: bool,
        pub timestamp: Timestamp,
        pub response_connection_id: usize,
        pub tx_timestamps: Vec<xylem_transport::TxTimestampInfo>,
        /// Report EOF once `responses` is empty
        pub eof: bool,
        /// Stamp sends and receives with the current time instead of `timestamp`
        pub live_timestamps: bool,
        /// Report a datagram group; read when the pool is built
        pub datagram: bool,
    }

    impl Default for FakeIo {
        fn default() -> Self {
            Self {
                responses: VecDeque::new(),
                sent: Vec::new(),
                fail_send: false,
                timeouts: Vec::new(),
                tx_polls: 0,
                closed: false,
                timestamp: Timestamp::now(),
                response_connection_id: 0,
                tx_timestamps: Vec::new(),
                eof: false,
                live_timestamps: false,
                datagram: false,
            }
        }
    }

    impl FakeIo {
        fn now(&self) -> Timestamp {
            if self.live_timestamps {
                Timestamp::now()
            } else {
                self.timestamp
            }
        }
    }

    #[derive(Default)]
    pub struct FakeFactory(pub Arc<Mutex<FakeIo>>);

    pub struct FakeGroup {
        io: Arc<Mutex<FakeIo>>,
        connections: usize,
    }

    impl FakeGroup {
        fn check(&self, id: usize) -> xylem_transport::Result<()> {
            if id < self.connections {
                Ok(())
            } else {
                Err(xylem_transport::Error::Connection("Connection not found".into()))
            }
        }
    }

    impl ConnectionGroup for FakeGroup {
        fn add_connection(&mut self, _: &SocketAddr) -> xylem_transport::Result<usize> {
            let id = self.connections;
            self.connections += 1;
            Ok(id)
        }

        fn send(
            &mut self,
            conn_id: usize,
            data: &[u8],
        ) -> xylem_transport::Result<xylem_transport::SendReceipt> {
            self.check(conn_id)?;
            let mut io = self.io.lock().unwrap();
            if io.fail_send {
                return Err(xylem_transport::Error::Io(std::io::Error::other("send failed")));
            }
            io.sent.push(data.to_vec());
            let end = io.sent.len() as u32;
            Ok(xylem_transport::SendReceipt {
                timestamp: io.now(),
                tx: Some(TxRange { start: end - 1, end }),
            })
        }

        // An empty queued response stands for "no data right now"
        fn recv(&mut self, conn_id: usize, buf: &mut [u8]) -> xylem_transport::Result<RecvOutcome> {
            self.check(conn_id)?;
            let mut io = self.io.lock().unwrap();
            if io.eof && io.responses.is_empty() {
                return Ok(RecvOutcome::Eof);
            }
            let data = io.responses.pop_front().unwrap_or_else(|| Ok(Vec::new()))?;
            if data.is_empty() {
                return Ok(RecvOutcome::WouldBlock);
            }
            buf[..data.len()].copy_from_slice(&data);
            Ok(RecvOutcome::Data { len: data.len(), timestamp: io.now() })
        }

        fn is_datagram(&self) -> bool {
            self.io.lock().unwrap().datagram
        }

        fn poll(
            &mut self,
            timeout: Duration,
            ready: &mut Vec<usize>,
        ) -> xylem_transport::Result<()> {
            let mut io = self.io.lock().unwrap();
            io.timeouts.push(timeout);
            ready.clear();
            if !io.responses.is_empty() {
                ready.push(io.response_connection_id);
            }
            Ok(())
        }

        fn poll_tx_timestamps(
            &mut self,
            out: &mut Vec<xylem_transport::TxTimestampInfo>,
        ) -> xylem_transport::Result<()> {
            let mut io = self.io.lock().unwrap();
            io.tx_polls += 1;
            out.clear();
            out.append(&mut io.tx_timestamps);
            Ok(())
        }

        fn len(&self) -> usize {
            self.connections
        }

        fn close(&mut self, _: usize) -> xylem_transport::Result<()> {
            self.io.lock().unwrap().closed = true;
            Ok(())
        }

        fn close_all(&mut self) -> xylem_transport::Result<()> {
            self.close(0)
        }
    }

    impl TransportFactory for FakeFactory {
        type Group = FakeGroup;

        fn create_group(&self) -> xylem_transport::Result<Self::Group> {
            Ok(FakeGroup { io: self.0.clone(), connections: 0 })
        }
    }
}
