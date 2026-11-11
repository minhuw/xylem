//! Connection management with pipelining support
//!
//! This module implements Lancet-style connection pooling, allowing multiple
//! outstanding requests per connection for higher throughput.
//!
//! Uses `ConnectionGroup` internally for efficient I/O multiplexing - a single
//! poll syscall checks all connections in the pool.

use crate::scheduler::{GroupDispatcher, Policy, ReadyHeap};
use crate::stats::{LatencyBreakdown, MAX_ERROR_KINDS, MAX_ERROR_KIND_LEN, OTHER_ERROR_KIND};
use crate::traffic_group::OnError;
use crate::Result;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::net::SocketAddr;
use std::time::Duration;
use xylem_common::{ConnectionRole, ErrorReply, FastHashMap, Parsed, Protocol};
use xylem_transport::{
    ConnectionGroup, KernelTimestamp, RecvOutcome, SendProgress, Timestamp, TransportFactory,
    TxRange, TxTimestampInfo,
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

/// Kind of an error reply, numbered by the connection that received it; see
/// [`ConnectionPool::error_kind`]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorKindId(u8);

impl ErrorKindId {
    /// Every kind after the first [`MAX_ERROR_KINDS`] of a connection
    const OTHER: Self = Self(u8::MAX);
}

/// A request completed by its response, as the receive path returns it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completion<ReqId> {
    /// The request's ID
    pub id: ReqId,
    /// The request's send lag plus its service latency, from the send to the
    /// receive timestamp
    pub latency: Duration,
    /// Whether the request was a warmup (or setup) request, whose stats are
    /// not collected
    pub is_warmup: bool,
    /// Bytes the response took: those the protocol consumed, plus those it
    /// absorbed before it ([`Parsed::Absorbed`])
    pub bytes_consumed: usize,
    /// Set for an error reply; see [`ConnectionPool::error_kind`]
    pub error: Option<ErrorKindId>,
    /// Where the latency went. The worker records it only for a successful
    /// response, as it records only those latencies in the latency
    /// distribution.
    pub breakdown: LatencyBreakdown,
}

/// What a connection does with error replies, and the kinds it has seen
#[derive(Debug, Default)]
struct ErrorReplies {
    /// Fail on an error reply instead of counting it (`on_error = "abort"`)
    abort: bool,
    /// Kinds seen, in order; at most [`MAX_ERROR_KINDS`], each cut to
    /// [`MAX_ERROR_KIND_LEN`] bytes. Only a new kind allocates.
    kinds: Vec<Box<str>>,
}

impl ErrorReplies {
    /// Report an error reply of traffic group `group_id` on `connection_id`:
    /// an error when aborting, else the ID of its kind
    fn report(
        &mut self,
        group_id: usize,
        connection_id: usize,
        error: &ErrorReply<'_>,
    ) -> Result<ErrorKindId> {
        if self.abort {
            return Err(crate::Error::Protocol(format!(
                "Traffic group {group_id}, connection {connection_id}: error reply {} \
                 (on_error = \"abort\"): {}",
                error.kind.escape_debug(),
                error.message.escape_debug()
            )));
        }
        let mut len = error.kind.len().min(MAX_ERROR_KIND_LEN);
        while !error.kind.is_char_boundary(len) {
            len -= 1;
        }
        let kind = &error.kind[..len];
        if let Some(index) = self.kinds.iter().position(|known| **known == *kind) {
            return Ok(ErrorKindId(index as u8));
        }
        if self.kinds.len() == MAX_ERROR_KINDS {
            return Ok(ErrorKindId::OTHER);
        }
        self.kinds.push(kind.into());
        Ok(ErrorKindId((self.kinds.len() - 1) as u8))
    }

    fn kind(&self, id: ErrorKindId) -> &str {
        self.kinds.get(usize::from(id.0)).map_or(OTHER_ERROR_KIND, |kind| kind)
    }
}

/// A request the transport took the last byte of; see
/// [`ConnectionPool::take_sent`]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SentRequest {
    pub group_id: usize,
    pub connection_id: usize,
    /// The request's bytes
    pub bytes: usize,
    /// Time from its intended send time to its send; `None` without a
    /// schedule (closed loop)
    pub lag: Option<Duration>,
    /// Whether it is a measurement request: no warmup or setup request
    pub measured: bool,
}

/// A request queued on a connection whose last byte the transport has not
/// taken yet
#[derive(Debug)]
struct QueuedRequest<ReqId> {
    /// Stream offset past its last byte, counted in [`ConnectionState::queued_bytes`]
    end: u64,
    len: usize,
    req_id: ReqId,
    intended_ns: Option<u64>,
    is_warmup: bool,
    is_setup: bool,
}

/// Pending request tracking
#[derive(Debug)]
struct PendingRequest {
    /// Cycles read when the send call completed
    send_ts: Timestamp,
    /// Time from the intended send time to the send; zero without a schedule
    send_lag: Duration,
    is_warmup: bool,
    /// Connection setup request; implies `is_warmup`
    is_setup: bool,
    /// Kernel TX timestamp of the send, once reported
    tx_kernel_ts: Option<KernelTimestamp>,
}

/// Per-connection application state (no transport - I/O goes through ConnectionGroup)
///
/// # Pending Request Tracking
///
/// Uses two data structures for efficient request tracking:
///
/// 1. `pending_map: FastHashMap<ReqId, PendingRequest>` - Primary storage, O(1) lookup by request ID
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
/// # Send Queue
///
/// Requests and protocol-driven bytes go into `out` in order and reach the
/// transport through non-blocking writes: a send round's requests on the
/// connection leave in one write at the pool's flush, and bytes the
/// transport does not take wait for the connection to become writable,
/// ahead of anything queued later. A request enters `pending_map`, with
/// the timestamp of the write that took its last byte, only then; until
/// then it waits in `unsent` and holds its pipeline slot.
///
/// # Scheduling Invariant
///
/// A connection is on the ready heap if and only if it can accept more requests:
/// - `on_heap == true` ⟺ `pending_count < max_pending`
///
/// The heap stores ALL connections that can send, including rate-limited connections
/// with future send times. This allows efficient timeout calculation by peeking at the
/// earliest ready time (needed for epoll_wait timeout).
///
/// A connection of a group-dispatched traffic group is on the heap only while
/// it sets up; once set up, it waits on its dispatcher's idle list instead.
pub struct ConnectionState<ReqId: Eq + Hash + Clone> {
    /// Connection ID in the ConnectionGroup
    connection_id: usize,
    /// Target address
    target: SocketAddr,
    /// Maximum pending requests allowed
    max_pending_requests: usize,
    /// Primary storage: ReqId -> PendingRequest for O(1) response lookup
    pending_map: FastHashMap<ReqId, PendingRequest>,
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
    /// Whether this connection is currently in the ready heap, or on its
    /// dispatcher's idle list
    /// Invariant: on_heap == true ⟺ connection can accept more requests
    on_heap: bool,
    /// Index of the pool's group dispatcher that sends on this connection;
    /// `None` when the connection follows its own policy
    dispatcher: Option<usize>,
    /// Whether the connection was last scheduled for connection setup: it is
    /// on the heap as ready now and sends without consulting its policy
    in_setup: bool,
    /// How long a request may stay pending before it times out; `None` waits forever
    request_timeout: Option<Duration>,
    /// `(deadline_ns, req_id)` of sends with a request timeout, in send order.
    /// Entries of completed requests are skipped and pruned lazily.
    deadlines: VecDeque<(u64, ReqId)>,
    /// Timed-out requests whose late responses are still recognised, mapped
    /// to whether each was a warmup request
    timed_out_ids: FastHashMap<ReqId, bool>,
    /// `timed_out_ids` keys in timeout order, for forgetting the oldest
    timed_out_order: VecDeque<ReqId>,
    /// Late responses to timed-out measurement requests, not yet taken by the worker
    late_responses: u64,
    /// Bytes the protocol absorbed without completing a request; they are
    /// attributed to the next completed response
    absorbed_bytes: usize,
    /// Error reply handling and kinds
    error_replies: ErrorReplies,
    /// Opened for the protocol's own use ([`ConnectionRole::Control`]): it
    /// has no traffic policy of its own and sends only setup requests
    control: bool,
    /// Bytes queued to send; the transport took those before `out_head`
    out: Vec<u8>,
    out_head: usize,
    /// Bytes ever queued, and ever taken by the transport: stream offsets
    queued_bytes: u64,
    sent_bytes: u64,
    /// Queued requests whose last byte the transport has not taken, in order
    unsent: VecDeque<QueuedRequest<ReqId>>,
    /// On the pool's list of connections to write at its next flush
    staged: bool,
    /// The transport took fewer bytes than queued: the rest waits for the
    /// connection to become writable, with write interest set
    write_blocked: bool,
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
            pending_map: FastHashMap::with_capacity_and_hasher(
                max_pending_requests,
                Default::default(),
            ),
            tx_pending_queue: VecDeque::with_capacity(max_pending_requests),
            recv_buffer: Vec::new(),
            recv_len: 0,
            closed: false,
            policy,
            group_id,
            on_heap: false,
            dispatcher: None,
            in_setup: false,
            request_timeout: None,
            deadlines: VecDeque::new(),
            timed_out_ids: FastHashMap::default(),
            timed_out_order: VecDeque::new(),
            late_responses: 0,
            absorbed_bytes: 0,
            error_replies: ErrorReplies::default(),
            control: false,
            out: Vec::new(),
            out_head: 0,
            queued_bytes: 0,
            sent_bytes: 0,
            unsent: VecDeque::new(),
            staged: false,
            write_blocked: false,
        }
    }

    /// Check if this connection can accept more requests
    pub fn can_send(&self) -> bool {
        !self.closed && self.pending_count() < self.max_pending_requests
    }

    /// Get connection ID
    pub fn connection_id(&self) -> usize {
        self.connection_id
    }

    /// Get target address
    pub fn target(&self) -> SocketAddr {
        self.target
    }

    /// Get pending request count: requests sent and waiting for their
    /// responses, and requests queued but not yet taken by the transport
    pub fn pending_count(&self) -> usize {
        self.pending_map.len() + self.unsent.len()
    }

    /// Whether queued bytes wait for the connection to become writable
    pub fn is_write_blocked(&self) -> bool {
        self.write_blocked
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

    /// Whether the connection was opened for the protocol's own use
    /// ([`ConnectionRole::Control`]) rather than to carry load
    pub fn is_control(&self) -> bool {
        self.control
    }

    /// Get the next send or policy-refresh wakeup time (in nanoseconds)
    pub fn next_send_time(&mut self, current_time_ns: u64) -> Option<u64> {
        self.policy.next_send_time(current_time_ns)
    }

    /// Intended send time of the next request; `None` without a schedule
    pub fn intended_send_time(&self) -> Option<u64> {
        self.policy.intended_send_time()
    }

    /// Number of pending measurement (non-warmup) requests the transport took
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

    /// Queue `data` after the bytes queued before it; `request` is the
    /// request it carries, if any
    fn queue(&mut self, data: &[u8], request: Option<(ReqId, Option<u64>, bool, bool)>) {
        self.out.extend_from_slice(data);
        self.queued_bytes += data.len() as u64;
        if let Some((req_id, intended_ns, is_warmup, is_setup)) = request {
            self.unsent.push_back(QueuedRequest {
                end: self.queued_bytes,
                len: data.len(),
                req_id,
                intended_ns,
                is_warmup,
                is_setup,
            });
        }
    }

    /// Mark the connection closed and drop its pending and queued requests
    fn close(&mut self) {
        self.closed = true;
        self.pending_map.clear();
        self.unsent.clear();
        self.out.clear();
        self.out_head = 0;
    }

    /// Queued bytes the transport has not taken
    fn unsent_bytes(&self) -> &[u8] {
        &self.out[self.out_head..]
    }

    /// The transport took the next `progress.accepted` queued bytes: record
    /// every request whose last byte it took as sent, with the write's
    /// timestamp, and push it to `sent`
    ///
    /// A request's lag runs from its intended send time to that timestamp.
    /// The TX range of the write is given to its last request; one ending
    /// earlier in it ends that many bytes into the range. Datagram
    /// transports count datagrams, but write one request at a time.
    fn on_progress(&mut self, progress: SendProgress, sent: &mut Vec<SentRequest>) {
        let before = self.sent_bytes;
        self.sent_bytes += progress.accepted as u64;
        self.out_head += progress.accepted;
        if self.out_head == self.out.len() {
            self.out.clear();
            self.out_head = 0;
        } else if self.out_head > self.out.len() / 2 {
            self.out.drain(..self.out_head);
            self.out_head = 0;
        }
        let receipt = progress.receipt;
        while self.unsent.front().is_some_and(|request| request.end <= self.sent_bytes) {
            let request = self.unsent.pop_front().expect("checked the front");
            let tx = receipt.tx.map(|range| {
                if request.end == self.sent_bytes {
                    range
                } else {
                    let end = range.start.wrapping_add((request.end - before) as u32);
                    TxRange { start: range.start, end }
                }
            });
            let lag = request.intended_ns.map(|intended| {
                Duration::from_nanos(receipt.timestamp.clock_ns().saturating_sub(intended))
            });
            self.record_request(
                request.req_id,
                receipt.timestamp,
                lag.unwrap_or_default(),
                tx,
                request.is_warmup,
                request.is_setup,
            );
            sent.push(SentRequest {
                group_id: self.group_id,
                connection_id: self.connection_id,
                bytes: request.len,
                lag,
                measured: !request.is_warmup && !request.is_setup,
            });
        }
    }

    /// Record a sent request, with its TX range when the transport reports TX timestamps
    ///
    /// Only `tx.end` is needed to determine which TX timestamp covers this request.
    #[cfg(test)]
    fn record_send(
        &mut self,
        req_id: ReqId,
        send_ts: Timestamp,
        send_lag: Duration,
        tx: Option<TxRange>,
        is_warmup: bool,
    ) {
        self.record_request(req_id, send_ts, send_lag, tx, is_warmup, false);
    }

    /// Record a sent request, marking connection setup requests
    fn record_request(
        &mut self,
        req_id: ReqId,
        send_ts: Timestamp,
        send_lag: Duration,
        tx: Option<TxRange>,
        is_warmup: bool,
        is_setup: bool,
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
                is_warmup: is_warmup || is_setup,
                is_setup,
                tx_kernel_ts: None,
            },
        );
    }

    /// Restart the intended schedule once the last pending setup request of
    /// the connection completed. Setup requests never advance the schedule,
    /// but a schedule started before setup would otherwise turn the time
    /// spent waiting for setup replies into send lag and a burst of overdue
    /// sends. The schedule restarts at its first intended time from then on.
    fn finish_setup_request(&mut self) {
        let unsent_setup = self.unsent.iter().any(|request| request.is_setup);
        if !unsent_setup && !self.pending_map.values().any(|pending| pending.is_setup) {
            self.policy.reset();
        }
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

    /// Complete the pending request `req_id`, returning its latency, whether
    /// it was a warmup request, and where its latency went
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
    ) -> Result<Option<(Duration, bool, LatencyBreakdown)>> {
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
        let breakdown = LatencyBreakdown::between(&send_ts, &recv_ts);
        let latency = pending.send_lag + breakdown.service;
        if pending.is_setup {
            self.finish_setup_request();
        }
        Ok(Some((latency, pending.is_warmup, breakdown)))
    }

    /// Process `data` as if the transport had just read it into the spare tail
    #[cfg(test)]
    fn process_recv<F>(
        &mut self,
        data: &[u8],
        recv_ts: Timestamp,
        process_fn: F,
    ) -> Result<Vec<Completion<ReqId>>>
    where
        F: for<'a> FnMut(&'a [u8]) -> Result<Parsed<'a, ReqId>>,
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
    /// Returns a [`Completion`] for each completed response; its error kind
    /// is set for an error reply. Bytes the parser absorbs count toward the
    /// next completed response. The latency is the request's send lag plus
    /// its service latency, from the send to the receive timestamp. With
    /// `on_error = "abort"`, an error reply is an error.
    fn process_received<F>(
        &mut self,
        len: usize,
        recv_ts: Timestamp,
        mut process_fn: F,
    ) -> Result<Vec<Completion<ReqId>>>
    where
        F: for<'a> FnMut(&'a [u8]) -> Result<Parsed<'a, ReqId>>,
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
            let parsed = process_fn(&self.recv_buffer[cursor..self.recv_len])?;
            let consumed = parsed.consumed();
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
            cursor += consumed;
            self.absorbed_bytes += consumed;
            let Parsed::Complete { id: req_id, error, .. } = parsed else {
                continue;
            };
            let error = error
                .map(|error| self.error_replies.report(self.group_id, self.connection_id, &error))
                .transpose()?;
            if let Some((latency, is_warmup, breakdown)) = self.complete(&req_id, recv_ts, false)? {
                responses.push(Completion {
                    id: req_id,
                    latency,
                    is_warmup,
                    bytes_consumed: std::mem::take(&mut self.absorbed_bytes),
                    error,
                    breakdown,
                });
            }
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
    ) -> Result<Vec<Completion<ReqId>>>
    where
        F: for<'a> FnMut(&'a [u8]) -> Result<Parsed<'a, ReqId>>,
    {
        debug_assert_eq!(self.recv_len, 0, "datagrams leave no unparsed bytes");
        let mut responses = Vec::new();
        let mut cursor = 0;
        while cursor < len {
            let available = len - cursor;
            let parsed = process_fn(&self.recv_buffer[cursor..len])?;
            let consumed = parsed.consumed();
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
            let Parsed::Complete { id: req_id, error, .. } = parsed else {
                continue;
            };
            let error = error
                .map(|error| self.error_replies.report(self.group_id, self.connection_id, &error))
                .transpose()?;
            if let Some((latency, is_warmup, breakdown)) = self.complete(&req_id, recv_ts, true)? {
                responses.push(Completion {
                    id: req_id,
                    latency,
                    is_warmup,
                    bytes_consumed: std::mem::take(&mut self.absorbed_bytes),
                    error,
                    breakdown,
                });
            }
        }
        Ok(responses)
    }
}

/// A connection picked to send, and the schedule its request follows
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pick {
    /// Connection to send on
    pub connection_id: usize,
    /// Index of the group dispatcher whose due arrival the request takes;
    /// `None` when the request follows the connection's own policy
    dispatcher: Option<usize>,
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
    connection_states: FastHashMap<usize, ConnectionState<ReqId>>,
    /// Ordered list of connection IDs for iteration
    connection_ids: Vec<usize>,
    /// Min-heap for O(log N) lookup of next ready connection
    ready_heap: ReadyHeap,
    /// Arrival processes of the group-dispatched traffic groups, one per
    /// group, with their idle connections
    dispatchers: Vec<GroupDispatcher>,
    /// Reused buffer for TX timestamps drained from the group
    tx_timestamps: Vec<TxTimestampInfo>,
    /// Whether the group carries datagrams, which are parsed one at a time
    datagram: bool,
    /// No pending request times out before this (a `timing::time_ns` value);
    /// `None` when none can. May be earlier than the true next deadline.
    next_deadline_ns: Option<u64>,
    /// Connections with bytes queued since the last flush, which writes them
    staged: Vec<usize>,
    /// Requests the transport took since the last [`Self::take_sent`]
    sent: Vec<SentRequest>,
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
        Self::new_multi_group_with_roles(factory, groups, &HashMap::new())
    }

    /// Create a pool like [`Self::new_multi_group_with_targets`], opening
    /// each group's connections in the order and with the roles `roles` maps
    /// its group ID to (see [`Protocol::connection_roles`]); a group without
    /// an entry gets only load connections
    pub fn new_multi_group_with_roles<F: TransportFactory<Group = G>>(
        factory: &F,
        groups: Vec<TrafficGroupConfig>,
        roles: &HashMap<usize, Vec<ConnectionRole>>,
    ) -> Result<Self> {
        Self::build(factory.create_group()?, groups, roles, |group, target| {
            let connection_id = group.add_connection(target)?;
            Ok((connection_id, *target))
        })
    }

    /// Build a pool by adding every group's connections to `group`
    ///
    /// `add_connection` opens one connection to a group's target and returns its
    /// connection ID and the address recorded in its state. Each group's
    /// scheduler assigns policies by the load connection's index within that
    /// group; a control connection gets a closed-loop policy and sends only
    /// while its protocol is setting it up.
    fn build<T>(
        mut group: G,
        groups: Vec<GroupConfig<T>>,
        roles: &HashMap<usize, Vec<ConnectionRole>>,
        mut add_connection: impl FnMut(&mut G, &T) -> Result<(usize, SocketAddr)>,
    ) -> Result<Self> {
        let total_connections: usize = groups
            .iter()
            .map(|(group_id, _, count, _, _)| roles.get(group_id).map_or(*count, Vec::len))
            .sum();
        let mut connection_states =
            FastHashMap::with_capacity_and_hasher(total_connections, Default::default());
        let mut connection_ids = Vec::with_capacity(total_connections);
        let mut ready_heap = ReadyHeap::new();
        let mut dispatchers: Vec<GroupDispatcher> = Vec::new();

        for (
            group_id,
            target,
            connection_count,
            max_pending_per_connection,
            mut policy_scheduler,
        ) in groups
        {
            let group_dispatcher = policy_scheduler.dispatch_policy().map(|policy| {
                dispatchers.push(GroupDispatcher::new(group_id, policy));
                dispatchers.len() - 1
            });
            let load_roles;
            let group_roles = match roles.get(&group_id) {
                Some(group_roles) => group_roles.as_slice(),
                None => {
                    load_roles = vec![ConnectionRole::Load; connection_count];
                    &load_roles
                }
            };
            let load = group_roles.iter().filter(|&&role| role == ConnectionRole::Load).count();
            if load != connection_count {
                return Err(crate::Error::Config(format!(
                    "Traffic group {group_id}: the protocol asked for {load} load connections, \
                     not the {connection_count} configured"
                )));
            }
            let mut load_index = 0;
            for &role in group_roles {
                let (connection_id, address) = add_connection(&mut group, &target)?;
                // A control connection takes no arrivals of the group's
                // dispatcher: it sends its setup requests off the heap
                let (mut policy, dispatcher) = match role {
                    ConnectionRole::Load => {
                        load_index += 1;
                        (policy_scheduler.assign_policy(load_index - 1), group_dispatcher)
                    }
                    ConnectionRole::Control => {
                        let policy: Box<dyn Policy> = Box::new(crate::scheduler::ClosedLoopPolicy);
                        (policy, None)
                    }
                };

                // The first call starts the connection's intended schedule once
                // it is connected, or for a group-dispatched connection, the
                // group's; anchoring the schedules restarts it. Then add it to
                // the heap or its dispatcher's idle list.
                let now_ns = crate::timing::time_ns();
                match dispatcher {
                    Some(dispatcher) => dispatchers[dispatcher].push_idle(connection_id, now_ns),
                    None => ready_heap.push(connection_id, policy.next_send_time(now_ns)),
                }

                let mut state = ConnectionState::new(
                    connection_id,
                    address,
                    max_pending_per_connection,
                    policy,
                    group_id,
                );

                state.dispatcher = dispatcher;
                state.control = role == ConnectionRole::Control;
                state.set_on_heap(true);
                connection_states.insert(connection_id, state);
                connection_ids.push(connection_id);
            }
        }

        Ok(Self {
            datagram: group.is_datagram(),
            group,
            connection_states,
            connection_ids,
            ready_heap,
            dispatchers,
            tx_timestamps: Vec::new(),
            next_deadline_ns: None,
            staged: Vec::new(),
            sent: Vec::new(),
        })
    }

    /// Whether the connections carry datagrams (see [`ConnectionGroup::is_datagram`])
    pub fn is_datagram(&self) -> bool {
        self.datagram
    }

    /// See [`ConnectionGroup::clock_calibration`]
    pub fn clock_calibration(&self) -> Option<xylem_transport::ClockCalibrationReport> {
        self.group.clock_calibration()
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

    /// Set what error replies on traffic group `group_id` do: count them
    /// (the default), or fail the receive that got one
    pub fn set_on_error(&mut self, group_id: usize, on_error: OnError) {
        for state in self.connection_states.values_mut() {
            if state.group_id == group_id {
                state.error_replies.abort = on_error == OnError::Abort;
            }
        }
    }

    /// The kind of an error reply received on `connection_id`, as numbered
    /// by [`Self::recv_responses`]; [`OTHER_ERROR_KIND`] past the
    /// connection's first [`MAX_ERROR_KINDS`] kinds
    pub fn error_kind(&self, connection_id: usize, id: ErrorKindId) -> &str {
        self.connection_states
            .get(&connection_id)
            .map_or(OTHER_ERROR_KIND, |state| state.error_replies.kind(id))
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

    /// Write the bytes queued since the last flush, one write per
    /// connection, and put them on the wire (see [`ConnectionGroup::flush`])
    ///
    /// A connection waiting to become writable is left for
    /// [`Self::write_queued`] once it is.
    pub fn flush(&mut self) -> Result<()> {
        let mut staged = std::mem::take(&mut self.staged);
        let mut result = Ok(());
        for &connection_id in &staged {
            let Some(state) = self.connection_states.get_mut(&connection_id) else {
                continue;
            };
            state.staged = false;
            if result.is_ok() && !state.write_blocked {
                result = self.write_queued(connection_id);
            }
        }
        staged.clear();
        self.staged = staged;
        result?;
        self.group.flush()?;
        Ok(())
    }

    /// Hand the bytes queued on `connection_id` to the transport, as many as
    /// it takes now
    ///
    /// Requests whose last byte it took count as sent (see
    /// [`Self::take_sent`]). If bytes are left, the connection gets write
    /// interest, so a poll reports it once it can take more; call this
    /// again then. Does nothing on a closed connection.
    pub fn write_queued(&mut self, connection_id: usize) -> Result<()> {
        let state = self
            .connection_states
            .get_mut(&connection_id)
            .ok_or_else(|| crate::Error::Connection("Connection not found".to_string()))?;
        if state.closed {
            return Ok(());
        }
        let data = state.unsent_bytes();
        if !data.is_empty() {
            let progress = self.group.try_send(connection_id, data)?;
            let deadlines = state.deadlines.len();
            state.on_progress(progress, &mut self.sent);
            if state.deadlines.len() > deadlines {
                // Deadlines grow in send order, so the first new one is the earliest
                let deadline_ns = state.deadlines[deadlines].0;
                self.next_deadline_ns =
                    Some(self.next_deadline_ns.map_or(deadline_ns, |next| next.min(deadline_ns)));
            }
        }
        let blocked = !state.unsent_bytes().is_empty();
        if blocked != state.write_blocked {
            state.write_blocked = blocked;
            self.group.set_write_interest(connection_id, blocked)?;
        }
        Ok(())
    }

    /// Take the requests the transport took the last byte of since the last
    /// call, in the order it took them
    pub fn take_sent(&mut self, sent: &mut Vec<SentRequest>) {
        sent.clear();
        std::mem::swap(sent, &mut self.sent);
    }

    /// Queue `data` on `connection_id` to write at the next flush, before
    /// anything queued later; a datagram is written at once
    fn queue(
        &mut self,
        connection_id: usize,
        data: &[u8],
        request: Option<(ReqId, Option<u64>, bool, bool)>,
    ) -> Result<()> {
        let state = self
            .connection_states
            .get_mut(&connection_id)
            .ok_or_else(|| crate::Error::Connection("Connection not found".to_string()))?;
        if request.is_some() && !state.can_send() {
            return Err(crate::Error::Connection(
                "Connection cannot accept more requests".to_string(),
            ));
        }
        state.queue(data, request);
        // Datagrams keep their boundaries: one write each
        if self.datagram {
            return self.write_queued(connection_id);
        }
        if !state.staged {
            state.staged = true;
            self.staged.push(connection_id);
        }
        Ok(())
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

    /// Send a request on a connection now: queue it and write the
    /// connection's queue (see [`Self::write_queued`])
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
        self.queue_request(connection_id, data, req_id, is_warmup, None)?;
        self.write_queued(connection_id)
    }

    /// Queue a request that was intended to go out at `intended_ns`, to
    /// write at the next flush
    ///
    /// Once the transport takes its last byte it is sent (see
    /// [`Self::take_sent`]): its send lag is the time from `intended_ns` to
    /// the cycles the transport read when that write completed, on the
    /// [`crate::timing::time_ns`] clock, and is added to its latency. A
    /// request without an intended time (closed loop) has no lag. It holds
    /// a pipeline slot from now on.
    pub fn queue_request(
        &mut self,
        connection_id: usize,
        data: &[u8],
        req_id: ReqId,
        is_warmup: bool,
        intended_ns: Option<u64>,
    ) -> Result<()> {
        self.queue(connection_id, data, Some((req_id, intended_ns, is_warmup, false)))
    }

    /// Send protocol-driven data (see [`xylem_common::Outbox::data`]) on a
    /// connection: no request, so nothing is tracked and the pipeline limit
    /// does not apply
    ///
    /// It goes out after the bytes queued before it and before those queued
    /// later. With nothing queued, the transport takes what it can of it at
    /// once, without a copy; the rest is queued for the connection to become
    /// writable (see [`Self::write_queued`]).
    pub fn send_driven(&mut self, connection_id: usize, data: &[u8]) -> Result<()> {
        let state = self
            .connection_states
            .get_mut(&connection_id)
            .ok_or_else(|| crate::Error::Connection("Connection not found".to_string()))?;
        if state.closed {
            return Err(crate::Error::Connection("Connection closed".to_string()));
        }
        if !state.unsent_bytes().is_empty() {
            return self.queue(connection_id, data, None);
        }
        // The transport counts these bytes in its TX ranges too, so later
        // requests' ranges stay right. This send's own TX timestamp ends past
        // every earlier request, whose own timestamps came first.
        let accepted = self.group.try_send(connection_id, data)?.accepted;
        state.queued_bytes += accepted as u64;
        state.sent_bytes += accepted as u64;
        if accepted < data.len() {
            state.queue(&data[accepted..], None);
            state.write_blocked = true;
            self.group.set_write_interest(connection_id, true)?;
        }
        Ok(())
    }

    /// Queue a connection setup request (a handshake, AUTH, ...)
    ///
    /// The request is tracked like a warmup request, but has no intended send
    /// time and does not advance the policy's schedule, which restarts: while
    /// setup is pending it is not running, so it intends no sends and counts
    /// none as unsent. When the reply to the connection's last pending setup
    /// request arrives, the schedule starts afresh the next time the
    /// connection is scheduled.
    pub fn queue_setup(&mut self, connection_id: usize, data: &[u8], req_id: ReqId) -> Result<()> {
        self.queue(connection_id, data, Some((req_id, None, true, true)))?;
        if let Some(state) = self.connection_states.get_mut(&connection_id) {
            state.policy.reset();
        }
        Ok(())
    }

    /// Receive and process responses for a connection
    ///
    /// Returns a [`Completion`] for each completed response.
    /// The `is_warmup` flag indicates whether the original request was a warmup request
    /// and stats should not be collected. The error kind is set for an error
    /// reply; see [`Self::error_kind`]. Only successful measurement responses
    /// feed their latency back to the policy.
    pub fn recv_responses<F>(
        &mut self,
        connection_id: usize,
        process_fn: F,
    ) -> Result<Vec<Completion<ReqId>>>
    where
        F: for<'a> FnMut(&'a [u8]) -> Result<Parsed<'a, ReqId>>,
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
        for response in &responses {
            // A fast error reply would make an adaptive policy speed up
            if !response.is_warmup && response.error.is_none() {
                match state.dispatcher {
                    Some(dispatcher) => self.dispatchers[dispatcher].on_response(response.latency),
                    None => state.policy.on_response(response.latency),
                }
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

    /// Pick the next connection to send on: one the ready heap holds ready,
    /// else the idle connection a group dispatcher's due arrival goes to
    ///
    /// Send the request with [`Self::intended_send_time`] of the pick, then
    /// call [`Self::on_request_sent`]; a connection setup request takes no
    /// arrival and calls neither.
    pub fn pick_send(&mut self) -> Option<Pick> {
        self.pick_send_at(crate::timing::time_ns())
    }

    pub(crate) fn pick_send_at(&mut self, current_time_ns: u64) -> Option<Pick> {
        if let Some(connection_id) = self.pick_connection_at(current_time_ns) {
            return Some(Pick { connection_id, dispatcher: None });
        }
        let states = &mut self.connection_states;
        for (index, dispatcher) in self.dispatchers.iter_mut().enumerate() {
            let picked = dispatcher.pop_due(current_time_ns, |connection_id| {
                // A connection closed while idle stays off the list for good
                states.get_mut(&connection_id).is_some_and(|state| {
                    state.set_on_heap(false);
                    state.can_send()
                })
            });
            if let Some(connection_id) = picked {
                return Some(Pick { connection_id, dispatcher: Some(index) });
            }
        }
        None
    }

    /// Intended send time of the request sent on `pick`: the due arrival of
    /// its dispatcher, or the next one of its connection's policy
    pub fn intended_send_time(&self, pick: Pick) -> Option<u64> {
        match pick.dispatcher {
            Some(dispatcher) => self.dispatchers[dispatcher].intended_send_time(),
            None => self.get_state(pick.connection_id)?.intended_send_time(),
        }
    }

    /// Advance the schedule `pick`'s request followed past it
    pub fn on_request_sent(&mut self, pick: Pick, sent_time_ns: u64) {
        match pick.dispatcher {
            Some(dispatcher) => self.dispatchers[dispatcher].on_request_sent(sent_time_ns),
            None => {
                if let Some(state) = self.get_state_mut(pick.connection_id) {
                    state.on_request_sent(sent_time_ns);
                }
            }
        }
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
            if state.in_setup || state.policy.ready_to_send(current_time_ns) {
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
    /// A connection whose protocol reports [`Protocol::setup_pending`] is
    /// ready at once, off its schedule, so its setup requests go out as soon
    /// as it can send. Any other connection waits for its policy's next
    /// wakeup, which starts its schedule if setup restarted it, or for a
    /// group-dispatched connection, joins the back of its dispatcher's idle
    /// list.
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
            // - Connection setup or closed-loop (None): ready immediately
            // - Rate-limited (Some(future)): will be popped when time arrives
            state.in_setup = protocol.setup_pending(connection_id);
            if let (Some(dispatcher), false) = (state.dispatcher, state.in_setup) {
                state.set_on_heap(true);
                self.dispatchers[dispatcher].push_idle(connection_id, current_time_ns);
                return true;
            }
            let next_send_time = if state.in_setup {
                None
            } else {
                state.next_send_time(current_time_ns)
            };
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
        let heap = self.ready_heap.peek_next_time();
        if heap.is_none() && !self.ready_heap.is_empty() {
            return None;
        }
        self.dispatchers
            .iter()
            .filter_map(GroupDispatcher::next_wakeup)
            .chain(heap)
            .min()
    }

    pub(crate) fn has_ready(&self, current_time_ns: u64) -> bool {
        self.ready_heap.has_ready(current_time_ns)
            || self
                .dispatchers
                .iter()
                .any(|dispatcher| dispatcher.next_wakeup().is_some_and(|t| t <= current_time_ns))
    }

    /// Anchor every connection's intended schedule at `start_ns`, the start
    /// time all connections of all threads share (see [`Policy::anchor`]), and
    /// take every connection off the ready heap. Reschedule them with
    /// [`Self::try_schedule_connection`]; a schedule then starts at its first
    /// intended time at or after both `start_ns` and that call.
    pub fn anchor_schedules(&mut self, start_ns: u64) {
        self.ready_heap.clear();
        for dispatcher in &mut self.dispatchers {
            dispatcher.anchor(start_ns);
        }
        for state in self.connection_states.values_mut() {
            state.policy.anchor(start_ns);
            state.set_on_heap(false);
            state.in_setup = false;
        }
    }

    /// Count each connection's intended sends from its next intended time up
    /// to `end_ns`, walking at most `cap` per connection
    ///
    /// Returns `(group_id, connection_id, count, capped)` for every connection
    /// with a nonzero count, including closed ones. A group dispatcher's
    /// arrivals count under its lowest connection ID, with the cap applying
    /// to the dispatcher as a whole. Consumes the schedules, so call it only
    /// once the connections stop sending.
    pub fn count_unsent(&mut self, end_ns: u64, cap: u64) -> Vec<(usize, usize, u64, bool)> {
        let mut dispatcher_connections = vec![usize::MAX; self.dispatchers.len()];
        let mut unsent: Vec<_> = self
            .connection_states
            .values_mut()
            .filter_map(|state| {
                if let Some(dispatcher) = state.dispatcher {
                    let lowest = &mut dispatcher_connections[dispatcher];
                    *lowest = (*lowest).min(state.connection_id);
                }
                let (count, capped) = state.policy.count_intended_before(end_ns, cap);
                (count > 0).then_some((state.group_id, state.connection_id, count, capped))
            })
            .collect();
        for (dispatcher, connection_id) in self.dispatchers.iter_mut().zip(dispatcher_connections) {
            let (count, capped) = dispatcher.count_intended_before(end_ns, cap);
            if count > 0 {
                unsent.push((dispatcher.group_id(), connection_id, count, capped));
            }
        }
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

    /// IDs of the connections of traffic group `group_id`, in the order they
    /// were opened
    pub fn group_connection_ids(&self, group_id: usize) -> Vec<usize> {
        self.connection_ids
            .iter()
            .copied()
            .filter(|id| self.connection_states.get(id).is_some_and(|s| s.group_id == group_id))
            .collect()
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
            state.close();
        }
        // Remove from connection_ids to prevent drain_iteration from waiting on it
        self.connection_ids.retain(|&id| id != connection_id);
        self.group.close(connection_id)?;
        Ok(())
    }

    /// Close all connections
    pub fn close_all(&mut self) -> Result<()> {
        for state in self.connection_states.values_mut() {
            state.close();
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
        Self::new_multi_group_unix_with_roles(factory, groups, &HashMap::new())
    }

    /// Create a Unix socket pool whose groups open connections with the
    /// roles `roles` maps their group IDs to; see
    /// [`ConnectionPool::new_multi_group_with_roles`]
    pub fn new_multi_group_unix_with_roles(
        factory: &xylem_transport::UnixTransportFactory,
        groups: Vec<UnixTrafficGroupConfig>,
        roles: &HashMap<usize, Vec<ConnectionRole>>,
    ) -> Result<Self> {
        // Placeholder address for Unix sockets (not used for actual connections)
        let placeholder_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();

        Self::build(factory.create_group()?, groups, roles, |group, path| {
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

    fn parse_frame(data: &[u8]) -> Result<Parsed<'_, u64>> {
        if data.len() < 8 {
            return Ok(Parsed::Incomplete);
        }
        let size = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
        let id = u32::from_le_bytes(data[4..8].try_into().unwrap()) as u64;
        Ok(if data.len() < size {
            Parsed::Incomplete
        } else {
            Parsed::ok(size, id)
        })
    }

    /// Give a test parser closure the signature the receive path takes
    fn parser<F>(parse: F) -> F
    where
        F: for<'a> FnMut(&'a [u8]) -> Result<Parsed<'a, u64>>,
    {
        parse
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
            responses.iter().map(|r| (r.id, r.bytes_consumed)).collect::<Vec<_>>(),
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
        assert_eq!(state.process_recv(&tail[6..], ts, parse_frame).unwrap()[0].id, 100);
        assert!(state.buffered().is_empty());
    }

    #[test]
    fn invalid_parser_consumption_does_not_complete_request() {
        let mut state = state();
        let ts = Timestamp::now();
        state.record_send(1, ts, Duration::ZERO, tx(0, 1), false);
        assert!(state.process_recv(&[0], ts, |_| Ok(Parsed::ok(2, 1))).is_err());
        assert_eq!(state.pending_count(), 1);
    }

    #[test]
    fn absorbed_stream_bytes_count_toward_the_next_response() {
        let mut state = state();
        let ts = Timestamp::now();
        state.record_send(1, ts, Duration::ZERO, tx(0, 1), false);
        // A 3-byte frame that completes nothing, then the 5-byte response
        // split across reads: the parser streams its first 2 bytes
        let mut calls = 0;
        let mut parse = parser(|data| {
            calls += 1;
            Ok(match calls {
                1 => Parsed::Absorbed(3),
                2 => Parsed::Absorbed(2),
                _ if data.len() < 3 => Parsed::Incomplete,
                _ => Parsed::ok(3, 1),
            })
        });
        assert!(state.process_recv(b"r2tab", ts, &mut parse).unwrap().is_empty());
        assert_eq!(state.pending_count(), 1);
        let responses = state.process_recv(b"cde", ts, &mut parse).unwrap();
        assert_eq!(
            responses.iter().map(|r| (r.id, r.bytes_consumed)).collect::<Vec<_>>(),
            [(1, 8)]
        );
        assert!(state.buffered().is_empty());

        // Consuming nothing is an incomplete response, with or without an ID
        for parsed in [Parsed::Incomplete, Parsed::ok(0, 1)] {
            let mut state = self::state();
            state.record_send(1, ts, Duration::ZERO, tx(0, 1), false);
            let responses = state.process_recv(b"hel", ts, |_| Ok(parsed)).unwrap();
            assert!(responses.is_empty());
            assert_eq!(state.buffered(), b"hel");
            assert_eq!(state.pending_count(), 1);
        }
    }

    #[test]
    fn roles_must_hold_the_configured_load_connections() {
        let factory = test_support::FakeFactory::default();
        let groups = || {
            vec![(
                0,
                "127.0.0.1:1".parse().unwrap(),
                2,
                1,
                Box::new(crate::scheduler::UniformPolicyScheduler::closed_loop())
                    as Box<dyn crate::scheduler::PolicyScheduler>,
            )]
        };
        let roles = HashMap::from([(0, vec![ConnectionRole::Control, ConnectionRole::Load])]);
        let error =
            ConnectionPool::<_, u64>::new_multi_group_with_roles(&factory, groups(), &roles)
                .err()
                .unwrap();
        assert!(error.to_string().contains("asked for 1 load connections, not the 2"));

        let roles = HashMap::from([(
            0,
            vec![ConnectionRole::Load, ConnectionRole::Control, ConnectionRole::Load],
        )]);
        let pool = ConnectionPool::<_, u64>::new_multi_group_with_roles(&factory, groups(), &roles)
            .unwrap();
        assert_eq!(pool.group_connection_ids(0).len(), 3);
        let control: Vec<_> = (0..3).map(|id| pool.get_state(id).unwrap().is_control()).collect();
        assert_eq!(control, [false, true, false]);
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
        let breakdown = LatencyBreakdown { service: Duration::ZERO, kernel: None };
        assert_eq!(
            pool.recv_responses(0, |_| Ok(Parsed::ok(1, 7))).unwrap(),
            vec![Completion {
                id: 7,
                latency: Duration::ZERO,
                is_warmup: true,
                bytes_consumed: 1,
                error: None,
                breakdown,
            }]
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
        assert_eq!(responses.iter().map(|r| r.id).collect::<Vec<_>>(), vec![1, 2]);
        let state = pool.get_state(0).unwrap();
        assert!(state.buffered().is_empty());
        assert!(state.recv_buffer.len() >= MAX_PAYLOAD);
        let buffer = (state.recv_buffer.as_ptr(), state.recv_buffer.len());

        // Once large enough, later reads neither grow nor reallocate it
        pool.send(0, &[0], 3u64, false).unwrap();
        io.lock().unwrap().responses.push_back(Ok(frame(3, 8)));
        assert_eq!(pool.recv_responses(0, parse_frame).unwrap()[0].id, 3);
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
            let latency =
                state.process_recv(&frame(1, 8), recv_ts, parse_frame).unwrap()[0].latency;
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
        assert_eq!(responses[0].latency, lag + Duration::from_micros(500));
        // Without one, the service part is measured in cycles
        assert_eq!(responses[1].latency, lag + recv_ts.duration_since(&send_ts));
        // The breakdown's service part is the latency without the send lag;
        // only the stamped request splits it, against the send's own cycles
        let (stamped, unstamped) = (responses[0].breakdown, responses[1].breakdown);
        assert_eq!(stamped.service, Duration::from_micros(500));
        let split = stamped.kernel.unwrap();
        assert_eq!(split.network_server, Duration::from_micros(500));
        assert_eq!(
            split.host_stack,
            recv_ts.cycles_duration_since(&send_ts).saturating_sub(split.network_server)
        );
        assert_eq!(unstamped.service, recv_ts.duration_since(&send_ts));
        assert_eq!(unstamped.kernel, None);
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
        pool.queue_request(0, &[1], 1u64, false, Some(intended)).unwrap();
        // A request without an intended time has no lag
        pool.queue_request(0, &[2, 2], 2u64, true, None).unwrap();
        // Queued requests hold their slots, but are sent only by the flush
        assert_eq!(pool.get_state(0).unwrap().pending_count(), 2);
        assert_eq!(pool.get_state(0).unwrap().measured_pending_count(), 0);
        assert!(factory.0.lock().unwrap().sent.is_empty());
        pool.flush().unwrap();

        // Both went out in one write, which timed them both
        assert_eq!(factory.0.lock().unwrap().sent, [vec![1, 2, 2]]);
        let mut sent = Vec::new();
        pool.take_sent(&mut sent);
        let lag = sent[0].lag.unwrap();
        assert!(lag >= Duration::from_millis(5) && lag < Duration::from_secs(1), "{lag:?}");
        let second = SentRequest {
            connection_id: 0,
            group_id: 0,
            bytes: 2,
            lag: None,
            measured: false,
        };
        assert_eq!(sent[1], second);
        assert_eq!((sent.len(), sent[0].bytes, sent[0].measured), (2, 1, true));
        let state = pool.get_state(0).unwrap();
        assert_eq!((state.measured_pending_count(), state.pending_count()), (1, 2));
        let send_ns = |id: u64| state.pending_map[&id].send_ts.clock_ns();
        assert_eq!(send_ns(1), send_ns(2), "one send timestamp");
    }

    #[test]
    fn a_partial_write_keeps_the_rest_queued_in_order_until_writable() {
        let factory = test_support::FakeFactory::default();
        let mut pool = ConnectionPool::new(
            &factory,
            "127.0.0.1:1".parse().unwrap(),
            1,
            4,
            Box::new(crate::scheduler::UniformPolicyScheduler::closed_loop()),
            0,
        )
        .unwrap();
        factory.0.lock().unwrap().accept_limit = Some(4);
        let written = |factory: &test_support::FakeFactory| factory.0.lock().unwrap().sent.concat();

        // The transport takes 4 bytes of the driven data; the rest waits
        // for the connection to become writable
        pool.send_driven(0, b"0123456789").unwrap();
        assert!(pool.get_state(0).unwrap().is_write_blocked());
        assert_eq!(factory.0.lock().unwrap().write_interest, [0]);
        assert_eq!(written(&factory), b"0123");

        // A request queued later waits behind it, holding its slot, and a
        // flush leaves the blocked connection alone; more driven data goes
        // after the request
        pool.queue_request(0, b"REQ", 7u64, false, None).unwrap();
        pool.flush().unwrap();
        pool.send_driven(0, b"ab").unwrap();
        assert_eq!(written(&factory), b"0123");
        assert_eq!(pool.get_state(0).unwrap().pending_count(), 1);

        // Once it is writable, each write takes 4 more bytes. The request is
        // sent once the transport takes its last byte, not before.
        factory.0.lock().unwrap().writable = true;
        let mut sent = Vec::new();
        for expected in [&b"01234567"[..], b"0123456789RE"] {
            pool.write_queued(0).unwrap();
            pool.take_sent(&mut sent);
            assert_eq!(written(&factory), expected);
            assert!(sent.is_empty());
            assert!(pool.get_state(0).unwrap().pending_map.is_empty());
        }
        pool.write_queued(0).unwrap();
        pool.take_sent(&mut sent);
        assert_eq!(written(&factory), b"0123456789REQab");
        let request = SentRequest {
            group_id: 0,
            connection_id: 0,
            bytes: 3,
            lag: None,
            measured: true,
        };
        assert_eq!(sent, [request]);
        let state = pool.get_state(0).unwrap();
        assert!(state.pending_map.contains_key(&7));
        assert_eq!(state.pending_count(), 1);

        // Everything went out: no more write interest, and a write is a no-op
        assert!(!state.is_write_blocked());
        assert!(factory.0.lock().unwrap().write_interest.is_empty());
        let writes = factory.0.lock().unwrap().sent.len();
        pool.write_queued(0).unwrap();
        assert_eq!(factory.0.lock().unwrap().sent.len(), writes);
    }

    #[test]
    fn requests_sharing_a_write_end_at_their_own_bytes_of_its_tx_range() {
        let mut state = state();
        state.queue(&[0; 3], Some((1, None, false, false)));
        state.queue(&[0; 5], Some((2, None, true, false)));
        let tx = Some(TxRange { start: 100, end: 108 });
        let receipt = xylem_transport::SendReceipt { timestamp: Timestamp::now(), tx };
        let mut sent = Vec::new();
        state.on_progress(SendProgress { accepted: 8, receipt }, &mut sent);
        let ends: Vec<_> = state.tx_pending_queue.iter().map(|&(end, id, _)| (id, end)).collect();
        assert_eq!(ends, [(1, 103), (2, 108)]);
        // The warmup request is sent, but not measured
        assert_eq!(sent.iter().map(|sent| sent.measured).collect::<Vec<_>>(), [true, false]);
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

    #[test]
    fn anchored_groups_number_their_connections_from_zero() {
        use crate::scheduler::{FixedRatePolicy, GroupPolicyScheduler};
        // Two groups of one pool, each this thread's connections 2.. of 4
        // sharing 4000 req/s, whose slots are 250 µs apart
        let striped = || -> Box<dyn crate::scheduler::PolicyScheduler> {
            Box::new(GroupPolicyScheduler::new(2, |c| {
                Box::new(FixedRatePolicy::striped(4000.0, c, 4))
            }))
        };
        let factory = test_support::FakeFactory::default();
        let target: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let mut pool: ConnectionPool<test_support::FakeGroup, u64> =
            ConnectionPool::new_multi_group_with_targets(
                &factory,
                vec![(0, target, 2, 1, striped()), (1, target, 2, 1, striped())],
            )
            .unwrap();
        let start = crate::timing::time_ns() + 1_000_000_000;
        pool.anchor_schedules(start);
        assert_eq!(pool.peek_next_ready_time(), None, "anchoring empties the heap");
        let firsts: Vec<_> = (0..4)
            .map(|id| pool.get_state_mut(id).unwrap().next_send_time(0).unwrap() - start)
            .collect();
        assert_eq!(firsts, [500_000, 750_000, 500_000, 750_000]);
    }

    /// Test datagram parser: an eight-byte frame completes its ID; a datagram
    /// starting with `F` is a four-byte fragment that completes nothing.
    fn parse_datagram(data: &[u8]) -> Result<Parsed<'_, u64>> {
        if data[0] == b'F' {
            return Ok(Parsed::Absorbed(4));
        }
        parse_frame(data)
    }

    /// A test frame whose payload is the kind of an error reply
    fn error_frame(id: u32, kind: &str) -> Vec<u8> {
        let mut data = frame(id, 8 + kind.len());
        data[8..].copy_from_slice(kind.as_bytes());
        data
    }

    /// Test parser: a frame with a payload is an error reply of the kind in
    /// it, borrowed from the receive buffer; a datagram starting with `F` is a
    /// four-byte fragment
    fn parse_error_frame(data: &[u8]) -> Result<Parsed<'_, u64>> {
        Ok(match parse_datagram(data)? {
            Parsed::Complete { consumed, id, .. } if consumed > 8 => {
                let kind = std::str::from_utf8(&data[8..consumed]).unwrap();
                Parsed::error(consumed, id, ErrorReply::new(kind, "test error"))
            }
            parsed => parsed,
        })
    }

    #[test]
    fn error_replies_complete_their_requests_with_a_kind() {
        for datagram in [false, true] {
            let (factory, mut pool) = fake_pool(datagram, 8);
            for id in 1..=5 {
                pool.send(0, &[0], id, false).unwrap();
            }
            // Pipelined successes and error replies, in one read
            let data = [
                frame(1, 8),
                error_frame(2, "WRONGTYPE"),
                frame(3, 8),
                error_frame(4, "OOM"),
                error_frame(5, "WRONGTYPE"),
            ]
            .concat();
            factory.0.lock().unwrap().responses.push_back(Ok(data));
            let responses = pool.recv_responses(0, parse_error_frame).unwrap();
            assert_eq!(pool.get_state(0).unwrap().pending_count(), 0);
            let kinds: Vec<_> = responses
                .iter()
                .map(|r| (r.id, r.bytes_consumed, r.error.map(|kind| pool.error_kind(0, kind))))
                .collect();
            assert_eq!(
                kinds,
                [
                    (1, 8, None),
                    (2, 17, Some("WRONGTYPE")),
                    (3, 8, None),
                    (4, 11, Some("OOM")),
                    (5, 17, Some("WRONGTYPE")),
                ]
            );
            // A kind is numbered once per connection
            assert_eq!(responses[1].error, responses[4].error);
        }
    }

    #[test]
    fn error_kinds_are_bounded_and_cut() {
        let mut replies = ErrorReplies::default();
        let report = |replies: &mut ErrorReplies, kind: &str| {
            replies.report(0, 0, &ErrorReply::new(kind, kind)).unwrap()
        };
        let ids: Vec<_> = (0..MAX_ERROR_KINDS + 2)
            .map(|n| report(&mut replies, &format!("E{n}")))
            .collect();
        assert_eq!(replies.kind(ids[0]), "E0");
        assert_eq!(replies.kind(ids[MAX_ERROR_KINDS - 1]), format!("E{}", MAX_ERROR_KINDS - 1));
        assert_eq!(ids[MAX_ERROR_KINDS], ErrorKindId::OTHER);
        assert_eq!(ids[MAX_ERROR_KINDS + 1], ErrorKindId::OTHER);
        assert_eq!(replies.kind(ErrorKindId::OTHER), OTHER_ERROR_KIND);
        assert_eq!(report(&mut replies, "E3"), ids[3]);
        assert_eq!(replies.kinds.len(), MAX_ERROR_KINDS);

        // A long kind is cut at a character boundary
        let mut replies = ErrorReplies::default();
        let long = format!("{}é{}", "x".repeat(MAX_ERROR_KIND_LEN - 1), "y".repeat(10));
        let id = report(&mut replies, &long);
        assert_eq!(replies.kind(id), "x".repeat(MAX_ERROR_KIND_LEN - 1));
        assert_eq!(report(&mut replies, &long), id);
    }

    #[test]
    fn abort_on_error_fails_without_completing_the_request() {
        for datagram in [false, true] {
            let (factory, mut pool) = fake_pool(datagram, 4);
            pool.set_on_error(0, OnError::Abort);
            pool.send(0, &[0], 1, false).unwrap();
            pool.send(0, &[0], 2, false).unwrap();
            let data = [frame(1, 8), error_frame(2, "MOVED")].concat();
            factory.0.lock().unwrap().responses.push_back(Ok(data));
            let error = pool.recv_responses(0, parse_error_frame).unwrap_err();
            assert!(matches!(error, crate::Error::Protocol(_)), "{error}");
            assert_eq!(
                error.to_string(),
                "Protocol error: Traffic group 0, connection 0: error reply MOVED \
                 (on_error = \"abort\"): test error"
            );
            assert!(pool.get_state(0).unwrap().pending_map.contains_key(&2));

            // Counted, the default, the reply completes its request
            let (factory, mut pool) = fake_pool(datagram, 4);
            pool.send(0, &[0], 2, false).unwrap();
            factory.0.lock().unwrap().responses.push_back(Ok(error_frame(2, "MOVED")));
            assert_eq!(pool.recv_responses(0, parse_error_frame).unwrap().len(), 1);
        }
    }

    /// Records the latencies a policy is told about
    struct FeedbackPolicy(std::sync::Arc<std::sync::Mutex<Vec<Duration>>>);

    impl Policy for FeedbackPolicy {
        fn next_send_time(&mut self, _: u64) -> Option<u64> {
            None
        }
        fn on_response(&mut self, latency: Duration) {
            self.0.lock().unwrap().push(latency);
        }
        fn on_request_sent(&mut self, _: u64) {}
        fn reset(&mut self) {}
        fn name(&self) -> &'static str {
            "Feedback"
        }
    }

    #[test]
    fn error_replies_give_the_policy_no_latency_feedback() {
        let (factory, mut pool) = fake_pool(false, 4);
        let feedback = std::sync::Arc::<std::sync::Mutex<Vec<Duration>>>::default();
        pool.get_state_mut(0).unwrap().policy = Box::new(FeedbackPolicy(feedback.clone()));
        for id in 1..=3 {
            pool.send(0, &[0], id, false).unwrap();
        }
        let data = [frame(1, 8), error_frame(2, "ERR"), frame(3, 8)].concat();
        factory.0.lock().unwrap().responses.push_back(Ok(data));
        assert_eq!(pool.recv_responses(0, parse_error_frame).unwrap().len(), 3);
        assert_eq!(feedback.lock().unwrap().len(), 2);
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
        assert_eq!(
            responses.iter().map(|r| (r.id, r.bytes_consumed)).collect::<Vec<_>>(),
            vec![(1, 16)]
        );

        // One datagram may hold several frames, and a fragment after them
        let data = [frame(2, 8), frame(3, 12), b"Fxyz".to_vec()].concat();
        io.lock().unwrap().responses.push_back(Ok(data));
        let responses = pool.recv_responses(0, parse_datagram).unwrap();
        assert_eq!(
            responses.iter().map(|r| (r.id, r.bytes_consumed)).collect::<Vec<_>>(),
            vec![(2, 8), (3, 12)]
        );
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
        assert_eq!(pool.recv_responses(0, parse_frame).unwrap()[0].id, 1);
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
        /// The connection of each of `sent`
        pub sent_on: Vec<usize>,
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
        /// The number of sends made when each `flush` was called
        pub flushes: Vec<usize>,
        /// Most bytes each `try_send` takes; `None` takes all
        pub accept_limit: Option<usize>,
        /// Connections with write interest: `poll` reports them, and
        /// `try_send` takes bytes on them, only while `writable` is set
        pub write_interest: Vec<usize>,
        pub writable: bool,
    }

    impl Default for FakeIo {
        fn default() -> Self {
            Self {
                responses: VecDeque::new(),
                sent: Vec::new(),
                sent_on: Vec::new(),
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
                flushes: Vec::new(),
                accept_limit: None,
                write_interest: Vec::new(),
                writable: false,
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
            io.sent_on.push(conn_id);
            let end = io.sent.len() as u32;
            Ok(xylem_transport::SendReceipt {
                timestamp: io.now(),
                tx: Some(TxRange { start: end - 1, end }),
            })
        }

        fn try_send(
            &mut self,
            conn_id: usize,
            data: &[u8],
        ) -> xylem_transport::Result<SendProgress> {
            let io = self.io.lock().unwrap();
            // A connection waiting to become writable takes nothing until it is
            let blocked = io.write_interest.contains(&conn_id) && !io.writable;
            let limit = if blocked { Some(0) } else { io.accept_limit };
            drop(io);
            let accepted = limit.map_or(data.len(), |limit| limit.min(data.len()));
            if accepted == 0 && !data.is_empty() {
                self.check(conn_id)?;
                let timestamp = self.io.lock().unwrap().now();
                let receipt = xylem_transport::SendReceipt { timestamp, tx: None };
                return Ok(SendProgress { accepted, receipt });
            }
            let receipt = self.send(conn_id, &data[..accepted])?;
            Ok(SendProgress { accepted, receipt })
        }

        fn set_write_interest(
            &mut self,
            conn_id: usize,
            interested: bool,
        ) -> xylem_transport::Result<()> {
            self.check(conn_id)?;
            let mut io = self.io.lock().unwrap();
            io.write_interest.retain(|&id| id != conn_id);
            if interested {
                io.write_interest.push(conn_id);
            }
            Ok(())
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

        fn flush(&mut self) -> xylem_transport::Result<()> {
            let mut io = self.io.lock().unwrap();
            let sends = io.sent.len();
            io.flushes.push(sends);
            Ok(())
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
            if io.writable {
                let writable: Vec<usize> =
                    io.write_interest.iter().copied().filter(|id| !ready.contains(id)).collect();
                ready.extend(writable);
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
