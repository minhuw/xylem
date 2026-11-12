//! Worker implementation with connection pooling and request pipelining
//!
//! This implements Lancet's symmetric agent model, allowing multiple outstanding
//! requests per connection for maximum throughput with accurate latency tracking.
//!
//! The worker can operate in different modes based on configuration:
//! - Single connection, single pending request: Simple closed-loop latency measurement
//! - Multiple connections with pipelining: High-throughput load testing

use crate::connection::{ConnectionPool, SentRequest, TrafficGroupConfig};
use crate::request_dump::RequestDumper;
use crate::stats::{StatsRecorder, TupleStatsCollector, UNSENT_WALK_CAP};
use crate::timing;
use crate::traffic_group::OnError;
use crate::Result;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use xylem_common::{ConnectionRole, FastHashMap, Outbox, Protocol};
use xylem_transport::{ConnectionGroup, TransportFactory};

fn unix_time_us() -> Option<u64> {
    SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_micros().try_into().ok()
}

/// Configuration for worker
pub struct WorkerConfig {
    /// Target server address
    pub target: SocketAddr,
    /// Experiment duration
    pub duration: Duration,
    /// Number of connections per worker
    pub conn_count: usize,
    /// Maximum pending requests per connection
    pub max_pending_per_conn: usize,
    /// Longest wait for pending responses after the measurement window;
    /// requests still pending then count as timed out
    pub drain_timeout: Duration,
}

/// Default for [`WorkerConfig::drain_timeout`]
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

/// Default for [`Worker::set_setup_timeout`]
pub const DEFAULT_SETUP_TIMEOUT: Duration = Duration::from_secs(10);

/// Time to the next send that lets the stats do work ahead of recording
/// ([`StatsRecorder::prepare`]): making the next time bucket's map of
/// entries for 1000 connections takes up to 30 us in a release build
const PREPARE_SLACK_NS: u64 = 200_000;

/// Worker with connection pooling and request pipelining
///
/// Uses `ConnectionGroup` internally for efficient I/O multiplexing.
///
/// The worker is generic over the stats collector type `S` which must implement
/// `StatsRecorder`. The default, `TupleStatsCollector`, aggregates by
/// (time_bucket, group_id, connection_id).
pub struct Worker<G: ConnectionGroup, P: Protocol, S: StatsRecorder = TupleStatsCollector> {
    pool: ConnectionPool<G, P::RequestId>,
    /// Map of traffic group ID to protocol instance
    protocols: FastHashMap<usize, P>,
    stats: S,
    config: WorkerConfig,
    /// Optional request dumper for recording all requests
    request_dumper: Option<Arc<RequestDumper>>,
    /// Thread ID for request dumping
    thread_id: usize,
    /// Coarse maintenance deadline for stale TX timestamp correlation entries.
    next_tx_cleanup_ns: u64,
    /// Reused buffer for the connection IDs each poll reports ready.
    ready_ids: Vec<usize>,
    /// Reused buffer the protocols drain their driven sends and wakeups into
    outbox: Outbox,
    /// Reused buffer for the requests the transport took
    sent: Vec<SentRequest>,
    /// How long after the last I/O polls do not wait, in nanoseconds; `None`
    /// (the default) lets every poll wait (see [`Self::set_busy_poll`])
    busy_poll_ns: Option<u64>,
    /// When the worker last sent or a poll reported a ready connection (a
    /// `timing::time_ns` value)
    last_io_ns: u64,
    /// See [`Self::set_setup_timeout`]
    setup_timeout: Duration,
    /// `(deadline_ns, connection_id)` of the connections in setup at the
    /// start of the run, in deadline order. Entries of connections whose
    /// setup finished are pruned once their deadline passes.
    setup_deadlines: VecDeque<(u64, usize)>,
}

impl<G: ConnectionGroup, P: Protocol, S: StatsRecorder> Worker<G, P, S> {
    /// Create a new pipelined worker with per-connection policies
    pub fn new<F: TransportFactory<Group = G>>(
        factory: &F,
        protocol: P,
        stats: S,
        config: WorkerConfig,
        policy_scheduler: Box<dyn crate::scheduler::PolicyScheduler>,
    ) -> Result<Self> {
        let mut protocols = HashMap::new();
        protocols.insert(0, protocol);
        // Default group_id 0 for legacy single-pool workers
        let groups = vec![(
            0,
            config.target,
            config.conn_count,
            config.max_pending_per_conn,
            policy_scheduler,
        )];
        let roles = connection_roles(&protocols, groups.iter().map(|g| (g.0, g.2)));
        let pool = ConnectionPool::new_multi_group_with_roles(factory, groups, &roles)?;

        Ok(Self::from_pool(pool, protocols, stats, config))
    }

    /// Create a new pipelined worker with closed-loop policy (max throughput)
    pub fn with_closed_loop<F: TransportFactory<Group = G>>(
        factory: &F,
        protocol: P,
        stats: S,
        config: WorkerConfig,
    ) -> Result<Self> {
        let policy_scheduler = Box::new(crate::scheduler::UniformPolicyScheduler::closed_loop());
        Self::new(factory, protocol, stats, config, policy_scheduler)
    }

    /// Create a new pipelined worker with multiple traffic groups
    pub fn new_multi_group<F: TransportFactory<Group = G>>(
        factory: &F,
        protocols: HashMap<usize, P>,
        stats: S,
        config: WorkerConfig,
        groups: Vec<(usize, usize, usize, Box<dyn crate::scheduler::PolicyScheduler>)>,
    ) -> Result<Self> {
        let target = config.target;
        let groups = groups
            .into_iter()
            .map(|(group_id, count, max_pending, scheduler)| {
                (group_id, target, count, max_pending, scheduler)
            })
            .collect();
        Self::new_multi_group_with_targets(factory, protocols, stats, config, groups)
    }

    /// Create a new pipelined worker with multiple traffic groups, each with its own target
    pub fn new_multi_group_with_targets<F: TransportFactory<Group = G>>(
        factory: &F,
        protocols: HashMap<usize, P>,
        stats: S,
        config: WorkerConfig,
        groups: Vec<TrafficGroupConfig>,
    ) -> Result<Self> {
        let roles = connection_roles(&protocols, groups.iter().map(|g| (g.0, g.2)));
        let pool = ConnectionPool::new_multi_group_with_roles(factory, groups, &roles)?;

        Ok(Self::from_pool(pool, protocols, stats, config))
    }

    /// Create a worker around an already built connection pool, telling each
    /// group's protocol the connections it got
    fn from_pool(
        pool: ConnectionPool<G, P::RequestId>,
        protocols: HashMap<usize, P>,
        stats: S,
        config: WorkerConfig,
    ) -> Self {
        let mut protocols: FastHashMap<usize, P> = protocols.into_iter().collect();
        for (&group_id, protocol) in protocols.iter_mut() {
            protocol.bind_connections(&pool.group_connection_ids(group_id));
        }
        Self {
            pool,
            protocols,
            stats,
            config,
            request_dumper: None,
            thread_id: 0,
            next_tx_cleanup_ns: timing::time_ns().saturating_add(1_000_000_000),
            ready_ids: Vec::new(),
            outbox: Outbox::default(),
            sent: Vec::new(),
            busy_poll_ns: None,
            last_io_ns: 0,
            setup_timeout: DEFAULT_SETUP_TIMEOUT,
            setup_deadlines: VecDeque::new(),
        }
    }

    /// Get mapping of connection ID to target address
    pub fn connection_targets(&self) -> Vec<(usize, SocketAddr)> {
        self.pool.connection_targets()
    }

    /// See [`ConnectionPool::connection_local_addrs`]
    pub fn connection_local_addrs(&self) -> Vec<(usize, usize, Option<SocketAddr>)> {
        self.pool.connection_local_addrs()
    }

    /// Set the request timeout of traffic group `group_id`; see
    /// [`ConnectionPool::set_request_timeout`]
    ///
    /// A request that times out frees its pipeline slot and counts in
    /// `timed_out` without a latency; a response that still arrives counts in
    /// `late_responses` and is dropped. The timeout runs from the send, so it
    /// excludes an open-loop request's send lag.
    pub fn set_request_timeout(&mut self, group_id: usize, timeout: Option<Duration>) {
        self.pool.set_request_timeout(group_id, timeout);
    }

    /// Set what error replies on traffic group `group_id` do; see
    /// [`ConnectionPool::set_on_error`]
    ///
    /// Counted error replies (the default) complete their request: they
    /// count in `completed` and `errors`, by kind, and their latency is
    /// recorded apart from the latency of successful responses. With
    /// [`OnError::Abort`], the run fails at the first one, warmup included.
    pub fn set_on_error(&mut self, group_id: usize, on_error: OnError) {
        self.pool.set_on_error(group_id, on_error);
    }

    /// Fail the run when a connection's setup (see [`Protocol::setup_pending`])
    /// has not finished `timeout` after the pool started to open the
    /// connection; [`DEFAULT_SETUP_TIMEOUT`] by default
    ///
    /// The timeout includes the connect and the wait for the run's start,
    /// before which no setup request goes out.
    pub fn set_setup_timeout(&mut self, timeout: Duration) {
        self.setup_timeout = timeout;
    }

    /// Busy-poll for up to `budget` after the last I/O; `None` (the default)
    /// turns busy polling off
    ///
    /// For `budget` after the worker last sent or a poll reported a ready
    /// connection, from the start of the run on, the worker polls the
    /// transport with a zero timeout instead of waiting in it, so it sees a
    /// response without a wakeup; then its polls wait as without busy
    /// polling, until the next send or event. The thread's core stays busy
    /// while it spins. Deadlines are kept as without busy polling, since
    /// every loop pass checks them, and timestamps are taken where they are
    /// without it.
    pub fn set_busy_poll(&mut self, budget: Option<Duration>) {
        self.busy_poll_ns = budget.map(|budget| budget.as_nanos().min(u64::MAX as u128) as u64);
    }

    /// `timeout`, or no wait while busy polling at `now_ns`
    fn busy_poll_timeout(&self, timeout: Duration, now_ns: u64) -> Duration {
        match self.busy_poll_ns {
            Some(budget) if now_ns.saturating_sub(self.last_io_ns) < budget => Duration::ZERO,
            _ => timeout,
        }
    }

    /// Set the request dumper for recording all requests
    ///
    /// # Arguments
    /// * `dumper` - The request dumper instance (shared across threads)
    /// * `thread_id` - The thread ID for this worker (used in dump records)
    pub fn set_request_dumper(&mut self, dumper: Arc<RequestDumper>, thread_id: usize) {
        self.request_dumper = Some(dumper);
        self.thread_id = thread_id;
    }

    /// Record a request to the dump file (helper to reduce nesting)
    fn record_request(
        &self,
        dumper: &RequestDumper,
        group_id: usize,
        conn_id: usize,
        data: &[u8],
        req_id_str: &str,
    ) {
        if let Err(e) = dumper.record(self.thread_id, group_id, conn_id, data, req_id_str) {
            tracing::warn!("Failed to record request to dump: {}", e);
        }
    }

    /// Run the pipelined measurement loop from now; see [`Self::run_at`]
    pub fn run(&mut self) -> Result<()> {
        self.run_at(timing::time_ns())
    }

    /// Run the pipelined measurement loop (Lancet symmetric_tcp_main style)
    /// from `start_ns` (a `timing::time_ns` value) to `start_ns + duration`
    ///
    /// `start_ns` is the start time every thread shares: every connection's
    /// intended schedule is anchored at it, so the connections of a traffic
    /// group interleave across threads, and it is the epoch of the stats' time
    /// buckets. Nothing is sent before it. A connection still setting up
    /// starts its schedule once setup is done.
    pub fn run_at(&mut self, start_ns: u64) -> Result<()> {
        self.run_at_with_clock(start_ns, unix_time_us)
    }

    // A supplied wall clock lets tests exercise unusable samples at loop exit.
    fn run_at_with_clock(
        &mut self,
        start_ns: u64,
        mut wall_clock_us: impl FnMut() -> Option<u64>,
    ) -> Result<()> {
        self.stats.start_measurement(start_ns);
        self.anchor_schedules(start_ns);
        self.arm_setup_deadlines();
        let now_ns = timing::time_ns();
        if start_ns.saturating_sub(now_ns) > 2_000_000 {
            // Sleep until shortly before the start, then spin to it
            std::thread::sleep(Duration::from_nanos(start_ns - now_ns - 1_000_000));
        }
        timing::busy_wait_until(start_ns);
        let actual_start_us = wall_clock_us();

        let duration_ns = self.config.duration.as_nanos().min(u64::MAX as u128) as u64;
        let end_ns = start_ns.saturating_add(duration_ns);
        // Busy polling starts with the run
        self.last_io_ns = start_ns;

        let loop_result = loop {
            let now_ns = timing::time_ns();

            // Check if duration exceeded
            if now_ns >= end_ns {
                break Ok(());
            }

            match self.send_requests() {
                Ok(0) => {}
                Ok(_) => self.last_io_ns = now_ns,
                Err(error) => break Err(error),
            }

            // RECEIVE PHASE: Poll all connections for responses (single syscall)
            // Derive sleep duration from the ready heap (when next connection will be ready)
            let now_ns = timing::time_ns();

            let poll_timeout = self.poll_timeout(now_ns, end_ns);
            let poll_timeout = self.busy_poll_timeout(poll_timeout, now_ns);
            if let Err(error) = self.process_responses(poll_timeout) {
                break Err(error);
            }
            if !self.ready_ids.is_empty() {
                self.last_io_ns = now_ns;
            }
            let now_ns = timing::time_ns();
            self.expire_requests(now_ns);
            if let Err(error) = self.check_setup_deadlines(now_ns) {
                break Err(error);
            }
            if self.ready_ids.is_empty()
                && !self.pool.has_ready(now_ns)
                && self
                    .pool
                    .peek_next_ready_time()
                    .is_none_or(|next| next > now_ns.saturating_add(PREPARE_SLACK_NS))
            {
                self.stats.prepare(now_ns);
            }
        };
        let actual_end_us = wall_clock_us();
        self.stats.record_actual_load_loop_samples(actual_start_us, actual_end_us);
        loop_result?;
        self.stats.end_measurement(end_ns);
        self.record_unsent(end_ns);

        // Stop sending and wait for the responses still pending
        self.drain_responses()?;
        self.check_setup_done()?;

        // Close all connections
        self.pool.close_all()?;

        Ok(())
    }

    /// Anchor every connection's schedule at `start_ns` and reschedule the
    /// connections that can send
    fn anchor_schedules(&mut self, start_ns: u64) {
        self.pool.anchor_schedules(start_ns);
        for connection_id in self.pool.connection_ids().to_vec() {
            let Some(state) = self.pool.get_state(connection_id) else {
                continue;
            };
            let protocol =
                self.protocols.get(&state.group_id()).expect("Protocol not found for group_id");
            self.pool.try_schedule_connection(connection_id, protocol);
        }
    }

    /// Returns the number of requests it picked, setup requests included
    fn send_requests(&mut self) -> Result<usize> {
        // SEND PHASE: Send requests to ready connections
        // Rate limiting is handled by connection-level policies via pool's
        // ready_heap, or for group dispatch, by each group's arrival process
        //
        // Safety limit: Cap the number of consecutive sends to prevent infinite loops
        // For closed-loop with pipelining, we want to fill all pipelines, but not spin forever
        // With N connections and depth D, we should need at most N*D sends to fill all pipelines
        let max_sends_per_iteration = self.pool.connection_count() * 16; // Allow generous headroom
        let mut sends_this_iteration = 0;
        // The round's first connection, until its requests went out
        let mut first_connection = None;

        while let Some(pick) = self.pool.pick_send() {
            let connection_id = pick.connection_id;
            // The round's first connection sends at once when the round
            // moves on, as a kernel socket would have sent its first request;
            // its requests picked until then share that write
            match first_connection {
                None if sends_this_iteration == 0 => first_connection = Some(connection_id),
                Some(first) if first != connection_id => {
                    self.flush()?;
                    first_connection = None;
                }
                _ => {}
            }
            let state = self.pool.get_state(connection_id).expect("Connection state not found");
            let group_id = state.group_id();

            let request = {
                let protocol =
                    self.protocols.get_mut(&group_id).expect("Protocol not found for group_id");
                protocol.next_request(connection_id)
            };
            if state.is_control() && !request.metadata.is_setup {
                return Err(crate::Error::Protocol(format!(
                    "Traffic group {group_id}, connection {connection_id}: the protocol sent a \
                     request that is no setup request on a control connection"
                )));
            }

            if request.metadata.is_setup {
                // Connection setup (a handshake, AUTH, ...) may carry
                // credentials: it is never dumped, measured or scheduled.
                self.pool.queue_setup(connection_id, &request.data, request.request_id)?;
            } else {
                // Record request to dump file if enabled
                if let Some(ref dumper) = self.request_dumper {
                    let req_id_str = format!("{:?}", request.request_id);
                    self.record_request(
                        dumper,
                        group_id,
                        connection_id,
                        &request.data,
                        &req_id_str,
                    );
                }

                // Queue the request (track warmup status for response
                // handling); it is sent, and its stats recorded, when the
                // transport takes it. An open-loop request's latency starts
                // at its intended send time.
                let intended_ns = self.pool.intended_send_time(pick);
                self.pool.queue_request(
                    connection_id,
                    &request.data,
                    request.request_id,
                    request.metadata.is_warmup,
                    intended_ns,
                )?;

                // Advance the policy's schedule
                self.pool.on_request_sent(pick, timing::time_ns());
            }

            // Try to re-add connection to heap if it still has capacity
            // This enables pipelining for both closed-loop and rate-limited policies
            //
            // - Closed-loop (next_send_time = None): will be picked immediately to fill pipeline
            // - Rate-limited (next_send_time = future): stored on heap, popped when time arrives
            //
            // This maintains invariant: connection on heap ⟺ can send (pipeline not full AND protocol ready)
            let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
            self.pool.try_schedule_connection(connection_id, protocol);

            sends_this_iteration += 1;
            if sends_this_iteration >= max_sends_per_iteration {
                eprintln!(
                    "[WARN] Hit send limit ({}) in one iteration - forcing receive phase",
                    max_sends_per_iteration
                );
                break;
            }
        }
        // One write per connection and one transmit pass for the rest of
        // the round: requests pipelined on a connection share a write and
        // its packets, and a user-space stack's pass costs time per
        // connection, so one per send would grow with the square of the
        // connections that send together
        if sends_this_iteration > 0 {
            self.flush()?;
        }
        Ok(sends_this_iteration)
    }

    /// Write what was queued since the last flush and put it on the wire,
    /// recording the requests the transport took
    fn flush(&mut self) -> Result<()> {
        let flushed = self.pool.flush();
        self.record_sent();
        flushed
    }

    /// Record the stats of the measurement requests the transport took
    fn record_sent(&mut self) {
        self.pool.take_sent(&mut self.sent);
        for sent in &self.sent {
            if sent.measured {
                self.stats.record_tx_bytes(sent.group_id, sent.connection_id, sent.bytes);
                self.stats.record_sent(sent.group_id, sent.connection_id, sent.lag);
            }
        }
    }

    /// Write the bytes queued on `connection_id` that the transport takes
    /// now; on a connection error, close the connection
    fn write_queued(&mut self, connection_id: usize, group_id: usize) -> Result<()> {
        match self.pool.write_queued(connection_id) {
            Err(e @ crate::Error::Connection(_)) => self.close_failed(connection_id, group_id, e),
            result => result,
        }
    }

    /// Bound every receive wait by the experiment deadline, the next
    /// request timeout and the next setup deadline. A closed-loop entry has
    /// no timestamp, so test readiness before interpreting an empty peek.
    fn poll_timeout(&self, now_ns: u64, end_ns: u64) -> Duration {
        let end_ns = self.pool.next_request_deadline().map_or(end_ns, |next| next.min(end_ns));
        let end_ns = self.setup_deadlines.front().map_or(end_ns, |&(next, _)| next.min(end_ns));
        let remaining_ns = end_ns.saturating_sub(now_ns);
        let wait_ns = if self.pool.has_ready(now_ns) {
            0
        } else {
            // Wake a millisecond early and spin for rate-policy precision.
            self.pool
                .peek_next_ready_time()
                .map_or(remaining_ns, |next| next.saturating_sub(now_ns).saturating_sub(1_000_000))
        };
        Duration::from_nanos(wait_ns.min(remaining_ns))
    }

    /// Process responses from all connections
    fn process_responses(&mut self, timeout: Duration) -> Result<()> {
        // Poll all connections with one syscall. The ID buffer is taken out of
        // the worker so the loop below can call `&mut self` pool methods.
        let mut ready_ids = std::mem::take(&mut self.ready_ids);
        self.pool.poll(timeout, &mut ready_ids)?;

        // Poll for TX timestamps from error queue (non-blocking)
        // Must be called AFTER poll() because TX timestamps arrive asynchronously
        // and may not be available until after the NIC has transmitted the data
        if let Err(e) = self.pool.poll_tx_timestamps() {
            tracing::trace!("Error polling TX timestamps: {}", e);
        }

        // Clean up stale TX queue entries (entries older than 1 minute)
        // This prevents memory leaks when TX timestamps are never received
        self.maintain_tx_queues(timing::time_ns());

        let mut driven_sends = false;
        for &connection_id in &ready_ids {
            let state = match self.pool.get_state(connection_id) {
                Some(s) => s,
                None => continue,
            };

            // Queued bytes waited for the connection to become writable
            if state.is_write_blocked() {
                let group_id = state.group_id();
                self.write_queued(connection_id, group_id)?;
            }
            let state = match self.pool.get_state(connection_id) {
                Some(s) if !s.is_closed() => s,
                _ => continue,
            };

            // Skip connections with no pending requests. A datagram connection
            // is still read, so late or stray datagrams do not keep it ready.
            if state.pending_count() == 0 && !self.pool.is_datagram() {
                continue;
            }

            let group_id = state.group_id();

            let responses = {
                let protocol =
                    self.protocols.get_mut(&group_id).expect("Protocol not found for group_id");
                match self.pool.recv_responses(connection_id, |data| {
                    protocol.parse_response(connection_id, data).map_err(Into::into)
                }) {
                    Ok(responses) => responses,
                    Err(e @ crate::Error::Connection(_)) => {
                        self.close_failed(connection_id, group_id, e)?;
                        continue;
                    }
                    Err(e) => {
                        return Err(self.setup_failure(connection_id, group_id, &e).unwrap_or(e))
                    }
                }
            };

            for response in responses {
                if response.is_warmup {
                    continue;
                }
                self.stats.record_rx_bytes(group_id, connection_id, response.bytes_consumed);
                match response.error {
                    // An error reply's latency stays out of the latency
                    // distribution and of the breakdown alike
                    None => {
                        self.stats.record_latency(group_id, connection_id, response.latency);
                        self.stats.record_breakdown(group_id, connection_id, &response.breakdown);
                    }
                    Some(kind) => {
                        let kind = self.pool.error_kind(connection_id, kind);
                        self.stats.record_error_reply(
                            group_id,
                            connection_id,
                            kind,
                            response.latency,
                        );
                    }
                }
            }
            let late = self.pool.take_late_responses(connection_id);
            if late > 0 {
                self.stats.record_late_responses(group_id, connection_id, late);
            }

            // What the parsed data made the protocol send or unblock
            match self.drain_outbox(connection_id, group_id) {
                Ok(sent) => driven_sends |= sent,
                Err(e @ crate::Error::Connection(_)) => {
                    self.close_failed(connection_id, group_id, e)?;
                    continue;
                }
                Err(e) => return Err(e),
            }

            // Reschedule connection if it can send but isn't on heap
            // This handles two cases:
            // 1. Pipeline transitioned from full to available
            // 2. Protocol became ready (e.g., handshake completed)
            let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
            self.pool.try_schedule_connection(connection_id, protocol);
        }
        self.ready_ids = ready_ids;
        // Driven sends answer the peer at once, like the first send of a round
        if driven_sends {
            self.pool.flush()?;
        }
        // Requests written once their connections became writable
        self.record_sent();

        Ok(())
    }

    /// Take what the protocol asks for after parsing data received on
    /// `connection_id`: reschedule the connections it woke, and send its
    /// driven bytes on the connection before its next request. What the
    /// transport does not take now waits in the connection's send queue for
    /// the connection to become writable, while reads go on. Returns whether
    /// it sent any; on a load connection they count as sent bytes.
    fn drain_outbox(&mut self, connection_id: usize, group_id: usize) -> Result<bool> {
        let protocol = self.protocols.get_mut(&group_id).expect("Protocol not found for group_id");
        protocol.drain_outbox(connection_id, &mut self.outbox);
        if self.outbox.is_empty() {
            return Ok(false);
        }
        let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
        for woken in self.outbox.wake.drain(..) {
            self.pool.try_schedule_connection(woken, protocol);
        }
        if self.outbox.data.is_empty() {
            return Ok(false);
        }
        let sent = self.pool.send_driven(connection_id, &self.outbox.data);
        let len = self.outbox.data.len();
        self.outbox.data.clear();
        sent?;
        if self.pool.get_state(connection_id).is_some_and(|state| !state.is_control()) {
            self.stats.record_tx_bytes(group_id, connection_id, len);
        }
        Ok(true)
    }

    /// Time out the requests whose request timeout passed by `now_ns`, count
    /// the measurement ones, and reschedule the connections they freed.
    /// Returns whether any timed out.
    fn expire_requests(&mut self, now_ns: u64) -> bool {
        let mut freed = Vec::new();
        let stats = &mut self.stats;
        self.pool
            .expire_requests_at(now_ns, |connection_id, group_id, expired, measured| {
                if measured > 0 {
                    stats.record_timed_out(group_id, connection_id, measured);
                }
                tracing::debug!(connection_id, group_id, expired, "Requests timed out");
                freed.push((connection_id, group_id));
            });
        let expired = !freed.is_empty();
        for (connection_id, group_id) in freed {
            let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
            self.pool.try_schedule_connection(connection_id, protocol);
        }
        expired
    }

    fn maintain_tx_queues(&mut self, now_ns: u64) {
        if now_ns >= self.next_tx_cleanup_ns {
            self.pool.cleanup_stale_tx_queues_at(now_ns);
            self.next_tx_cleanup_ns = now_ns.saturating_add(1_000_000_000);
        }
    }

    /// Close a connection that failed with `error`, counting its pending
    /// measurement requests as failed. A connection still in setup fails the
    /// run instead: it never applied its load.
    fn close_failed(
        &mut self,
        connection_id: usize,
        group_id: usize,
        error: crate::Error,
    ) -> Result<()> {
        if let Some(failure) = self.setup_failure(connection_id, group_id, &error) {
            let _ = self.pool.close(connection_id);
            return Err(failure);
        }
        tracing::warn!(connection_id, "Connection disconnected: {}", error);
        if let Some(state) = self.pool.get_state_mut(connection_id) {
            let failed = state.measured_pending_count() as u64;
            if failed > 0 {
                self.stats.record_failed(group_id, connection_id, failed);
            }
            // Requests it had not sent yet never go out
            let unsent = state.abandon_unsent();
            if unsent > 0 {
                self.stats.record_unsent(group_id, connection_id, unsent, false);
            }
        }
        let _ = self.pool.close(connection_id);
        Ok(())
    }

    /// The run's error for the connection or I/O `error` of `connection_id`
    /// if the connection is still in setup, naming it; none otherwise
    fn setup_failure(
        &self,
        connection_id: usize,
        group_id: usize,
        error: &crate::Error,
    ) -> Option<crate::Error> {
        let cause = match error {
            crate::Error::Connection(cause) => cause.clone(),
            crate::Error::Io(e) => e.to_string(),
            _ => return None,
        };
        let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
        protocol.setup_pending(connection_id).then(|| {
            crate::Error::Connection(format!(
                "Traffic group {group_id}, connection {connection_id} ({}): connection closed \
                 during setup: {cause}",
                protocol.name()
            ))
        })
    }

    /// Give every connection in setup its setup deadline: the setup timeout
    /// after the pool started to open it
    fn arm_setup_deadlines(&mut self) {
        self.setup_deadlines.clear();
        let timeout_ns = self.setup_timeout.as_nanos().min(u64::MAX as u128) as u64;
        for &connection_id in self.pool.connection_ids() {
            let Some(state) = self.pool.get_state(connection_id) else {
                continue;
            };
            let protocol =
                self.protocols.get(&state.group_id()).expect("Protocol not found for group_id");
            if protocol.setup_pending(connection_id) {
                let deadline_ns = state.opened_ns().saturating_add(timeout_ns);
                self.setup_deadlines.push_back((deadline_ns, connection_id));
            }
        }
        // The pool opens its connections in ID order, one after the other
        debug_assert!(self.setup_deadlines.iter().is_sorted_by_key(|&(deadline, _)| deadline));
    }

    /// Fail the run if a connection whose setup deadline passed by `now_ns`
    /// is still in setup
    fn check_setup_deadlines(&mut self, now_ns: u64) -> Result<()> {
        while let Some(&(deadline_ns, connection_id)) = self.setup_deadlines.front() {
            if deadline_ns > now_ns {
                break;
            }
            self.setup_deadlines.pop_front();
            let Some(state) = self.pool.get_state(connection_id) else {
                continue;
            };
            let group_id = state.group_id();
            let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
            if protocol.setup_pending(connection_id) {
                return Err(crate::Error::Connection(format!(
                    "Traffic group {group_id}, connection {connection_id} ({}): connection setup \
                     did not finish within the setup timeout ({:?})",
                    protocol.name(),
                    self.setup_timeout
                )));
            }
        }
        Ok(())
    }

    /// Fail the run if a connection is still in setup: the server never
    /// finished it, so the connection never applied its load
    fn check_setup_done(&self) -> Result<()> {
        for &connection_id in self.pool.connection_ids() {
            let Some(state) = self.pool.get_state(connection_id) else {
                continue;
            };
            let group_id = state.group_id();
            let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
            if protocol.setup_pending(connection_id) {
                return Err(crate::Error::Connection(format!(
                    "Traffic group {group_id}, connection {connection_id} ({}): connection setup \
                     did not finish by the end of the run",
                    protocol.name()
                )));
            }
        }
        Ok(())
    }

    /// Count the intended sends of every connection scheduled before the end
    /// of the window that were never sent: those its schedule had not
    /// reached, and those still queued behind bytes the transport did not
    /// take, which stop counting as sent if they go out later
    fn record_unsent(&mut self, end_ns: u64) {
        let stats = &mut self.stats;
        self.pool.abandon_unsent(|connection_id, group_id, unsent| {
            stats.record_unsent(group_id, connection_id, unsent, false);
        });
        for (group_id, connection_id, count, capped) in
            self.pool.count_unsent(end_ns, UNSENT_WALK_CAP)
        {
            if capped {
                tracing::warn!(
                    connection_id,
                    cap = UNSENT_WALK_CAP,
                    "Unsent request count stopped at its cap; it is a lower bound"
                );
            }
            self.stats.record_unsent(group_id, connection_id, count, capped);
        }
    }

    /// Wait for pending responses after the measurement window, until none
    /// is pending or `drain_timeout` passes. Sends nothing. Measurement
    /// requests still pending then are counted as timed out, without a latency.
    fn drain_responses(&mut self) -> Result<()> {
        let timeout_ns = self.config.drain_timeout.as_nanos().min(u64::MAX as u128) as u64;
        let deadline_ns = timing::time_ns().saturating_add(timeout_ns);
        // Only ready connections and timeouts end pending requests, so a
        // poll that reports neither leaves them pending: a busy-polling
        // drain checks every connection only when one may have finished
        let mut pending = self.has_pending();
        while pending {
            let now_ns = timing::time_ns();
            if now_ns >= deadline_ns {
                break;
            }
            // Short waits keep TX timestamp polling and the deadline responsive.
            let wait_ns = (deadline_ns - now_ns).min(10_000_000);
            let wait_ns = self
                .pool
                .next_request_deadline()
                .map_or(wait_ns, |next| wait_ns.min(next.saturating_sub(now_ns)));
            self.process_responses(self.busy_poll_timeout(Duration::from_nanos(wait_ns), now_ns))?;
            let ready = !self.ready_ids.is_empty();
            if ready {
                self.last_io_ns = now_ns;
            }
            if self.expire_requests(timing::time_ns()) || ready {
                pending = self.has_pending();
            }
        }

        let mut timed_out = 0;
        for &connection_id in self.pool.connection_ids() {
            let Some(state) = self.pool.get_state(connection_id) else {
                continue;
            };
            let pending = state.measured_pending_count() as u64;
            if pending > 0 {
                self.stats.record_timed_out(state.group_id(), connection_id, pending);
                timed_out += pending;
            }
        }
        if timed_out > 0 {
            tracing::warn!(
                timed_out_requests = timed_out,
                drain_timeout = ?self.config.drain_timeout,
                "Requests still pending after the drain timeout count as timed out"
            );
        }
        Ok(())
    }

    fn has_pending(&self) -> bool {
        self.pool
            .connection_ids()
            .iter()
            .any(|&id| self.pool.get_state(id).is_some_and(|state| state.pending_count() > 0))
    }

    /// Get the stats collector
    pub fn stats(&self) -> &S {
        &self.stats
    }

    /// Consume the worker and return stats
    pub fn into_stats(self) -> S {
        self.stats
    }

    /// How the worker's connection group converted NIC timestamps, if it
    /// did (see [`ConnectionGroup::clock_calibration`])
    pub fn clock_calibration(&self) -> Option<xylem_transport::ClockCalibrationReport> {
        self.pool.clock_calibration()
    }
}

/// Specialized implementation for Unix domain sockets
impl<P: Protocol, S: StatsRecorder> Worker<xylem_transport::UnixConnectionGroup, P, S> {
    /// Create a new pipelined worker for Unix domain sockets
    ///
    /// This constructor handles Unix socket paths instead of SocketAddr targets.
    pub fn new_multi_group_unix(
        factory: &xylem_transport::UnixTransportFactory,
        protocols: HashMap<usize, P>,
        stats: S,
        config: WorkerConfig,
        groups: Vec<crate::connection::UnixTrafficGroupConfig>,
    ) -> Result<Self> {
        let roles = connection_roles(&protocols, groups.iter().map(|g| (g.0, g.2)));
        let pool = ConnectionPool::new_multi_group_unix_with_roles(factory, groups, &roles)?;

        Ok(Self::from_pool(pool, protocols, stats, config))
    }
}

/// The connection roles each group's protocol asks for, by group ID, given
/// `(group_id, load connections)` for every group of the worker
fn connection_roles<P: Protocol>(
    protocols: &HashMap<usize, P>,
    groups: impl Iterator<Item = (usize, usize)>,
) -> HashMap<usize, Vec<ConnectionRole>> {
    groups
        .filter_map(|(group_id, load)| {
            let protocol = protocols.get(&group_id)?;
            Some((group_id, protocol.connection_roles(load)))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use xylem_common::{ErrorReply, Parsed, Request, RequestMeta};
    use xylem_transport::TcpTransportFactory;

    use crate::connection::test_support::{FakeFactory, FakeGroup, FakeIo};
    use crate::scheduler::{PolicyScheduler, UniformPolicyScheduler};
    use crate::stats::SamplingPolicy;

    #[derive(Default)]
    struct FakeProtocol {
        next_id: u64,
        fail: bool,
        /// Requests with a lower ID are warmup requests.
        warmup: u64,
        /// Requests with a lower ID are setup requests, sent one at a time:
        /// the protocol cannot send while one is pending.
        setup: u64,
        setup_pending: bool,
        /// Report setup through `Protocol::setup_pending`, so it goes out
        /// off the schedule
        report_setup: bool,
        /// Responses to these requests are error replies of the given kind
        errors: HashMap<u64, &'static str>,
        /// Bytes each request carries after its 8-byte ID
        padding: usize,
    }

    impl Protocol for FakeProtocol {
        type RequestId = u64;

        fn next_request(&mut self, _: usize) -> Request<u64> {
            let id = self.next_id;
            self.next_id += 1;
            if id < self.setup {
                self.setup_pending = true;
                return Request::setup(id.to_le_bytes().to_vec(), id);
            }
            if id < self.warmup {
                return Request::new(id.to_le_bytes().to_vec(), id, RequestMeta::warmup());
            }
            let mut data = id.to_le_bytes().to_vec();
            data.resize(8 + self.padding, 0);
            Request::measurement(data, id)
        }

        fn parse_response<'a>(
            &mut self,
            _: usize,
            data: &'a [u8],
        ) -> anyhow::Result<Parsed<'a, u64>> {
            if self.fail {
                anyhow::bail!("invalid test response");
            }
            if data.len() < 8 {
                return Ok(Parsed::Incomplete);
            }
            let id = u64::from_le_bytes(data[..8].try_into().unwrap());
            if id < self.setup {
                self.setup_pending = false;
            }
            let error = self.errors.get(&id).map(|&kind| ErrorReply::new(kind, "test error"));
            Ok(Parsed::Complete { consumed: 8, id, error })
        }

        fn can_send(&self, _: usize) -> bool {
            !self.setup_pending
        }

        fn setup_pending(&self, _: usize) -> bool {
            self.report_setup && (self.setup_pending || self.next_id < self.setup)
        }

        fn name(&self) -> &'static str {
            "Fake"
        }
        fn reset(&mut self) {}
    }

    /// The fake transport never blocks in poll, so keep its drain short.
    const FAKE_DRAIN_TIMEOUT: Duration = Duration::from_millis(20);

    fn global(worker: &Worker<FakeGroup, FakeProtocol>) -> crate::stats::AggregatedStats {
        worker.stats.aggregate_global(Duration::from_secs(1))
    }

    fn fake_worker(
        factory: &FakeFactory,
        max_pending: usize,
        scheduler: Box<dyn PolicyScheduler>,
    ) -> Worker<FakeGroup, FakeProtocol> {
        let mut stats = TupleStatsCollector::default();
        stats.register_group_policy(0, SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 });
        Worker::new(
            factory,
            FakeProtocol::default(),
            stats,
            WorkerConfig {
                target: "127.0.0.1:1".parse().unwrap(),
                duration: Duration::from_millis(100),
                conn_count: 1,
                max_pending_per_conn: max_pending,
                drain_timeout: FAKE_DRAIN_TIMEOUT,
            },
            scheduler,
        )
        .unwrap()
    }

    /// Requests the fake transport took: the fake protocol's are 8 bytes
    fn sent_requests(factory: &FakeFactory) -> usize {
        factory.0.lock().unwrap().sent.iter().map(Vec::len).sum::<usize>() / 8
    }

    #[test]
    fn a_round_writes_each_connection_once_and_flushes_once() {
        let factory = FakeFactory::default();
        // Closed loop with 4 requests in flight: one round sends 4, in one write
        let mut worker = fake_worker(&factory, 4, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        let sent: Vec<u8> = (0u64..4).flat_map(u64::to_le_bytes).collect();
        assert_eq!(factory.0.lock().unwrap().sent, [sent]);
        assert_eq!(factory.0.lock().unwrap().flushes, [1]);
        assert_eq!(worker.stats.request_counts_global().sent, 4);

        // A round that sends nothing flushes nothing
        worker.send_requests().unwrap();
        assert_eq!(factory.0.lock().unwrap().flushes, [1]);
    }

    #[test]
    fn a_round_sends_its_first_connection_once_it_moves_on() {
        let factory = FakeFactory::default();
        let mut stats = TupleStatsCollector::default();
        stats.register_group_policy(0, SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 });
        let config = WorkerConfig {
            target: "127.0.0.1:1".parse().unwrap(),
            duration: Duration::from_millis(100),
            conn_count: 3,
            max_pending_per_conn: 2,
            drain_timeout: FAKE_DRAIN_TIMEOUT,
        };
        let mut worker =
            Worker::with_closed_loop(&factory, FakeProtocol::default(), stats, config).unwrap();
        worker.send_requests().unwrap();
        let io = factory.0.lock().unwrap();
        // The round picks the connections in turn. The first connection's
        // request went out once the round moved on, as a kernel socket would
        // have sent it; then each connection's queued requests took one
        // write, and the round one more flush
        assert_eq!(io.flushes, [1, 4]);
        assert_eq!(io.sent_on, [0, 1, 2, 0]);
        let sizes: Vec<usize> = io.sent.iter().map(Vec::len).collect();
        assert_eq!(sizes, [8, 16, 16, 8]);
    }

    #[test]
    fn a_round_writes_its_queue_once_it_holds_a_write_batch() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 8, Box::new(UniformPolicyScheduler::closed_loop()));
        // Four requests fill a write batch
        let batch = crate::connection::WRITE_BATCH_BYTES;
        worker.protocols.get_mut(&0).unwrap().padding = batch / 4 - 8;
        worker.send_requests().unwrap();
        let io = factory.0.lock().unwrap();
        let sizes: Vec<usize> = io.sent.iter().map(Vec::len).collect();
        assert_eq!(sizes, [batch, batch]);
        assert_eq!(io.flushes, [2]);
        drop(io);
        assert_eq!(worker.stats.request_counts_global().sent, 8);
    }

    #[test]
    fn a_round_of_one_send_flushes_once() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        assert_eq!(factory.0.lock().unwrap().flushes, [1]);
    }

    #[test]
    fn measurement_window_starts_after_warmup_and_ends_with_the_loop() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.protocols.get_mut(&0).unwrap().warmup = 1;
        let created_ns = timing::time_ns();

        // A warmup request and its response record nothing.
        worker.send_requests().unwrap();
        factory.0.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        assert_eq!(worker.stats.measured_duration(), None);
        thread::sleep(Duration::from_millis(50));

        // The first measured send opens the window; the loop's end closes it.
        let measured_send_ns = timing::time_ns();
        worker.send_requests().unwrap();
        worker.config.duration = Duration::ZERO;
        worker.run().unwrap();
        let (start, end) = worker.stats.measurement_window_ns().unwrap();
        assert!(start >= measured_send_ns, "window starts at the first measured send");
        assert!(start - created_ns >= 50_000_000, "warmup time is outside the window");
        assert!(end >= start && end <= timing::time_ns());
        assert_eq!(worker.stats.aggregate_global(Duration::from_secs(1)).total_requests, 0);
    }

    #[test]
    fn error_replies_complete_requests_with_their_own_latency() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 4, Box::new(UniformPolicyScheduler::closed_loop()));
        let protocol = worker.protocols.get_mut(&0).unwrap();
        protocol.warmup = 1;
        protocol.errors = HashMap::from([(0, "WARMUP"), (2, "WRONGTYPE"), (3, "OOM")]);
        // Requests 0 (warmup) to 3 fill the pipeline; one read answers them all
        worker.send_requests().unwrap();
        let replies: Vec<u8> = (0..4u64).flat_map(u64::to_le_bytes).collect();
        factory.0.lock().unwrap().responses.push_back(Ok(replies));
        worker.process_responses(Duration::ZERO).unwrap();

        // The warmup error reply is not counted
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.completed, counts.errors), (3, 3, 2));
        let kinds: Vec<_> = counts.errors_by_kind.iter().collect();
        assert_eq!(kinds, [("OOM", 1), ("WRONGTYPE", 1)]);
        // Error replies count toward throughput and bytes, but only the
        // success has a latency in the distribution
        assert_eq!(global(&worker).total_requests, 3);
        assert_eq!(global(&worker).latency_sample_count, 1);
        assert_eq!(worker.stats.error_latency(0).latency_sample_count, 2);
        assert_eq!(worker.stats.error_latency_global().latency_sample_count, 2);
        assert_eq!(
            worker
                .stats
                .entries()
                .map(|(_, entry)| entry)
                .map(|entry| entry.rx_bytes)
                .sum::<u64>(),
            24
        );
        // Like the latency, the breakdown leaves the error replies out
        assert_eq!(worker.stats.latency_breakdown_global().service.sample_count, 1);
        assert_eq!(worker.stats.latency_breakdown(0).service.sample_count, 1);

        worker.config.duration = Duration::ZERO;
        worker.run().unwrap();
        let counts = worker.stats.request_counts_global();
        assert_eq!(counts.sent, counts.completed + counts.timed_out + counts.failed);
    }

    #[test]
    fn on_error_abort_fails_at_the_first_error_reply_warmup_included() {
        for warmup in [0, 1] {
            let factory = FakeFactory::default();
            let mut worker =
                fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
            worker.set_on_error(0, OnError::Abort);
            let protocol = worker.protocols.get_mut(&0).unwrap();
            protocol.warmup = warmup;
            protocol.errors = HashMap::from([(0, "WRONGTYPE")]);
            worker.send_requests().unwrap();
            factory.0.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
            let error = worker.process_responses(Duration::ZERO).unwrap_err().to_string();
            assert_eq!(
                error,
                "Protocol error: Traffic group 0, connection 0: error reply WRONGTYPE \
                 (on_error = \"abort\"): test error"
            );
            assert_eq!(worker.stats.request_counts_global().completed, 0);
        }
    }

    #[test]
    fn run_preserves_completed_stats_and_closes_after_disconnect_or_drain_timeout() {
        for disconnect in [true, false] {
            let factory = FakeFactory::default();
            let mut worker =
                fake_worker(&factory, 2, Box::new(UniformPolicyScheduler::closed_loop()));
            worker.send_requests().unwrap();
            factory.0.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
            worker.process_responses(Duration::ZERO).unwrap();
            assert_eq!(global(&worker).total_requests, 1);
            if disconnect {
                factory
                    .0
                    .lock()
                    .unwrap()
                    .responses
                    .push_back(Err(xylem_transport::Error::Connection("closed".into())));
            }
            worker.config.duration = Duration::ZERO;
            worker.run().unwrap();
            assert_eq!(global(&worker).total_requests, 1);
            assert_eq!(
                worker
                    .stats
                    .entries()
                    .map(|(_, entry)| entry)
                    .map(|entry| entry.rx_bytes)
                    .sum::<u64>(),
                8
            );
            assert!(factory.0.lock().unwrap().closed);
            assert!(worker.pool.get_state(0).unwrap().is_closed());
            assert_eq!(worker.pool.get_state(0).unwrap().pending_count(), 0);
            assert!(!worker.has_pending());
            // The second request was pending on a dropped connection, or
            // still pending at the drain deadline; either way it is counted
            let counts = worker.stats.request_counts_global();
            assert_eq!((counts.sent, counts.completed), (2, 1));
            assert_eq!((counts.failed, counts.timed_out), if disconnect { (1, 0) } else { (0, 1) });
            assert_eq!(global(&worker).latency_sample_count, 1, "no invented latency");
        }
    }

    #[test]
    fn ready_work_after_send_cap_polls_without_waiting() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 32, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        assert_eq!(sent_requests(&factory), 16);
        assert_eq!(worker.pool.peek_next_ready_time(), None);
        let timeout = worker.poll_timeout(100, 100_000_100);
        assert_eq!(timeout, Duration::ZERO);
        worker.process_responses(timeout).unwrap();
        assert_eq!(factory.0.lock().unwrap().timeouts, vec![Duration::ZERO]);
    }

    #[test]
    fn future_policy_wait_is_clamped_to_experiment_end() {
        let factory = FakeFactory::default();
        let mut worker =
            fake_worker(&factory, 2, Box::new(UniformPolicyScheduler::fixed_rate(0.001)));
        // The first request is due at the connection's start
        assert_eq!(worker.pool.pick_connection(), Some(0));
        worker.pool.get_state_mut(0).unwrap().on_request_sent(timing::time_ns());
        worker.pool.try_schedule_connection(0, &worker.protocols[&0]);
        let now = timing::time_ns();
        let timeout = worker.poll_timeout(now, now + 5_000_000);
        assert_eq!(timeout, Duration::from_millis(5));
        worker.process_responses(timeout).unwrap();
        assert_eq!(factory.0.lock().unwrap().timeouts, vec![Duration::from_millis(5)]);
        assert_eq!(worker.poll_timeout(now + 5_000_001, now + 5_000_000), Duration::ZERO);
    }

    #[test]
    fn full_pipeline_wait_is_clamped_to_experiment_end() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        assert_eq!(worker.poll_timeout(100, 5_000_100), Duration::from_millis(5));
    }

    #[test]
    fn busy_poll_does_not_wait_until_its_budget_after_the_last_io_passed() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        let wait = Duration::from_millis(5);
        worker.last_io_ns = 1_000_000_000;
        assert_eq!(worker.busy_poll_timeout(wait, 1_000_000_000), wait, "off by default");
        worker.set_busy_poll(Some(Duration::from_millis(1)));
        assert_eq!(worker.busy_poll_timeout(wait, 1_000_000_000), Duration::ZERO);
        assert_eq!(worker.busy_poll_timeout(wait, 1_000_999_999), Duration::ZERO);
        assert_eq!(worker.busy_poll_timeout(wait, 1_001_000_000), wait);
        worker.set_busy_poll(None);
        assert_eq!(worker.busy_poll_timeout(wait, 1_000_000_000), wait);
    }

    #[test]
    fn busy_poll_spins_after_each_send_then_waits_as_without_it() {
        const MS: u64 = 1_000_000;
        for busy_poll in [None, Some(Duration::from_millis(5))] {
            let factory = FakeFactory::default();
            // Sends at the start and about 15 ms after it
            let mut worker = fake_worker(
                &factory,
                2,
                Box::new(UniformPolicyScheduler::fixed_rate(1000.0 / 15.0)),
            );
            worker.set_busy_poll(busy_poll);
            worker.config.duration = Duration::from_millis(25);
            let start = timing::time_ns() + MS;
            worker.run_at(start).unwrap();
            assert_eq!(worker.stats.request_counts_global().sent, 2);
            let io = factory.0.lock().unwrap();
            // When each poll was made, from the start, and whether it waited
            let polls: Vec<(u64, bool)> = io
                .poll_times
                .iter()
                .zip(&io.timeouts)
                .map(|(&time, timeout)| (time - start, !timeout.is_zero()))
                .collect();
            let spins_in = |from: u64, to: u64| {
                polls
                    .iter()
                    .any(|&(time, waits)| !waits && (from * MS..to * MS).contains(&time))
            };
            // Without busy polling, only the millisecond before a send spins,
            // for rate-policy precision; with it, the 5 ms after each send too
            assert_eq!(spins_in(1, 5), busy_poll.is_some(), "{busy_poll:?}: {polls:?}");
            assert_eq!(spins_in(17, 20), busy_poll.is_some(), "{busy_poll:?}: {polls:?}");
            // Then polls wait again, until the next send's early wakeup
            assert!(!spins_in(7, 13), "{busy_poll:?}: {polls:?}");
            assert!(!spins_in(22, 25), "{busy_poll:?}: {polls:?}");
            assert!(polls.iter().any(|&(time, waits)| waits && (7 * MS..13 * MS).contains(&time)));
        }
    }

    #[test]
    fn busy_poll_ends_the_window_and_the_drain_on_time() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        // A budget longer than the run: no poll waits
        worker.set_busy_poll(Some(Duration::from_secs(3600)));
        worker.config.duration = Duration::from_millis(10);
        let start = timing::time_ns();
        worker.run_at(start).unwrap();
        let elapsed = timing::time_ns() - start;
        assert!(factory.0.lock().unwrap().timeouts.iter().all(Duration::is_zero));
        // The request never answered was pending until the drain deadline
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.timed_out), (1, 1));
        assert_eq!(worker.stats.measurement_window_ns().unwrap().1, start + 10_000_000);
        let floor = 10_000_000 + FAKE_DRAIN_TIMEOUT.as_nanos() as u64;
        assert!((floor..floor + 1_000_000_000).contains(&elapsed), "{elapsed} ns");
    }

    #[test]
    fn a_ready_connection_restarts_the_busy_poll() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 2, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.set_busy_poll(Some(Duration::from_millis(5)));
        worker.send_requests().unwrap();
        worker.last_io_ns = timing::time_ns();
        thread::sleep(Duration::from_millis(6));
        // The budget has passed: the drain's first poll waits, and its
        // response restarts the spin, which lasts until the budget passes
        // again, while the second request stays pending to the deadline
        factory.0.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        worker.drain_responses().unwrap();
        let timeouts = factory.0.lock().unwrap().timeouts.clone();
        assert!(!timeouts[0].is_zero(), "{:?}", timeouts[0]);
        let spins = timeouts[1..].iter().take_while(|timeout| timeout.is_zero()).count();
        assert!(spins > 0 && 1 + spins < timeouts.len(), "{spins} of {}", timeouts.len());
        assert!(timeouts[1 + spins..].iter().all(|timeout| !timeout.is_zero()));
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.completed, counts.timed_out), (1, 1));
    }

    #[test]
    fn drain_records_response_and_polls_tx() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        factory.0.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        worker.drain_responses().unwrap();
        assert_eq!(global(&worker).total_requests, 1);
        assert_eq!(factory.0.lock().unwrap().tx_polls, 1);
        // Already drained: no extra blocking poll.
        worker.drain_responses().unwrap();
        assert_eq!(factory.0.lock().unwrap().timeouts.len(), 1);
        assert_eq!(worker.stats.request_counts_global().timed_out, 0);
    }

    #[test]
    fn drain_propagates_parse_and_io_errors() {
        for parse_error in [true, false] {
            let factory = FakeFactory::default();
            let mut worker =
                fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
            worker.send_requests().unwrap();
            worker.protocols.get_mut(&0).unwrap().fail = parse_error;
            factory.0.lock().unwrap().responses.push_back(if parse_error {
                Ok(0u64.to_le_bytes().to_vec())
            } else {
                Err(xylem_transport::Error::Io(std::io::Error::other("test I/O error")))
            });
            assert!(worker.drain_responses().is_err());
            assert_eq!(global(&worker).total_requests, 0);
        }
    }

    #[test]
    fn drain_closes_disconnected_connection_and_counts_its_requests_as_failed() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        factory
            .0
            .lock()
            .unwrap()
            .responses
            .push_back(Err(xylem_transport::Error::Connection("closed".into())));
        worker.drain_responses().unwrap();
        assert!(factory.0.lock().unwrap().closed);
        assert_eq!(worker.pool.get_state(0).unwrap().pending_count(), 0);
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.failed, counts.timed_out), (1, 1, 0));
    }

    /// A worker whose protocol sets each connection up with one request
    fn setup_worker(factory: &FakeFactory) -> Worker<FakeGroup, FakeProtocol> {
        let mut worker = fake_worker(factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        let protocol = worker.protocols.get_mut(&0).unwrap();
        protocol.setup = 1;
        protocol.report_setup = true;
        worker
    }

    #[test]
    fn a_connection_closed_during_setup_fails_the_run_naming_it() {
        // A closed connection is closed; an I/O error ends the run as before
        for (error, cause, closed) in [
            (
                xylem_transport::Error::Connection("Connection closed by peer".into()),
                "Connection closed by peer",
                true,
            ),
            (
                xylem_transport::Error::Io(std::io::ErrorKind::ConnectionReset.into()),
                "connection reset",
                false,
            ),
        ] {
            let factory = FakeFactory::default();
            let mut worker = setup_worker(&factory);
            worker.send_requests().unwrap();
            factory.0.lock().unwrap().responses.push_back(Err(error));
            let error = worker.process_responses(Duration::ZERO).unwrap_err();
            assert!(matches!(error, crate::Error::Connection(_)), "{error}");
            assert_eq!(
                error.to_string(),
                format!(
                    "Connection error: Traffic group 0, connection 0 (Fake): connection closed \
                     during setup: {cause}"
                )
            );
            assert_eq!(factory.0.lock().unwrap().closed, closed);
            assert_eq!(worker.stats.request_counts_global(), Default::default());
        }
    }

    #[test]
    fn a_connection_closed_after_setup_only_fails_its_requests() {
        let factory = FakeFactory::default();
        let mut worker = setup_worker(&factory);
        worker.send_requests().unwrap();
        factory.0.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        worker.send_requests().unwrap();
        factory
            .0
            .lock()
            .unwrap()
            .responses
            .push_back(Err(xylem_transport::Error::Connection("closed".into())));
        worker.process_responses(Duration::ZERO).unwrap();
        assert!(factory.0.lock().unwrap().closed);
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.failed), (1, 1));
    }

    #[test]
    fn a_run_whose_setup_never_finishes_fails_naming_the_connection() {
        let factory = FakeFactory::default();
        let mut worker = setup_worker(&factory);
        worker.config.duration = Duration::from_millis(10);
        // The setup reply never arrives
        let error = worker.run().unwrap_err();
        assert_eq!(
            error.to_string(),
            "Connection error: Traffic group 0, connection 0 (Fake): connection setup did not \
             finish by the end of the run"
        );
        assert_eq!(sent_requests(&factory), 1);
    }

    #[test]
    fn a_setup_that_outlasts_the_setup_timeout_fails_the_run_at_once() {
        let factory = FakeFactory::default();
        let mut worker = setup_worker(&factory);
        worker.config.duration = Duration::from_secs(10);
        worker.set_setup_timeout(Duration::from_millis(20));
        // The setup reply never arrives
        let started = std::time::Instant::now();
        let error = worker.run().unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
        assert_eq!(
            error.to_string(),
            "Connection error: Traffic group 0, connection 0 (Fake): connection setup did not \
             finish within the setup timeout (20ms)"
        );
        assert_eq!(sent_requests(&factory), 1);
    }

    #[test]
    fn the_setup_timeout_counts_from_the_connect() {
        let factory = FakeFactory::default();
        let mut worker = setup_worker(&factory);
        worker.set_setup_timeout(Duration::from_millis(5));
        let opened_ns = worker.pool.get_state(0).unwrap().opened_ns();
        worker.arm_setup_deadlines();
        assert_eq!(worker.setup_deadlines, [(opened_ns + 5_000_000, 0)]);
        worker.check_setup_deadlines(opened_ns + 4_999_999).unwrap();
        worker.check_setup_deadlines(opened_ns + 5_000_000).unwrap_err();
    }

    #[test]
    fn a_finished_setup_outlives_its_setup_deadline() {
        let factory = FakeFactory::default();
        let mut worker = setup_worker(&factory);
        worker.config.duration = Duration::from_millis(30);
        worker.set_setup_timeout(Duration::from_millis(1));
        factory.0.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        worker.run().unwrap();
        assert!(worker.setup_deadlines.is_empty());
        assert!(worker.stats.request_counts_global().sent > 0);
    }

    #[test]
    fn a_connection_without_setup_has_no_setup_deadline() {
        let factory = FakeFactory::default();
        let scheduler = Box::new(UniformPolicyScheduler::closed_loop());
        let mut worker = fake_worker(&factory, 1, scheduler);
        worker.arm_setup_deadlines();
        assert!(worker.setup_deadlines.is_empty());
    }

    #[test]
    fn disconnect_keeps_collecting_other_connections() {
        let factory = FakeFactory::default();
        let mut stats = TupleStatsCollector::default();
        stats.register_group_policy(0, SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 });
        let mut worker = Worker::with_closed_loop(
            &factory,
            FakeProtocol::default(),
            stats,
            WorkerConfig {
                target: "127.0.0.1:1".parse().unwrap(),
                duration: Duration::ZERO,
                conn_count: 2,
                max_pending_per_conn: 1,
                drain_timeout: DEFAULT_DRAIN_TIMEOUT,
            },
        )
        .unwrap();
        worker.pool.send(0, &0u64.to_le_bytes(), 0, false).unwrap();
        worker.pool.send(1, &1u64.to_le_bytes(), 1, false).unwrap();
        factory
            .0
            .lock()
            .unwrap()
            .responses
            .push_back(Err(xylem_transport::Error::Connection("closed".into())));
        worker.process_responses(Duration::ZERO).unwrap();
        assert!(worker.pool.get_state(0).unwrap().is_closed());
        assert!(!worker.pool.get_state(1).unwrap().is_closed());
        {
            let mut io = factory.0.lock().unwrap();
            io.response_connection_id = 1;
            io.responses.push_back(Ok(1u64.to_le_bytes().to_vec()));
        }
        worker.process_responses(Duration::ZERO).unwrap();
        worker.run().unwrap();
        assert_eq!(worker.stats.aggregate_global(Duration::from_secs(1)).total_requests, 1);
        assert_eq!(
            worker
                .stats
                .entries()
                .map(|(_, entry)| entry)
                .map(|entry| entry.rx_bytes)
                .sum::<u64>(),
            8
        );
        assert_eq!(worker.stats.request_counts_global().failed, 1);
        assert_eq!(factory.0.lock().unwrap().timeouts.len(), 2);
        assert!(!worker.has_pending());
        assert!(worker.pool.get_state(1).unwrap().is_closed());
    }

    #[test]
    fn unanswered_request_times_out_at_the_drain_deadline() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 2, Box::new(UniformPolicyScheduler::closed_loop()));
        // One warmup request and one measured request never get a response
        worker.protocols.get_mut(&0).unwrap().warmup = 1;
        worker.send_requests().unwrap();
        // The worker's clock, which sets the deadline; another clock may
        // differ from it by the calibration error
        let start_ns = timing::time_ns();
        worker.drain_responses().unwrap();
        assert!(timing::time_ns() - start_ns >= FAKE_DRAIN_TIMEOUT.as_nanos() as u64);
        assert!(worker.has_pending());
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.completed, counts.timed_out), (1, 0, 1));
        assert_eq!(worker.stats.request_counts(0), counts);
        assert_eq!(global(&worker).latency_sample_count, 0);
    }

    #[test]
    fn drain_completion_counts_toward_latency_but_not_throughput() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 2, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        // The first response arrives in the window, the second during the drain
        factory.0.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        factory.0.lock().unwrap().responses.push_back(Ok(1u64.to_le_bytes().to_vec()));
        worker.config.duration = Duration::ZERO;
        worker.run().unwrap();
        let stats = global(&worker);
        assert_eq!(stats.total_requests, 1);
        assert_eq!(stats.latency_sample_count, 2);
        assert_eq!(
            worker
                .stats
                .entries()
                .map(|(_, entry)| entry)
                .map(|entry| entry.rx_bytes)
                .sum::<u64>(),
            8
        );
        let points = worker.stats.time_series_global();
        assert_eq!(points.iter().map(|p| p.request_count).sum::<u64>(), 1);
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.completed, counts.timed_out), (2, 2, 0));
    }

    #[test]
    fn setup_requests_neither_consume_nor_delay_the_open_loop_schedule() {
        let factory = FakeFactory::default();
        factory.0.lock().unwrap().live_timestamps = true;
        let dir = tempfile::tempdir().unwrap();
        let dumper = RequestDumper::new(
            crate::request_dump::DumpConfigBuilder::new().directory(dir.path()).build(),
        )
        .unwrap();
        // 1000 req/s: one intended send per millisecond
        let mut worker =
            fake_worker(&factory, 100, Box::new(UniformPolicyScheduler::fixed_rate(1000.0)));
        worker.set_request_dumper(Arc::clone(&dumper), 0);
        worker.protocols.get_mut(&0).unwrap().setup = 2;

        // Each setup request goes out alone and waits 20 intervals for its reply
        let mut last_reply_ns = 0;
        for id in 0u64..2 {
            worker.send_requests().unwrap();
            assert_eq!(factory.0.lock().unwrap().sent.len() as u64, id + 1);
            thread::sleep(Duration::from_millis(20));
            worker.send_requests().unwrap();
            assert_eq!(factory.0.lock().unwrap().sent.len() as u64, id + 1, "setup is pending");
            factory.0.lock().unwrap().responses.push_back(Ok(id.to_le_bytes().to_vec()));
            last_reply_ns = timing::time_ns();
            worker.process_responses(Duration::ZERO).unwrap();
        }

        // The schedule restarted when the last setup reply was processed, so
        // the 40 ms spent on setup are neither a backlog of overdue sends nor
        // send lag. The checks read the clock around each step rather than
        // assume the test thread runs without preemption: a stall after the
        // restart makes more slots due, and they allow exactly those.
        let intended = worker.pool.get_state(0).unwrap().intended_send_time().unwrap();
        assert!(intended >= last_reply_ns, "schedule restarted after setup");
        let before_send_ns = timing::time_ns();
        worker.send_requests().unwrap();
        let after_send_ns = timing::time_ns();
        // Slots due by a time: the first at `intended`, then one per millisecond
        let due = |time_ns: u64| (time_ns.saturating_sub(intended) / 1_000_000 + 1) as usize;
        let measured = factory.0.lock().unwrap().sent.len() - 2;
        // One call sends at most 16 requests per connection
        assert!(
            (due(before_send_ns).min(16)..=due(after_send_ns)).contains(&measured),
            "{measured} measured sends, due {} to {}: no burst",
            due(before_send_ns),
            due(after_send_ns)
        );
        let counts = worker.stats.request_counts_global();
        assert_eq!(
            (counts.sent, counts.completed),
            (measured as u64, 0),
            "setup requests are not counted"
        );
        let lag = worker.stats.send_lag_global();
        assert_eq!(lag.sample_count, measured as u64);
        let max_lag = lag.max.unwrap();
        assert!(max_lag <= Duration::from_nanos(after_send_ns - intended), "{lag:?}");
        assert_eq!(
            worker.pool.get_state(0).unwrap().intended_send_time(),
            Some(intended + measured as u64 * 1_000_000),
            "the measured requests took the first intended times"
        );
        // Setup bytes are neither dumped nor counted
        assert_eq!(dumper.total_records(), measured as u64);
        factory.0.lock().unwrap().responses.push_back(Ok(2u64.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        let tx_bytes: u64 =
            worker.stats.entries().map(|(_, entry)| entry).map(|entry| entry.tx_bytes).sum();
        assert_eq!(tx_bytes, 8 * measured as u64);
        assert_eq!(
            worker
                .stats
                .entries()
                .map(|(_, entry)| entry)
                .map(|entry| entry.rx_bytes)
                .sum::<u64>(),
            8
        );
        assert_eq!(global(&worker).latency_sample_count, 1);
    }

    /// A worker whose one connection is connection 1 of 4 sharing 40 req/s:
    /// its slots are 25 ms, 125 ms, 225 ms, ... after the start
    fn striped_worker(factory: &FakeFactory) -> Worker<FakeGroup, FakeProtocol> {
        use crate::scheduler::{FixedRatePolicy, GroupPolicyScheduler};
        let scheduler =
            GroupPolicyScheduler::new(1, |c| Box::new(FixedRatePolicy::striped(40.0, c, 4)));
        fake_worker(factory, 100, Box::new(scheduler))
    }

    #[test]
    fn setup_goes_out_at_once_and_the_schedule_starts_at_the_first_slot_after_it() {
        for setup_done_after_start in [false, true] {
            let factory = FakeFactory::default();
            factory.0.lock().unwrap().live_timestamps = true;
            let mut worker = striped_worker(&factory);
            let protocol = worker.protocols.get_mut(&0).unwrap();
            protocol.setup = 2;
            protocol.report_setup = true;
            // The start is now, and setup ends 30 ms after it, or 10 s ahead
            let start = if setup_done_after_start {
                timing::time_ns()
            } else {
                timing::time_ns() + 10_000_000_000
            };
            worker.anchor_schedules(start);

            // Each setup request goes out as soon as the connection can send,
            // not at its slots
            for id in 0u64..2 {
                worker.send_requests().unwrap();
                assert_eq!(factory.0.lock().unwrap().sent.len() as u64, id + 1);
                if id == 1 && setup_done_after_start {
                    thread::sleep(Duration::from_millis(30));
                }
                factory.0.lock().unwrap().responses.push_back(Ok(id.to_le_bytes().to_vec()));
                worker.process_responses(Duration::ZERO).unwrap();
            }

            // The schedule starts at the first slot at or after both the start
            // and the end of setup, so setup is neither send lag nor unsent
            let first = if setup_done_after_start {
                125_000_000
            } else {
                25_000_000
            };
            let state = worker.pool.get_state(0).unwrap();
            assert_eq!(state.intended_send_time(), Some(start + first));
            worker.record_unsent(start + first);
            assert_eq!(worker.stats.request_counts_global().unsent, 0);
            worker.record_unsent(start + first + 100_000_001);
            assert_eq!(worker.stats.request_counts_global().unsent, 2);
            if !setup_done_after_start {
                worker.send_requests().unwrap();
                assert_eq!(factory.0.lock().unwrap().sent.len(), 2, "nothing is due yet");
            }
        }
    }

    #[test]
    fn a_connection_stuck_in_setup_intends_no_sends() {
        for report_setup in [false, true] {
            let factory = FakeFactory::default();
            let mut worker =
                fake_worker(&factory, 100, Box::new(UniformPolicyScheduler::fixed_rate(1000.0)));
            let protocol = worker.protocols.get_mut(&0).unwrap();
            protocol.setup = 1;
            protocol.report_setup = report_setup;
            let start = timing::time_ns();
            if report_setup {
                // Setup goes out at once, long before the first slot
                worker.anchor_schedules(start + 10_000_000_000);
            }
            // Otherwise the setup request takes the unanchored schedule's
            // first slot, due at once
            worker.send_requests().unwrap();
            assert_eq!(factory.0.lock().unwrap().sent.len(), 1);
            // The setup reply never arrives
            worker.record_unsent(start + 20_000_000_000);
            let counts = worker.stats.request_counts_global();
            assert_eq!((counts.sent, counts.unsent), (0, 0), "report_setup {report_setup}");
        }
    }

    #[test]
    fn run_at_starts_at_the_shared_start_and_takes_it_as_the_bucket_epoch() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.config.duration = Duration::from_millis(10);
        let start = timing::time_ns() + 30_000_000;
        worker.run_at(start).unwrap();
        assert_eq!(worker.stats.experiment_start_ns(), start);
        let (first_send, end) = worker.stats.measurement_window_ns().unwrap();
        assert!(first_send >= start, "nothing is sent before the start");
        assert_eq!(end, start + 10_000_000);
        assert_eq!(worker.stats.request_counts_global().sent, 1);
    }

    #[test]
    fn actual_load_loop_starts_after_the_shared_wait() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.config.duration = Duration::ZERO;
        let start = timing::time_ns() + 1_000_000;
        let mut samples = [Some(100), Some(200)].into_iter();
        worker
            .run_at_with_clock(start, || {
                assert!(timing::time_ns() >= start, "the shared wait completed");
                assert!(factory.0.lock().unwrap().sent.is_empty());
                samples.next().expect("the wall clock is sampled exactly twice")
            })
            .unwrap();
        assert!(samples.next().is_none());
        assert_eq!(worker.stats.actual_load_loop_unix_us(), Some((100, 200)));
    }

    #[test]
    fn actual_load_loop_excludes_pending_response_drain() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        // A response is waiting when the zero-duration loop exits.
        worker.send_requests().unwrap();
        factory.0.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        worker.config.duration = Duration::ZERO;
        worker.config.drain_timeout = Duration::from_secs(1);
        let mut samples = [Some(100), Some(200)].into_iter();
        worker
            .run_at_with_clock(timing::time_ns(), || {
                let io = factory.0.lock().unwrap();
                assert!(io.timeouts.is_empty(), "the drain has not polled yet");
                assert_eq!(io.responses.len(), 1, "the response is still pending");
                samples.next().expect("the wall clock is sampled exactly twice")
            })
            .unwrap();
        assert!(samples.next().is_none());
        assert_eq!(worker.stats.actual_load_loop_unix_us(), Some((100, 200)));
        {
            let io = factory.0.lock().unwrap();
            assert_eq!(io.timeouts.len(), 1, "the pending response was drained");
            assert!(io.responses.is_empty());
        }
        assert_eq!(worker.stats.request_counts_global().completed, 1);
        assert_eq!(worker.stats.request_counts_global().timed_out, 0);
        assert_eq!(global(&worker).total_requests, 0);
        assert_eq!(global(&worker).latency_sample_count, 1);
    }

    #[test]
    fn actual_load_loop_records_errors_and_invalid_clock_samples() {
        let samples_and_expected = [
            (Some(100), Some(200), Some((100, 200))),
            (None, Some(200), None),
            (Some(100), None, None),
            (None, None, None),
            (Some(200), Some(100), None),
            (Some(100), Some(1 << 53), None),
        ];
        for fail_send in [true, false] {
            for (start_sample, end_sample, expected) in samples_and_expected {
                assert_actual_load_loop_error_case(fail_send, start_sample, end_sample, expected);
            }
        }
    }

    fn assert_actual_load_loop_error_case(
        fail_send: bool,
        start_sample: Option<u64>,
        end_sample: Option<u64>,
        expected: Option<(u64, u64)>,
    ) {
        let factory = FakeFactory::default();
        {
            let mut io = factory.0.lock().unwrap();
            if fail_send {
                io.fail_send = true;
            } else {
                // Complete one request before the next receive fails.
                io.responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
                io.responses.push_back(Err(xylem_transport::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "receive failed",
                ))));
            }
            // A drain would consume this response after the receive error.
            io.responses.push_back(Ok(1u64.to_le_bytes().to_vec()));
        }
        let mut worker =
            fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::fixed_rate(10_000.0)));
        worker.config.duration = Duration::from_secs(5);
        // A first event makes an erroneous end_measurement visible.
        worker.stats.record_tx_bytes(0, 0, 0);
        let mut samples = [start_sample, end_sample].into_iter();
        let mut clock_calls = 0;
        let result = worker.run_at_with_clock(timing::time_ns(), || {
            let io = factory.0.lock().unwrap();
            assert!(!io.closed);
            if clock_calls == 0 {
                assert!(io.sent.is_empty());
            } else if !fail_send {
                let sent = io.sent.iter().map(Vec::len).sum::<usize>() / 8;
                assert_eq!(sent, 2, "the end sample follows the error");
                assert_eq!(io.responses.len(), 1, "the end sample precedes draining");
            }
            clock_calls += 1;
            samples.next().expect("the wall clock is sampled exactly twice")
        });
        assert!(samples.next().is_none());
        match result.unwrap_err() {
            crate::Error::Io(error) => {
                let (kind, message) = if fail_send {
                    (std::io::ErrorKind::Other, "send failed")
                } else {
                    (std::io::ErrorKind::PermissionDenied, "receive failed")
                };
                assert_eq!((error.kind(), error.to_string().as_str()), (kind, message));
            }
            error => panic!("unexpected error: {error:?}"),
        }

        assert_eq!(worker.stats.measurement_window_ns(), None);
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.unsent, counts.timed_out, counts.failed), (0, 0, 0));
        let completed = u64::from(!fail_send);
        assert_eq!((counts.sent, counts.completed), (completed * 2, completed));
        assert_eq!(global(&worker).total_requests, completed);
        // A request whose write failed was never sent, so nothing waits for it
        let measured_pending = worker.pool.get_state(0).unwrap().measured_pending_count();
        assert_eq!(measured_pending, completed as usize);
        {
            let io = factory.0.lock().unwrap();
            assert!(!io.closed);
            assert_eq!(io.responses.len(), 1, "the loop error must not drain responses");
        }

        let failed = worker.into_stats();
        assert_eq!(failed.actual_load_loop_unix_us(), expected);
        let mut peer = TupleStatsCollector::default();
        peer.record_actual_load_loop(120, 140);
        let merged = TupleStatsCollector::merge(vec![peer, failed, TupleStatsCollector::default()]);
        assert_eq!(
            merged.actual_load_loop_unix_us(),
            expected,
            "fail_send={fail_send}, samples=({start_sample:?}, {end_sample:?})"
        );
    }

    #[test]
    fn actual_load_loop_records_error_through_public_run() {
        let factory = FakeFactory::default();
        factory.0.lock().unwrap().fail_send = true;
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        let before = unix_time_us().unwrap();
        let error = worker.run().unwrap_err();
        let after = unix_time_us().unwrap();
        assert!(matches!(error, crate::Error::Io(ref e) if e.to_string() == "send failed"));
        let (start, end) = worker.into_stats().actual_load_loop_unix_us().unwrap();
        assert!(before <= start && start <= end && end <= after);
    }

    #[test]
    fn latency_breakdown_skips_setup_and_warmup_requests() {
        use crate::stats::BreakdownClock;
        let factory = FakeFactory::default();
        factory.0.lock().unwrap().live_timestamps = true;
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        let protocol = worker.protocols.get_mut(&0).unwrap();
        // Request 0 sets the connection up, 1 is warmup, 2 and 3 are measured
        protocol.setup = 1;
        protocol.warmup = 2;
        for id in 0u64..4 {
            worker.send_requests().unwrap();
            factory.0.lock().unwrap().responses.push_back(Ok(id.to_le_bytes().to_vec()));
            worker.process_responses(Duration::ZERO).unwrap();
        }
        assert_eq!(factory.0.lock().unwrap().sent.len(), 4);
        let breakdown = worker.stats.latency_breakdown_global();
        assert_eq!(breakdown.service.sample_count, 2);
        assert_eq!(global(&worker).latency_sample_count, 2);
        // User-space timestamps only: the service latency, no kernel split
        assert_eq!(breakdown.clock, Some(BreakdownClock::UserSpace));
        assert_eq!((breakdown.kernel_split_count, breakdown.host_stack), (0, None));
        assert_eq!(worker.stats.latency_breakdown(0), breakdown);
    }

    #[test]
    fn kernel_tx_and_rx_timestamps_split_the_recorded_round_trip() {
        use crate::stats::BreakdownClock;
        use xylem_transport::{KernelClock, KernelTimestamp, Timestamp, TxTimestampInfo};
        let kernel = |nanos| KernelTimestamp { nanos, clock: KernelClock::Realtime };
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        thread::sleep(Duration::from_millis(1));
        {
            // The send's TX stamp arrives; the response has an RX stamp 5 µs later
            let mut io = factory.0.lock().unwrap();
            io.tx_timestamps.push(TxTimestampInfo {
                conn_id: 0,
                tx_end: 1,
                cumulative: true,
                timestamp: kernel(1_000),
            });
            io.timestamp = Timestamp::now().with_kernel(kernel(6_000));
            io.responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        }
        worker.process_responses(Duration::ZERO).unwrap();
        let breakdown = worker.stats.latency_breakdown_global();
        assert_eq!(breakdown.clock, Some(BreakdownClock::KernelSoftware));
        assert_eq!((breakdown.kernel_split_count, breakdown.service.sample_count), (1, 1));
        let network = breakdown.network_server.unwrap();
        assert_eq!(network.max, Some(Duration::from_micros(5)));
        assert_eq!(breakdown.service.max, network.max, "the latency's clock");
        // The cycles round trip spans the 1 ms sleep
        let host = breakdown.host_stack.unwrap().max.unwrap();
        assert!(host >= Duration::from_micros(900), "{host:?}");
        assert_eq!(global(&worker).max_latency, Some(Duration::from_micros(5)));
    }

    #[test]
    fn closed_loop_requests_have_no_send_lag_or_unsent_requests() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 4, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        let responses: Vec<u8> = (0u64..4).flat_map(u64::to_le_bytes).collect();
        factory.0.lock().unwrap().responses.push_back(Ok(responses));
        worker.config.duration = Duration::ZERO;
        worker.run().unwrap();
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.completed, counts.late_sends, counts.unsent), (4, 4, 0, 0));
        assert_eq!(worker.stats.send_lag_global(), crate::stats::SendLagSummary::default());
        // Latency is the service latency alone, as before
        assert_eq!(global(&worker).max_latency, Some(Duration::ZERO));
    }

    #[test]
    fn stalled_sender_sends_overdue_requests_and_their_latency_includes_the_lag() {
        let factory = FakeFactory::default();
        factory.0.lock().unwrap().live_timestamps = true;
        let mut worker =
            fake_worker(&factory, 1000, Box::new(UniformPolicyScheduler::fixed_rate(1000.0)));
        let start_ns = worker.pool.get_state(0).unwrap().intended_send_time().unwrap();
        worker.send_requests().unwrap();
        assert_eq!(factory.0.lock().unwrap().sent.len(), 1);

        // The sender stalls for 100 ms at 1000 req/s
        thread::sleep(Duration::from_millis(100));
        while worker.pool.has_ready(timing::time_ns()) {
            worker.send_requests().unwrap();
        }
        let sent = sent_requests(&factory) as u64;
        assert!(sent >= 101, "the overdue requests go out: {sent}");
        assert_eq!(
            worker.pool.get_state(0).unwrap().intended_send_time(),
            Some(start_ns + sent * 1_000_000),
            "each request took its own intended time"
        );
        let lag = worker.stats.send_lag_global();
        assert_eq!(lag.sample_count, sent);
        assert!(lag.max.unwrap() >= Duration::from_millis(99), "{lag:?}");
        let counts = worker.stats.request_counts_global();
        assert!(counts.late_sends >= 90, "{counts:?}");

        // Each latency is its send lag plus the service latency
        let responses: Vec<u8> = (0..sent).flat_map(u64::to_le_bytes).collect();
        factory.0.lock().unwrap().responses.push_back(Ok(responses));
        worker.process_responses(Duration::ZERO).unwrap();
        let stats = global(&worker);
        assert_eq!(stats.total_requests, sent);
        assert!(stats.max_latency.unwrap() >= lag.max.unwrap());
        assert!(stats.mean_latency >= lag.mean.unwrap());
        assert!(stats.mean_latency >= Duration::from_millis(40), "{:?}", stats.mean_latency);
    }

    #[test]
    fn intended_sends_that_never_went_out_count_as_unsent() {
        let factory = FakeFactory::default();
        let mut worker =
            fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::fixed_rate(1000.0)));
        let start_ns = worker.pool.get_state(0).unwrap().intended_send_time().unwrap();
        // The only request never gets a response, so the pipeline stays full
        worker.send_requests().unwrap();
        let end_ns = start_ns + 50_000_000;
        worker.record_unsent(end_ns);
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.unsent, counts.unsent_capped), (1, 49, false));
        assert_eq!(worker.stats.request_counts(0).unsent, 49);
    }

    /// Run `worker` at 1000 arrivals/s for 50 ms against a peer that reads 84
    /// bytes, 10.5 requests, and then nothing: each of the 50 arrivals of the
    /// window counts once, as sent or unsent
    fn assert_requests_behind_a_full_socket_count_as_unsent(
        factory: &FakeFactory,
        mut worker: Worker<FakeGroup, FakeProtocol>,
    ) {
        factory.0.lock().unwrap().capacity = Some(84);
        worker.config.duration = Duration::from_millis(50);
        worker.run_at(timing::time_ns() + 5_000_000).unwrap();
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.unsent, counts.unsent_capped), (10, 40, false));
        // The sent ones got no reply; the rest never went out, even partly
        assert_eq!((counts.completed, counts.timed_out, counts.failed), (0, 10, 0));
    }

    #[test]
    fn requests_queued_behind_a_full_socket_at_the_end_count_as_unsent() {
        let factory = FakeFactory::default();
        let worker =
            fake_worker(&factory, 1000, Box::new(UniformPolicyScheduler::fixed_rate(1000.0)));
        assert_requests_behind_a_full_socket_count_as_unsent(&factory, worker);
    }

    #[test]
    fn group_arrivals_queued_behind_a_full_socket_at_the_end_count_as_unsent() {
        use crate::scheduler::FixedRatePolicy;
        let factory = FakeFactory::default();
        let worker = group_worker(&factory, 1, 1000, Box::new(FixedRatePolicy::new(1000.0)));
        assert_requests_behind_a_full_socket_count_as_unsent(&factory, worker);
    }

    #[test]
    fn a_request_unsent_at_the_end_that_goes_out_later_is_not_measured() {
        let factory = FakeFactory::default();
        let mut worker =
            fake_worker(&factory, 1000, Box::new(UniformPolicyScheduler::fixed_rate(1000.0)));
        let io = factory.0.clone();
        io.lock().unwrap().capacity = Some(4);
        let start_ns = worker.pool.get_state(0).unwrap().intended_send_time().unwrap();
        worker.send_requests().unwrap();
        assert!(worker.pool.get_state(0).unwrap().is_write_blocked());
        worker.record_unsent(start_ns + 1);
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.unsent), (0, 1));

        // Its rest goes out in the drain, and its reply arrives: neither counts
        {
            let mut io = io.lock().unwrap();
            io.capacity = None;
            io.writable = true;
            io.responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        }
        worker.drain_responses().unwrap();
        assert_eq!(sent_requests(&factory), 1);
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.unsent, counts.completed, counts.timed_out), (0, 1, 0, 0));
    }

    #[test]
    fn requests_queued_on_a_failed_connection_count_as_unsent() {
        let factory = FakeFactory::default();
        let mut worker =
            fake_worker(&factory, 1000, Box::new(UniformPolicyScheduler::fixed_rate(100.0)));
        factory.0.lock().unwrap().capacity = Some(12);
        let start_ns = worker.pool.get_state(0).unwrap().intended_send_time().unwrap();
        timing::busy_wait_until(start_ns + 10_000_000);
        worker.send_requests().unwrap();
        let counts = worker.stats.request_counts_global();
        assert_eq!(counts.sent, 1);

        factory
            .0
            .lock()
            .unwrap()
            .responses
            .push_back(Err(xylem_transport::Error::Connection("closed".into())));
        worker.process_responses(Duration::ZERO).unwrap();
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.failed, counts.unsent), (1, 1, 1));
        // The failed connection's schedule counts on from after them
        worker.record_unsent(start_ns + 20_000_001);
        assert_eq!(worker.stats.request_counts_global().unsent, 2);
    }

    /// A worker whose `connections` connections share the arrivals of `policy`
    fn group_worker(
        factory: &FakeFactory,
        connections: usize,
        max_pending: usize,
        policy: Box<dyn crate::scheduler::Policy>,
    ) -> Worker<FakeGroup, FakeProtocol> {
        let mut stats = TupleStatsCollector::default();
        stats.register_group_policy(0, SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 });
        Worker::new(
            factory,
            FakeProtocol::default(),
            stats,
            WorkerConfig {
                target: "127.0.0.1:1".parse().unwrap(),
                duration: Duration::from_millis(100),
                conn_count: connections,
                max_pending_per_conn: max_pending,
                drain_timeout: FAKE_DRAIN_TIMEOUT,
            },
            Box::new(crate::scheduler::GroupDispatchScheduler::new(policy)),
        )
        .unwrap()
    }

    #[test]
    fn group_dispatch_sends_arrivals_on_idle_connections_and_counts_waits_as_late() {
        use crate::scheduler::FixedRatePolicy;
        let factory = FakeFactory::default();
        factory.0.lock().unwrap().live_timestamps = true;
        // Two connections, one request in flight each, one arrival per ms
        let mut worker = group_worker(&factory, 2, 1, Box::new(FixedRatePolicy::new(1000.0)));
        let ms = 1_000_000;
        let start = timing::time_ns() + 2 * ms;
        worker.anchor_schedules(start);
        worker.send_requests().unwrap();
        assert_eq!(factory.0.lock().unwrap().sent.len(), 0, "nothing before the start");
        assert_eq!(worker.pool.peek_next_ready_time(), Some(start));

        // The first two arrivals go out on the two idle connections
        timing::busy_wait_until(start);
        worker.send_requests().unwrap();
        timing::busy_wait_until(start + ms);
        worker.send_requests().unwrap();
        assert_eq!(factory.0.lock().unwrap().sent.len(), 2);
        let pending =
            |worker: &Worker<_, _>, id| worker.pool.get_state(id).unwrap().pending_count();
        assert_eq!((pending(&worker, 0), pending(&worker, 1)), (1, 1));

        // No connection is idle: the arrivals at 2 and 3 ms wait, and with
        // nothing to send there is no send wakeup
        timing::busy_wait_until(start + 3 * ms + ms / 2);
        worker.send_requests().unwrap();
        assert_eq!(factory.0.lock().unwrap().sent.len(), 2);
        assert!(!worker.pool.has_ready(timing::time_ns()));
        assert_eq!(worker.pool.peek_next_ready_time(), None);

        // Connection 0 frees up: the arrival intended at 2 ms goes out on it
        // at once, late, and connection 0 is busy again
        let factory_io = std::sync::Arc::clone(&factory.0);
        {
            let mut io = factory_io.lock().unwrap();
            io.response_connection_id = 0;
            io.responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        }
        worker.process_responses(Duration::ZERO).unwrap();
        worker.send_requests().unwrap();
        assert_eq!(factory.0.lock().unwrap().sent.len(), 3);
        assert_eq!((pending(&worker, 0), pending(&worker, 1)), (1, 1));
        let counts = worker.stats.request_counts_global();
        assert_eq!(counts.sent, 3);
        assert!(counts.late_sends >= 1, "{counts:?}");
        let lag = worker.stats.send_lag_global();
        assert!(lag.max.unwrap() >= Duration::from_micros(1_500), "{lag:?}");

        // Its latency runs from its intended time, so it includes the wait
        {
            let mut io = factory_io.lock().unwrap();
            io.responses.push_back(Ok(2u64.to_le_bytes().to_vec()));
        }
        worker.process_responses(Duration::ZERO).unwrap();
        assert!(global(&worker).max_latency.unwrap() >= lag.max.unwrap());

        // The arrivals at 3, 4 and 5 ms never went out
        worker.record_unsent(start + 5 * ms + 1);
        assert_eq!(worker.stats.request_counts_global().unsent, 3);
    }

    #[test]
    fn group_dispatch_starts_once_a_connection_is_set_up() {
        use crate::scheduler::FixedRatePolicy;
        let factory = FakeFactory::default();
        factory.0.lock().unwrap().live_timestamps = true;
        let mut worker = group_worker(&factory, 2, 1, Box::new(FixedRatePolicy::new(1000.0)));
        let protocol = worker.protocols.get_mut(&0).unwrap();
        protocol.setup = 2;
        protocol.report_setup = true;
        let start = timing::time_ns();
        worker.anchor_schedules(start);

        // Each connection sends its setup request at once, off the arrivals
        worker.send_requests().unwrap();
        assert_eq!(factory.0.lock().unwrap().sent.len(), 2);
        thread::sleep(Duration::from_millis(5));
        worker.send_requests().unwrap();
        assert_eq!(factory.0.lock().unwrap().sent.len(), 2, "setup is pending");

        // Connection 1 fails in setup; the setup reply makes connection 0
        // idle, and the arrivals start then: setup is neither late nor unsent
        worker.pool.close(1).unwrap();
        let reply_ns = timing::time_ns();
        {
            let mut io = factory.0.lock().unwrap();
            io.response_connection_id = 0;
            io.responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        }
        worker.process_responses(Duration::ZERO).unwrap();
        worker.record_unsent(reply_ns);
        assert_eq!(worker.stats.request_counts_global().unsent, 0);
        let deadline = timing::time_ns() + 50_000_000;
        while factory.0.lock().unwrap().sent.len() < 3 && timing::time_ns() < deadline {
            worker.send_requests().unwrap();
        }
        assert_eq!(factory.0.lock().unwrap().sent.len(), 3);
        assert_eq!(worker.pool.get_state(0).unwrap().pending_count(), 1);
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.unsent), (1, 0));
    }

    /// Records the latencies a policy is told about; sends without a schedule
    struct FeedbackPolicy(Arc<std::sync::Mutex<Vec<Duration>>>);

    impl crate::scheduler::Policy for FeedbackPolicy {
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
    fn group_dispatch_feeds_responses_back_to_the_arrival_policy() {
        let factory = FakeFactory::default();
        let feedback = Arc::<std::sync::Mutex<Vec<Duration>>>::default();
        let mut worker =
            group_worker(&factory, 3, 1, Box::new(FeedbackPolicy(Arc::clone(&feedback))));
        worker.protocols.get_mut(&0).unwrap().errors.insert(1, "ERR");
        // Connection 1 closes while idle and leaves the idle list. Without a
        // schedule, every other idle connection sends at once.
        worker.pool.close(1).unwrap();
        worker.send_requests().unwrap();
        assert_eq!(factory.0.lock().unwrap().sent.len(), 2);
        for (connection_id, id) in [(0, 0u64), (2, 1)] {
            let mut io = factory.0.lock().unwrap();
            io.response_connection_id = connection_id;
            io.responses.push_back(Ok(id.to_le_bytes().to_vec()));
            drop(io);
            worker.process_responses(Duration::ZERO).unwrap();
        }
        // The error reply to request 1 gives no feedback
        assert_eq!(feedback.lock().unwrap().len(), 1);
    }

    #[test]
    fn adaptive_policy_receives_measurement_feedback_through_worker() {
        for warmup in [true, false] {
            let factory = FakeFactory::default();
            let mut worker = fake_worker(
                &factory,
                100,
                Box::new(UniformPolicyScheduler::adaptive(
                    100.0,
                    Duration::from_millis(1),
                    1.0,
                    1000.0,
                )),
            );
            // Remove the initial heap entry so response processing can reschedule.
            assert_eq!(worker.pool.pick_connection(), Some(0));
            let state = worker.pool.get_state_mut(0).unwrap();
            let start = state.intended_send_time().unwrap();
            state.on_request_sent(timing::time_ns());
            let mut responses = Vec::new();
            for id in 0u64..100 {
                worker.pool.send(0, &id.to_le_bytes(), id, warmup).unwrap();
                responses.extend(id.to_le_bytes());
            }
            factory.0.lock().unwrap().responses.push_back(Ok(responses));
            worker.process_responses(Duration::ZERO).unwrap();
            // Inspect the actual deadline scheduled by response processing; do not
            // call the adaptive policy directly to apply or simulate feedback.
            let next = worker.pool.peek_next_ready_time().unwrap();
            if warmup {
                assert_eq!(next - start, 10_000_000, "warmup must not change the adaptive rate");
                assert_eq!(global(&worker).total_requests, 0);
            } else {
                assert_eq!(
                    next - start,
                    9_090_909,
                    "100 zero-latency responses increase rate to 110/s"
                );
                assert_eq!(global(&worker).total_requests, 100);
            }
        }
    }

    #[test]
    fn tx_queue_maintenance_runs_at_most_once_per_second() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.next_tx_cleanup_ns = 100;
        worker.maintain_tx_queues(99);
        assert_eq!(worker.next_tx_cleanup_ns, 100);
        worker.maintain_tx_queues(100);
        assert_eq!(worker.next_tx_cleanup_ns, 1_000_000_100);
        worker.maintain_tx_queues(101);
        assert_eq!(worker.next_tx_cleanup_ns, 1_000_000_100);
        worker.maintain_tx_queues(1_000_000_100);
        assert_eq!(worker.next_tx_cleanup_ns, 2_000_000_100);
    }

    #[test]
    fn timed_out_request_frees_the_pipeline_and_its_late_response_counts() {
        let factory = FakeFactory::default();
        factory.0.lock().unwrap().datagram = true;
        factory.0.lock().unwrap().live_timestamps = true;
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.set_request_timeout(0, Some(Duration::from_millis(20)));
        worker.send_requests().unwrap();
        assert_eq!(factory.0.lock().unwrap().sent.len(), 1);
        // The full pipeline waits for the request's deadline, not the end
        let now = timing::time_ns();
        let wait = worker.poll_timeout(now, now + 10_000_000_000);
        assert!(wait <= Duration::from_millis(20), "{wait:?}");
        thread::sleep(Duration::from_millis(25));
        worker.expire_requests(timing::time_ns());
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.timed_out, counts.completed), (1, 1, 0));

        // The freed slot sends the next request at once
        worker.send_requests().unwrap();
        assert_eq!(factory.0.lock().unwrap().sent.len(), 2);
        // Request 0's late response is dropped and counted; request 1 completes
        factory.0.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        factory.0.lock().unwrap().responses.push_back(Ok(1u64.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.completed, counts.timed_out, counts.late_responses), (1, 1, 1));
        assert_eq!(global(&worker).latency_sample_count, 1, "the late response has no latency");
        // Every received datagram counts toward the received bytes
        assert_eq!(
            worker
                .stats
                .entries()
                .map(|(_, entry)| entry)
                .map(|entry| entry.rx_bytes)
                .sum::<u64>(),
            16
        );
    }

    #[test]
    fn open_loop_keeps_sending_through_losses_with_a_request_timeout() {
        let factory = FakeFactory::default();
        factory.0.lock().unwrap().datagram = true;
        factory.0.lock().unwrap().live_timestamps = true;
        let mut worker =
            fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::fixed_rate(1000.0)));
        worker.set_request_timeout(0, Some(Duration::from_millis(5)));
        // Nothing is ever answered; each timeout frees the single slot
        let start = std::time::Instant::now();
        worker.run().unwrap();
        assert!(start.elapsed() < Duration::from_millis(100) + FAKE_DRAIN_TIMEOUT * 3);
        let counts = worker.stats.request_counts_global();
        assert!(counts.sent >= 10 && counts.sent <= 21, "{counts:?}");
        // Every request timed out, in the window or during the drain
        assert_eq!((counts.timed_out, counts.completed, counts.failed), (counts.sent, 0, 0));
        assert!(!worker.has_pending());
        assert!(counts.unsent >= 70, "{counts:?}");
        assert!(counts.late_sends > 0, "the blocked schedule shows as send lag");
        assert_eq!(global(&worker).latency_sample_count, 0);
    }

    #[test]
    fn warmup_timeouts_are_not_counted() {
        let factory = FakeFactory::default();
        factory.0.lock().unwrap().datagram = true;
        factory.0.lock().unwrap().live_timestamps = true;
        let mut worker = fake_worker(&factory, 2, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.protocols.get_mut(&0).unwrap().warmup = 2;
        worker.set_request_timeout(0, Some(Duration::from_millis(1)));
        worker.send_requests().unwrap();
        thread::sleep(Duration::from_millis(5));
        worker.expire_requests(timing::time_ns());
        factory.0.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.timed_out, counts.late_responses), (0, 0, 0));
        assert_eq!(worker.pool.get_state(0).unwrap().pending_count(), 0);
    }

    fn udp_worker<P: Protocol>(
        target: SocketAddr,
        protocol: P,
        max_pending: usize,
        duration: Duration,
    ) -> Worker<xylem_transport::UdpConnectionGroup, P> {
        let mut stats = TupleStatsCollector::default();
        stats.register_group_policy(0, SamplingPolicy::Limited { max_samples: 100_000, rate: 1.0 });
        let config = WorkerConfig {
            target,
            duration,
            conn_count: 2,
            max_pending_per_conn: max_pending,
            drain_timeout: Duration::from_millis(200),
        };
        let factory = xylem_transport::UdpTransportFactory::default();
        let mut worker = Worker::with_closed_loop(&factory, protocol, stats, config).unwrap();
        worker.set_request_timeout(0, Some(Duration::from_millis(20)));
        worker
    }

    #[test]
    fn udp_echo_over_loopback_survives_lost_datagrams() {
        // Echoes requests, but drops every fifth one
        let server = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();
        server.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        thread::spawn(move || {
            let mut buf = [0u8; 64];
            let mut count = 0u64;
            while let Ok((len, peer)) = server.recv_from(&mut buf) {
                count += 1;
                if !count.is_multiple_of(5) {
                    let _ = server.send_to(&buf[..len], peer);
                }
            }
        });

        let protocol = xylem_protocols::xylem_echo::XylemEchoProtocol::new(0);
        let mut worker = udp_worker(addr, protocol, 4, Duration::from_millis(300));
        assert!(worker.pool.is_datagram());
        worker.run().unwrap();
        let counts = worker.stats.request_counts_global();
        // Losses time out instead of stalling the closed loop
        assert!(counts.completed > 100, "{counts:?}");
        assert!(counts.timed_out > 0, "{counts:?}");
        assert_eq!(counts.sent, counts.completed + counts.timed_out, "{counts:?}");
        assert_eq!(counts.failed, 0);
        assert_eq!(
            worker.stats.aggregate_global(Duration::from_secs(1)).latency_sample_count,
            counts.completed
        );
    }

    #[test]
    fn udp_memcached_over_loopback_reassembles_reordered_duplicated_responses() {
        use xylem_protocols::memcached::{MemcachedAsciiProtocol, MemcachedOp};
        use xylem_protocols::workload::KeyGeneration;
        // Answers each GET with a 4000-byte value in three datagrams, sent
        // last first, with the middle one duplicated
        let server = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();
        server.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        thread::spawn(move || {
            let mut buf = [0u8; 2048];
            while let Ok((len, peer)) = server.recv_from(&mut buf) {
                let request = &buf[..len];
                assert_eq!(&request[2..8], &[0, 0, 0, 1, 0, 0], "single-datagram request");
                let key = std::str::from_utf8(&request[12..len - 2]).unwrap();
                let value = vec![b'v'; 4000];
                let mut body = format!("VALUE {key} 0 {}\r\n", value.len()).into_bytes();
                body.extend_from_slice(&value);
                body.extend_from_slice(b"\r\nEND\r\n");
                let chunks: Vec<_> = body.chunks(1400).collect();
                for number in [2usize, 1, 1, 0] {
                    let mut datagram = request[..2].to_vec();
                    datagram.extend_from_slice(&(number as u16).to_be_bytes());
                    datagram.extend_from_slice(&(chunks.len() as u16).to_be_bytes());
                    datagram.extend_from_slice(&[0, 0]);
                    datagram.extend_from_slice(chunks[number]);
                    let _ = server.send_to(&datagram, peer);
                }
            }
        });

        let protocol = MemcachedAsciiProtocol::with_workload(
            MemcachedOp::Get,
            KeyGeneration::sequential(0),
            8,
        )
        .with_udp_framing();
        let mut worker = udp_worker(addr, protocol, 2, Duration::from_millis(200));
        worker.run().unwrap();
        let counts = worker.stats.request_counts_global();
        assert!(counts.completed > 20, "{counts:?}");
        assert_eq!(counts.sent, counts.completed + counts.timed_out, "{counts:?}");
        assert_eq!((counts.failed, counts.late_responses), (0, 0), "{counts:?}");
        // Each response's bytes are its datagrams, duplicates included
        let rx: u64 =
            worker.stats.entries().map(|(_, entry)| entry).map(|entry| entry.rx_bytes).sum();
        assert!(rx >= counts.completed * 4000, "{rx}");
    }

    #[test]
    fn test_pipelined_worker_basic() {
        // Start echo server
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buf = vec![0u8; 4096];

            loop {
                let n = match socket.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };

                if socket.write_all(&buf[..n]).is_err() {
                    break;
                }
            }
        });

        thread::sleep(Duration::from_millis(50));

        // Create pipelined worker
        let protocol = xylem_protocols::xylem_echo::XylemEchoProtocol::new(0);
        let mut stats = TupleStatsCollector::default();
        stats.register_group_policy(0, SamplingPolicy::Limited { max_samples: 100000, rate: 1.0 });
        let config = WorkerConfig {
            target: addr,
            duration: Duration::from_millis(100),
            conn_count: 1,
            max_pending_per_conn: 1,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
        };

        let factory = TcpTransportFactory::default();
        let mut worker = Worker::with_closed_loop(&factory, protocol, stats, config).unwrap();

        let result = worker.run();
        assert!(result.is_ok(), "Worker should complete: {:?}", result.err());

        let stats = worker.stats();
        assert!(
            stats.aggregate_global(Duration::from_millis(100)).total_requests > 0,
            "Should have sent requests"
        );
    }

    #[test]
    fn connections_keep_their_local_addresses_after_they_close() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let config = WorkerConfig {
            target: listener.local_addr().unwrap(),
            duration: Duration::from_millis(10),
            conn_count: 2,
            max_pending_per_conn: 1,
            drain_timeout: FAKE_DRAIN_TIMEOUT,
        };
        let mut worker = Worker::new(
            &TcpTransportFactory::default(),
            FakeProtocol::default(),
            TupleStatsCollector::default(),
            config,
            Box::new(UniformPolicyScheduler::closed_loop()),
        )
        .unwrap();
        let peers = [listener.accept().unwrap().1, listener.accept().unwrap().1];
        let expected = vec![(0, 0, Some(peers[0])), (0, 1, Some(peers[1]))];
        assert_eq!(worker.connection_local_addrs(), expected);
        worker.pool.close_all().unwrap();
        assert_eq!(worker.connection_local_addrs(), expected);
    }

    #[test]
    fn test_fixed_rate_policy() {
        use crate::scheduler::UniformPolicyScheduler;
        use std::time::Instant;

        // Start echo server
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buf = vec![0u8; 4096];

            loop {
                let n = match socket.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };

                if socket.write_all(&buf[..n]).is_err() {
                    break;
                }
            }
        });

        thread::sleep(Duration::from_millis(50));

        // Create worker with fixed-rate policy
        let protocol = xylem_protocols::xylem_echo::XylemEchoProtocol::new(0);

        let mut stats = TupleStatsCollector::default();
        stats.register_group_policy(0, SamplingPolicy::Limited { max_samples: 100000, rate: 1.0 });
        let config = WorkerConfig {
            target: addr,
            duration: Duration::from_millis(200),
            conn_count: 1,
            max_pending_per_conn: 10,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
        };

        // Fixed rate: 100 req/s = 10ms per request
        let policy_scheduler = Box::new(UniformPolicyScheduler::fixed_rate(100.0));

        let start = Instant::now();
        let factory = TcpTransportFactory::default();
        let mut worker = Worker::new(&factory, protocol, stats, config, policy_scheduler).unwrap();

        let result = worker.run();
        assert!(result.is_ok(), "Worker should complete: {:?}", result.err());

        let elapsed = start.elapsed();
        let stats = worker.stats();

        // At 100 req/s for 200ms, expect ~20 requests
        let expected = 20;
        let actual = stats.aggregate_global(Duration::from_millis(200)).total_requests;
        assert!(
            actual >= expected - 5 && actual <= expected + 5,
            "Expected ~{} requests at 100 req/s for {:?}, got {}",
            expected,
            elapsed,
            actual
        );

        println!("✓ Fixed-rate policy sent {} requests in {:?}", actual, elapsed);
    }

    /// Setup request ID of the session protocol's control connection
    const SESSION_SETUP: u64 = u64::MAX;

    /// A protocol whose session spans connections: a control connection
    /// sets up first and unblocks the load connections. A load request's
    /// reply may follow a 4-byte PULL frame, which completes nothing and
    /// makes the protocol send DATA on the connection.
    #[derive(Default)]
    struct SessionProtocol {
        control: Option<usize>,
        load: Vec<usize>,
        setup_sent: bool,
        ready: bool,
        next_id: u64,
        /// Send a measurement request on the control connection
        misbehave: bool,
        driven: HashMap<usize, Vec<u8>>,
        /// What a PULL frame makes it send; `DATA` when empty
        pull_data: Vec<u8>,
        wake: Vec<usize>,
    }

    impl Protocol for SessionProtocol {
        type RequestId = u64;

        fn connection_roles(&self, load: usize) -> Vec<ConnectionRole> {
            let mut roles = vec![ConnectionRole::Control];
            roles.extend(std::iter::repeat_n(ConnectionRole::Load, load));
            roles
        }

        fn bind_connections(&mut self, conn_ids: &[usize]) {
            self.control = Some(conn_ids[0]);
            self.load = conn_ids[1..].to_vec();
        }

        fn next_request(&mut self, conn_id: usize) -> Request<u64> {
            if Some(conn_id) == self.control && !self.misbehave {
                self.setup_sent = true;
                return Request::setup(SESSION_SETUP.to_le_bytes().to_vec(), SESSION_SETUP);
            }
            let id = self.next_id;
            self.next_id += 1;
            Request::measurement(id.to_le_bytes().to_vec(), id)
        }

        fn parse_response<'a>(
            &mut self,
            conn_id: usize,
            data: &'a [u8],
        ) -> anyhow::Result<Parsed<'a, u64>> {
            if data.starts_with(b"PULL") {
                let pull: &[u8] = if self.pull_data.is_empty() {
                    b"DATA"
                } else {
                    &self.pull_data
                };
                self.driven.entry(conn_id).or_default().extend_from_slice(pull);
                return Ok(Parsed::Absorbed(4));
            }
            if data.len() < 8 {
                return Ok(Parsed::Incomplete);
            }
            let id = u64::from_le_bytes(data[..8].try_into().unwrap());
            if id == SESSION_SETUP {
                self.ready = true;
                self.wake.extend_from_slice(&self.load);
            }
            Ok(Parsed::ok(8, id))
        }

        fn can_send(&self, conn_id: usize) -> bool {
            if Some(conn_id) == self.control {
                !self.setup_sent
            } else {
                self.ready
            }
        }

        fn setup_pending(&self, conn_id: usize) -> bool {
            Some(conn_id) == self.control && !self.ready
        }

        fn drain_outbox(&mut self, conn_id: usize, outbox: &mut Outbox) {
            if let Some(data) = self.driven.remove(&conn_id) {
                outbox.data.extend_from_slice(&data);
            }
            outbox.wake.append(&mut self.wake);
        }

        fn name(&self) -> &'static str {
            "Session"
        }
        fn reset(&mut self) {}
    }

    fn session_worker(
        factory: &FakeFactory,
        protocol: SessionProtocol,
    ) -> Worker<FakeGroup, SessionProtocol> {
        session_worker_with(factory, protocol, Box::new(UniformPolicyScheduler::closed_loop()))
    }

    fn session_worker_with(
        factory: &FakeFactory,
        protocol: SessionProtocol,
        scheduler: Box<dyn PolicyScheduler>,
    ) -> Worker<FakeGroup, SessionProtocol> {
        let mut stats = TupleStatsCollector::default();
        stats.register_group_policy(0, SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 });
        let config = WorkerConfig {
            target: "127.0.0.1:1".parse().unwrap(),
            duration: Duration::from_millis(100),
            conn_count: 2,
            max_pending_per_conn: 1,
            drain_timeout: FAKE_DRAIN_TIMEOUT,
        };
        let mut worker = Worker::new(factory, protocol, stats, config, scheduler).unwrap();
        worker.anchor_schedules(timing::time_ns());
        worker
    }

    #[test]
    fn control_connection_sets_up_first_and_wakes_the_load_connections() {
        let factory = FakeFactory::default();
        let mut worker = session_worker(&factory, SessionProtocol::default());
        let io = factory.0.clone();
        // Opened first, the control connection sends only its setup request
        let protocol = &worker.protocols[&0];
        assert_eq!((protocol.control, protocol.load.clone()), (Some(0), vec![1, 2]));
        assert!(worker.pool.get_state(0).unwrap().is_control());
        assert!(!worker.pool.get_state(1).unwrap().is_control());
        worker.send_requests().unwrap();
        assert_eq!(io.lock().unwrap().sent_on, [0]);

        // Its reply unblocks the load connections at once
        io.lock().unwrap().responses.push_back(Ok(SESSION_SETUP.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        worker.send_requests().unwrap();
        assert_eq!(io.lock().unwrap().sent_on, [0, 1, 2]);

        // Only the load connections' requests are measured
        let counts = worker.stats.request_counts_global();
        assert_eq!(counts.sent, 2);
        assert!(worker.stats.entries().map(|(key, _)| key).all(|key| key.connection_id != 0));
    }

    #[test]
    fn driven_data_goes_out_before_the_next_request_and_is_no_request() {
        let factory = FakeFactory::default();
        let mut worker = session_worker(&factory, SessionProtocol::default());
        let io = factory.0.clone();
        worker.send_requests().unwrap();
        io.lock().unwrap().responses.push_back(Ok(SESSION_SETUP.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        worker.send_requests().unwrap();
        let flushes_before = io.lock().unwrap().flushes.len();

        // Request 0 on connection 1: a PULL frame, then its reply in a later read
        io.lock().unwrap().response_connection_id = 1;
        io.lock().unwrap().responses.push_back(Ok(b"PULL".to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        {
            let io = io.lock().unwrap();
            assert_eq!(io.sent.last().unwrap(), b"DATA");
            assert_eq!(io.sent_on, [0, 1, 2, 1]);
            // The driven send is flushed at once
            assert_eq!(io.flushes.len(), flushes_before + 1);
        }
        assert_eq!(worker.pool.get_state(1).unwrap().pending_count(), 1);

        io.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        worker.send_requests().unwrap();
        assert_eq!(io.lock().unwrap().sent_on, [0, 1, 2, 1, 1]);

        // One request completed with one latency; its reply's bytes include
        // the PULL frame, and the DATA bytes count as sent bytes
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.completed), (3, 1));
        let global = worker.stats.aggregate_global(Duration::from_secs(1));
        assert_eq!(global.latency_sample_count, 1);
        let rx_bytes: u64 = worker.stats.entries().map(|(_, entry)| entry.rx_bytes).sum();
        assert_eq!(rx_bytes, 12);
        let tx_bytes: u64 = worker.stats.entries().map(|(_, entry)| entry.tx_bytes).sum();
        assert_eq!(tx_bytes, 3 * 8 + 4);
    }

    #[test]
    fn driven_data_on_a_slow_link_waits_for_writability_while_reads_go_on() {
        let factory = FakeFactory::default();
        let pull_data: Vec<u8> = (0..20).collect();
        let protocol = SessionProtocol {
            pull_data: pull_data.clone(),
            ..Default::default()
        };
        let mut worker = session_worker(&factory, protocol);
        let io = factory.0.clone();
        worker.send_requests().unwrap();
        io.lock().unwrap().responses.push_back(Ok(SESSION_SETUP.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        worker.send_requests().unwrap();
        let on_connection_1 = |io: &FakeIo| -> Vec<u8> {
            let writes = io.sent.iter().zip(&io.sent_on).skip(3);
            writes.filter(|&(_, &on)| on == 1).flat_map(|(data, _)| data.clone()).collect()
        };

        // From now on the transport takes at most 8 bytes per write. A PULL
        // makes connection 1 send 20 bytes: 8 go out, 12 wait
        io.lock().unwrap().accept_limit = Some(8);
        io.lock().unwrap().response_connection_id = 1;
        io.lock().unwrap().responses.push_back(Ok(b"PULL".to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        assert_eq!(on_connection_1(&io.lock().unwrap()), pull_data[..8]);
        assert!(worker.pool.get_state(1).unwrap().is_write_blocked());

        // While it waits, the reply to request 0 is still read, and the next
        // request queues behind the rest of the data instead of going out
        io.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        assert_eq!(worker.stats.request_counts_global().completed, 1);
        worker.send_requests().unwrap();
        assert_eq!(on_connection_1(&io.lock().unwrap()), pull_data[..8]);
        assert_eq!(worker.stats.request_counts_global().sent, 2);

        // Once the connection is writable, each poll reports it and its
        // queue drains in order: the data, then request 2
        io.lock().unwrap().writable = true;
        for _ in 0..3 {
            worker.process_responses(Duration::ZERO).unwrap();
        }
        let mut expected = pull_data.clone();
        expected.extend_from_slice(&2u64.to_le_bytes());
        assert_eq!(on_connection_1(&io.lock().unwrap()), expected);
        assert!(!worker.pool.get_state(1).unwrap().is_write_blocked());
        assert!(io.lock().unwrap().write_interest.is_empty());
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.completed), (3, 1));
        let tx_bytes: u64 =
            worker.stats.entries().map(|(_, entry)| entry).map(|entry| entry.tx_bytes).sum();
        assert_eq!(tx_bytes, 3 * 8 + 20);
    }

    #[test]
    fn a_request_on_a_control_connection_fails_the_run() {
        let factory = FakeFactory::default();
        let protocol = SessionProtocol { misbehave: true, ..Default::default() };
        let mut worker = session_worker(&factory, protocol);
        let error = worker.send_requests().unwrap_err();
        assert!(error.to_string().contains("no setup request on a control connection"));
        assert!(factory.0.lock().unwrap().sent.is_empty());
    }

    #[test]
    fn group_dispatch_never_sends_arrivals_on_a_control_connection() {
        use crate::scheduler::{FixedRatePolicy, GroupDispatchScheduler};
        let factory = FakeFactory::default();
        factory.0.lock().unwrap().live_timestamps = true;
        let scheduler =
            Box::new(GroupDispatchScheduler::new(Box::new(FixedRatePolicy::new(10_000.0))));
        let mut worker = session_worker_with(&factory, SessionProtocol::default(), scheduler);
        let io = factory.0.clone();

        // The control connection sets up off the arrivals; the load
        // connections wait for it
        worker.send_requests().unwrap();
        assert_eq!(io.lock().unwrap().sent_on, [0]);
        io.lock().unwrap().responses.push_back(Ok(SESSION_SETUP.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();

        // Arrivals go to the idle load connections only, one each, while
        // the control connection stays idle
        let deadline = timing::time_ns() + 50_000_000;
        while io.lock().unwrap().sent.len() < 3 && timing::time_ns() < deadline {
            worker.send_requests().unwrap();
        }
        thread::sleep(Duration::from_millis(1));
        worker.send_requests().unwrap();
        let mut sent_on = io.lock().unwrap().sent_on.clone();
        sent_on[1..].sort_unstable();
        assert_eq!(sent_on, [0, 1, 2]);

        // A driven send goes out on a dispatched connection too, and its
        // reply frees the connection for the next arrival
        {
            let mut io = io.lock().unwrap();
            io.response_connection_id = 2;
            io.responses.push_back(Ok([&b"PULL"[..], &1u64.to_le_bytes()].concat()));
        }
        worker.process_responses(Duration::ZERO).unwrap();
        assert_eq!(io.lock().unwrap().sent.last().unwrap(), b"DATA");
        let deadline = timing::time_ns() + 50_000_000;
        while io.lock().unwrap().sent.len() < 5 && timing::time_ns() < deadline {
            worker.send_requests().unwrap();
        }
        assert_eq!(io.lock().unwrap().sent_on[3..], [2, 2]);
        assert_eq!(worker.stats.request_counts_global().completed, 1);
    }
}
