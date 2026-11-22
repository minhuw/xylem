//! Worker implementation with connection pooling and request pipelining
//!
//! This implements Lancet's symmetric agent model, allowing multiple outstanding
//! requests per connection for maximum throughput with accurate latency tracking.
//!
//! The worker can operate in different modes based on configuration:
//! - Single connection, single pending request: Simple closed-loop latency measurement
//! - Multiple connections with pipelining: High-throughput load testing

use crate::connection::{ConnectionPool, DeferredSample, SentRequest, TrafficGroupConfig};
use crate::request_dump::RequestDumper;
use crate::stats::{ReceivedAt, StatsRecorder, TupleStatsCollector, UNSENT_WALK_CAP};
use crate::threading::{IdleHoldBarrier, IdleHoldGuard, PreloadTicket, PreloadWait, StartTicket};
use crate::timing;
use crate::traffic_group::OnError;
use crate::Result;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use xylem_common::{ConnectionRole, FastHashMap, Outbox, Parsed, Protocol};
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

/// How long a thread that finished preloading waits in a poll for the
/// others to finish
const PRELOAD_BARRIER_POLL: Duration = Duration::from_millis(1);

/// How long after a worker that runs alone finished preloading its
/// measured run starts, so its schedules are anchored before their start
const ALONE_START_DELAY_NS: u64 = 1_000_000;

/// Where a worker that preloads (see [`Protocol::preload_pending`]) waits
/// until it may measure
enum PreloadBarrier {
    /// The worker runs alone: it measures shortly after its own preload is
    /// done
    Alone(Option<u64>),
    /// The worker's thread waits for the run's other threads
    Shared(PreloadTicket),
}

impl PreloadBarrier {
    fn arrive(&mut self) {
        match self {
            Self::Alone(start_ns) => {
                *start_ns = Some(timing::time_ns().saturating_add(ALONE_START_DELAY_NS));
            }
            Self::Shared(ticket) => ticket.arrive(),
        }
    }

    fn poll(&self) -> PreloadWait {
        match self {
            Self::Alone(start_ns) => start_ns.map_or(PreloadWait::Waiting, PreloadWait::Start),
            Self::Shared(ticket) => ticket.poll(),
        }
    }
}

/// Outcome of initiating a reconnect on a picked connection
enum ReconnectStep {
    /// Connected immediately; send the request right away with this connect start timestamp
    Immediate(xylem_transport::Timestamp),
    /// Non-blocking handshake in progress; wait for `poll()` + `finish_reconnect`
    InProgress,
    /// Connect failed and the connection was closed cleanly
    Closed,
}

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
    /// Reused buffer for the samples of responses that waited for their TX
    /// timestamps
    deferred: Vec<DeferredSample>,
    /// When the measurement window closed (a `timing::time_ns` value), just
    /// before [`StatsRecorder::end_measurement`]: responses received from
    /// then on were drained
    window_closed_ns: Option<u64>,
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
    /// Optional time-based warm-up before the measurement window starts
    warmup: Duration,
    /// Whether the worker is currently inside the time-based warm-up loop
    in_warmup: bool,
    /// Optional quiescent hold after the last response is drained, before closing
    idle_hold: Duration,
    /// Optional barrier synchronizing all worker threads entering `idle_hold`
    idle_hold_barrier: Option<Arc<IdleHoldBarrier>>,
    /// True when no fixed duration bounds the measurement loop (stops once
    /// every protocol reports `is_done()` and all writes/responses are drained)
    unbounded_duration: bool,
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
            deferred: Vec::new(),
            window_closed_ns: None,
            busy_poll_ns: None,
            last_io_ns: 0,
            setup_timeout: DEFAULT_SETUP_TIMEOUT,
            setup_deadlines: VecDeque::new(),
            warmup: Duration::ZERO,
            in_warmup: false,
            idle_hold: Duration::ZERO,
            idle_hold_barrier: None,
            unbounded_duration: false,
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

    /// Set the maximum requests per connection for traffic group `group_id`
    /// before closing and reconnecting (`None` keeps connections persistent)
    pub fn set_max_requests_per_connection(&mut self, group_id: usize, limit: Option<u64>) {
        self.pool.set_max_requests_per_connection(group_id, limit);
    }

    /// Set the timeout for non-blocking reconnect handshakes
    pub fn set_connect_timeout(&mut self, timeout: Duration) {
        self.pool.set_connect_timeout(timeout);
    }

    /// Fail the run when a connection's setup (see [`Protocol::setup_pending`])
    /// has not finished `timeout` after the pool started to open the
    /// connection; [`DEFAULT_SETUP_TIMEOUT`] by default
    ///
    /// The timeout includes the connect and the wait for the run's start,
    /// before which no setup request goes out. It also fails the run when
    /// the worker, preloading (see [`Protocol::preload_pending`]), receives
    /// nothing for `timeout`: preload takes as long as there is data to
    /// load, but no reply should take that long.
    pub fn set_setup_timeout(&mut self, timeout: Duration) {
        self.setup_timeout = timeout;
    }

    /// Let the latency sample of a response that came before its request's
    /// kernel TX timestamp wait up to `timeout` for the stamp, counted from
    /// the receive; see [`ConnectionPool::set_tx_timestamp_timeout`]
    ///
    /// The request completes at once: its pipeline slot is freed and the
    /// traffic policy gets the latency measured so far, in cycles. Only its
    /// sample waits, and counts once recorded where a sample recorded at
    /// the receive would. A sample whose stamp did not arrive in time, or by
    /// the end of the drain, is measured in cycles and counts in
    /// `kernel_split_missing.tx`.
    pub fn set_tx_timestamp_timeout(&mut self, timeout: Duration) {
        self.pool.set_tx_timestamp_timeout(timeout);
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

    /// Set time-based warm-up duration before measurement starts (default `0s`).
    /// During warm-up, requests are sent and responses polled normally without
    /// recording stats; `Protocol::on_measurement_start()` is invoked when the
    /// measurement window begins.
    pub fn set_warmup(&mut self, warmup: Duration) {
        self.warmup = warmup;
    }

    /// Set quiescent hold duration after draining responses and before closing
    /// connections (default `0s`). When `barrier` is provided, all worker
    /// threads wait on it before starting the hold timer; if any peer aborts
    /// the barrier (e.g., on error), waiting workers skip the hold and close
    /// immediately.
    pub fn set_idle_hold(&mut self, idle_hold: Duration, barrier: Option<Arc<IdleHoldBarrier>>) {
        self.idle_hold = idle_hold;
        self.idle_hold_barrier = barrier;
    }

    /// Disable the fixed experiment duration timer (for byte-limited runs that
    /// terminate once every protocol reports [`Protocol::is_done`]).
    pub fn set_unbounded_duration(&mut self, unbounded: bool) {
        self.unbounded_duration = unbounded;
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
    ///
    /// If a connection has preload work (see [`Protocol::preload_pending`]),
    /// the worker preloads from `start_ns` on, and the measured run, its
    /// schedules and its time buckets start once preload is done instead.
    pub fn run_at(&mut self, start_ns: u64) -> Result<()> {
        let preload = self.has_preload().then_some(PreloadBarrier::Alone(None));
        self.run_at_with_clock(start_ns, preload, unix_time_us)
    }

    /// Wait at the start gate with the run's other threads, then run from
    /// the start time they share; see [`Self::run_at`]
    ///
    /// If a connection of any thread has preload work (see
    /// [`Protocol::preload_pending`]), every thread preloads from the shared
    /// start on, and the measured run starts at one time shortly after the
    /// last thread finished: its schedules, its time buckets and
    /// `experiment.duration` count from there. If another thread fails
    /// first, this one returns without measuring, and the run's error is
    /// the other thread's.
    pub fn run_with_start(&mut self, start: StartTicket) -> Result<()> {
        let (start_ns, preload) = start.wait_for_run(self.has_preload());
        self.run_at_with_clock(start_ns, preload.map(PreloadBarrier::Shared), unix_time_us)
    }

    // A supplied wall clock lets tests exercise unusable samples at loop exit.
    fn run_at_with_clock(
        &mut self,
        start_ns: u64,
        preload: Option<PreloadBarrier>,
        mut wall_clock_us: impl FnMut() -> Option<u64>,
    ) -> Result<()> {
        let idle_hold_guard = (!self.idle_hold.is_zero())
            .then(|| self.idle_hold_barrier.as_ref().map(IdleHoldBarrier::guard))
            .flatten();
        self.pool.set_preloading(preload.is_some());
        self.anchor_schedules(start_ns);
        self.arm_setup_deadlines();
        wait_until(start_ns);
        // Busy polling starts with the run
        self.last_io_ns = start_ns;
        let start_ns = match preload {
            None => start_ns,
            Some(barrier) => {
                let preloaded = self.preload(barrier);
                self.pool.set_preloading(false);
                let Some(measure_ns) = preloaded? else {
                    // Another thread failed; its error is the run's
                    self.pool.close_all()?;
                    return Ok(());
                };
                self.anchor_schedules(measure_ns);
                wait_until(measure_ns);
                self.last_io_ns = measure_ns;
                measure_ns
            }
        };
        let warmup_ns = self.warmup.as_nanos().min(u64::MAX as u128) as u64;
        let measure_start_ns = if warmup_ns > 0 {
            let warmup_end_ns = start_ns.saturating_add(warmup_ns);
            self.in_warmup = true;
            let warmup_result = loop {
                let now_ns = timing::time_ns();
                if now_ns >= warmup_end_ns {
                    break Ok(());
                }
                if self.unbounded_duration && self.all_connections_closed() {
                    break Err(crate::Error::Connection(
                        "All connections closed during warm-up".to_string(),
                    ));
                }
                match self.send_requests_before(warmup_end_ns) {
                    Ok(0) => {}
                    Ok(_) => self.last_io_ns = now_ns,
                    Err(error) => break Err(error),
                }
                let now_ns = timing::time_ns();
                let poll_timeout = self.poll_timeout(now_ns, warmup_end_ns);
                let poll_timeout = self.busy_poll_timeout(poll_timeout, now_ns);
                if let Err(error) = self.process_responses(poll_timeout) {
                    break Err(error);
                }
                if !self.ready_ids.is_empty() {
                    self.last_io_ns = now_ns;
                }
                let now_ns = timing::time_ns();
                if let Err(error) = self.expire_requests(now_ns) {
                    break Err(error);
                }
                if let Err(error) = self.check_setup_deadlines(now_ns) {
                    break Err(error);
                }
            };
            self.in_warmup = false;
            warmup_result?;
            warmup_end_ns
        } else {
            start_ns
        };

        for protocol in self.protocols.values_mut() {
            protocol.on_measurement_start();
        }
        self.reschedule_all_connections();

        self.stats.start_measurement(measure_start_ns);
        let actual_start_us = wall_clock_us();

        let end_ns = if self.unbounded_duration {
            u64::MAX
        } else {
            let duration_ns = self.config.duration.as_nanos().min(u64::MAX as u128) as u64;
            measure_start_ns.saturating_add(duration_ns)
        };

        let mut stopped_early = false;
        let loop_result = loop {
            let now_ns = timing::time_ns();

            // Check if duration exceeded or all protocols completed their work quota
            if !self.unbounded_duration && now_ns >= end_ns {
                break Ok(());
            }
            if self.all_protocols_done() {
                stopped_early = true;
                break Ok(());
            }
            if self.unbounded_duration && self.all_connections_closed() {
                break Err(crate::Error::Connection(
                    "All connections closed before work completed".to_string(),
                ));
            }

            match self.send_requests_before(end_ns) {
                Ok(0) => {}
                Ok(_) => self.last_io_ns = now_ns,
                Err(error) => break Err(error),
            }

            if self.all_protocols_done() {
                stopped_early = true;
                break Ok(());
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
            if self.all_protocols_done() {
                stopped_early = true;
                break Ok(());
            }
            let now_ns = timing::time_ns();
            if let Err(error) = self.expire_requests(now_ns) {
                break Err(error);
            }
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
        let now_closed_ns = timing::time_ns();
        let actual_end_ns = if stopped_early { now_closed_ns } else { end_ns };
        self.window_closed_ns = Some(now_closed_ns);
        self.stats.end_measurement(actual_end_ns);
        self.record_unsent(actual_end_ns);

        // Stop sending and wait for the responses still pending, and for the
        // TX timestamps of those that came first
        self.drain_responses()?;
        self.pool.poll_tx_timestamps()?;
        self.pool.give_up_deferred();
        self.record_deferred();
        self.check_setup_done()?;

        // Optional quiescent hold before tearing down connections
        self.run_idle_hold(idle_hold_guard)?;

        // Close all connections
        self.pool.close_all()?;

        Ok(())
    }

    /// Reschedule every connection whose pool state and protocol allow sending
    fn reschedule_all_connections(&mut self) {
        for connection_id in self.pool.connection_ids().to_vec() {
            let Some(state) = self.pool.get_state(connection_id) else {
                continue;
            };
            let protocol =
                self.protocols.get(&state.group_id()).expect("Protocol not found for group_id");
            self.pool.try_schedule_connection(connection_id, protocol);
        }
    }

    /// True when every protocol has completed its configured work quota and no
    /// connection still has queued writes or pending responses.
    fn all_protocols_done(&self) -> bool {
        !self.protocols.is_empty()
            && self.protocols.values().all(Protocol::is_done)
            && self.pool.connection_ids().iter().all(|&id| {
                self.pool
                    .get_state(id)
                    .is_none_or(|s| s.pending_count() == 0 && !s.has_queued_writes())
            })
    }

    /// True when every connection in the pool has been closed.
    fn all_connections_closed(&self) -> bool {
        self.pool
            .connection_ids()
            .iter()
            .all(|&id| self.pool.get_state(id).is_none_or(|s| s.is_closed()))
    }

    /// Quiescent post-run hold: keeps connections open while polling the
    /// transport and discarding any inbound bytes until `idle_hold` elapses.
    fn run_idle_hold(&mut self, barrier_guard: Option<IdleHoldGuard>) -> Result<()> {
        if self.idle_hold.is_zero() {
            if let Some(mut guard) = barrier_guard {
                guard.disarm();
            }
            return Ok(());
        }
        self.pool.clear_write_interest_all();
        if let Some(guard) = barrier_guard {
            if !guard.arrive() {
                return Ok(());
            }
        }
        let hold_ns = self.idle_hold.as_nanos().min(u64::MAX as u128) as u64;
        let hold_end_ns = timing::time_ns().saturating_add(hold_ns);
        let mut ready_ids = std::mem::take(&mut self.ready_ids);
        let result = loop {
            let now_ns = timing::time_ns();
            if now_ns >= hold_end_ns {
                break Ok(());
            }
            let remaining_ns = hold_end_ns.saturating_sub(now_ns);
            let poll_timeout = Duration::from_nanos(remaining_ns.min(10_000_000));
            if let Err(err) = self.pool.poll(poll_timeout, &mut ready_ids) {
                break Err(err);
            }
            for &connection_id in &ready_ids {
                if let Err(err) = self.pool.discard_inbound(connection_id) {
                    ready_ids.clear();
                    self.ready_ids = ready_ids;
                    return Err(err);
                }
            }
        };
        ready_ids.clear();
        self.ready_ids = ready_ids;
        result
    }

    /// Whether a connection has preload work; see [`Protocol::preload_pending`]
    fn has_preload(&self) -> bool {
        self.pool.connection_ids().iter().any(|&connection_id| {
            self.pool.get_state(connection_id).is_some_and(|state| {
                let protocol =
                    self.protocols.get(&state.group_id()).expect("Protocol not found for group_id");
                protocol.preload_pending(connection_id)
            })
        })
    }

    /// Whether every connection is done with setup and preload, with no
    /// reply pending
    fn preload_done(&self) -> bool {
        self.preloading_connection().is_none()
    }

    /// The first connection still in setup or preload, or with a reply
    /// pending, and its group
    fn preloading_connection(&self) -> Option<(usize, usize)> {
        self.pool.connection_ids().iter().find_map(|&connection_id| {
            let state = self.pool.get_state(connection_id)?;
            let group_id = state.group_id();
            let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
            let busy = state.pending_count() > 0
                || protocol.setup_pending(connection_id)
                || protocol.preload_pending(connection_id);
            busy.then_some((connection_id, group_id))
        })
    }

    /// Preload: send every connection's setup and preload requests, off its
    /// schedule, and hold every other request, until this worker has none of
    /// them left nor a reply pending and `barrier` lets it measure
    ///
    /// Returns the start of the measured run, or `None` if another thread
    /// failed first. Receiving nothing for the setup timeout while preload
    /// is left fails the run.
    fn preload(&mut self, mut barrier: PreloadBarrier) -> Result<Option<u64>> {
        let stall_ns = self.setup_timeout.as_nanos().min(u64::MAX as u128) as u64;
        let mut last_data_ns = timing::time_ns();
        let mut arrived = false;
        loop {
            let now_ns = timing::time_ns();
            if !arrived && self.preload_done() {
                barrier.arrive();
                arrived = true;
            }
            let wait_end_ns = if arrived {
                match barrier.poll() {
                    PreloadWait::Start(start_ns) => return Ok(Some(start_ns)),
                    PreloadWait::Abandoned => return Ok(None),
                    PreloadWait::Waiting => {}
                }
                now_ns.saturating_add(PRELOAD_BARRIER_POLL.as_nanos() as u64)
            } else {
                let stall_end_ns = last_data_ns.saturating_add(stall_ns);
                if now_ns >= stall_end_ns {
                    return Err(self.preload_stalled());
                }
                stall_end_ns
            };

            if self.send_requests()? > 0 {
                self.last_io_ns = now_ns;
            }
            let poll_timeout = self.poll_timeout(now_ns, wait_end_ns);
            self.process_responses(self.busy_poll_timeout(poll_timeout, now_ns))?;
            let now_ns = timing::time_ns();
            if !self.ready_ids.is_empty() {
                self.last_io_ns = now_ns;
                last_data_ns = now_ns;
            }
            self.expire_requests(now_ns)?;
            self.check_setup_deadlines(now_ns)?;
        }
    }

    /// The run's error for a preload that received nothing for the setup
    /// timeout, naming a connection still in it
    fn preload_stalled(&self) -> crate::Error {
        let (connection_id, group_id) =
            self.preloading_connection().expect("a connection is still preloading");
        let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
        crate::Error::Connection(format!(
            "Traffic group {group_id}, connection {connection_id} ({}): preload received nothing \
             within the setup timeout ({:?})",
            protocol.name(),
            self.setup_timeout
        ))
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

    /// Send the requests due now; see [`Self::send_requests_before`]
    fn send_requests(&mut self) -> Result<usize> {
        self.send_requests_before(u64::MAX)
    }

    /// Returns the number of requests it picked, setup requests included
    ///
    /// Picks no arrival intended at or after `end_ns`, the end of the
    /// measurement window, even once the clock passed it: the window's
    /// arrivals are those intended before its end, each counted once as
    /// sent or unsent.
    fn send_requests_before(&mut self, end_ns: u64) -> Result<usize> {
        // SEND PHASE: Send requests to ready connections
        // Rate limiting is handled by connection-level policies via pool's
        // ready_heap, or for group dispatch, by each group's arrival process
        //
        // Safety limit: Cap the number of consecutive sends to prevent infinite loops
        // For closed-loop with pipelining, we want to fill all pipelines, but not spin forever
        // With N connections and depth D, we should need at most N*D sends to fill all pipelines
        let max_sends_per_iteration = self.pool.connection_count() * 16; // Allow generous headroom
        let mut sends_this_iteration = 0;
        let mut saw_one_way = false;
        // The round's first connection, until its requests went out
        let mut first_connection = None;

        let last_ns = end_ns.saturating_sub(1);
        while let Some(pick) = self.pool.pick_send_at(timing::time_ns().min(last_ns)) {
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
            let needs_reconnect = state.needs_reconnect();
            let preloading = self.pool.is_preloading();
            {
                let protocol =
                    self.protocols.get(&group_id).expect("Protocol not found for group_id");
                if protocol.is_done() {
                    continue;
                }
                if preloading
                    && !protocol.setup_pending(connection_id)
                    && !protocol.preload_pending(connection_id)
                {
                    // Held until the measured run starts
                    continue;
                }
            }

            let intended_ns = if preloading {
                None
            } else {
                self.pool.intended_send_time(pick)
            };

            let mut connect_start_ts = None;
            if needs_reconnect {
                match self.initiate_reconnect(
                    connection_id,
                    group_id,
                    pick,
                    intended_ns,
                    preloading,
                )? {
                    ReconnectStep::Immediate(ts) => connect_start_ts = Some(ts),
                    ReconnectStep::InProgress => {
                        sends_this_iteration += 1;
                        continue;
                    }
                    ReconnectStep::Closed => continue,
                }
            }

            let (was_setup, is_one_way) =
                self.queue_next_request(connection_id, group_id, intended_ns, connect_start_ts)?;
            saw_one_way |= is_one_way;

            if !was_setup && !preloading {
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
            if saw_one_way && end_ns != u64::MAX && timing::time_ns() >= end_ns {
                break;
            }
            if sends_this_iteration >= max_sends_per_iteration {
                if !saw_one_way {
                    eprintln!(
                        "[WARN] Hit send limit ({}) in one iteration - forcing receive phase",
                        max_sends_per_iteration
                    );
                }
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

    /// Start a reconnect on `connection_id` for `pick`.
    fn initiate_reconnect(
        &mut self,
        connection_id: usize,
        group_id: usize,
        pick: crate::connection::Pick,
        intended_ns: Option<u64>,
        preloading: bool,
    ) -> Result<ReconnectStep> {
        match self.pool.start_reconnect(connection_id, intended_ns) {
            Ok(Some(connect_start_ts)) => Ok(ReconnectStep::Immediate(connect_start_ts)),
            Ok(None) => {
                if !preloading {
                    self.pool.on_request_sent(pick, timing::time_ns());
                }
                Ok(ReconnectStep::InProgress)
            }
            Err(e @ crate::Error::Connection(_)) => {
                self.close_failed(connection_id, group_id, e)?;
                Ok(ReconnectStep::Closed)
            }
            Err(e) => Err(e),
        }
    }

    /// Ask `group_id`'s protocol for its next request on `connection_id` and
    /// queue it on the pool. Returns `(was_setup, is_one_way)`.
    fn queue_next_request(
        &mut self,
        connection_id: usize,
        group_id: usize,
        intended_ns: Option<u64>,
        connect_start_ts: Option<xylem_transport::Timestamp>,
    ) -> Result<(bool, bool)> {
        let is_control = self
            .pool
            .get_state(connection_id)
            .expect("Connection state not found")
            .is_control();
        let preloading = self.pool.is_preloading();

        let request = {
            let protocol =
                self.protocols.get_mut(&group_id).expect("Protocol not found for group_id");
            protocol.next_request(connection_id)
        };
        if is_control && !request.metadata.is_setup {
            return Err(crate::Error::Protocol(format!(
                "Traffic group {group_id}, connection {connection_id}: the protocol sent a \
                 request that is no setup request on a control connection"
            )));
        }
        if preloading && !request.metadata.is_warmup {
            return Err(crate::Error::Protocol(format!(
                "Traffic group {group_id}, connection {connection_id}: the protocol sent a \
                 measurement request during preload"
            )));
        }

        if request.metadata.is_setup {
            // Connection setup (a handshake, AUTH, ...) may carry
            // credentials: it is never dumped, measured or scheduled.
            self.pool.queue_setup(connection_id, &request.data, request.request_id)?;
            Ok((true, false))
        } else {
            // Record request to dump file if enabled
            if let Some(ref dumper) = self.request_dumper {
                let req_id_str = format!("{:?}", request.request_id);
                self.record_request(dumper, group_id, connection_id, &request.data, &req_id_str);
            }

            let is_warmup = request.metadata.is_warmup || self.in_warmup;
            if !request.metadata.expects_response {
                self.pool.queue_one_way(
                    connection_id,
                    &request.data,
                    request.request_id,
                    is_warmup,
                    intended_ns,
                    connect_start_ts,
                )?;
                Ok((false, true))
            } else {
                // Queue the request (track warmup status for response
                // handling); it is sent, and its stats recorded, when the
                // transport takes it. An open-loop request's latency starts
                // at its intended send time. Preload follows no schedule.
                self.pool.queue_request_with_connect(
                    connection_id,
                    &request.data,
                    request.request_id,
                    is_warmup,
                    intended_ns,
                    connect_start_ts,
                )?;
                Ok((false, false))
            }
        }
    }

    /// Write what was queued since the last flush and put it on the wire,
    /// recording the requests the transport took
    fn flush(&mut self) -> Result<()> {
        let flushed = self.pool.flush();
        self.record_sent();
        flushed
    }

    /// Record the stats of the measurement requests the transport took, and
    /// reschedule connections whose one-way writes completed
    fn record_sent(&mut self) {
        self.pool.take_sent(&mut self.sent);
        let mut reschedule = Vec::new();
        for sent in &self.sent {
            if sent.expects_response {
                if sent.measured {
                    self.stats.record_tx_bytes(sent.group_id, sent.connection_id, sent.bytes);
                    self.stats.record_sent(sent.group_id, sent.connection_id, sent.lag);
                }
            } else {
                if sent.measured && !self.in_warmup && self.window_closed_ns.is_none() {
                    self.stats.record_tx_bytes(sent.group_id, sent.connection_id, sent.bytes);
                }
                if self.window_closed_ns.is_none() {
                    reschedule.push((sent.connection_id, sent.group_id));
                }
            }
        }
        for (connection_id, group_id) in reschedule {
            let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
            self.pool.try_schedule_connection(connection_id, protocol);
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
    /// request timeout, the next connect timeout, any draining-for-reconnect
    /// TX timestamp deadline, and the next setup deadline. A closed-loop entry
    /// has no timestamp, so test readiness before interpreting an empty peek.
    fn poll_timeout(&self, now_ns: u64, end_ns: u64) -> Duration {
        let end_ns = self.pool.next_request_deadline().map_or(end_ns, |next| next.min(end_ns));
        let end_ns = self.pool.next_connect_deadline().map_or(end_ns, |next| next.min(end_ns));
        let end_ns = if self.pool.draining_for_reconnect_count() > 0 {
            self.pool.next_deferred_deadline().map_or(end_ns, |next| next.min(end_ns))
        } else {
            end_ns
        };
        let end_ns = self.setup_deadlines.front().map_or(end_ns, |&(next, _)| next.min(end_ns));
        let remaining_ns = if end_ns == u64::MAX {
            100_000_000
        } else {
            end_ns.saturating_sub(now_ns)
        };
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
        // and may not be available until after the NIC has transmitted the data.
        // A failed read fails the run: its requests would lose their kernel split.
        self.pool.poll_tx_timestamps()?;

        // Samples of responses that came before their TX timestamps: those
        // whose stamps just arrived, and those that waited long enough
        let now_ns = timing::time_ns();
        self.pool.expire_deferred_at(now_ns);
        self.record_deferred();
        self.finish_draining_for_reconnect()?;

        // Clean up stale TX queue entries (entries older than 1 minute)
        // This prevents memory leaks when TX timestamps are never received
        self.maintain_tx_queues(now_ns);

        let mut driven_sends = false;
        for &connection_id in &ready_ids {
            let state = match self.pool.get_state(connection_id) {
                Some(s) => s,
                None => continue,
            };

            // In-progress non-blocking reconnect became ready
            if state.is_connecting() {
                let group_id = state.group_id();
                driven_sends |= self.handle_connecting_ready(connection_id, group_id)?;
                continue;
            }

            // Draining deferred TX timestamps before closing: already polled
            // above; never call recv() on a draining socket.
            if state.is_draining_for_reconnect() {
                continue;
            }

            // Queued bytes waited for the connection to become writable
            if state.is_write_blocked() {
                let group_id = state.group_id();
                self.write_queued(connection_id, group_id)?;
            }
            let state = match self.pool.get_state(connection_id) {
                Some(s) if !s.is_closed() => s,
                _ => continue,
            };

            // Skip connections with no pending requests unless the protocol
            // accepts unsolicited server payloads (e.g. receive-only stream)
            // or is still in setup. A datagram connection is always read so
            // late or stray datagrams do not keep it ready.
            let group_id = state.group_id();
            let (accepts_unsolicited, setup_pending) = {
                let protocol =
                    self.protocols.get(&group_id).expect("Protocol not found for group_id");
                (protocol.accepts_unsolicited_responses(), protocol.setup_pending(connection_id))
            };
            if state.pending_count() == 0
                && !setup_pending
                && !accepts_unsolicited
                && !self.pool.is_datagram()
            {
                continue;
            }

            let responses = {
                let protocol =
                    self.protocols.get_mut(&group_id).expect("Protocol not found for group_id");
                let preloading = self.pool.is_preloading();
                let outcome = self.pool.recv_responses_with_options(
                    connection_id,
                    accepts_unsolicited,
                    |data| {
                        let name = protocol.name();
                        let parsed = protocol.parse_response(connection_id, data);
                        reject_preload_error(preloading, parsed, group_id, connection_id, name)
                    },
                );
                let done = accepts_unsolicited && protocol.is_done();
                match outcome {
                    Ok(responses) => responses,
                    Err(crate::Error::Connection(_)) if done => {
                        let _ = self.pool.close(connection_id);
                        continue;
                    }
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
                if response.unsolicited {
                    self.record_unsolicited_response(group_id, connection_id, &response);
                    continue;
                }
                // A deferred response is recorded once its TX timestamp arrives
                if response.is_warmup || response.deferred {
                    continue;
                }
                match response.error {
                    // An error reply's latency stays out of the latency
                    // distribution and of the breakdown alike
                    None => self.stats.record_response(
                        group_id,
                        connection_id,
                        response.bytes_consumed,
                        response.latency,
                        &response.breakdown,
                    ),
                    Some(kind) => {
                        self.stats.record_rx_bytes(
                            group_id,
                            connection_id,
                            response.bytes_consumed,
                        );
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

            // If the connection reached `max_requests_per_connection` and has
            // no pending requests left, drain any deferred TX timestamps or
            // close now so the next scheduled send initiates a fresh TCP connect.
            if self.pool.get_state(connection_id).is_some_and(|s| s.reached_request_limit()) {
                if self.pool.get_state(connection_id).is_some_and(|s| s.has_deferred()) {
                    self.pool.poll_tx_timestamps_for(connection_id)?;
                    self.record_deferred();
                }
                if self.pool.get_state(connection_id).is_some_and(|s| s.has_deferred()) {
                    self.pool.begin_drain_for_reconnect(connection_id)?;
                    continue;
                }
                self.pool.close_for_reconnect(connection_id)?;
                if self.window_closed_ns.is_some() {
                    continue;
                }
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
        self.expire_connects(timing::time_ns())?;

        Ok(())
    }

    /// Record an unsolicited response payload (e.g. receive-only stream)
    fn record_unsolicited_response(
        &mut self,
        group_id: usize,
        connection_id: usize,
        response: &crate::connection::Completion<P::RequestId>,
    ) {
        if self.in_warmup || self.window_closed_ns.is_some() {
            return;
        }
        self.stats.record_rx_bytes(group_id, connection_id, response.bytes_consumed);
        if let Some(kind) = response.error {
            let kind = self.pool.error_kind(connection_id, kind);
            self.stats.record_error_reply(group_id, connection_id, kind, Duration::ZERO);
        }
    }

    /// Complete an in-progress non-blocking reconnect once `poll()` reports it ready.
    /// Returns `true` if a request was queued and needs a flush.
    fn handle_connecting_ready(&mut self, connection_id: usize, group_id: usize) -> Result<bool> {
        let Some((connect_start_ts, intended_ns, abandoned)) =
            (match self.pool.finish_reconnect(connection_id) {
                Ok(outcome) => outcome,
                Err(e @ crate::Error::Connection(_)) => {
                    self.close_failed(connection_id, group_id, e)?;
                    return Ok(false);
                }
                Err(e) => return Err(e),
            })
        else {
            return Ok(false);
        };
        if self.window_closed_ns.is_some() || abandoned {
            self.pool.close_for_reconnect(connection_id)?;
            return Ok(false);
        }
        let _ =
            self.queue_next_request(connection_id, group_id, intended_ns, Some(connect_start_ts))?;
        let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
        self.pool.try_schedule_connection(connection_id, protocol);
        self.write_queued(connection_id, group_id)?;
        Ok(true)
    }

    /// Close and reschedule any `DrainingForReconnect` connections whose last
    /// deferred TX timestamp just resolved or expired.
    fn finish_draining_for_reconnect(&mut self) -> Result<()> {
        if !self.pool.has_ready_to_close_for_reconnect() {
            return Ok(());
        }
        let mut ready = Vec::new();
        self.pool.take_ready_to_close_for_reconnect(&mut ready);
        for connection_id in ready {
            let Some(state) = self.pool.get_state(connection_id) else {
                continue;
            };
            if state.is_closed() || !state.is_draining_for_reconnect() || state.has_deferred() {
                continue;
            }
            let group_id = state.group_id();
            self.pool.close_for_reconnect(connection_id)?;
            if self.window_closed_ns.is_none() {
                let protocol =
                    self.protocols.get(&group_id).expect("Protocol not found for group_id");
                self.pool.try_schedule_connection(connection_id, protocol);
            }
        }
        Ok(())
    }

    /// Close any in-flight non-blocking reconnect that exceeded its connect timeout.
    fn expire_connects(&mut self, now_ns: u64) -> Result<()> {
        let mut expired = Vec::new();
        self.pool.expire_connects_at(now_ns, |connection_id, group_id| {
            expired.push((connection_id, group_id));
        });
        for (connection_id, group_id) in expired {
            self.close_failed(
                connection_id,
                group_id,
                crate::Error::Connection("Connection timeout".to_string()),
            )?;
        }
        Ok(())
    }

    /// Record the samples of the responses that waited for their TX
    /// timestamps, each where a sample recorded at its receive would count
    fn record_deferred(&mut self) {
        self.pool.take_deferred(&mut self.deferred);
        for sample in &self.deferred {
            let received = ReceivedAt {
                ns: sample.received_ns,
                drained: self.window_closed_ns.is_some_and(|closed| sample.received_ns >= closed),
            };
            self.stats.record_response_at(
                sample.group_id,
                sample.connection_id,
                sample.bytes,
                sample.latency,
                &sample.breakdown,
                received,
            );
        }
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
        // Before the measured run, nothing is recorded
        if !self.pool.is_preloading()
            && !self.in_warmup
            && self.pool.get_state(connection_id).is_some_and(|state| !state.is_control())
        {
            self.stats.record_tx_bytes(group_id, connection_id, len);
        }
        Ok(true)
    }

    /// Time out the requests whose request timeout passed by `now_ns`, count
    /// the measurement ones, and reschedule the connections they freed.
    /// Returns whether any timed out; during preload, one timing out fails
    /// the run, since its data may not have arrived.
    fn expire_requests(&mut self, now_ns: u64) -> Result<bool> {
        let mut freed = Vec::new();
        let stats = &mut self.stats;
        self.pool
            .expire_requests_at(now_ns, |connection_id, group_id, expired, measured| {
                if measured > 0 {
                    stats.record_timed_out(group_id, connection_id, measured);
                }
                tracing::debug!(connection_id, group_id, expired, "Requests timed out");
                freed.push((connection_id, group_id, expired));
            });
        if let (true, Some(&(connection_id, group_id, expired))) =
            (self.pool.is_preloading(), freed.first())
        {
            let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
            return Err(crate::Error::Connection(format!(
                "Traffic group {group_id}, connection {connection_id} ({}): {expired} \
                 request(s) timed out during preload",
                protocol.name()
            )));
        }
        let expired = !freed.is_empty();
        for (connection_id, group_id, _) in freed {
            let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
            self.pool.try_schedule_connection(connection_id, protocol);
        }
        Ok(expired)
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
        let phase = if protocol.setup_pending(connection_id) {
            "setup"
        } else if self.pool.is_preloading() {
            "preload"
        } else {
            return None;
        };
        Some(crate::Error::Connection(format!(
            "Traffic group {group_id}, connection {connection_id} ({}): connection closed during \
             {phase}: {cause}",
            protocol.name()
        )))
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

    /// Wait for pending responses after the measurement window, and for the
    /// TX timestamps of responses that came before them, until none is
    /// pending or waiting or `drain_timeout` passes. Sends nothing.
    /// Measurement requests still pending then are counted as timed out,
    /// without a latency.
    fn drain_responses(&mut self) -> Result<()> {
        // Never wait on an unsent in-flight reconnect during post-window drain;
        // pending responses and DrainingForReconnect TX timestamps still drain.
        self.pool.abort_connecting_for_drain()?;
        let timeout_ns = self.config.drain_timeout.as_nanos().min(u64::MAX as u128) as u64;
        let deadline_ns = timing::time_ns().saturating_add(timeout_ns);
        // Only ready connections and timeouts end pending requests, so a
        // poll that reports neither leaves them pending: a busy-polling
        // drain checks every connection only when one may have finished
        let mut pending = self.has_pending();
        while pending || self.pool.has_deferred() {
            let now_ns = timing::time_ns();
            if now_ns >= deadline_ns {
                break;
            }
            // Short waits keep TX timestamp polling and the deadline responsive.
            let wait_ns = (deadline_ns - now_ns).min(10_000_000);
            let wait_ns = [self.pool.next_request_deadline(), self.pool.next_deferred_deadline()]
                .into_iter()
                .flatten()
                .fold(wait_ns, |wait_ns, next| wait_ns.min(next.saturating_sub(now_ns)));
            self.process_responses(self.busy_poll_timeout(Duration::from_nanos(wait_ns), now_ns))?;
            let ready = !self.ready_ids.is_empty();
            if ready {
                self.last_io_ns = now_ns;
            }
            if self.expire_requests(timing::time_ns())? || ready {
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

    /// Payload verification reports for traffic groups on this worker that
    /// have opt-in verification enabled
    pub fn verification_reports(&self) -> Vec<(usize, xylem_common::VerificationReport)> {
        let mut reports: Vec<_> = self
            .protocols
            .iter()
            .filter_map(|(&group_id, proto)| {
                proto.verification_report().map(|report| (group_id, report))
            })
            .collect();
        reports.sort_by_key(|(group_id, _)| *group_id);
        reports
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

/// `parsed`, or the run's error if it is an error reply during preload:
/// preload must fill the server, so an error reply to it fails the run,
/// whatever `on_error` says
fn reject_preload_error<'a, Id>(
    preloading: bool,
    parsed: anyhow::Result<Parsed<'a, Id>>,
    group_id: usize,
    connection_id: usize,
    protocol: &str,
) -> Result<Parsed<'a, Id>> {
    match parsed? {
        Parsed::Complete { error: Some(error), .. } if preloading => {
            Err(crate::Error::Protocol(format!(
                "Traffic group {group_id}, connection {connection_id} ({protocol}): error reply \
                 {} during preload: {}",
                error.kind.escape_debug(),
                error.message.escape_debug()
            )))
        }
        parsed => Ok(parsed),
    }
}

/// Wait until `time_ns` (a `timing::time_ns` value): sleep until shortly
/// before it, then spin to it
fn wait_until(time_ns: u64) {
    let now_ns = timing::time_ns();
    if time_ns.saturating_sub(now_ns) > 2_000_000 {
        std::thread::sleep(Duration::from_nanos(time_ns - now_ns - 1_000_000));
    }
    timing::busy_wait_until(time_ns);
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

    use crate::connection::test_support::{FakeFactory, FakeGroup, FakeIo, ReconnectMode};
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
        /// Requests with a lower ID are preload requests, which every
        /// connection draws from
        preload: u64,
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
            if id < self.warmup || id < self.preload {
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

        fn preload_pending(&self, _: usize) -> bool {
            self.next_id < self.preload
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

    /// A worker whose protocol preloads with one request
    fn preload_worker(factory: &FakeFactory) -> Worker<FakeGroup, FakeProtocol> {
        let mut worker = fake_worker(factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.protocols.get_mut(&0).unwrap().preload = 1;
        worker.config.duration = Duration::from_millis(10);
        worker
    }

    #[test]
    fn an_error_reply_during_preload_fails_the_run_whatever_on_error_says() {
        let factory = FakeFactory::default();
        let mut worker = preload_worker(&factory);
        worker.protocols.get_mut(&0).unwrap().errors.insert(0, "NOT_STORED");
        factory.0.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        let error = worker.run().unwrap_err();
        assert_eq!(
            error.to_string(),
            "Protocol error: Traffic group 0, connection 0 (Fake): error reply NOT_STORED during \
             preload: test error"
        );
        assert_eq!(sent_requests(&factory), 1, "nothing measured went out");
    }

    #[test]
    fn a_connection_closed_during_preload_fails_the_run_naming_it() {
        let factory = FakeFactory::default();
        let mut worker = preload_worker(&factory);
        let closed = xylem_transport::Error::Connection("Connection closed by peer".into());
        factory.0.lock().unwrap().responses.push_back(Err(closed));
        let error = worker.run().unwrap_err();
        assert_eq!(
            error.to_string(),
            "Connection error: Traffic group 0, connection 0 (Fake): connection closed during \
             preload: Connection closed by peer"
        );
        assert!(factory.0.lock().unwrap().closed);
    }

    #[test]
    fn a_preload_that_receives_nothing_for_the_setup_timeout_fails_the_run() {
        let factory = FakeFactory::default();
        let mut worker = preload_worker(&factory);
        worker.config.duration = Duration::from_secs(10);
        worker.set_setup_timeout(Duration::from_millis(20));
        // The preload reply never arrives
        let started = std::time::Instant::now();
        let error = worker.run().unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
        assert_eq!(
            error.to_string(),
            "Connection error: Traffic group 0, connection 0 (Fake): preload received nothing \
             within the setup timeout (20ms)"
        );
        assert_eq!(sent_requests(&factory), 1);
    }

    #[test]
    fn a_preload_request_timing_out_fails_the_run() {
        let factory = FakeFactory::default();
        let mut worker = preload_worker(&factory);
        worker.set_request_timeout(0, Some(Duration::from_millis(5)));
        let error = worker.run().unwrap_err();
        assert_eq!(
            error.to_string(),
            "Connection error: Traffic group 0, connection 0 (Fake): 1 request(s) timed out \
             during preload"
        );
    }

    #[test]
    fn preload_requests_are_neither_measured_nor_scheduled() {
        let factory = FakeFactory::default();
        // One request every 10 ms: preload goes out at once, off the schedule
        let scheduler = Box::new(UniformPolicyScheduler::fixed_rate(100.0));
        let mut worker = fake_worker(&factory, 4, scheduler);
        worker.protocols.get_mut(&0).unwrap().preload = 3;
        worker.config.duration = Duration::from_millis(25);
        let replies = (0u64..3).map(|id| Ok(id.to_le_bytes().to_vec()));
        factory.0.lock().unwrap().responses.extend(replies);
        let mut samples = [Some(100), Some(200)].into_iter();
        let preload = Some(PreloadBarrier::Alone(None));
        worker
            .run_at_with_clock(timing::time_ns(), preload, || {
                if samples.len() == 2 {
                    // The window starts once preload is done
                    assert_eq!(sent_requests(&factory), 3);
                    assert!(factory.0.lock().unwrap().responses.is_empty());
                }
                samples.next().expect("the wall clock is sampled exactly twice")
            })
            .unwrap();
        let counts = worker.stats.request_counts_global();
        // Requests 3, 4 and 5 at 0, 10 and 20 ms into the measured run
        assert_eq!(counts.sent, 3, "{counts:?}");
        assert_eq!(sent_requests(&factory), 6);
    }

    /// When a request with an ID arrived at a server, and when its reply left
    type ServerLog = Arc<std::sync::Mutex<Vec<(u64, std::time::Instant, std::time::Instant)>>>;

    /// A server that records each request in a [`ServerLog`], and replies to
    /// the IDs below `slow` only after `delay`
    fn recording_echo_server(
        listener: TcpListener,
        connections: usize,
        slow: u64,
        delay: Duration,
    ) -> ServerLog {
        let log = ServerLog::default();
        for _ in 0..connections {
            let socket = listener.accept().unwrap().0;
            let log = Arc::clone(&log);
            thread::spawn(move || echo_recording(socket, &log, slow, delay));
        }
        log
    }

    fn echo_recording(
        mut socket: std::net::TcpStream,
        log: &ServerLog,
        slow: u64,
        delay: Duration,
    ) {
        let mut id = [0u8; 8];
        while socket.read_exact(&mut id).is_ok() {
            let received = std::time::Instant::now();
            if u64::from_le_bytes(id) < slow {
                thread::sleep(delay);
            }
            if socket.write_all(&id).is_err() {
                break;
            }
            let entry = (u64::from_le_bytes(id), received, std::time::Instant::now());
            log.lock().unwrap().push(entry);
        }
    }

    #[test]
    fn measurement_waits_for_every_preload_reply_of_every_thread() {
        // Two threads of two connections, whose protocols each preload two
        // requests, which the server answers slowly. Without the barrier,
        // a connection that drew no preload request would measure at once.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let target = listener.local_addr().unwrap();
        let runtime = crate::threading::ThreadingRuntime::new(2);
        let server =
            thread::spawn(move || recording_echo_server(listener, 4, 2, Duration::from_millis(30)));
        let windows = runtime
            .run_workers_with_start(move |_, start| {
                let mut stats = TupleStatsCollector::default();
                stats.register_group_policy(0, SamplingPolicy::Unlimited);
                let protocol = FakeProtocol { preload: 2, ..Default::default() };
                let config = WorkerConfig {
                    target,
                    duration: Duration::from_millis(20),
                    conn_count: 2,
                    max_pending_per_conn: 4,
                    drain_timeout: DEFAULT_DRAIN_TIMEOUT,
                };
                let factory = TcpTransportFactory::default();
                let mut worker = Worker::with_closed_loop(&factory, protocol, stats, config)?;
                worker.run_with_start(start)?;
                Ok(worker.stats.actual_load_loop_unix_us())
            })
            .unwrap();
        let log = server.join().unwrap();
        let log = log.lock().unwrap();
        let last_preload_reply = log.iter().filter(|e| e.0 < 2).map(|e| e.2).max().unwrap();
        let first_measured = log.iter().filter(|e| e.0 >= 2).map(|e| e.1).min().unwrap();
        assert_eq!(log.iter().filter(|e| e.0 < 2).count(), 4, "each thread preloads");
        assert!(first_measured > last_preload_reply, "measured before every preload reply");
        // Both threads measured, from one start after preload
        let unix_us = |instant: std::time::Instant| {
            let ago = std::time::Instant::now() - instant;
            unix_time_us().unwrap() - ago.as_micros() as u64
        };
        for window in windows {
            let (start_us, _) = window.expect("the thread measured");
            assert!(start_us >= unix_us(last_preload_reply), "the window starts after preload");
        }
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
            .run_at_with_clock(start, None, || {
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
            .run_at_with_clock(timing::time_ns(), None, || {
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
        let result = worker.run_at_with_clock(timing::time_ns(), None, || {
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
        assert_eq!(breakdown.user_round_trip, None);
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
        // One request: its user-space round trip is the sum of the parts
        let rtt = breakdown.user_round_trip.unwrap();
        assert_eq!((rtt.sample_count, rtt.max), (1, Some(host + Duration::from_micros(5))));
        assert_eq!(worker.stats.latency_breakdown(0), breakdown);
        assert_eq!(global(&worker).max_latency, Some(Duration::from_micros(5)));
    }

    #[test]
    fn requests_without_a_kernel_split_count_by_the_stamp_they_lacked() {
        use crate::stats::SplitMissCounts;
        use xylem_transport::{KernelClock, KernelTimestamp, Timestamp, TxTimestampInfo};
        let sw = |nanos| KernelTimestamp { nanos, clock: KernelClock::Realtime };
        let hw = |nanos| KernelTimestamp { nanos, clock: KernelClock::NicHardware };
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        // Request i's TX stamp (its send ends at i + 1) and RX stamp: split,
        // no TX stamp, no RX stamp, stamps of different clocks
        let cases = [
            (Some(sw(1_000)), Some(sw(2_000))),
            (None, Some(sw(2_000))),
            (Some(sw(1_000)), None),
            (Some(sw(1_000)), Some(hw(2_000))),
        ];
        for (id, (tx, rx)) in cases.into_iter().enumerate() {
            // The fake stamps sends with its receive timestamp: none here
            factory.0.lock().unwrap().timestamp = Timestamp::now();
            worker.send_requests().unwrap();
            let mut io = factory.0.lock().unwrap();
            if let Some(timestamp) = tx {
                let tx_end = id as u32 + 1;
                io.tx_timestamps.push(TxTimestampInfo {
                    conn_id: 0,
                    tx_end,
                    cumulative: true,
                    timestamp,
                });
            }
            let now = Timestamp::now();
            io.timestamp = rx.map_or(now, |rx| now.with_kernel(rx));
            io.responses.push_back(Ok((id as u64).to_le_bytes().to_vec()));
            drop(io);
            worker.process_responses(Duration::ZERO).unwrap();
            // The request without a TX stamp waits for it; give up at once,
            // before the next stamp, which covers every earlier send, comes
            worker.pool.expire_deferred_at(u64::MAX);
            worker.record_deferred();
        }
        let breakdown = worker.stats.latency_breakdown(0);
        assert_eq!((breakdown.kernel_split_count, breakdown.service.sample_count), (1, 4));
        assert_eq!(
            breakdown.kernel_split_missing,
            SplitMissCounts { tx: 1, rx: 1, clock_mismatch: 1 }
        );
        assert_eq!(worker.stats.latency_breakdown_global(), breakdown);
    }

    fn sw_stamp(nanos: u64) -> xylem_transport::KernelTimestamp {
        xylem_transport::KernelTimestamp {
            nanos,
            clock: xylem_transport::KernelClock::Realtime,
        }
    }

    /// Queue on `factory` the response to request `id`, with a kernel RX
    /// stamp at `rx_ns`; the fake stamps later sends with the cycles alone
    fn respond_with_rx_stamp(factory: &FakeFactory, id: u64, rx_ns: u64) {
        let mut io = factory.0.lock().unwrap();
        io.timestamp = xylem_transport::Timestamp::now().with_kernel(sw_stamp(rx_ns));
        io.responses.push_back(Ok(id.to_le_bytes().to_vec()));
    }

    /// Process the queued response on `worker`, then let the fake stamp
    /// sends with the cycles alone again
    fn process_rx_stamped(worker: &mut Worker<FakeGroup, FakeProtocol>, factory: &FakeFactory) {
        worker.process_responses(Duration::ZERO).unwrap();
        factory.0.lock().unwrap().timestamp = xylem_transport::Timestamp::now();
    }

    /// Report the TX stamp at `tx_ns` of the fake's `n`th send (from 1)
    fn report_tx_stamp(factory: &FakeFactory, n: u32, tx_ns: u64) {
        factory.0.lock().unwrap().tx_timestamps.push(xylem_transport::TxTimestampInfo {
            conn_id: 0,
            tx_end: n,
            cumulative: true,
            timestamp: sw_stamp(tx_ns),
        });
    }

    #[test]
    fn a_tx_stamp_arriving_after_the_response_still_splits_its_sample() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        respond_with_rx_stamp(&factory, 0, 6_000);
        process_rx_stamped(&mut worker, &factory);
        // The request is complete, but its sample waits for the TX stamp
        assert_eq!(worker.pool.get_state(0).unwrap().pending_count(), 0);
        assert!(worker.pool.has_deferred());
        assert_eq!(worker.stats.request_counts_global().completed, 0);
        assert_eq!(worker.stats.latency_breakdown_global().service.sample_count, 0);

        report_tx_stamp(&factory, 1, 1_000);
        worker.process_responses(Duration::ZERO).unwrap();
        assert!(!worker.pool.has_deferred());
        let breakdown = worker.stats.latency_breakdown_global();
        assert_eq!((breakdown.kernel_split_count, breakdown.service.sample_count), (1, 1));
        assert_eq!(breakdown.kernel_split_missing, crate::stats::SplitMissCounts::default());
        assert_eq!(breakdown.network_server.unwrap().max, Some(Duration::from_micros(5)));
        let stats = global(&worker);
        assert_eq!((stats.total_requests, stats.max_latency), (1, Some(Duration::from_micros(5))));
        let rx_bytes: u64 = worker.stats.entries().map(|(_, entry)| entry.rx_bytes).sum();
        assert_eq!(rx_bytes, 8);
        assert_eq!(worker.stats.request_counts_global().completed, 1);
    }

    #[test]
    fn a_tx_stamp_that_never_arrives_leaves_the_sample_in_cycles_after_the_timeout() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.set_tx_timestamp_timeout(Duration::from_millis(50));
        worker.send_requests().unwrap();
        let sent = timing::time_ns();
        respond_with_rx_stamp(&factory, 0, 6_000);
        process_rx_stamped(&mut worker, &factory);
        let deadline = worker.pool.next_deferred_deadline().unwrap();
        assert!(deadline >= sent + 50_000_000 && deadline <= timing::time_ns() + 50_000_000);
        // Still waiting before its deadline
        worker.process_responses(Duration::ZERO).unwrap();
        assert!(timing::time_ns() < deadline, "the test outran its timeout");
        assert_eq!(worker.stats.latency_breakdown_global().service.sample_count, 0);

        thread::sleep(Duration::from_nanos(deadline.saturating_sub(timing::time_ns())));
        worker.process_responses(Duration::ZERO).unwrap();
        assert!(!worker.pool.has_deferred());
        let breakdown = worker.stats.latency_breakdown_global();
        assert_eq!((breakdown.kernel_split_count, breakdown.service.sample_count), (0, 1));
        let missing = crate::stats::SplitMissCounts { tx: 1, rx: 0, clock_mismatch: 0 };
        assert_eq!(breakdown.kernel_split_missing, missing);
        assert_eq!(breakdown.clock, Some(crate::stats::BreakdownClock::UserSpace));
        // Its queue entry went with it: a late stamp gives nothing
        report_tx_stamp(&factory, 1, 1_000);
        worker.process_responses(Duration::ZERO).unwrap();
        assert_eq!(worker.stats.latency_breakdown_global().service.sample_count, 1);
        assert_eq!(global(&worker).total_requests, 1);
    }

    #[test]
    fn the_end_of_the_run_records_every_sample_still_waiting_for_its_tx_stamp() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        // Longer than the drain: the end of the run gives up on the rest
        worker.set_tx_timestamp_timeout(Duration::from_secs(10));
        for id in 0..2 {
            worker.send_requests().unwrap();
            respond_with_rx_stamp(&factory, id, 9_000);
            process_rx_stamped(&mut worker, &factory);
        }
        assert_eq!(sent_requests(&factory), 2);
        assert_eq!(worker.pool.get_state(0).unwrap().pending_count(), 0);
        // Request 0's stamp arrives, request 1's never does
        report_tx_stamp(&factory, 1, 1_000);
        let started = timing::time_ns();
        worker.config.duration = Duration::ZERO;
        worker.run().unwrap();
        assert!(timing::time_ns() - started < 1_000_000_000, "the drain bounds the wait");
        assert!(!worker.pool.has_deferred());
        let breakdown = worker.stats.latency_breakdown_global();
        assert_eq!((breakdown.kernel_split_count, breakdown.service.sample_count), (1, 2));
        assert_eq!(breakdown.kernel_split_missing.tx, 1);
        assert_eq!(breakdown.network_server.unwrap().max, Some(Duration::from_micros(8)));
        // Both arrived in the window
        assert_eq!(global(&worker).total_requests, 2);
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.completed, counts.timed_out), (2, 2, 0));
    }

    #[test]
    fn a_waiting_sample_counts_in_the_window_its_response_arrived_in() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.set_tx_timestamp_timeout(Duration::from_millis(5));
        // Request 0's response arrives in the window, its TX stamp after the
        // window; request 1's response arrives during the drain, its TX
        // stamp never
        worker.send_requests().unwrap();
        respond_with_rx_stamp(&factory, 0, 9_000);
        process_rx_stamped(&mut worker, &factory);
        worker.send_requests().unwrap();
        assert_eq!(sent_requests(&factory), 2);
        report_tx_stamp(&factory, 1, 1_000);
        respond_with_rx_stamp(&factory, 1, 9_000);
        worker.config.duration = Duration::ZERO;
        worker.run().unwrap();
        let breakdown = worker.stats.latency_breakdown_global();
        assert_eq!((breakdown.kernel_split_count, breakdown.service.sample_count), (1, 2));
        assert_eq!(breakdown.kernel_split_missing.tx, 1);
        // Only the window's response counts toward throughput, in the
        // window's bucket, with its bytes
        let stats = global(&worker);
        assert_eq!((stats.total_requests, stats.latency_sample_count), (1, 2));
        let points = worker.stats.time_series_global();
        assert_eq!(points.iter().map(|p| p.request_count).sum::<u64>(), 1);
        let entries: Vec<_> = worker
            .stats
            .entries()
            .map(|(key, entry)| (key.time_bucket, entry.rx_bytes))
            .collect();
        assert_eq!(entries, [(0, 8)]);
        assert_eq!(worker.stats.request_counts_global().completed, 2);
    }

    #[test]
    fn a_response_waiting_for_its_tx_stamp_neither_delays_the_next_send_nor_the_feedback() {
        let factory = FakeFactory::default();
        let feedback = Arc::<std::sync::Mutex<Vec<Duration>>>::default();
        let mut worker =
            group_worker(&factory, 1, 1, Box::new(FeedbackPolicy(Arc::clone(&feedback))));
        worker.send_requests().unwrap();
        assert_eq!(sent_requests(&factory), 1);
        respond_with_rx_stamp(&factory, 0, 6_000);
        process_rx_stamped(&mut worker, &factory);
        assert!(worker.pool.has_deferred());
        // The policy has the latency measured so far, and the full pipeline
        // sends its next request at once
        assert_eq!(feedback.lock().unwrap().len(), 1);
        worker.send_requests().unwrap();
        assert_eq!(sent_requests(&factory), 2);
        assert!(worker.pool.has_deferred());
        assert_eq!(worker.stats.latency_breakdown_global().service.sample_count, 0);
    }

    #[test]
    fn an_error_reading_tx_timestamps_fails_the_run() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        factory.0.lock().unwrap().tx_error = Some(|| {
            xylem_transport::Error::Io(std::io::Error::other(
                "Could not read the TX timestamps of TCP connection 0: recvmsg(MSG_ERRQUEUE): \
                 boom",
            ))
        });
        let error = worker.process_responses(Duration::ZERO).unwrap_err().to_string();
        assert_eq!(
            error,
            "I/O error: Could not read the TX timestamps of TCP connection 0: \
             recvmsg(MSG_ERRQUEUE): boom"
        );
        worker.config.duration = Duration::from_secs(5);
        assert!(worker.run().unwrap_err().to_string().contains("recvmsg(MSG_ERRQUEUE)"));
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

    /// Send at 1000 arrivals/s with the clock 2 ms past the end of a 3 ms
    /// window, as when the run loop's end check runs just before the end and
    /// its send phase after it: the arrivals intended at and after the end,
    /// the first exactly at it, are not the window's, so they neither go out
    /// nor count as unsent
    fn assert_arrivals_from_the_end_on_are_not_sent(mut worker: Worker<FakeGroup, FakeProtocol>) {
        let start_ns = timing::time_ns() + 1_000_000;
        let end_ns = start_ns + 3_000_000;
        worker.anchor_schedules(start_ns);
        timing::busy_wait_until(end_ns + 2_000_000);
        assert_eq!(worker.send_requests_before(end_ns).unwrap(), 3);
        worker.record_unsent(end_ns);
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.unsent), (3, 0));
    }

    #[test]
    fn arrivals_from_the_end_of_the_window_on_are_not_sent() {
        let factory = FakeFactory::default();
        let worker =
            fake_worker(&factory, 1000, Box::new(UniformPolicyScheduler::fixed_rate(1000.0)));
        assert_arrivals_from_the_end_on_are_not_sent(worker);
    }

    #[test]
    fn group_arrivals_from_the_end_of_the_window_on_are_not_sent() {
        use crate::scheduler::FixedRatePolicy;
        let factory = FakeFactory::default();
        let worker = group_worker(&factory, 1, 1000, Box::new(FixedRatePolicy::new(1000.0)));
        assert_arrivals_from_the_end_on_are_not_sent(worker);
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
        worker.expire_requests(timing::time_ns()).unwrap();
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
        worker.expire_requests(timing::time_ns()).unwrap();
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

    #[test]
    fn reconnect_immediate_max_requests_1_and_2() {
        for limit in [1u64, 2u64] {
            let factory = FakeFactory::default();
            factory.0.lock().unwrap().live_timestamps = true;
            factory.0.lock().unwrap().reconnect_mode = ReconnectMode::Immediate;

            let mut stats = TupleStatsCollector::default();
            stats.register_group_policy(0, SamplingPolicy::Unlimited);
            let config = WorkerConfig {
                target: "127.0.0.1:1".parse().unwrap(),
                duration: Duration::from_secs(1),
                conn_count: 1,
                max_pending_per_conn: 1,
                drain_timeout: FAKE_DRAIN_TIMEOUT,
            };
            let mut worker = Worker::new(
                &factory,
                FakeProtocol::default(),
                stats,
                config,
                Box::new(UniformPolicyScheduler::closed_loop()),
            )
            .unwrap();
            worker.set_max_requests_per_connection(0, Some(limit));
            worker.anchor_schedules(timing::time_ns());

            let io = factory.0.clone();
            // Send 4 requests sequentially
            for req_id in 0u64..4 {
                assert_eq!(worker.send_requests().unwrap(), 1, "limit={limit} req={req_id}");
                io.lock().unwrap().responses.push_back(Ok(req_id.to_le_bytes().to_vec()));
                worker.process_responses(Duration::ZERO).unwrap();
            }

            let expected_reconnects = match limit {
                1 => 3, // before req 1, 2, 3
                2 => 1, // before req 2
                _ => unreachable!(),
            };
            assert_eq!(
                io.lock().unwrap().reconnect_started.len(),
                expected_reconnects,
                "limit={limit}"
            );
            assert_eq!(worker.stats.request_counts_global().completed, 4);
            let bd = worker.stats.latency_breakdown_global();
            assert_eq!(bd.service.sample_count, 4);
            assert_eq!(
                bd.connect.unwrap().sample_count,
                expected_reconnects as u64,
                "only the first request after each reconnect records connect latency"
            );
            assert_eq!(bd.connect_request.unwrap().sample_count, expected_reconnects as u64);
        }
    }

    #[test]
    fn reconnect_async_einprogress_separates_sw_connect_from_hw_request_only() {
        use xylem_transport::{KernelClock, KernelTimestamp, Timestamp, TxTimestampInfo};

        let factory = FakeFactory::default();
        {
            let mut io = factory.0.lock().unwrap();
            io.live_timestamps = true;
            io.reconnect_mode = ReconnectMode::Async;
        }
        let mut stats = TupleStatsCollector::default();
        stats.register_group_policy(0, SamplingPolicy::Unlimited);
        let config = WorkerConfig {
            target: "127.0.0.1:1".parse().unwrap(),
            duration: Duration::from_secs(1),
            conn_count: 1,
            max_pending_per_conn: 1,
            drain_timeout: FAKE_DRAIN_TIMEOUT,
        };
        let mut worker = Worker::new(
            &factory,
            FakeProtocol::default(),
            stats,
            config,
            Box::new(UniformPolicyScheduler::closed_loop()),
        )
        .unwrap();
        worker.set_max_requests_per_connection(0, Some(1));
        worker.anchor_schedules(timing::time_ns());

        let io = factory.0.clone();

        // Request 0 on initial pre-opened connection
        worker.send_requests().unwrap();
        io.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        assert!(worker.pool.get_state(0).unwrap().needs_reconnect());

        // Request 1 starts async reconnect (EINPROGRESS)
        worker.send_requests().unwrap();
        assert!(worker.pool.get_state(0).unwrap().is_connecting());
        assert_eq!(io.lock().unwrap().sent.len(), 1, "must not send before handshake finishes");

        // Simulate a 3 ms TCP handshake
        thread::sleep(Duration::from_millis(3));

        // Handshake completes on next poll -> request 1 is queued and flushed immediately
        io.lock().unwrap().connecting_ready = true;
        worker.process_responses(Duration::ZERO).unwrap();
        assert_eq!(io.lock().unwrap().reconnect_finished, vec![0]);
        assert_eq!(io.lock().unwrap().sent.len(), 2, "request 1 sent immediately on connect");

        // Deliver HW TX timestamp (500 us before HW RX timestamp) and response
        let hw_tx = KernelTimestamp {
            nanos: 10_000_000,
            clock: KernelClock::NicHardware,
        };
        let hw_rx = KernelTimestamp {
            nanos: 10_500_000,
            clock: KernelClock::NicHardware,
        };
        {
            let mut io = io.lock().unwrap();
            io.live_timestamps = false;
            io.connecting_ready = false;
            io.timestamp = Timestamp::now().with_kernel(hw_rx);
            io.tx_timestamps.push(TxTimestampInfo {
                conn_id: 0,
                tx_end: 2,
                cumulative: true,
                timestamp: hw_tx,
            });
            io.responses.push_back(Ok(1u64.to_le_bytes().to_vec()));
        }
        worker.process_responses(Duration::ZERO).unwrap();

        let bd = worker.stats.latency_breakdown_global();
        assert_eq!(bd.kernel_split_count, 1);
        assert_eq!(bd.kernel_split_missing.tx, 1); // req 0 had no HW stamp; req 1 did!
                                                   // Pure HW request-only network_server is exactly 500 us (NOT polluted by 3 ms connect!)
        let net = bd.network_server.unwrap();
        assert_eq!(net.sample_count, 1);
        assert_eq!(net.mean, Some(Duration::from_micros(500)));

        // SW connect latency captured the >= 3 ms handshake, and connect_request >= connect
        let connect = bd.connect.unwrap();
        let connect_request = bd.connect_request.unwrap();
        assert_eq!(connect.sample_count, 1);
        assert_eq!(connect_request.sample_count, 1);
        assert!(connect.mean.unwrap() >= Duration::from_millis(2), "connect={:?}", connect.mean);
        assert!(
            connect_request.mean.unwrap() >= connect.mean.unwrap(),
            "connect_request={:?} < connect={:?}",
            connect_request.mean,
            connect.mean
        );
    }

    #[test]
    fn reconnect_drains_deferred_tx_timestamp_before_closing_socket() {
        use xylem_transport::{KernelClock, KernelTimestamp, Timestamp, TxTimestampInfo};

        let factory = FakeFactory::default();
        factory.0.lock().unwrap().reconnect_mode = ReconnectMode::Immediate;

        let mut stats = TupleStatsCollector::default();
        stats.register_group_policy(0, SamplingPolicy::Unlimited);
        let config = WorkerConfig {
            target: "127.0.0.1:1".parse().unwrap(),
            duration: Duration::from_secs(1),
            conn_count: 1,
            max_pending_per_conn: 1,
            drain_timeout: FAKE_DRAIN_TIMEOUT,
        };
        let mut worker = Worker::new(
            &factory,
            FakeProtocol::default(),
            stats,
            config,
            Box::new(UniformPolicyScheduler::closed_loop()),
        )
        .unwrap();
        worker.set_max_requests_per_connection(0, Some(1));
        worker.anchor_schedules(timing::time_ns());

        let io = factory.0.clone();

        // Request 0 -> finish so req 1 goes through a reconnect
        worker.send_requests().unwrap();
        io.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();

        // Request 1 reconnects immediately and sends
        let send_ts = Timestamp::now();
        io.lock().unwrap().timestamp = send_ts;
        worker.send_requests().unwrap();
        assert_eq!(io.lock().unwrap().sent.len(), 2);

        // Response 1 arrives with HW RX timestamp BEFORE TX timestamp is in MSG_ERRQUEUE
        let hw_rx = KernelTimestamp {
            nanos: 2_000_000,
            clock: KernelClock::NicHardware,
        };
        {
            let mut io = io.lock().unwrap();
            io.timestamp = Timestamp::now().with_kernel(hw_rx);
            io.responses.push_back(Ok(1u64.to_le_bytes().to_vec()));
        }
        let closes_before = io.lock().unwrap().closed_conns.len();
        worker.process_responses(Duration::ZERO).unwrap();

        // Must have attempted O(1) poll_tx_timestamps_for, then paused I/O and entered DrainingForReconnect
        assert_eq!(io.lock().unwrap().tx_polls_for, vec![0]);
        assert_eq!(io.lock().unwrap().paused_io, vec![0]);
        assert_eq!(
            io.lock().unwrap().closed_conns.len(),
            closes_before,
            "socket must stay open until deferred TX stamp arrives"
        );
        assert!(worker.pool.get_state(0).unwrap().is_draining_for_reconnect());
        assert_eq!(worker.pool.draining_for_reconnect_count(), 1);
        assert!(!worker.pool.get_state(0).unwrap().can_send());

        // Now deliver the delayed HW TX timestamp (300 us before hw_rx)
        let hw_tx = KernelTimestamp {
            nanos: 1_700_000,
            clock: KernelClock::NicHardware,
        };
        io.lock().unwrap().tx_timestamps.push(TxTimestampInfo {
            conn_id: 0,
            tx_end: 2,
            cumulative: true,
            timestamp: hw_tx,
        });
        worker.process_responses(Duration::ZERO).unwrap();

        // Draining completed: socket closed for reconnect, rescheduled, 0 lost TX stamps!
        assert_eq!(io.lock().unwrap().closed_conns.len(), closes_before + 1);
        assert_eq!(worker.pool.draining_for_reconnect_count(), 0);
        assert!(worker.pool.get_state(0).unwrap().needs_reconnect());
        assert!(worker.pool.get_state(0).unwrap().can_send());

        let bd = worker.stats.latency_breakdown_global();
        assert_eq!(bd.kernel_split_count, 1);
        assert_eq!(bd.network_server.unwrap().mean, Some(Duration::from_micros(300)));
        assert_eq!(bd.connect.unwrap().sample_count, 1);
        assert_eq!(bd.connect_request.unwrap().sample_count, 1);
    }

    #[test]
    fn reconnect_failures_and_timeout_close_failed_connection_cleanly() {
        let conn_err: fn() -> xylem_transport::Error =
            || xylem_transport::Error::Connection("connection refused".into());

        // 1. FailStart and FailFinish close the failed connection while sibling keeps going
        for mode in [ReconnectMode::FailStart(conn_err), ReconnectMode::FailFinish(conn_err)] {
            let factory = FakeFactory::default();
            factory.0.lock().unwrap().reconnect_mode = ReconnectMode::Immediate;
            let mut stats = TupleStatsCollector::default();
            stats.register_group_policy(0, SamplingPolicy::Unlimited);
            let config = WorkerConfig {
                target: "127.0.0.1:1".parse().unwrap(),
                duration: Duration::from_secs(1),
                conn_count: 2,
                max_pending_per_conn: 1,
                drain_timeout: FAKE_DRAIN_TIMEOUT,
            };
            let mut worker = Worker::new(
                &factory,
                FakeProtocol::default(),
                stats,
                config,
                Box::new(UniformPolicyScheduler::closed_loop()),
            )
            .unwrap();
            worker.set_max_requests_per_connection(0, Some(1));
            worker.anchor_schedules(timing::time_ns());

            let io = factory.0.clone();
            // Initial sends on conn 0 and conn 1
            worker.send_requests().unwrap();
            // Reply on conn 0 only so conn 0 enters NeedsReconnect while conn 1 stays healthy
            {
                let mut io = io.lock().unwrap();
                io.response_connection_id = 0;
                io.responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
            }
            worker.process_responses(Duration::ZERO).unwrap();
            assert!(worker.pool.get_state(0).unwrap().needs_reconnect());

            // Now inject reconnect failure mode on conn 0
            io.lock().unwrap().reconnect_mode = mode;
            worker.send_requests().unwrap();
            if matches!(mode, ReconnectMode::FailFinish(_)) {
                assert!(worker.pool.get_state(0).unwrap().is_connecting());
                io.lock().unwrap().connecting_ready = true;
                worker.process_responses(Duration::ZERO).unwrap();
            }

            assert!(worker.pool.get_state(0).unwrap().is_closed(), "{mode:?}");
            assert!(!worker.pool.get_state(1).unwrap().is_closed(), "{mode:?}");
            assert_eq!(worker.pool.connection_ids(), &[1]);
        }

        // 2. Hung SYN times out and counts open-loop arrival as unsent
        let factory = FakeFactory::default();
        factory.0.lock().unwrap().reconnect_mode = ReconnectMode::HangAsync;
        let mut stats = TupleStatsCollector::default();
        stats.register_group_policy(0, SamplingPolicy::Unlimited);
        let config = WorkerConfig {
            target: "127.0.0.1:1".parse().unwrap(),
            duration: Duration::from_secs(1),
            conn_count: 1,
            max_pending_per_conn: 1,
            drain_timeout: FAKE_DRAIN_TIMEOUT,
        };
        let mut worker = Worker::new(
            &factory,
            FakeProtocol::default(),
            stats,
            config,
            Box::new(UniformPolicyScheduler::fixed_rate(10_000.0)),
        )
        .unwrap();
        worker.set_max_requests_per_connection(0, Some(1));
        worker.set_connect_timeout(Duration::from_millis(5));
        let start_ns = timing::time_ns();
        worker.anchor_schedules(start_ns);

        let io = factory.0.clone();
        // Send req 0 and complete it
        while io.lock().unwrap().sent.is_empty() {
            worker.send_requests().unwrap();
            thread::sleep(Duration::from_micros(200));
        }
        io.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        assert!(worker.pool.get_state(0).unwrap().needs_reconnect());

        // Trigger async reconnect (which hangs)
        while !worker.pool.get_state(0).unwrap().is_connecting() {
            worker.send_requests().unwrap();
            thread::sleep(Duration::from_micros(200));
        }
        assert!(worker.pool.next_connect_deadline().is_some());

        // Wait past 5 ms connect timeout and poll -> closes failed conn & counts 1 unsent
        thread::sleep(Duration::from_millis(10));
        worker.process_responses(Duration::ZERO).unwrap();
        assert!(worker.pool.get_state(0).unwrap().is_closed());
        assert_eq!(worker.stats.request_counts_global().unsent, 1);
    }

    fn drive_one_open_loop_send(
        worker: &mut Worker<FakeGroup, FakeProtocol>,
        io: &std::sync::Mutex<FakeIo>,
        target_sent: usize,
    ) {
        while io.lock().unwrap().sent.len() < target_sent {
            worker.send_requests().unwrap();
            if worker.pool.get_state(0).unwrap().is_connecting() {
                io.lock().unwrap().connecting_ready = true;
                worker.process_responses(Duration::ZERO).unwrap();
                io.lock().unwrap().connecting_ready = false;
            }
            thread::sleep(Duration::from_micros(200));
        }
    }

    #[test]
    fn reconnect_open_loop_fixed_rate_and_poisson_with_drain_abort() {
        for scheduler_kind in ["fixed-rate", "poisson"] {
            let factory = FakeFactory::default();
            {
                let mut io = factory.0.lock().unwrap();
                io.live_timestamps = true;
                io.reconnect_mode = ReconnectMode::Async;
            }
            let scheduler: Box<dyn PolicyScheduler> = match scheduler_kind {
                "fixed-rate" => Box::new(UniformPolicyScheduler::fixed_rate(1_000.0)),
                "poisson" => Box::new(UniformPolicyScheduler::poisson(1_000.0).unwrap()),
                _ => unreachable!(),
            };
            let mut stats = TupleStatsCollector::default();
            stats.register_group_policy(0, SamplingPolicy::Unlimited);
            let config = WorkerConfig {
                target: "127.0.0.1:1".parse().unwrap(),
                duration: Duration::from_secs(1),
                conn_count: 1,
                max_pending_per_conn: 1,
                drain_timeout: Duration::from_millis(20),
            };
            let mut worker =
                Worker::new(&factory, FakeProtocol::default(), stats, config, scheduler).unwrap();
            worker.set_max_requests_per_connection(0, Some(1));
            let start_ns = timing::time_ns();
            worker.anchor_schedules(start_ns);

            let io = factory.0.clone();
            // Run 3 requests on open-loop schedule with async reconnects
            for req_id in 0u64..3 {
                drive_one_open_loop_send(&mut worker, &io, req_id as usize + 1);
                io.lock().unwrap().responses.push_back(Ok(req_id.to_le_bytes().to_vec()));
                worker.process_responses(Duration::ZERO).unwrap();
            }

            assert_eq!(io.lock().unwrap().reconnect_started.len(), 2, "{scheduler_kind}");
            assert_eq!(io.lock().unwrap().reconnect_finished.len(), 2, "{scheduler_kind}");
            assert_eq!(worker.stats.request_counts_global().completed, 3, "{scheduler_kind}");
            assert_eq!(worker.stats.send_lag_global().sample_count, 3, "{scheduler_kind}");
            let bd = worker.stats.latency_breakdown_global();
            assert_eq!(bd.connect.unwrap().sample_count, 2, "{scheduler_kind}");
            assert_eq!(bd.connect_request.unwrap().sample_count, 2, "{scheduler_kind}");

            // Start one more async reconnect, then close window and verify drain aborts it cleanly
            while !worker.pool.get_state(0).unwrap().is_connecting() {
                worker.send_requests().unwrap();
                thread::sleep(Duration::from_micros(200));
            }
            let end_ns = timing::time_ns();
            worker.window_closed_ns = Some(end_ns);
            worker.record_unsent(end_ns);
            worker.drain_responses().unwrap();
            assert!(!worker.pool.get_state(0).unwrap().is_connecting());
            assert!(worker.stats.request_counts_global().unsent >= 1, "{scheduler_kind}");
        }
    }

    #[derive(Default)]
    struct OneWayProtocol {
        next_id: u64,
        chunk_size: usize,
        limit: Option<u64>,
        in_measurement: bool,
        measured_bytes: u64,
        measurement_starts: usize,
    }

    impl Protocol for OneWayProtocol {
        type RequestId = u64;

        fn next_request(&mut self, _: usize) -> Request<u64> {
            let id = self.next_id;
            self.next_id += 1;
            let len = if self.in_measurement {
                self.limit.map_or(self.chunk_size, |l| {
                    (self.chunk_size as u64).min(l.saturating_sub(self.measured_bytes)) as usize
                })
            } else {
                self.chunk_size
            };
            if self.in_measurement {
                self.measured_bytes += len as u64;
            }
            let data = vec![0x5Au8; len];
            if self.in_measurement {
                Request::one_way(data, id)
            } else {
                Request::one_way_warmup(data, id)
            }
        }

        fn parse_response<'a>(&mut self, _: usize, _: &'a [u8]) -> anyhow::Result<Parsed<'a, u64>> {
            Ok(Parsed::Incomplete)
        }

        fn can_send(&self, _: usize) -> bool {
            !(self.in_measurement && self.limit.is_some_and(|l| self.measured_bytes >= l))
        }

        fn is_done(&self) -> bool {
            self.in_measurement && self.limit.is_some_and(|l| self.measured_bytes >= l)
        }

        fn on_measurement_start(&mut self) {
            self.in_measurement = true;
            self.measured_bytes = 0;
            self.measurement_starts += 1;
        }

        fn name(&self) -> &'static str {
            "OneWay"
        }

        fn reset(&mut self) {
            self.next_id = 0;
            self.in_measurement = false;
            self.measured_bytes = 0;
        }
    }

    #[test]
    fn one_way_request_completes_without_pending_map_and_reschedules_under_backpressure() {
        let factory = FakeFactory::default();
        let io = factory.0.clone();
        // Allow only 4 bytes initially so the first 16-byte one-way chunk hits backpressure
        io.lock().unwrap().capacity = Some(4);

        let mut stats = TupleStatsCollector::default();
        stats.register_group_policy(0, SamplingPolicy::None);
        let config = WorkerConfig {
            target: "127.0.0.1:1".parse().unwrap(),
            duration: Duration::from_secs(1),
            conn_count: 1,
            max_pending_per_conn: 16,
            drain_timeout: FAKE_DRAIN_TIMEOUT,
        };
        let mut proto = OneWayProtocol {
            chunk_size: 16,
            limit: Some(32),
            ..Default::default()
        };
        proto.on_measurement_start();
        let mut worker = Worker::new(
            &factory,
            proto,
            stats,
            config,
            Box::new(UniformPolicyScheduler::closed_loop()),
        )
        .unwrap();
        worker.anchor_schedules(timing::time_ns());

        // First send round queues one chunk, writes 4 bytes, blocks on backpressure
        worker.send_requests().unwrap();
        let state = worker.pool.get_state(0).unwrap();
        assert!(state.is_write_blocked());
        // Must not insert into pending_map and must not accept another chunk while blocked
        assert_eq!(state.measured_pending_count(), 0);
        assert_eq!(state.pending_count(), 1);
        assert!(!state.can_send());
        // No tx_bytes counted until the chunk actually completes writing
        assert_eq!(worker.stats.aggregate_global(Duration::from_secs(1)).tx_bytes, 0);

        // Unblock transport: process_responses writes remaining 12 bytes, records tx_bytes,
        // and reschedules conn 0 even though pending_count() drops to 0!
        {
            let mut guard = io.lock().unwrap();
            guard.capacity = None;
            guard.writable = true;
        }
        worker.process_responses(Duration::ZERO).unwrap();
        let state = worker.pool.get_state(0).unwrap();
        assert!(!state.is_write_blocked());
        assert_eq!(state.pending_count(), 0);
        assert!(state.is_on_heap(), "one-way completion must reschedule connection");
        assert_eq!(worker.stats.aggregate_global(Duration::from_secs(1)).tx_bytes, 16);

        // Second send round completes the remaining 16 bytes and hits byte limit
        worker.send_requests().unwrap();
        assert!(worker.all_protocols_done());
        assert_eq!(worker.stats.aggregate_global(Duration::from_secs(1)).tx_bytes, 32);
        assert_eq!(worker.stats.request_counts_global().sent, 0);
    }

    #[test]
    fn time_based_warmup_suppresses_stats_and_invokes_on_measurement_start() {
        let factory = FakeFactory::default();
        let mut stats = TupleStatsCollector::default();
        stats.register_group_policy(0, SamplingPolicy::None);
        let config = WorkerConfig {
            target: "127.0.0.1:1".parse().unwrap(),
            duration: Duration::from_secs(5),
            conn_count: 1,
            max_pending_per_conn: 1,
            drain_timeout: FAKE_DRAIN_TIMEOUT,
        };
        let proto = OneWayProtocol {
            chunk_size: 1024,
            limit: Some(4096),
            ..Default::default()
        };
        let mut worker = Worker::new(
            &factory,
            proto,
            stats,
            config,
            Box::new(UniformPolicyScheduler::closed_loop()),
        )
        .unwrap();
        worker.set_warmup(Duration::from_millis(30));
        worker.set_unbounded_duration(true);
        worker.run().unwrap();

        let proto = worker.protocols.get(&0).unwrap();
        assert_eq!(proto.measurement_starts, 1);
        assert!(proto.is_done());

        // Total bytes sent on the wire must exceed 4096 (warmup + 4096 measured)
        let wire_bytes: usize = factory.0.lock().unwrap().sent.iter().map(Vec::len).sum();
        assert!(
            wire_bytes > 4096,
            "expected warmup traffic before measurement, got {wire_bytes}"
        );

        // Measured stats must contain ONLY the 4096 measurement bytes
        let agg = worker.stats.aggregate_global(Duration::from_secs(1));
        assert_eq!(agg.tx_bytes, 4096);
    }

    #[test]
    fn idle_hold_waits_polls_and_discards_inbound_without_late_responses() {
        let factory = FakeFactory::default();
        let io = factory.0.clone();
        // Queue a stray inbound frame that arrives during idle_hold
        io.lock().unwrap().responses.push_back(Ok(vec![1, 2, 3, 4, 5, 6, 7, 8]));

        let mut stats = TupleStatsCollector::default();
        stats.register_group_policy(0, SamplingPolicy::None);
        let config = WorkerConfig {
            target: "127.0.0.1:1".parse().unwrap(),
            duration: Duration::from_millis(10),
            conn_count: 1,
            max_pending_per_conn: 1,
            drain_timeout: FAKE_DRAIN_TIMEOUT,
        };
        let proto = OneWayProtocol {
            chunk_size: 64,
            limit: Some(64),
            ..Default::default()
        };
        let mut worker = Worker::new(
            &factory,
            proto,
            stats,
            config,
            Box::new(UniformPolicyScheduler::closed_loop()),
        )
        .unwrap();
        worker.set_idle_hold(Duration::from_millis(40), None);
        let started = std::time::Instant::now();
        worker.run().unwrap();
        let elapsed = started.elapsed();

        assert!(
            elapsed >= Duration::from_millis(35),
            "expected idle_hold >= 35ms, elapsed={elapsed:?}"
        );
        let guard = io.lock().unwrap();
        assert!(guard.closed, "connections must close after idle_hold");
        assert!(guard.responses.is_empty(), "inbound data must be drained during idle_hold");
        assert!(
            guard
                .timeouts
                .iter()
                .any(|&t| t > Duration::ZERO && t <= Duration::from_millis(10)),
            "idle_hold must poll with capped timeout <= 10ms, got {:?}",
            guard.timeouts
        );
        drop(guard);

        let counts = worker.stats.request_counts_global();
        assert_eq!(counts.late_responses, 0);
        assert_eq!(counts.errors, 0);
        let agg = worker.stats.aggregate_global(Duration::from_secs(1));
        assert_eq!(agg.tx_bytes, 64);
        assert_eq!(agg.rx_bytes, 0);
    }

    #[test]
    fn idle_hold_barrier_abort_unblocks_waiting_peer_without_deadlock() {
        let barrier = Arc::new(IdleHoldBarrier::new(2));
        let peer_barrier = Arc::clone(&barrier);

        let factory = FakeFactory::default();
        let io = factory.0.clone();
        let mut stats = TupleStatsCollector::default();
        stats.register_group_policy(0, SamplingPolicy::None);
        let config = WorkerConfig {
            target: "127.0.0.1:1".parse().unwrap(),
            duration: Duration::from_millis(10),
            conn_count: 1,
            max_pending_per_conn: 1,
            drain_timeout: FAKE_DRAIN_TIMEOUT,
        };
        let proto = OneWayProtocol {
            chunk_size: 64,
            limit: Some(64),
            ..Default::default()
        };
        let mut worker = Worker::new(
            &factory,
            proto,
            stats,
            config,
            Box::new(UniformPolicyScheduler::closed_loop()),
        )
        .unwrap();
        // Long idle_hold ensures we verify the worker skips the hold once aborted.
        worker.set_idle_hold(Duration::from_secs(5), Some(peer_barrier));

        let guard = barrier.guard();
        let started = std::time::Instant::now();
        let handle = std::thread::spawn(move || {
            worker.run().unwrap();
        });

        // Give the worker time to finish sending and enter the barrier, then
        // simulate a peer failing by dropping its RAII guard without arriving.
        std::thread::sleep(Duration::from_millis(25));
        drop(guard);

        handle.join().expect("peer worker must unblock cleanly on barrier abort");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "aborted idle_hold must skip hold and return immediately, elapsed={elapsed:?}"
        );
        assert!(
            io.lock().unwrap().closed,
            "connections must still close cleanly after aborted idle_hold"
        );
    }
}
