//! Worker implementation with connection pooling and request pipelining
//!
//! This implements Lancet's symmetric agent model, allowing multiple outstanding
//! requests per connection for maximum throughput with accurate latency tracking.
//!
//! The worker can operate in different modes based on configuration:
//! - Single connection, single pending request: Simple closed-loop latency measurement
//! - Multiple connections with pipelining: High-throughput load testing

use crate::connection::{ConnectionPool, TrafficGroupConfig};
use crate::request_dump::RequestDumper;
use crate::stats::{StatsRecorder, TupleStatsCollector, UNSENT_WALK_CAP};
use crate::timing;
use crate::traffic_group::OnError;
use crate::Result;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use xylem_common::Protocol;
use xylem_transport::{ConnectionGroup, TransportFactory};

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
    protocols: HashMap<usize, P>,
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
        let pool = ConnectionPool::new(
            factory,
            config.target,
            config.conn_count,
            config.max_pending_per_conn,
            policy_scheduler,
            0, // Default group_id for legacy single-pool workers
        )?;

        let mut protocols = HashMap::new();
        protocols.insert(0, protocol);

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
        let pool = ConnectionPool::new_multi_group(factory, config.target, groups)?;

        Ok(Self::from_pool(pool, protocols, stats, config))
    }

    /// Create a new pipelined worker with multiple traffic groups, each with its own target
    pub fn new_multi_group_with_targets<F: TransportFactory<Group = G>>(
        factory: &F,
        protocols: HashMap<usize, P>,
        stats: S,
        config: WorkerConfig,
        groups: Vec<TrafficGroupConfig>,
    ) -> Result<Self> {
        let pool = ConnectionPool::new_multi_group_with_targets(factory, groups)?;

        Ok(Self::from_pool(pool, protocols, stats, config))
    }

    /// Create a worker around an already built connection pool
    fn from_pool(
        pool: ConnectionPool<G, P::RequestId>,
        protocols: HashMap<usize, P>,
        stats: S,
        config: WorkerConfig,
    ) -> Self {
        Self {
            pool,
            protocols,
            stats,
            config,
            request_dumper: None,
            thread_id: 0,
            next_tx_cleanup_ns: timing::time_ns().saturating_add(1_000_000_000),
            ready_ids: Vec::new(),
        }
    }

    /// Get mapping of connection ID to target address
    pub fn connection_targets(&self) -> Vec<(usize, SocketAddr)> {
        self.pool.connection_targets()
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

    /// Run the pipelined measurement loop (Lancet symmetric_tcp_main style)
    pub fn run(&mut self) -> Result<()> {
        let start_ns = timing::time_ns();
        let duration_ns = self.config.duration.as_nanos().min(u64::MAX as u128) as u64;
        let end_ns = start_ns.saturating_add(duration_ns);

        loop {
            let now_ns = timing::time_ns();

            // Check if duration exceeded
            if now_ns >= end_ns {
                self.stats.end_measurement(end_ns);
                self.record_unsent(end_ns);
                break;
            }

            self.send_requests()?;

            // RECEIVE PHASE: Poll all connections for responses (single syscall)
            // Derive sleep duration from the ready heap (when next connection will be ready)
            let now_ns = timing::time_ns();

            let poll_timeout = self.poll_timeout(now_ns, end_ns);
            self.process_responses(poll_timeout)?;
            self.expire_requests(timing::time_ns());
        }

        // Stop sending and wait for the responses still pending
        self.drain_responses()?;

        // Close all connections
        self.pool.close_all()?;

        Ok(())
    }

    fn send_requests(&mut self) -> Result<()> {
        // SEND PHASE: Send requests to ready connections
        // Rate limiting is handled by connection-level policies via pool's ready_heap
        //
        // Safety limit: Cap the number of consecutive sends to prevent infinite loops
        // For closed-loop with pipelining, we want to fill all pipelines, but not spin forever
        // With N connections and depth D, we should need at most N*D sends to fill all pipelines
        let max_sends_per_iteration = self.pool.connection_count() * 16; // Allow generous headroom
        let mut sends_this_iteration = 0;

        while let Some(connection_id) = self.pool.pick_connection() {
            let state = self.pool.get_state(connection_id).expect("Connection state not found");
            let group_id = state.group_id();

            let request = {
                let protocol =
                    self.protocols.get_mut(&group_id).expect("Protocol not found for group_id");
                protocol.next_request(connection_id)
            };

            if request.metadata.is_setup {
                // Connection setup (a handshake, AUTH, ...) may carry
                // credentials: it is never dumped, measured or scheduled.
                self.pool.send_setup(connection_id, &request.data, request.request_id)?;
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

                // Send request (track warmup status for response handling). An
                // open-loop request's latency starts at its intended send time.
                let intended_ns = state.intended_send_time();
                let lag = self.pool.send_scheduled(
                    connection_id,
                    &request.data,
                    request.request_id,
                    request.metadata.is_warmup,
                    intended_ns,
                )?;

                // Advance the policy's schedule and record stats (skip stats for
                // warmup requests)
                if let Some(state) = self.pool.get_state_mut(connection_id) {
                    state.on_request_sent(timing::time_ns());
                }
                if !request.metadata.is_warmup {
                    self.stats.record_tx_bytes(group_id, connection_id, request.data.len());
                    self.stats.record_sent(group_id, connection_id, lag);
                }
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
        Ok(())
    }

    /// Bound every receive wait by the experiment deadline and the next
    /// request timeout. A closed-loop entry has no timestamp, so test
    /// readiness before interpreting an empty peek.
    fn poll_timeout(&self, now_ns: u64, end_ns: u64) -> Duration {
        let end_ns = self.pool.next_request_deadline().map_or(end_ns, |next| next.min(end_ns));
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

        for &connection_id in &ready_ids {
            let state = match self.pool.get_state(connection_id) {
                Some(s) => s,
                None => continue,
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
                        self.close_failed(connection_id, group_id);
                        tracing::warn!(connection_id, "Connection disconnected: {}", e);
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            };

            for (_, latency, is_warmup, bytes_consumed, error) in responses {
                if is_warmup {
                    continue;
                }
                self.stats.record_rx_bytes(group_id, connection_id, bytes_consumed);
                match error {
                    None => self.stats.record_latency(group_id, connection_id, latency),
                    Some(kind) => {
                        let kind = self.pool.error_kind(connection_id, kind);
                        self.stats.record_error_reply(group_id, connection_id, kind, latency);
                    }
                }
            }
            let late = self.pool.take_late_responses(connection_id);
            if late > 0 {
                self.stats.record_late_responses(group_id, connection_id, late);
            }

            // Reschedule connection if it can send but isn't on heap
            // This handles two cases:
            // 1. Pipeline transitioned from full to available
            // 2. Protocol became ready (e.g., handshake completed)
            let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
            self.pool.try_schedule_connection(connection_id, protocol);
        }
        self.ready_ids = ready_ids;

        Ok(())
    }

    /// Time out the requests whose request timeout passed by `now_ns`, count
    /// the measurement ones, and reschedule the connections they freed
    fn expire_requests(&mut self, now_ns: u64) {
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
        for (connection_id, group_id) in freed {
            let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
            self.pool.try_schedule_connection(connection_id, protocol);
        }
    }

    fn maintain_tx_queues(&mut self, now_ns: u64) {
        if now_ns >= self.next_tx_cleanup_ns {
            self.pool.cleanup_stale_tx_queues_at(now_ns);
            self.next_tx_cleanup_ns = now_ns.saturating_add(1_000_000_000);
        }
    }

    /// Close a connection that failed, counting its pending measurement
    /// requests as failed
    fn close_failed(&mut self, connection_id: usize, group_id: usize) {
        if let Some(state) = self.pool.get_state(connection_id) {
            let failed = state.measured_pending_count() as u64;
            if failed > 0 {
                self.stats.record_failed(group_id, connection_id, failed);
            }
        }
        let _ = self.pool.close(connection_id);
    }

    /// Count the intended sends of every connection scheduled before the end
    /// of the window that were never sent
    fn record_unsent(&mut self, end_ns: u64) {
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
        while self.has_pending() {
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
            self.process_responses(Duration::from_nanos(wait_ns))?;
            self.expire_requests(timing::time_ns());
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
        let pool = ConnectionPool::new_multi_group_unix(factory, groups)?;

        Ok(Self::from_pool(pool, protocols, stats, config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use xylem_common::{ErrorReply, Parsed, Request, RequestMeta};
    use xylem_transport::TcpTransportFactory;

    use crate::connection::test_support::{FakeFactory, FakeGroup};
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
        /// Responses to these requests are error replies of the given kind
        errors: HashMap<u64, &'static str>,
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
            Request::measurement(id.to_le_bytes().to_vec(), id)
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
        assert_eq!(worker.stats.entries().values().map(|entry| entry.rx_bytes).sum::<u64>(), 24);

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
            assert_eq!(worker.stats.entries().values().map(|entry| entry.rx_bytes).sum::<u64>(), 8);
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
        assert_eq!(factory.0.lock().unwrap().sent.len(), 16);
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
        assert_eq!(worker.stats.entries().values().map(|entry| entry.rx_bytes).sum::<u64>(), 8);
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
        assert_eq!(worker.stats.entries().values().map(|entry| entry.rx_bytes).sum::<u64>(), 8);
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
        for id in 0u64..2 {
            worker.send_requests().unwrap();
            assert_eq!(factory.0.lock().unwrap().sent.len() as u64, id + 1);
            thread::sleep(Duration::from_millis(20));
            worker.send_requests().unwrap();
            assert_eq!(factory.0.lock().unwrap().sent.len() as u64, id + 1, "setup is pending");
            factory.0.lock().unwrap().responses.push_back(Ok(id.to_le_bytes().to_vec()));
            worker.process_responses(Duration::ZERO).unwrap();
        }
        let setup_done_ns = timing::time_ns();

        // The schedule restarted when setup completed, so the 40 ms spent on
        // setup are neither a backlog of overdue sends nor send lag
        let intended = worker.pool.get_state(0).unwrap().intended_send_time().unwrap();
        assert!(intended + 1_000_000 >= setup_done_ns, "schedule restarted after setup");
        worker.send_requests().unwrap();
        assert_eq!(factory.0.lock().unwrap().sent.len(), 3, "one measured send, no burst");
        let counts = worker.stats.request_counts_global();
        assert_eq!((counts.sent, counts.completed), (1, 0), "setup requests are not counted");
        let lag = worker.stats.send_lag_global();
        assert_eq!(lag.sample_count, 1);
        assert!(lag.max.unwrap() < Duration::from_millis(10), "{lag:?}");
        assert_eq!(
            worker.pool.get_state(0).unwrap().intended_send_time(),
            Some(intended + 1_000_000),
            "the first measured request took the first intended time"
        );
        // Setup bytes are neither dumped nor counted
        assert_eq!(dumper.total_records(), 1);
        factory.0.lock().unwrap().responses.push_back(Ok(2u64.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        assert_eq!(worker.stats.entries().values().map(|entry| entry.tx_bytes).sum::<u64>(), 8);
        assert_eq!(worker.stats.entries().values().map(|entry| entry.rx_bytes).sum::<u64>(), 8);
        assert_eq!(global(&worker).latency_sample_count, 1);
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
        let sent = factory.0.lock().unwrap().sent.len() as u64;
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
        assert_eq!(worker.stats.entries().values().map(|entry| entry.rx_bytes).sum::<u64>(), 16);
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
        let rx: u64 = worker.stats.entries().values().map(|entry| entry.rx_bytes).sum();
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
}
