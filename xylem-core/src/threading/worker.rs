//! Worker implementation with connection pooling and request pipelining
//!
//! This implements Lancet's symmetric agent model, allowing multiple outstanding
//! requests per connection for maximum throughput with accurate latency tracking.
//!
//! The worker can operate in different modes based on configuration:
//! - Single connection, single pending request: Simple closed-loop latency measurement
//! - Multiple connections with pipelining: High-throughput load testing

use crate::connection::{ConnectionPool, ReceivedResponse, RequestTiming, TrafficGroupConfig};
use crate::request_dump::RequestDumper;
use crate::stats::{StatsRecorder, TupleStatsCollector};
use crate::timing;
use crate::Result;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use xylem_common::{Protocol, RetryRequest};
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
    protocols: HashMap<usize, P>,
    stats: S,
    config: WorkerConfig,
    /// Optional request dumper for recording all requests
    request_dumper: Option<Arc<RequestDumper>>,
    /// Thread ID for request dumping
    thread_id: usize,
    /// Coarse maintenance deadline for stale TX timestamp correlation entries.
    next_tx_cleanup_ns: u64,
    /// Redirects waiting for capacity on their target connection.
    retry_queue: Vec<(usize, RetryRequest<P::RequestId>, RequestTiming)>,
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
            retry_queue: Vec::new(),
            ready_ids: Vec::new(),
        }
    }

    /// Get mapping of connection ID to target address
    ///
    /// Useful for wiring up protocols that need to know connection routing
    /// (e.g., Redis Cluster). Call this after construction but before run().
    pub fn connection_targets(&self) -> Vec<(usize, SocketAddr)> {
        self.pool.connection_targets()
    }

    /// Get mapping of connection ID to target address for a specific traffic group
    ///
    /// Filters connections to only those belonging to the specified group_id.
    /// Essential for Redis Cluster to avoid routing to connections from other groups.
    pub fn connection_targets_for_group(&self, group_id: usize) -> Vec<(usize, SocketAddr)> {
        self.pool.connection_targets_for_group(group_id)
    }

    /// Get mutable access to protocols map
    ///
    /// Useful for post-construction wiring (e.g., registering cluster connections).
    pub fn protocols_mut(&mut self) -> &mut HashMap<usize, P> {
        &mut self.protocols
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
                break;
            }

            self.send_requests()?;

            // RECEIVE PHASE: Poll all connections for responses (single syscall)
            // Derive sleep duration from the ready heap (when next connection will be ready)
            let now_ns = timing::time_ns();

            let poll_timeout = self.poll_timeout(now_ns, end_ns);
            self.process_responses(poll_timeout)?;
        }

        // Drain remaining responses
        self.drain_responses()?;

        // Close all connections
        self.pool.close_all()?;

        Ok(())
    }

    fn send_requests(&mut self) -> Result<()> {
        // Give redirected requests capacity before generating new work. This
        // also bounds the queue when a target is slower than the source nodes.
        let blocked_groups: HashSet<_> =
            self.retry_queue.iter().map(|(group, _, _)| *group).collect();
        let mut blocked_connections = Vec::new();
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
            if blocked_groups.contains(&group_id) {
                blocked_connections.push((connection_id, group_id));
                continue;
            }

            let request = {
                let protocol =
                    self.protocols.get_mut(&group_id).expect("Protocol not found for group_id");
                protocol.next_request(connection_id)
            };

            // Record request to dump file if enabled
            if let Some(ref dumper) = self.request_dumper {
                let req_id_str = format!("{:?}", request.request_id);
                self.record_request(dumper, group_id, connection_id, &request.data, &req_id_str);
            }

            // Send request (track warmup status for response handling)
            let sent_time_ns = timing::time_ns();
            self.pool.send(
                connection_id,
                &request.data,
                request.request_id,
                request.metadata.is_warmup,
            )?;

            // Notify policy and record stats (skip stats for warmup requests)
            if let Some(state) = self.pool.get_state_mut(connection_id) {
                state.on_request_sent(sent_time_ns);
            }
            if !request.metadata.is_warmup {
                self.stats.record_tx_bytes(group_id, connection_id, request.data.len());
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
        // Reinsert only after scanning: an immediately ready closed-loop entry
        // would otherwise be popped forever. A short delay avoids busy polling
        // when only blocked groups are ready; responses still wake the poll.
        // Leave at least a millisecond for transports whose poll rounds down.
        let retry_wakeup_ns = timing::time_ns().saturating_add(2_000_000);
        for (connection_id, group_id) in blocked_connections {
            let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
            self.pool
                .try_schedule_connection_after(connection_id, protocol, retry_wakeup_ns);
        }
        Ok(())
    }

    /// Bound every receive wait by the experiment deadline. A closed-loop
    /// entry has no timestamp, so test readiness before interpreting an empty peek.
    fn poll_timeout(&self, now_ns: u64, end_ns: u64) -> Duration {
        let remaining_ns = end_ns.saturating_sub(now_ns);
        let wait_ns = if self.pool.has_ready(now_ns) {
            0
        } else {
            self.pool.peek_next_ready_time().map_or(remaining_ns, |next| {
                // Retry-blocked entries need a real wait, not the precision spin
                // used for ordinary rate-policy deadlines.
                let spin_ns = if self.retry_queue.is_empty() {
                    1_000_000
                } else {
                    0
                };
                next.saturating_sub(now_ns).saturating_sub(spin_ns)
            })
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

        // Collect retries to process after response handling
        let mut retries = std::mem::take(&mut self.retry_queue);

        for &connection_id in &ready_ids {
            let state = match self.pool.get_state(connection_id) {
                Some(s) => s,
                None => continue,
            };

            // Skip connections with no pending requests
            if state.pending_count() == 0 {
                continue;
            }

            let group_id = state.group_id();

            // Preserve the parser's completion/retry distinction through pending
            // tracking, so redirects neither record latency nor tune the policy.
            let responses = {
                let protocol =
                    self.protocols.get_mut(&group_id).expect("Protocol not found for group_id");
                match self.pool.recv_responses_extended(connection_id, |data| {
                    protocol.parse_response_extended(connection_id, data).map_err(Into::into)
                }) {
                    Ok(responses) => responses,
                    Err(e @ crate::Error::Connection(_)) => {
                        let _ = self.pool.close(connection_id);
                        tracing::warn!(connection_id, "Connection disconnected: {}", e);
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            };

            for response in responses {
                let (timing, bytes_consumed) = match &response {
                    ReceivedResponse::Complete { timing, bytes_consumed, .. } => {
                        (*timing, *bytes_consumed)
                    }
                    ReceivedResponse::Retry { request, timing } => {
                        (*timing, request.bytes_consumed)
                    }
                };
                if !timing.is_warmup {
                    self.stats.record_rx_bytes(group_id, connection_id, bytes_consumed);
                }
                match response {
                    ReceivedResponse::Complete { latency, .. } if !timing.is_warmup => {
                        self.stats.record_latency(group_id, timing.origin_connection_id, latency);
                    }
                    ReceivedResponse::Retry { request, timing } => {
                        retries.push((group_id, request, timing));
                    }
                    ReceivedResponse::Complete { .. } => {}
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

        // Process all retries after response handling.
        for (group_id, retry_req, timing) in retries {
            self.handle_retry(group_id, retry_req, timing)?;
        }

        Ok(())
    }

    fn maintain_tx_queues(&mut self, now_ns: u64) {
        if now_ns >= self.next_tx_cleanup_ns {
            self.pool.cleanup_stale_tx_queues_at(now_ns);
            self.next_tx_cleanup_ns = now_ns.saturating_add(1_000_000_000);
        }
    }

    /// Handle a retry request by sending preparation commands and retry request
    fn handle_retry(
        &mut self,
        group_id: usize,
        retry_req: RetryRequest<P::RequestId>,
        timing: RequestTiming,
    ) -> Result<()> {
        let target_connection_id = match retry_req.target_conn_id {
            Some(connection_id) => connection_id,
            None => return Err(crate::Error::Connection("Retry target is not registered".into())),
        };

        // Check connection exists and belongs to the group
        let state = match self.pool.get_state(target_connection_id) {
            Some(s) if s.group_id() == group_id => s,
            _ => {
                return Err(crate::Error::Connection(
                    "Retry target is outside the traffic group".into(),
                ))
            }
        };

        if state.is_closed() {
            return Err(crate::Error::Connection("Retry target is closed".into()));
        }
        if !state.can_send() {
            self.retry_queue.push((group_id, retry_req, timing));
            return Ok(());
        }

        // Send preparation commands
        for prep_cmd in &retry_req.prepare_commands {
            self.pool.send_untracked(target_connection_id, prep_cmd)?;
            if !timing.is_warmup {
                self.stats.record_tx_bytes(group_id, target_connection_id, prep_cmd.len());
            }
        }

        // Generate and send retry request using the original request ID
        let protocol = self.protocols.get_mut(&group_id).expect("Protocol not found for group_id");

        let request =
            protocol.regenerate_request(target_connection_id, retry_req.original_request_id);

        // Record retry request to dump file if enabled
        if let Some(ref dumper) = self.request_dumper {
            let req_id_str = format!("{:?}_retry{}", request.request_id, retry_req.attempt);
            self.record_request(dumper, group_id, target_connection_id, &request.data, &req_id_str);
        }

        // Send the retry request
        // Note: We intentionally do NOT call state.on_request_sent() or generator.mark_request_sent()
        // because retries are already counted as in-flight from the original request. Double-counting
        // would skew rate limiting and statistics (especially for Redis Cluster MOVED/ASK redirects).
        self.pool
            .send_retry(target_connection_id, &request.data, request.request_id, timing)?;
        if !timing.is_warmup {
            self.stats.record_tx_bytes(group_id, target_connection_id, request.data.len());
        }

        Ok(())
    }

    /// Drain any remaining responses after measurement period
    fn drain_responses(&mut self) -> Result<()> {
        for _ in 0..100 {
            let has_pending = self.drain_iteration()?;
            if !has_pending {
                return Ok(());
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        let pending: usize = self
            .pool
            .connection_ids()
            .iter()
            .filter_map(|&id| self.pool.get_state(id))
            .map(|state| state.pending_count())
            .sum();
        tracing::warn!(
            pending_requests = pending,
            queued_retries = self.retry_queue.len(),
            "Response drain exhausted; retaining collected statistics"
        );
        Ok(())
    }

    /// Single drain iteration. Redirect retries remain enabled, using the same
    /// extended parser and error handling as measurement. They can add pending
    /// work, but the outer drain loop still bounds the number of poll iterations.
    fn drain_iteration(&mut self) -> Result<bool> {
        if !self.has_pending() {
            return Ok(false);
        }
        self.process_responses(Duration::from_millis(10))?;
        Ok(self.has_pending())
    }

    fn has_pending(&self) -> bool {
        !self.retry_queue.is_empty()
            || self
                .pool
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
    use xylem_common::{ParseResult, Request};
    use xylem_transport::TcpTransportFactory;

    use crate::connection::test_support::{FakeFactory, FakeGroup};
    use crate::scheduler::{PolicyScheduler, UniformPolicyScheduler};
    use crate::stats::SamplingPolicy;

    #[derive(Default)]
    struct FakeProtocol {
        next_id: u64,
        retry: bool,
        retry_target: usize,
        retry_prepare: Vec<Vec<u8>>,
        fail: bool,
    }

    impl Protocol for FakeProtocol {
        type RequestId = u64;

        fn next_request(&mut self, _: usize) -> Request<u64> {
            let id = self.next_id;
            self.next_id += 1;
            Request::measurement(id.to_le_bytes().to_vec(), id)
        }

        fn parse_response(&mut self, _: usize, _: &[u8]) -> anyhow::Result<(usize, Option<u64>)> {
            panic!("worker must use extended parsing, including during drain")
        }

        fn parse_response_extended(
            &mut self,
            _: usize,
            data: &[u8],
        ) -> anyhow::Result<ParseResult<u64>> {
            if self.fail {
                anyhow::bail!("invalid test response");
            }
            if data.len() < 8 {
                return Ok(ParseResult::Incomplete);
            }
            let id = u64::from_le_bytes(data[..8].try_into().unwrap());
            if std::mem::take(&mut self.retry) {
                return Ok(ParseResult::Retry(RetryRequest {
                    bytes_consumed: 8,
                    original_request_id: id,
                    is_warmup: false,
                    target_conn_id: Some(self.retry_target),
                    prepare_commands: self.retry_prepare.clone(),
                    attempt: 0,
                }));
            }
            Ok(ParseResult::Complete { bytes_consumed: 8, request_id: id })
        }

        fn name(&self) -> &'static str {
            "Fake"
        }
        fn reset(&mut self) {}
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
            },
            scheduler,
        )
        .unwrap()
    }

    #[test]
    fn full_retry_target_defers_preparation_until_capacity_is_available() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        worker
            .handle_retry(
                0,
                RetryRequest {
                    bytes_consumed: 0,
                    original_request_id: 99,
                    is_warmup: false,
                    target_conn_id: Some(0),
                    prepare_commands: vec![b"ASKING".to_vec()],
                    attempt: 0,
                },
                RequestTiming {
                    send_ts: xylem_transport::Timestamp::now(),
                    is_warmup: false,
                    origin_connection_id: 0,
                },
            )
            .unwrap();
        assert_eq!(factory.0.lock().unwrap().sent.len(), 1);
        assert_eq!(worker.retry_queue.len(), 1);
        assert_eq!(worker.protocols[&0].next_id, 1);
        factory.0.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        assert!(worker.retry_queue.is_empty());
        assert_eq!(
            factory.0.lock().unwrap().sent,
            [0u64.to_le_bytes().to_vec(), b"ASKING".to_vec(), 1u64.to_le_bytes().to_vec()]
        );
        assert_eq!(worker.pool.get_state(0).unwrap().pending_count(), 1);
    }

    #[test]
    fn queued_retry_blocks_only_its_group_and_resumes_after_capacity_returns() {
        let factory = FakeFactory::default();
        let mut stats = TupleStatsCollector::default();
        for group in 0..2 {
            stats.register_group_policy(
                group,
                SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 },
            );
        }
        let mut worker = Worker::new_multi_group(
            &factory,
            HashMap::from([(0, FakeProtocol::default()), (1, FakeProtocol::default())]),
            stats,
            WorkerConfig {
                target: "127.0.0.1:1".parse().unwrap(),
                duration: Duration::ZERO,
                conn_count: 3,
                max_pending_per_conn: 1,
            },
            vec![
                (0, 2, 1, Box::new(UniformPolicyScheduler::closed_loop())),
                (1, 1, 1, Box::new(UniformPolicyScheduler::closed_loop())),
            ],
        )
        .unwrap();
        // Group 0 has a ready source and a full retry target. Group 1 is ready.
        worker.pool.send(1, &99u64.to_le_bytes(), 99, true).unwrap();
        worker
            .handle_retry(
                0,
                RetryRequest {
                    bytes_consumed: 8,
                    original_request_id: 42,
                    is_warmup: false,
                    target_conn_id: Some(1),
                    prepare_commands: vec![b"ASKING".to_vec()],
                    attempt: 0,
                },
                RequestTiming {
                    send_ts: xylem_transport::Timestamp::now(),
                    is_warmup: false,
                    origin_connection_id: 0,
                },
            )
            .unwrap();
        worker.send_requests().unwrap();
        assert_eq!(worker.protocols[&0].next_id, 0);
        assert_eq!(worker.protocols[&1].next_id, 1);
        assert_eq!(worker.pool.get_state(0).unwrap().pending_count(), 0);
        assert!(worker.pool.get_state(0).unwrap().is_on_heap());
        assert_eq!(worker.pool.get_state(2).unwrap().pending_count(), 1);
        let wakeup = worker.pool.peek_next_ready_time().unwrap();
        assert_eq!(worker.poll_timeout(wakeup - 2_000_000, wakeup + 100), Duration::from_millis(2));
        assert_eq!(worker.poll_timeout(wakeup - 1, wakeup + 100), Duration::from_nanos(1));
        {
            let mut io = factory.0.lock().unwrap();
            io.response_connection_id = 2;
            io.responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        }
        worker.process_responses(Duration::ZERO).unwrap();
        assert_eq!(worker.stats.aggregate_by_group(Duration::from_secs(1))[&1].total_requests, 1);
        assert_eq!(worker.retry_queue.len(), 1);
        worker.send_requests().unwrap();
        assert_eq!(worker.protocols[&1].next_id, 2);
        assert_eq!(worker.protocols[&0].next_id, 0);
        {
            let mut io = factory.0.lock().unwrap();
            io.response_connection_id = 1;
            io.responses.push_back(Ok(99u64.to_le_bytes().to_vec()));
        }
        worker.process_responses(Duration::ZERO).unwrap();
        assert!(worker.retry_queue.is_empty());
        assert_eq!(worker.pool.get_state(1).unwrap().pending_count(), 1);
        assert_eq!(worker.protocols[&0].next_id, 1);
        assert_eq!(factory.0.lock().unwrap().sent.len(), 5);
        // Advance the scheduler clock deterministically past the brief deferral.
        assert_eq!(worker.pool.pick_connection_at(u64::MAX), Some(0));
        assert!(!worker.pool.get_state(0).unwrap().is_on_heap());
        worker.pool.try_schedule_connection(0, &worker.protocols[&0]);
        worker.send_requests().unwrap();
        assert_eq!(worker.pool.get_state(0).unwrap().pending_count(), 1);
        assert_eq!(worker.pool.get_state(1).unwrap().pending_count(), 1);
        assert_eq!(worker.protocols[&0].next_id, 2);
    }

    #[test]
    fn queued_retry_still_errors_when_its_target_disconnects() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        worker
            .handle_retry(
                0,
                RetryRequest {
                    bytes_consumed: 8,
                    original_request_id: 99,
                    is_warmup: false,
                    target_conn_id: Some(0),
                    prepare_commands: vec![],
                    attempt: 0,
                },
                RequestTiming {
                    send_ts: xylem_transport::Timestamp::now(),
                    is_warmup: false,
                    origin_connection_id: 0,
                },
            )
            .unwrap();
        factory
            .0
            .lock()
            .unwrap()
            .responses
            .push_back(Err(xylem_transport::Error::Connection("closed".into())));
        assert!(worker
            .drain_iteration()
            .unwrap_err()
            .to_string()
            .contains("Retry target is closed"));
        assert!(worker.pool.get_state(0).unwrap().is_closed());
    }

    #[test]
    fn run_preserves_completed_stats_and_closes_after_disconnect_or_drain_exhaustion() {
        for disconnect in [true, false] {
            let factory = FakeFactory::default();
            let mut worker =
                fake_worker(&factory, 2, Box::new(UniformPolicyScheduler::closed_loop()));
            worker.send_requests().unwrap();
            factory.0.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
            worker.process_responses(Duration::ZERO).unwrap();
            assert_eq!(worker.stats.aggregate_global(Duration::from_secs(1)).total_requests, 1);
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
            assert_eq!(worker.stats.aggregate_global(Duration::from_secs(1)).total_requests, 1);
            assert_eq!(worker.stats.entries().values().map(|entry| entry.rx_bytes).sum::<u64>(), 8);
            assert!(factory.0.lock().unwrap().closed);
            assert!(worker.pool.get_state(0).unwrap().is_closed());
            assert_eq!(worker.pool.get_state(0).unwrap().pending_count(), 0);
            assert!(!worker.has_pending());
            assert_eq!(factory.0.lock().unwrap().timeouts.len(), if disconnect { 2 } else { 101 });
        }
    }

    #[test]
    fn retry_preparation_precedes_request_and_propagates_send_errors() {
        for fail in [false, true] {
            let factory = FakeFactory::default();
            let mut worker =
                fake_worker(&factory, 2, Box::new(UniformPolicyScheduler::closed_loop()));
            factory.0.lock().unwrap().fail_send = fail;
            let result = worker.handle_retry(
                0,
                RetryRequest {
                    bytes_consumed: 0,
                    original_request_id: 99,
                    is_warmup: false,
                    target_conn_id: Some(0),
                    prepare_commands: vec![b"ASKING".to_vec()],
                    attempt: 0,
                },
                RequestTiming {
                    send_ts: xylem_transport::Timestamp::now(),
                    is_warmup: false,
                    origin_connection_id: 0,
                },
            );
            if fail {
                assert!(result.is_err());
                assert_eq!(
                    worker.protocols[&0].next_id, 0,
                    "failed preparation must not regenerate"
                );
                assert_eq!(worker.pool.get_state(0).unwrap().pending_count(), 0);
                assert!(factory.0.lock().unwrap().sent.is_empty());
            } else {
                result.unwrap();
                assert_eq!(
                    factory.0.lock().unwrap().sent,
                    [b"ASKING".to_vec(), 0u64.to_le_bytes().to_vec()]
                );
                assert_eq!(worker.pool.get_state(0).unwrap().pending_count(), 1);
            }
        }
    }

    #[test]
    fn retry_rejects_missing_unknown_wrong_group_and_closed_targets() {
        for (target_conn_id, group_id, closed) in
            [(None, 0, false), (Some(99), 0, false), (Some(0), 1, false), (Some(0), 0, true)]
        {
            let factory = FakeFactory::default();
            let mut worker =
                fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
            if closed {
                worker.pool.close(0).unwrap();
            }
            let result = worker.handle_retry(
                group_id,
                RetryRequest {
                    bytes_consumed: 8,
                    original_request_id: 99,
                    is_warmup: false,
                    target_conn_id,
                    prepare_commands: vec![b"ASKING".to_vec()],
                    attempt: 0,
                },
                RequestTiming {
                    send_ts: xylem_transport::Timestamp::now(),
                    is_warmup: false,
                    origin_connection_id: 0,
                },
            );
            assert!(result.is_err());
            assert!(factory.0.lock().unwrap().sent.is_empty());
            assert!(worker.retry_queue.is_empty());
        }
    }

    #[test]
    fn asking_ack_fragments_do_not_complete_or_stall_pending_request() {
        let factory = FakeFactory::default();
        let mut cluster = xylem_protocols::RedisClusterProtocol::new(Box::new(
            xylem_protocols::FixedCommandSelector::new(xylem_protocols::RedisOp::Get),
        ));
        cluster.register_connection("127.0.0.1:1".parse().unwrap(), 0);
        let mut stats = TupleStatsCollector::default();
        stats.register_group_policy(0, SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 });
        let mut worker = Worker::with_closed_loop(
            &factory,
            cluster,
            stats,
            WorkerConfig {
                target: "127.0.0.1:1".parse().unwrap(),
                duration: Duration::from_millis(100),
                conn_count: 1,
                max_pending_per_conn: 1,
            },
        )
        .unwrap();
        worker.send_requests().unwrap();
        factory
            .0
            .lock()
            .unwrap()
            .responses
            .push_back(Ok(b"-ASK 1 127.0.0.1:1\r\n".to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        assert_eq!(factory.0.lock().unwrap().sent[1], b"*1\r\n$6\r\nASKING\r\n");
        assert_eq!(factory.0.lock().unwrap().sent.len(), 3);
        for fragment in [b"+O".as_slice(), b"K\r\n", b"$-1\r"] {
            factory.0.lock().unwrap().responses.push_back(Ok(fragment.to_vec()));
            worker.process_responses(Duration::ZERO).unwrap();
            assert_eq!(worker.pool.get_state(0).unwrap().pending_count(), 1);
        }
        factory.0.lock().unwrap().responses.push_back(Ok(b"\n".to_vec()));
        worker.process_responses(Duration::ZERO).unwrap();
        assert_eq!(worker.pool.get_state(0).unwrap().pending_count(), 0);
        assert_eq!(worker.stats.aggregate_global(Duration::from_secs(1)).total_requests, 1);
        assert_eq!(
            worker.stats.entries().values().map(|entry| entry.tx_bytes).sum::<u64>(),
            factory.0.lock().unwrap().sent.iter().map(|data| data.len() as u64).sum::<u64>()
        );
        assert_eq!(
            worker.stats.entries().values().map(|entry| entry.rx_bytes).sum::<u64>(),
            b"-ASK 1 127.0.0.1:1\r\n+OK\r\n$-1\r\n".len() as u64
        );
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
        assert_eq!(worker.pool.pick_connection_at(100), Some(0));
        worker.pool.get_state_mut(0).unwrap().on_request_sent(100);
        worker.pool.try_schedule_connection(0, &worker.protocols[&0]);
        let timeout = worker.poll_timeout(100, 5_000_100);
        assert_eq!(timeout, Duration::from_millis(5));
        worker.process_responses(timeout).unwrap();
        assert_eq!(factory.0.lock().unwrap().timeouts, vec![Duration::from_millis(5)]);
        assert_eq!(worker.poll_timeout(5_000_101, 5_000_100), Duration::ZERO);
    }

    #[test]
    fn full_pipeline_wait_is_clamped_to_experiment_end() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        assert_eq!(worker.poll_timeout(100, 5_000_100), Duration::from_millis(5));
    }

    #[test]
    fn drain_uses_extended_parser_records_response_and_polls_tx() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        factory.0.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        assert!(!worker.drain_iteration().unwrap());
        assert_eq!(worker.stats.aggregate_global(Duration::from_secs(1)).total_requests, 1);
        assert_eq!(factory.0.lock().unwrap().tx_polls, 1);
        // Already drained: no extra blocking poll.
        assert!(!worker.drain_iteration().unwrap());
        assert_eq!(factory.0.lock().unwrap().timeouts.len(), 1);
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
            assert!(worker.drain_iteration().is_err());
            assert_eq!(worker.stats.aggregate_global(Duration::from_secs(1)).total_requests, 0);
        }
    }

    #[test]
    fn drain_closes_disconnected_connection() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        factory
            .0
            .lock()
            .unwrap()
            .responses
            .push_back(Err(xylem_transport::Error::Connection("closed".into())));
        assert!(!worker.drain_iteration().unwrap());
        assert!(factory.0.lock().unwrap().closed);
        assert_eq!(worker.pool.get_state(0).unwrap().pending_count(), 0);
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
        worker.run().unwrap();
        assert_eq!(worker.stats.aggregate_global(Duration::from_secs(1)).total_requests, 1);
        assert_eq!(worker.stats.entries().values().map(|entry| entry.rx_bytes).sum::<u64>(), 8);
        assert_eq!(factory.0.lock().unwrap().timeouts.len(), 2);
        assert!(!worker.has_pending());
        assert!(worker.pool.get_state(1).unwrap().is_closed());
    }

    #[test]
    fn drain_follows_redirect_and_waits_for_retry_response() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        worker.protocols.get_mut(&0).unwrap().retry = true;
        factory.0.lock().unwrap().responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        assert!(worker.drain_iteration().unwrap());
        assert_eq!(factory.0.lock().unwrap().sent.len(), 2);
        // FakeProtocol's default regenerate_request assigns the next ID.
        factory.0.lock().unwrap().responses.push_back(Ok(1u64.to_le_bytes().to_vec()));
        assert!(!worker.drain_iteration().unwrap());
        assert_eq!(worker.stats.aggregate_global(Duration::from_secs(1)).total_requests, 1);
        assert_eq!(worker.stats.entries().values().map(|entry| entry.tx_bytes).sum::<u64>(), 16);
        assert_eq!(worker.stats.entries().values().map(|entry| entry.rx_bytes).sum::<u64>(), 16);
    }

    #[cfg(target_os = "linux")]
    struct FeedbackPolicy {
        connection_id: usize,
        feedback: Arc<std::sync::Mutex<Vec<(usize, Duration)>>>,
    }

    #[cfg(target_os = "linux")]
    impl crate::scheduler::Policy for FeedbackPolicy {
        fn next_send_time(&mut self, _: u64) -> Option<u64> {
            None
        }
        fn on_request_sent(&mut self, _: u64) {}
        fn on_response(&mut self, latency: Duration) {
            self.feedback.lock().unwrap().push((self.connection_id, latency));
        }
        fn reset(&mut self) {}
        fn name(&self) -> &'static str {
            "Feedback"
        }
    }

    #[cfg(target_os = "linux")]
    struct FeedbackScheduler(Arc<std::sync::Mutex<Vec<(usize, Duration)>>>);

    #[cfg(target_os = "linux")]
    impl PolicyScheduler for FeedbackScheduler {
        fn assign_policy(&mut self, connection_id: usize) -> Box<dyn crate::scheduler::Policy> {
            Box::new(FeedbackPolicy { connection_id, feedback: self.0.clone() })
        }
        fn name(&self) -> &'static str {
            "Feedback"
        }
    }

    // Exact hardware clock values make queue/preparation/retry timing assertions
    // deterministic. The first attempt's timestamp is the only permitted origin.
    #[cfg(target_os = "linux")]
    fn exercise_queued_redirects<S: StatsRecorder>(stats: S, warmup: bool, hardware_tx: bool) -> S {
        use xylem_transport::{Timestamp, TxTimestampInfo};
        let factory = FakeFactory::default();
        let feedback = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut worker = Worker::new(
            &factory,
            FakeProtocol {
                next_id: 1,
                retry_target: 1,
                retry_prepare: vec![b"ASKING".to_vec()],
                ..Default::default()
            },
            stats,
            WorkerConfig {
                target: "127.0.0.1:1".parse().unwrap(),
                duration: Duration::from_secs(1),
                conn_count: 2,
                max_pending_per_conn: 1,
            },
            Box::new(FeedbackScheduler(feedback.clone())),
        )
        .unwrap();
        factory.0.lock().unwrap().timestamp = Timestamp::from_hardware(1, 100);
        worker.pool.send(0, &0u64.to_le_bytes(), 0, warmup).unwrap();
        if !warmup {
            worker.stats.record_tx_bytes(0, 0, 8);
        }
        // A warmup request occupies the redirect destination's only slot.
        worker.pool.send(1, &100u64.to_le_bytes(), 100, true).unwrap();
        {
            let mut io = factory.0.lock().unwrap();
            io.timestamp = Timestamp::from_hardware(1, 300);
            if hardware_tx {
                io.tx_timestamps.push(TxTimestampInfo {
                    conn_id: 0,
                    tx_offset: 1,
                    timestamp: Timestamp::from_hardware(1, 200),
                });
            }
            io.responses.push_back(Ok(0u64.to_le_bytes().to_vec()));
        }
        worker.protocols.get_mut(&0).unwrap().retry = true;
        assert!(worker.drain_iteration().unwrap());
        assert!(feedback.lock().unwrap().is_empty(), "redirect is not feedback");
        assert_eq!(worker.retry_queue.len(), 1);
        assert_eq!(worker.pool.get_state(0).unwrap().pending_count(), 0);
        assert_eq!(factory.0.lock().unwrap().sent.len(), 2, "preparation must wait for capacity");
        assert!(
            worker.drain_iteration().unwrap(),
            "queued retry remains pending without new responses"
        );
        worker.send_requests().unwrap();
        assert_eq!(factory.0.lock().unwrap().sent.len(), 2, "queued retries precede new work");
        {
            let mut io = factory.0.lock().unwrap();
            io.timestamp = Timestamp::from_hardware(1, 500);
            io.response_connection_id = 1;
            io.responses.push_back(Ok(100u64.to_le_bytes().to_vec()));
        }
        assert!(worker.drain_iteration().unwrap());
        assert!(worker.retry_queue.is_empty());
        assert_eq!(factory.0.lock().unwrap().sent.len(), 4);
        {
            let mut io = factory.0.lock().unwrap();
            io.timestamp = Timestamp::from_hardware(1, 700);
            // This timestamp covers the retry, but must not reset logical timing.
            io.tx_timestamps.push(TxTimestampInfo {
                conn_id: 1,
                tx_offset: 4,
                timestamp: Timestamp::from_hardware(1, 600),
            });
            io.responses.push_back(Ok(1u64.to_le_bytes().to_vec()));
        }
        worker.protocols.get_mut(&0).unwrap().retry = true;
        assert!(worker.drain_iteration().unwrap());
        assert!(feedback.lock().unwrap().is_empty(), "repeated redirects are not feedback");
        {
            let mut io = factory.0.lock().unwrap();
            io.timestamp = Timestamp::from_hardware(1, 1000);
            io.tx_timestamps.push(TxTimestampInfo {
                conn_id: 1,
                tx_offset: 6,
                timestamp: Timestamp::from_hardware(1, 800),
            });
            io.responses.push_back(Ok(2u64.to_le_bytes().to_vec()));
        }
        assert!(!worker.drain_iteration().unwrap());
        let expected = if warmup {
            vec![]
        } else {
            vec![(0, Duration::from_nanos(if hardware_tx { 800 } else { 900 }))]
        };
        assert_eq!(
            *feedback.lock().unwrap(),
            expected,
            "only the original policy receives end-to-end feedback"
        );
        worker.into_stats()
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn queued_repeated_redirects_preserve_group_count_timing_bytes_and_warmup() {
        for (warmup, hardware_tx) in [(false, false), (false, true), (true, false), (true, true)] {
            let mut stats =
                TupleStatsCollector::new(SamplingPolicy::Unlimited, Duration::from_secs(1));
            stats.register_group_policy(0, SamplingPolicy::Unlimited);
            let stats = exercise_queued_redirects(stats, warmup, hardware_tx);
            let duration = Duration::from_secs(1);
            let global = stats.aggregate_global(duration);
            assert_eq!(global.total_requests, u64::from(!warmup));
            // 36 TX bytes plus 24 RX bytes over one second.
            assert_eq!(global.throughput_mbps, if warmup { 0.0 } else { 0.00048 });
            let group_entries: Vec<_> = stats
                .entries()
                .iter()
                .filter(|(key, _)| key.group_id == 0)
                .map(|(_, entry)| entry)
                .collect();
            assert_eq!(
                group_entries.iter().map(|entry| entry.request_count).sum::<u64>(),
                u64::from(!warmup)
            );
            assert_eq!(
                group_entries.iter().map(|entry| entry.tx_bytes).sum::<u64>(),
                if warmup { 0 } else { 36 }
            );
            assert_eq!(
                group_entries.iter().map(|entry| entry.rx_bytes).sum::<u64>(),
                if warmup { 0 } else { 24 }
            );
            assert_eq!(
                group_entries.iter().map(|entry| entry.latency.count()).sum::<u64>(),
                u64::from(!warmup)
            );
            if !warmup {
                // A single stored sample: its median and mean equal the expected latency.
                let expected = Duration::from_nanos(if hardware_tx { 800 } else { 900 });
                let group = &stats.aggregate_by_group(duration)[&0];
                for aggregated in [&global, group] {
                    assert_eq!(aggregated.latency_p50, expected);
                    assert_eq!(aggregated.mean_latency, expected);
                }
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn queued_repeated_redirects_preserve_tuple_count_timing_and_target_bytes() {
        for (warmup, hardware_tx) in [(false, false), (false, true), (true, false), (true, true)] {
            let stats = crate::stats::TupleStatsCollector::new(
                crate::stats::SamplingPolicy::Unlimited,
                Duration::from_secs(1),
            );
            let stats = exercise_queued_redirects(stats, warmup, hardware_tx);
            assert_eq!(
                stats.aggregate_global(Duration::from_secs(1)).total_requests,
                u64::from(!warmup)
            );
            for connection_id in 0..2 {
                let entries: Vec<_> = stats
                    .entries()
                    .iter()
                    .filter(|(key, _)| key.connection_id == connection_id)
                    .map(|(_, entry)| entry)
                    .collect();
                assert_eq!(
                    entries.iter().map(|entry| entry.request_count).sum::<u64>(),
                    u64::from(!warmup && connection_id == 0)
                );
                assert_eq!(
                    entries.iter().map(|entry| entry.tx_bytes).sum::<u64>(),
                    if warmup {
                        0
                    } else if connection_id == 0 {
                        8
                    } else {
                        28
                    }
                );
                assert_eq!(
                    entries.iter().map(|entry| entry.rx_bytes).sum::<u64>(),
                    if warmup {
                        0
                    } else if connection_id == 0 {
                        8
                    } else {
                        16
                    }
                );
            }
            if !warmup {
                assert_eq!(
                    stats.aggregate_global(Duration::from_secs(1)).mean_latency,
                    Duration::from_nanos(if hardware_tx { 800 } else { 900 })
                );
            }
        }
    }

    #[test]
    fn bounded_drain_reports_unfinished_requests_and_keeps_retry_queue() {
        let factory = FakeFactory::default();
        let mut worker = fake_worker(&factory, 1, Box::new(UniformPolicyScheduler::closed_loop()));
        worker.send_requests().unwrap();
        worker.retry_queue.push((
            0,
            RetryRequest {
                bytes_consumed: 8,
                original_request_id: 99,
                is_warmup: false,
                target_conn_id: Some(0),
                prepare_commands: vec![],
                attempt: 0,
            },
            RequestTiming {
                send_ts: xylem_transport::Timestamp::now(),
                is_warmup: false,
                origin_connection_id: 0,
            },
        ));
        worker.drain_responses().unwrap();
        assert!(worker.has_pending());
        assert_eq!(worker.retry_queue.len(), 1);
        assert_eq!(factory.0.lock().unwrap().timeouts.len(), 100);
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
            assert_eq!(worker.pool.pick_connection_at(100), Some(0));
            worker.pool.get_state_mut(0).unwrap().on_request_sent(100);
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
                assert_eq!(next, 10_000_100, "warmup must not change the adaptive rate");
                assert_eq!(worker.stats.aggregate_global(Duration::from_secs(1)).total_requests, 0);
            } else {
                assert_eq!(next, 9_091_009, "100 zero-latency responses increase rate to 110/s");
                assert_eq!(
                    worker.stats.aggregate_global(Duration::from_secs(1)).total_requests,
                    100
                );
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
