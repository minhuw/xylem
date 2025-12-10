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
use crate::stats::{GroupStatsCollector, StatsRecorder};
use crate::timing;
use crate::Result;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use xylem_common::Request;
use xylem_transport::{ConnectionGroup, TransportFactory};

/// Retry request information for protocol-level retries
#[derive(Debug, Clone)]
pub struct RetryRequest<ReqId> {
    /// Number of bytes consumed from the response buffer
    pub bytes_consumed: usize,
    /// Original request ID that triggered this retry (protocol uses this to look up request data)
    pub original_request_id: ReqId,
    /// Whether the original request was a warmup request
    pub is_warmup: bool,
    /// Target connection ID for the retry (None = use routing logic)
    pub target_conn_id: Option<usize>,
    /// Preparation commands to send before the retry (e.g., ASKING for Redis Cluster)
    pub prepare_commands: Vec<Vec<u8>>,
    /// Retry attempt number (0 for first retry)
    pub attempt: usize,
}

/// Extended parse result that supports retries
#[derive(Debug)]
pub enum ParseResult<ReqId> {
    /// Complete response parsed successfully
    Complete {
        /// Number of bytes consumed from buffer
        bytes_consumed: usize,
        /// Request ID this response corresponds to
        request_id: ReqId,
    },
    /// Response incomplete, need more data
    Incomplete,
    /// Retry needed (e.g., cluster redirect)
    Retry(RetryRequest<ReqId>),
}

/// Protocol trait for generating requests and parsing responses with request ID tracking
pub trait Protocol: Send {
    /// Type of request ID used by this protocol
    type RequestId: Eq + std::hash::Hash + Clone + Copy + std::fmt::Debug;

    /// Generate the next request for a connection
    ///
    /// The protocol generates the request based on its internal state
    /// (command selector, key generator, value size, etc.)
    ///
    /// # Arguments
    /// * `conn_id` - Connection identifier
    ///
    /// # Returns
    /// A Request containing data, ID, and metadata where metadata indicates
    /// whether this is a warm-up request (stats should not be collected)
    fn next_request(&mut self, conn_id: usize) -> Request<Self::RequestId>;

    /// Regenerate a request for retry using the original request ID
    ///
    /// The protocol looks up any stored metadata for the original request
    /// and generates a new request with the same parameters.
    /// This is used for retry scenarios (e.g., cluster redirects).
    ///
    /// # Arguments
    /// * `conn_id` - Target connection ID for the retry
    /// * `original_request_id` - The request ID from the original request
    ///
    /// # Returns
    /// A new Request with data, ID, and metadata
    ///
    /// Default implementation just generates a new request (ignores original).
    /// Retries are always measurement requests (not warmup).
    fn regenerate_request(
        &mut self,
        conn_id: usize,
        _original_request_id: Self::RequestId,
    ) -> Request<Self::RequestId> {
        let request = self.next_request(conn_id);
        // Retries are always measurement requests
        Request::measurement(request.data, request.request_id)
    }

    /// Parse a response and return the request ID it corresponds to
    /// Returns Ok((bytes_consumed, Some(request_id))) if complete response found
    /// Returns Ok((0, None)) if response is incomplete
    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> anyhow::Result<(usize, Option<Self::RequestId>)>;

    /// Check if the protocol is ready to send a request on this connection
    ///
    /// This allows protocols to impose their own constraints beyond pipeline depth.
    /// For example, protocols that require handshakes (like Masstree) can return
    /// false until the handshake completes, preventing duplicate handshake requests
    /// from filling the pipeline with the same request ID.
    ///
    /// # Arguments
    /// * `conn_id` - Connection identifier
    ///
    /// # Returns
    /// true if the protocol is ready to generate a request for this connection
    ///
    /// Default implementation always returns true (no protocol-level constraints).
    fn can_send(&self, _conn_id: usize) -> bool {
        true
    }

    /// Parse a response with retry support (extended version)
    ///
    /// Default implementation delegates to `parse_response()` for backward compatibility.
    fn parse_response_extended(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> anyhow::Result<ParseResult<Self::RequestId>> {
        match self.parse_response(conn_id, data)? {
            (bytes_consumed, Some(request_id)) => {
                Ok(ParseResult::Complete { bytes_consumed, request_id })
            }
            (0, None) => Ok(ParseResult::Incomplete),
            (bytes_consumed, None) => {
                if bytes_consumed > 0 {
                    Err(anyhow::anyhow!(
                        "Protocol consumed {} bytes but returned no request ID",
                        bytes_consumed
                    ))
                } else {
                    Ok(ParseResult::Incomplete)
                }
            }
        }
    }

    /// Protocol name
    fn name(&self) -> &'static str;

    /// Reset protocol state
    fn reset(&mut self);
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
}

/// Worker with connection pooling and request pipelining
///
/// Uses `ConnectionGroup` internally for efficient I/O multiplexing.
///
/// The worker is generic over the stats collector type `S` which must implement
/// `StatsRecorder`. This allows using either:
/// - `GroupStatsCollector` - aggregates by traffic group only (default)
/// - `TupleStatsCollector` - aggregates by (time_bucket, group_id, connection_id)
pub struct Worker<G: ConnectionGroup, P: Protocol, S: StatsRecorder = GroupStatsCollector> {
    pool: ConnectionPool<G, P::RequestId>,
    /// Map of traffic group ID to protocol instance
    protocols: HashMap<usize, P>,
    stats: S,
    config: WorkerConfig,
    /// Optional request dumper for recording all requests
    request_dumper: Option<Arc<RequestDumper>>,
    /// Thread ID for request dumping
    thread_id: usize,
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

        Ok(Self {
            pool,
            protocols,
            stats,
            config,
            request_dumper: None,
            thread_id: 0,
        })
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

        Ok(Self {
            pool,
            protocols,
            stats,
            config,
            request_dumper: None,
            thread_id: 0,
        })
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

        Ok(Self {
            pool,
            protocols,
            stats,
            config,
            request_dumper: None,
            thread_id: 0,
        })
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
        let duration_ns = self.config.duration.as_nanos() as u64;

        loop {
            let now_ns = timing::time_ns();

            // Check if duration exceeded
            if now_ns >= start_ns + duration_ns {
                break;
            }

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
                let protocol =
                    self.protocols.get(&group_id).expect("Protocol not found for group_id");
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

            // RECEIVE PHASE: Poll all connections for responses (single syscall)
            // Derive sleep duration from the ready heap (when next connection will be ready)
            let now_ns = timing::time_ns();

            // Calculate wait time based on connection policies
            let pool_wait_ns = match self.pool.peek_next_ready_time() {
                Some(next_ready_ns) if next_ready_ns > now_ns => Some(next_ready_ns - now_ns),
                Some(_) => Some(0), // Ready now
                None => None,       // Heap empty (all pipelines full)
            };

            let poll_timeout = match pool_wait_ns {
                Some(0) => Some(Duration::ZERO),
                Some(ns) if ns <= 1_000_000 => {
                    // Within 1ms: spin with non-blocking poll for precise timing
                    Some(Duration::ZERO)
                }
                Some(ns) => {
                    // Beyond 1ms: sleep until 1ms before, then spin for precision
                    Some(Duration::from_nanos(ns - 1_000_000))
                }
                None => {
                    // Heap empty - block on poll until responses arrive, bounded by experiment duration
                    let remaining_ns = (start_ns + duration_ns).saturating_sub(now_ns);
                    if remaining_ns > 0 {
                        Some(Duration::from_nanos(remaining_ns))
                    } else {
                        Some(Duration::ZERO)
                    }
                }
            };
            self.process_responses(poll_timeout)?;
        }

        // Drain remaining responses
        self.drain_responses()?;

        // Close all connections
        self.pool.close_all()?;

        Ok(())
    }

    /// Process responses from all connections
    fn process_responses(&mut self, timeout: Option<Duration>) -> Result<()> {
        // Poll all connections with one syscall
        let ready_ids = self.pool.poll(timeout)?;

        // Poll for TX timestamps from error queue (non-blocking)
        // Must be called AFTER poll() because TX timestamps arrive asynchronously
        // and may not be available until after the NIC has transmitted the data
        if let Err(e) = self.pool.poll_tx_timestamps() {
            tracing::trace!("Error polling TX timestamps: {}", e);
        }

        // Clean up stale TX queue entries (entries older than 1 minute)
        // This prevents memory leaks when TX timestamps are never received
        self.pool.cleanup_stale_tx_queues();

        // Collect retries to process after response handling
        let mut retries: Vec<(usize, RetryRequest<_>)> = Vec::new();

        for connection_id in ready_ids {
            let state = match self.pool.get_state(connection_id) {
                Some(s) => s,
                None => continue,
            };

            // Skip connections with no pending requests
            if state.pending_count() == 0 {
                continue;
            }

            let group_id = state.group_id();

            let mut connection_retries = Vec::new();
            let mut response_warmup: HashMap<P::RequestId, bool> = HashMap::new();

            // Parse responses (scope the mutable protocol borrow)
            #[allow(clippy::excessive_nesting)]
            let responses = {
                let protocol =
                    self.protocols.get_mut(&group_id).expect("Protocol not found for group_id");

                match self.pool.recv_responses(connection_id, |data| {
                    match protocol.parse_response_extended(connection_id, data) {
                        Ok(ParseResult::Complete { bytes_consumed, request_id }) => {
                            Ok((bytes_consumed, Some(request_id)))
                        }
                        Ok(ParseResult::Incomplete) => Ok((0, None)),
                        Ok(ParseResult::Retry(retry_req)) => {
                            connection_retries.push(retry_req.clone());
                            Ok((retry_req.bytes_consumed, Some(retry_req.original_request_id)))
                        }
                        Err(e) => Err(e.into()),
                    }
                }) {
                    Ok(responses) => responses,
                    Err(e) => {
                        if let crate::Error::Connection(_) = e {
                            let _ = self.pool.close(connection_id);
                            continue;
                        }
                        return Err(e);
                    }
                }
            };

            // Record latencies and RX bytes with group_id and connection_id (skip warmup requests)
            for (req_id, latency, is_warmup, bytes_consumed) in &responses {
                if *is_warmup {
                    response_warmup.insert(*req_id, true);
                    continue;
                }
                response_warmup.insert(*req_id, false);
                if *bytes_consumed > 0 {
                    self.stats.record_rx_bytes(group_id, connection_id, *bytes_consumed);
                }
                self.stats.record_latency(group_id, connection_id, *latency);
            }

            // Reschedule connection if it can send but isn't on heap
            // This handles two cases:
            // 1. Pipeline transitioned from full to available
            // 2. Protocol became ready (e.g., handshake completed)
            let protocol = self.protocols.get(&group_id).expect("Protocol not found for group_id");
            self.pool.try_schedule_connection(connection_id, protocol);

            // Collect retries for this connection
            for mut retry in connection_retries {
                if let Some(is_warmup) = response_warmup.get(&retry.original_request_id) {
                    retry.is_warmup = *is_warmup;
                }
                retries.push((group_id, retry));
            }
        }

        // Process all retries after response handling
        for (group_id, retry_req) in retries {
            self.handle_retry(group_id, retry_req)?;
        }

        Ok(())
    }

    /// Handle a retry request by sending preparation commands and retry request
    fn handle_retry(
        &mut self,
        group_id: usize,
        retry_req: RetryRequest<P::RequestId>,
    ) -> Result<()> {
        let target_connection_id = match retry_req.target_conn_id {
            Some(connection_id) => connection_id,
            None => return Ok(()), // Skip retry if no target specified
        };

        // Check connection exists and belongs to the group
        let state = match self.pool.get_state(target_connection_id) {
            Some(s) if s.group_id() == group_id => s,
            _ => return Ok(()),
        };

        if state.is_closed() {
            return Ok(());
        }

        // Send preparation commands
        for prep_cmd in &retry_req.prepare_commands {
            let _ = self.pool.send_untracked(target_connection_id, prep_cmd);
        }

        // Generate and send retry request using the original request ID
        let protocol = self.protocols.get_mut(&group_id).expect("Protocol not found for group_id");

        let mut request =
            protocol.regenerate_request(target_connection_id, retry_req.original_request_id);
        if retry_req.is_warmup {
            request.metadata.is_warmup = true;
        }

        // Record retry request to dump file if enabled
        if let Some(ref dumper) = self.request_dumper {
            let req_id_str = format!("{:?}_retry{}", request.request_id, retry_req.attempt);
            self.record_request(dumper, group_id, target_connection_id, &request.data, &req_id_str);
        }

        // Send the retry request
        // Note: We intentionally do NOT call state.on_request_sent() or generator.mark_request_sent()
        // because retries are already counted as in-flight from the original request. Double-counting
        // would skew rate limiting and statistics (especially for Redis Cluster MOVED/ASK redirects).
        self.pool.send(
            target_connection_id,
            &request.data,
            request.request_id,
            request.metadata.is_warmup,
        )?;
        if !request.metadata.is_warmup {
            self.stats.record_tx_bytes(group_id, target_connection_id, request.data.len());
        }

        Ok(())
    }

    /// Drain any remaining responses after measurement period
    fn drain_responses(&mut self) -> Result<()> {
        for _ in 0..100 {
            let has_pending = self.drain_iteration()?;
            if !has_pending {
                break;
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        Ok(())
    }

    /// Single drain iteration - returns true if any connections have pending requests
    fn drain_iteration(&mut self) -> Result<bool> {
        let mut any_pending = false;

        // Check if any connections have pending requests
        for &connection_id in self.pool.connection_ids() {
            if let Some(state) = self.pool.get_state(connection_id) {
                if state.pending_count() > 0 {
                    any_pending = true;
                    break;
                }
            }
        }

        if !any_pending {
            return Ok(false);
        }

        // Poll for ready connections
        let ready_ids = self.pool.poll(Some(Duration::from_millis(10)))?;

        for connection_id in ready_ids {
            let state = match self.pool.get_state(connection_id) {
                Some(s) => s,
                None => continue,
            };

            if state.pending_count() == 0 {
                continue;
            }

            let group_id = state.group_id();
            let protocol =
                self.protocols.get_mut(&group_id).expect("Protocol not found for group_id");

            #[allow(clippy::excessive_nesting)]
            if let Ok(responses) = self.pool.recv_responses(connection_id, |data| {
                match protocol.parse_response(connection_id, data) {
                    Ok((bytes_consumed, req_id_opt)) => Ok((bytes_consumed, req_id_opt)),
                    Err(e) => Err(e.into()),
                }
            }) {
                for (_req_id, latency, is_warmup, bytes_consumed) in responses {
                    if is_warmup {
                        continue;
                    }
                    if bytes_consumed > 0 {
                        self.stats.record_rx_bytes(group_id, connection_id, bytes_consumed);
                    }
                    self.stats.record_latency(group_id, connection_id, latency);
                }
            }
        }

        Ok(any_pending)
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

        Ok(Self {
            pool,
            protocols,
            stats,
            config,
            request_dumper: None,
            thread_id: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use xylem_transport::TcpTransportFactory;

    // Protocol adapter
    struct ProtocolAdapter<P: xylem_protocols::Protocol> {
        inner: P,
    }

    impl<P: xylem_protocols::Protocol> ProtocolAdapter<P> {
        fn new(protocol: P) -> Self {
            Self { inner: protocol }
        }
    }

    impl<P: xylem_protocols::Protocol> Protocol for ProtocolAdapter<P> {
        type RequestId = P::RequestId;

        fn next_request(&mut self, conn_id: usize) -> Request<Self::RequestId> {
            self.inner.next_request(conn_id)
        }

        fn regenerate_request(
            &mut self,
            conn_id: usize,
            original_request_id: Self::RequestId,
        ) -> Request<Self::RequestId> {
            self.inner.regenerate_request(conn_id, original_request_id)
        }

        fn parse_response(
            &mut self,
            conn_id: usize,
            data: &[u8],
        ) -> anyhow::Result<(usize, Option<Self::RequestId>)> {
            self.inner.parse_response(conn_id, data)
        }

        fn parse_response_extended(
            &mut self,
            conn_id: usize,
            data: &[u8],
        ) -> anyhow::Result<ParseResult<Self::RequestId>> {
            match self.inner.parse_response_extended(conn_id, data)? {
                xylem_protocols::ParseResult::Complete { bytes_consumed, request_id } => {
                    Ok(ParseResult::Complete { bytes_consumed, request_id })
                }
                xylem_protocols::ParseResult::Incomplete => Ok(ParseResult::Incomplete),
                xylem_protocols::ParseResult::Retry(r) => Ok(ParseResult::Retry(RetryRequest {
                    bytes_consumed: r.bytes_consumed,
                    original_request_id: r.original_request_id,
                    is_warmup: r.is_warmup,
                    target_conn_id: r.target_conn_id,
                    prepare_commands: r.prepare_commands,
                    attempt: r.attempt,
                })),
            }
        }

        fn name(&self) -> &'static str {
            self.inner.name()
        }

        fn reset(&mut self) {
            self.inner.reset()
        }

        fn can_send(&self, conn_id: usize) -> bool {
            self.inner.can_send(conn_id)
        }
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
        let echo_protocol = xylem_protocols::xylem_echo::XylemEchoProtocol::new(0);
        let protocol = ProtocolAdapter::new(echo_protocol);
        let mut stats = GroupStatsCollector::default();
        stats.register_group_legacy(0, 100000, 1.0);
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
        assert!(stats.global().tx_requests() > 0, "Should have sent requests");
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
        let echo_protocol = xylem_protocols::xylem_echo::XylemEchoProtocol::new(0);
        let protocol = ProtocolAdapter::new(echo_protocol);

        let mut stats = GroupStatsCollector::default();
        stats.register_group_legacy(0, 100000, 1.0);
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
        let actual = stats.global().tx_requests();
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
