//! Worker implementation with connection pooling and request pipelining
//!
//! This implements Lancet's symmetric agent model, allowing multiple outstanding
//! requests per connection for maximum throughput with accurate latency tracking.
//!
//! The worker can operate in different modes based on configuration:
//! - Single connection, single pending request: Simple closed-loop latency measurement
//! - Multiple connections with pipelining: High-throughput load testing

use crate::connection::{ConnectionPool, TrafficGroupConfig};
use crate::stats::GroupStatsCollector;
use crate::timing;
use crate::workload::RequestGenerator;
use crate::Result;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::time::Duration;
use xylem_transport::Transport;

/// Helper function to compute hash of a string key
fn hash_string_key(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Retry request information for protocol-level retries
#[derive(Debug, Clone)]
pub struct RetryRequest<ReqId> {
    /// Number of bytes consumed from the response buffer
    pub bytes_consumed: usize,
    /// Original request ID that triggered this retry
    pub original_request_id: ReqId,
    /// Key to use for the retry request
    pub key: u64,
    /// Value size for the retry request
    pub value_size: usize,
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

    /// Generate a request with an ID
    /// Returns (request_data, request_id)
    fn generate_request(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
    ) -> (Vec<u8>, Self::RequestId);

    /// Generate a SET request with imported data (string key + value bytes)
    /// Default implementation panics - protocols that support imported data should override this
    fn generate_set_with_imported_data(
        &mut self,
        _conn_id: usize,
        _key: &str,
        _value: &[u8],
    ) -> (Vec<u8>, Self::RequestId) {
        unimplemented!("generate_set_with_imported_data not supported by this protocol")
    }

    /// Parse a response and return the request ID it corresponds to
    /// Returns Ok((bytes_consumed, Some(request_id))) if complete response found
    /// Returns Ok((0, None)) if response is incomplete
    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> anyhow::Result<(usize, Option<Self::RequestId>)>;

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
                // Consumed bytes but no request ID - shouldn't happen normally
                if bytes_consumed > 0 {
                    // This is an error condition - consumed data but got no ID
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
    /// Value size for requests
    pub value_size: usize,
    /// Number of connections per worker
    pub conn_count: usize,
    /// Maximum pending requests per connection
    pub max_pending_per_conn: usize,
}

/// Worker with connection pooling and request pipelining
pub struct Worker<T: Transport, P: Protocol> {
    pool: ConnectionPool<T, P::RequestId>,
    /// Map of traffic group ID to protocol instance
    protocols: HashMap<usize, P>,
    generator: RequestGenerator,
    stats: GroupStatsCollector,
    config: WorkerConfig,
    /// Track keys for scheduler feedback
    current_key: u64,
}

impl<T: Transport, P: Protocol> Worker<T, P> {
    /// Create a new pipelined worker with per-connection policies
    pub fn new(
        transport_factory: impl Fn() -> T,
        protocol: P,
        generator: RequestGenerator,
        stats: GroupStatsCollector,
        config: WorkerConfig,
        policy_scheduler: Box<dyn crate::scheduler::PolicyScheduler>,
    ) -> Result<Self> {
        let pool = ConnectionPool::new(
            transport_factory,
            config.target,
            config.conn_count,
            config.max_pending_per_conn,
            policy_scheduler,
            0, // Default group_id for legacy single-pool workers
        )?;

        let mut protocols = HashMap::new();
        protocols.insert(0, protocol); // Single protocol uses group_id 0

        Ok(Self {
            pool,
            protocols,
            generator,
            stats,
            config,
            current_key: 0,
        })
    }

    /// Create a new pipelined worker with closed-loop policy (max throughput)
    pub fn with_closed_loop(
        transport_factory: impl Fn() -> T,
        protocol: P,
        generator: RequestGenerator,
        stats: GroupStatsCollector,
        config: WorkerConfig,
    ) -> Result<Self> {
        let policy_scheduler = Box::new(crate::scheduler::UniformPolicyScheduler::closed_loop());
        Self::new(transport_factory, protocol, generator, stats, config, policy_scheduler)
    }

    /// Create a new pipelined worker with multiple traffic groups
    pub fn new_multi_group(
        transport_factory: impl Fn() -> T,
        protocols: HashMap<usize, P>,
        generator: RequestGenerator,
        stats: GroupStatsCollector,
        config: WorkerConfig,
        groups: Vec<(usize, usize, usize, Box<dyn crate::scheduler::PolicyScheduler>)>,
    ) -> Result<Self> {
        let pool = ConnectionPool::new_multi_group(transport_factory, config.target, groups)?;

        Ok(Self {
            pool,
            protocols,
            generator,
            stats,
            config,
            current_key: 0,
        })
    }

    /// Create a new pipelined worker with multiple traffic groups, each with its own target
    ///
    /// # Parameters
    /// - `groups`: Vec of (group_id, target, conn_count, max_pending_per_conn, policy_scheduler)
    pub fn new_multi_group_with_targets(
        transport_factory: impl Fn() -> T,
        protocols: HashMap<usize, P>,
        generator: RequestGenerator,
        stats: GroupStatsCollector,
        config: WorkerConfig,
        groups: Vec<TrafficGroupConfig>,
    ) -> Result<Self> {
        let pool = ConnectionPool::new_multi_group_with_targets(transport_factory, groups)?;

        Ok(Self {
            pool,
            protocols,
            generator,
            stats,
            config,
            current_key: 0,
        })
    }

    /// Run the pipelined measurement loop (Lancet symmetric_tcp_main style)
    pub fn run(&mut self) -> Result<()> {
        let start_ns = timing::time_ns();
        let duration_ns = self.config.duration.as_nanos() as u64;
        let mut next_tx_ns = start_ns;

        loop {
            let now_ns = timing::time_ns();

            // Check if duration exceeded
            if now_ns >= start_ns + duration_ns {
                break;
            }

            // SEND PHASE: Send requests while we're ahead of schedule
            while now_ns >= next_tx_ns {
                // Pick a connection that can accept more requests
                let conn = match self.pool.pick_connection() {
                    Some(c) => c,
                    None => break, // No available connections, move to receive phase
                };

                let conn_id = conn.idx();
                let group_id = conn.group_id();

                // Look up protocol for this connection's traffic group
                let protocol =
                    self.protocols.get_mut(&group_id).expect("Protocol not found for group_id");

                // Generate request - check if using imported data
                let (request_data, req_id) = if self.generator.is_using_imported_data() {
                    let (key_string, value_bytes) = self.generator.next_request_from_import();
                    // Use string key hash for scheduler (not perfect but works)
                    self.current_key = hash_string_key(&key_string);
                    protocol.generate_set_with_imported_data(conn_id, &key_string, &value_bytes)
                } else {
                    // Normal synthetic data generation
                    let (key, value_size) = self.generator.next_request();
                    self.current_key = key;
                    protocol.generate_request(conn_id, key, value_size)
                };

                // Send request and notify policy
                let sent_time_ns = timing::time_ns();
                let group_id = conn.group_id();
                conn.send(&request_data, req_id)?;
                conn.on_request_sent(sent_time_ns);
                self.stats.record_tx_bytes(group_id, request_data.len());

                // Mark request sent for rate control
                self.generator.mark_request_sent();

                // Schedule next transmission
                if let Some(delay) = self.generator.delay_until_next() {
                    next_tx_ns = timing::time_ns() + delay.as_nanos() as u64;
                } else {
                    // Closed-loop: send immediately
                    next_tx_ns = timing::time_ns();
                }

                // Update current time
                let now_ns = timing::time_ns();

                // Break if we've caught up or exceeded the schedule
                if now_ns < next_tx_ns {
                    break;
                }
            }

            // RECEIVE PHASE: Poll all connections for responses
            self.process_responses()?;

            // Yield to scheduler
            std::hint::spin_loop();
        }

        // Drain remaining responses
        self.drain_responses()?;

        // Close all connections
        self.pool.close_all()?;

        Ok(())
    }

    /// Process responses from all connections
    fn process_responses(&mut self) -> Result<()> {
        let stats = &mut self.stats;

        // Collect retries to process after response handling
        let mut retries: Vec<(usize, RetryRequest<_>)> = Vec::new();

        for conn in self.pool.connections_mut() {
            // Skip connections with no pending requests
            if conn.pending_count() == 0 {
                continue;
            }

            if !conn.poll_readable()? {
                continue;
            }
            let conn_id = conn.idx();
            let group_id = conn.group_id();

            // Look up protocol for this connection's traffic group
            let protocol =
                self.protocols.get_mut(&group_id).expect("Protocol not found for group_id");

            // Use extended parse to support retries
            let mut conn_retries = Vec::new();

            let latencies = match conn.recv_responses(|data| {
                let result = protocol.parse_response_extended(conn_id, data);
                match result {
                    Ok(ParseResult::Complete { bytes_consumed, request_id }) => {
                        // Record received bytes if any
                        (bytes_consumed > 0)
                            .then(|| stats.record_rx_bytes(group_id, bytes_consumed));
                        Ok((bytes_consumed, Some(request_id)))
                    }
                    Ok(ParseResult::Incomplete) => Ok((0, None)),
                    Ok(ParseResult::Retry(retry_req)) => {
                        // Save retry for processing after response handling
                        conn_retries.push(retry_req.clone());
                        // Record received bytes if any
                        (retry_req.bytes_consumed > 0)
                            .then(|| stats.record_rx_bytes(group_id, retry_req.bytes_consumed));
                        // Return the original request ID to complete the pending request
                        Ok((retry_req.bytes_consumed, Some(retry_req.original_request_id)))
                    }
                    Err(e) => Err(e.into()),
                }
            }) {
                Ok(latencies) => latencies,
                Err(e) => {
                    // Handle connection errors gracefully
                    if let crate::Error::Connection(_) = e {
                        // Connection closed - mark it and continue
                        let _ = conn.close();
                        continue;
                    }
                    // Other errors are fatal
                    return Err(e);
                }
            };

            // Record all latencies with group_id
            for (_req_id, latency) in latencies {
                stats.record_latency(group_id, latency);
            }

            // Collect retries for this connection
            for retry in conn_retries {
                retries.push((group_id, retry));
            }
        }

        // Process all retries after response handling
        for (group_id, retry_req) in retries {
            self.handle_retry(group_id, retry_req)?;
        }

        // Per-connection policies don't need global feedback
        // Each connection manages its own timing independently

        Ok(())
    }

    /// Handle a retry request by sending preparation commands and retry request
    fn handle_retry(
        &mut self,
        group_id: usize,
        retry_req: RetryRequest<P::RequestId>,
    ) -> Result<()> {
        // Determine target connection
        let target_conn_id = if let Some(conn_id) = retry_req.target_conn_id {
            conn_id
        } else {
            // Use original connection or first connection in group
            // This is a simplified routing - proper routing would use protocol logic
            return Ok(()); // Skip retry if no target specified
        };

        // Get the connection
        let conn = self
            .pool
            .connections_mut()
            .iter_mut()
            .find(|c| c.idx() == target_conn_id && c.group_id() == group_id);

        if let Some(conn) = conn {
            // Send preparation commands (e.g., ASKING for Redis Cluster)
            for prep_cmd in &retry_req.prepare_commands {
                // Send without tracking (preparation commands don't get IDs)
                let _ = conn.send_untracked(prep_cmd);
            }

            // Generate and send retry request
            let protocol =
                self.protocols.get_mut(&group_id).expect("Protocol not found for group_id");

            let (retry_data, retry_req_id) =
                protocol.generate_request(target_conn_id, retry_req.key, retry_req.value_size);

            conn.send(&retry_data, retry_req_id)?;
            self.stats.record_tx_bytes(group_id, retry_data.len());
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
        let stats = &mut self.stats;

        for conn in self.pool.connections_mut() {
            if conn.pending_count() == 0 {
                continue;
            }

            any_pending = true;

            if !conn.poll_readable()? {
                continue;
            }

            let conn_id = conn.idx();
            let group_id = conn.group_id();

            // Look up protocol for this connection's traffic group
            let protocol =
                self.protocols.get_mut(&group_id).expect("Protocol not found for group_id");

            let _ = conn.recv_responses(|data| match protocol.parse_response(conn_id, data) {
                Ok((bytes_consumed, req_id_opt)) => {
                    if bytes_consumed > 0 {
                        stats.record_rx_bytes(group_id, bytes_consumed);
                    }
                    Ok((bytes_consumed, req_id_opt))
                }
                Err(e) => Err(e.into()),
            })?;
        }

        Ok(any_pending)
    }

    /// Get the stats collector
    pub fn stats(&self) -> &GroupStatsCollector {
        &self.stats
    }

    /// Consume the worker and return stats
    pub fn into_stats(self) -> GroupStatsCollector {
        self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::{FixedSize, KeyGeneration, RateControl};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use xylem_transport::TcpTransport;

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

        fn generate_request(
            &mut self,
            conn_id: usize,
            key: u64,
            value_size: usize,
        ) -> (Vec<u8>, Self::RequestId) {
            self.inner.generate_request(conn_id, key, value_size)
        }

        fn parse_response(
            &mut self,
            conn_id: usize,
            data: &[u8],
        ) -> anyhow::Result<(usize, Option<Self::RequestId>)> {
            self.inner.parse_response(conn_id, data)
        }

        fn name(&self) -> &'static str {
            self.inner.name()
        }

        fn reset(&mut self) {
            self.inner.reset()
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
        let generator = RequestGenerator::new(
            KeyGeneration::sequential(0),
            RateControl::ClosedLoop,
            Box::new(FixedSize::new(64)),
        );
        let mut stats = GroupStatsCollector::default();
        stats.register_group_legacy(0, 100000, 1.0);
        let config = WorkerConfig {
            target: addr,
            duration: Duration::from_millis(100),
            value_size: 64,
            conn_count: 1,
            max_pending_per_conn: 1,
        };

        let mut worker =
            Worker::with_closed_loop(TcpTransport::new, protocol, generator, stats, config)
                .unwrap();

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

        let generator = RequestGenerator::new(
            KeyGeneration::sequential(0),
            RateControl::ClosedLoop, // Let policy control rate
            Box::new(FixedSize::new(64)),
        );
        let mut stats = GroupStatsCollector::default();
        stats.register_group_legacy(0, 100000, 1.0);
        let config = WorkerConfig {
            target: addr,
            duration: Duration::from_millis(200),
            value_size: 64,
            conn_count: 1,
            max_pending_per_conn: 10,
        };

        // Fixed rate: 100 req/s = 10ms per request
        let policy_scheduler = Box::new(UniformPolicyScheduler::fixed_rate(100.0));

        let start = Instant::now();
        let mut worker =
            Worker::new(TcpTransport::new, protocol, generator, stats, config, policy_scheduler)
                .unwrap();

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
