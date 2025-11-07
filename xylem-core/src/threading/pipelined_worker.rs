//! Pipelined worker implementation with connection pooling
//!
//! This implements Lancet's symmetric agent model, allowing multiple outstanding
//! requests per connection for maximum throughput with accurate latency tracking.

use crate::connection::ConnectionPool;
use crate::stats::StatsCollector;
use crate::threading::worker::Protocol;
use crate::timing;
use crate::workload::RequestGenerator;
use crate::Result;
use std::net::SocketAddr;
use std::time::Duration;
use xylem_transport::Transport;

/// Configuration for pipelined worker
pub struct PipelinedWorkerConfig {
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
pub struct PipelinedWorker<T: Transport, P: Protocol> {
    pool: ConnectionPool<T, P::RequestId>,
    protocol: P,
    generator: RequestGenerator,
    stats: StatsCollector,
    config: PipelinedWorkerConfig,
    /// Track keys for scheduler feedback
    current_key: u64,
}

impl<T: Transport, P: Protocol> PipelinedWorker<T, P> {
    /// Create a new pipelined worker with a unified scheduler
    pub fn new(
        transport_factory: impl Fn() -> T,
        protocol: P,
        generator: RequestGenerator,
        stats: StatsCollector,
        config: PipelinedWorkerConfig,
        scheduler: crate::scheduler::UnifiedScheduler,
    ) -> Result<Self> {
        let pool = ConnectionPool::new(
            transport_factory,
            config.target,
            config.conn_count,
            config.max_pending_per_conn,
            scheduler,
        )?;

        Ok(Self {
            pool,
            protocol,
            generator,
            stats,
            config,
            current_key: 0,
        })
    }

    /// Create a new pipelined worker with closed-loop + round-robin scheduler
    pub fn with_round_robin(
        transport_factory: impl Fn() -> T,
        protocol: P,
        generator: RequestGenerator,
        stats: StatsCollector,
        config: PipelinedWorkerConfig,
    ) -> Result<Self> {
        let timing = Box::new(crate::scheduler::ClosedLoopTiming::new());
        let selector = Box::new(crate::scheduler::RoundRobinSelector::new());
        let scheduler = crate::scheduler::UnifiedScheduler::new(timing, selector);

        Self::new(transport_factory, protocol, generator, stats, config, scheduler)
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
                // Generate request first to get the key for scheduler
                let (key, value_size) = self.generator.next_request();
                self.current_key = key;

                // Pick a connection that can accept more requests
                let conn = match self.pool.pick_connection(key) {
                    Some(c) => c,
                    None => break, // No available connections, move to receive phase
                };

                let conn_id = conn.idx();
                let (request_data, req_id) =
                    self.protocol.generate_request(conn_id, key, value_size);

                // Send request
                conn.send(&request_data, req_id)?;
                self.stats.record_tx_bytes(request_data.len());

                // Notify scheduler that request was sent
                self.pool.on_request_sent(conn_id, key);

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
        let protocol = &mut self.protocol;
        let stats = &mut self.stats;
        let current_key = self.current_key;

        // Collect latency feedback for scheduler
        let mut feedback: Vec<(usize, Duration)> = Vec::new();

        for conn in self.pool.connections_mut() {
            if !conn.poll_readable()? {
                continue;
            }

            let conn_id = conn.idx();
            let latencies =
                conn.recv_responses(|data| match protocol.parse_response(conn_id, data) {
                    Ok((bytes_consumed, req_id_opt)) => {
                        if bytes_consumed > 0 {
                            stats.record_rx_bytes(bytes_consumed);
                        }
                        Ok((bytes_consumed, req_id_opt))
                    }
                    Err(e) => Err(e.into()),
                })?;

            // Record all latencies
            for (_req_id, latency) in latencies {
                stats.record_latency(latency);
                feedback.push((conn_id, latency));
            }
        }

        // Notify scheduler after releasing the borrow on connections
        for (conn_id, latency) in feedback {
            // Note: We use current_key as approximation - for precise key tracking,
            // would need to maintain req_id -> key mapping
            self.pool.on_response_received(conn_id, current_key, latency);
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
        let protocol = &mut self.protocol;
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
            let _ = conn.recv_responses(|data| match protocol.parse_response(conn_id, data) {
                Ok((bytes_consumed, req_id_opt)) => {
                    if bytes_consumed > 0 {
                        stats.record_rx_bytes(bytes_consumed);
                    }
                    Ok((bytes_consumed, req_id_opt))
                }
                Err(e) => Err(e.into()),
            })?;
        }

        Ok(any_pending)
    }

    /// Get the stats collector
    pub fn stats(&self) -> &StatsCollector {
        &self.stats
    }

    /// Consume the worker and return stats
    pub fn into_stats(self) -> StatsCollector {
        self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::{KeyGeneration, RateControl};
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
        let echo_protocol = xylem_protocols::echo::EchoProtocol::new(64);
        let protocol = ProtocolAdapter::new(echo_protocol);
        let generator =
            RequestGenerator::new(KeyGeneration::sequential(0), RateControl::ClosedLoop, 64);
        let stats = StatsCollector::default();
        let config = PipelinedWorkerConfig {
            target: addr,
            duration: Duration::from_millis(100),
            value_size: 64,
            conn_count: 1,
            max_pending_per_conn: 1,
        };

        let mut worker = PipelinedWorker::with_round_robin(
            TcpTransport::new,
            protocol,
            generator,
            stats,
            config,
        )
        .unwrap();

        let result = worker.run();
        assert!(result.is_ok(), "Worker should complete: {:?}", result.err());

        let stats = worker.stats();
        assert!(stats.tx_requests() > 0, "Should have sent requests");
    }

    #[test]
    #[allow(clippy::excessive_nesting)]
    fn test_open_loop_scheduler_time_driven() {
        use crate::scheduler::{FixedRateTiming, RoundRobinSelector, UnifiedScheduler};
        use std::sync::{Arc, Mutex};
        use std::time::Instant;

        // Track request arrival times at server
        let request_times = Arc::new(Mutex::new(Vec::new()));
        let request_times_clone = request_times.clone();

        // Start server that tracks request timing and adds delay
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            let mut connections = Vec::new();
            // Accept all connections
            for _ in 0..2 {
                if let Ok((socket, _)) = listener.accept() {
                    connections.push(socket);
                }
            }

            let start_time = Instant::now();

            // Handle connections in parallel
            let handles: Vec<_> = connections
                .into_iter()
                .map(|mut socket| {
                    let times = request_times_clone.clone();
                    thread::spawn(move || {
                        let mut buf = vec![0u8; 4096];
                        loop {
                            let n = match socket.read(&mut buf) {
                                Ok(0) => break,
                                Ok(n) => n,
                                Err(_) => break,
                            };

                            // Record when request arrived
                            times.lock().unwrap().push(start_time.elapsed());

                            // Add 50ms delay before responding (simulating slow server)
                            thread::sleep(Duration::from_millis(50));

                            if socket.write_all(&buf[..n]).is_err() {
                                break;
                            }
                        }
                    })
                })
                .collect();

            for h in handles {
                let _ = h.join();
            }
        });

        thread::sleep(Duration::from_millis(50));

        // Create worker with open-loop scheduler (fixed rate + round robin)
        let echo_protocol = xylem_protocols::echo::EchoProtocol::new(64);
        let protocol = ProtocolAdapter::new(echo_protocol);

        // Fixed rate: 100 req/s = 10ms per request
        let generator = RequestGenerator::new(
            KeyGeneration::sequential(0),
            RateControl::Fixed { rate: 100.0 },
            64,
        );
        let stats = StatsCollector::default();
        let config = PipelinedWorkerConfig {
            target: addr,
            duration: Duration::from_millis(500), // 500ms = ~50 requests
            value_size: 64,
            conn_count: 2,
            max_pending_per_conn: 10, // Allow pipelining
        };

        // Create open-loop scheduler: FixedRate timing + RoundRobin selector
        let timing = Box::new(FixedRateTiming::new(100.0)); // 100 req/s
        let selector = Box::new(RoundRobinSelector::new());
        let scheduler = UnifiedScheduler::new(timing, selector);

        let mut worker =
            PipelinedWorker::new(TcpTransport::new, protocol, generator, stats, config, scheduler)
                .unwrap();

        let result = worker.run();
        assert!(result.is_ok(), "Worker should complete: {:?}", result.err());

        // Verify time-driven behavior
        // Expected: 100 req/s * 0.5s = 50 requests
        // But limited by: 2 conns * 10 pipeline = 20 max in-flight
        // With 50ms response time and 10ms inter-arrival: fills pipeline quickly
        // So we expect ~20-25 requests (pipeline limit reached)
        let times = request_times.lock().unwrap();
        assert!(
            times.len() >= 15,
            "Should have sent ~20 requests (pipeline limited) despite slow server (got {})",
            times.len()
        );

        // Key verification: With 50ms response time and open-loop sending,
        // we should have multiple requests in-flight (pipelined)
        // If it were closed-loop with 50ms responses, we'd only get ~10 requests in 500ms
        // But with open-loop + pipelining, we can send more despite slow server

        println!(
            "✓ Open-loop sent {} requests with 50ms server delay (expected ~20 due to pipeline limit)",
            times.len()
        );

        // The KEY insight: open-loop continues sending despite slow responses
        // Compare: closed-loop would be limited to ~10 reqs (500ms / 50ms per req)
        // But open-loop with pipelining can achieve ~20 (pipeline fills up)
        assert!(times.len() >= 15, "Open-loop should pipeline requests despite slow server");
    }

    #[test]
    #[allow(clippy::excessive_nesting)]
    fn test_closed_loop_per_connection_scheduler() {
        use crate::scheduler::{ClosedLoopSelector, ClosedLoopTiming, UnifiedScheduler};
        use std::sync::{Arc, Mutex};

        // Track concurrent requests per connection
        #[derive(Debug)]
        struct ConnectionTracker {
            conn_id: usize,
            max_concurrent: usize,
            current_concurrent: usize,
        }

        let trackers = Arc::new(Mutex::new(vec![
            ConnectionTracker {
                conn_id: 0,
                max_concurrent: 0,
                current_concurrent: 0,
            },
            ConnectionTracker {
                conn_id: 1,
                max_concurrent: 0,
                current_concurrent: 0,
            },
        ]));
        let trackers_clone = trackers.clone();

        // Start server that tracks per-connection concurrency
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            let mut connections = Vec::new();
            // Accept both connections
            for conn_id in 0..2 {
                if let Ok((socket, _)) = listener.accept() {
                    connections.push((conn_id, socket));
                }
            }

            // Handle connections in parallel
            let handles: Vec<_> = connections
                .into_iter()
                .map(|(conn_id, mut socket)| {
                    let trackers = trackers_clone.clone();
                    thread::spawn(move || {
                        let mut buf = vec![0u8; 4096];
                        loop {
                            let n = match socket.read(&mut buf) {
                                Ok(0) => break,
                                Ok(n) => n,
                                Err(_) => break,
                            };

                            // Track concurrent request
                            {
                                let mut t = trackers.lock().unwrap();
                                t[conn_id].current_concurrent += 1;
                                t[conn_id].max_concurrent =
                                    t[conn_id].max_concurrent.max(t[conn_id].current_concurrent);
                            }

                            // Small delay
                            thread::sleep(Duration::from_micros(500));

                            // Echo response
                            if socket.write_all(&buf[..n]).is_err() {
                                break;
                            }

                            // Decrement concurrent
                            {
                                let mut t = trackers.lock().unwrap();
                                t[conn_id].current_concurrent -= 1;
                            }
                        }
                    })
                })
                .collect();

            for h in handles {
                let _ = h.join();
            }
        });

        thread::sleep(Duration::from_millis(100));

        // Create worker with closed-loop per-connection scheduler
        let echo_protocol = xylem_protocols::echo::EchoProtocol::new(64);
        let protocol = ProtocolAdapter::new(echo_protocol);
        let generator =
            RequestGenerator::new(KeyGeneration::sequential(0), RateControl::ClosedLoop, 64);
        let stats = StatsCollector::default();
        let config = PipelinedWorkerConfig {
            target: addr,
            duration: Duration::from_millis(200),
            value_size: 64,
            conn_count: 2,
            max_pending_per_conn: 1, // Per-connection limit
        };

        // Create closed-loop scheduler with per-connection limit
        let timing = Box::new(ClosedLoopTiming::new());
        let selector = Box::new(ClosedLoopSelector::per_connection(2, 1));
        let scheduler = UnifiedScheduler::new(timing, selector);

        let mut worker =
            PipelinedWorker::new(TcpTransport::new, protocol, generator, stats, config, scheduler)
                .unwrap();

        let result = worker.run();
        assert!(result.is_ok(), "Worker should complete: {:?}", result.err());

        // Verify per-connection behavior: max 1 concurrent per connection
        let final_trackers = trackers.lock().unwrap();
        for tracker in final_trackers.iter() {
            assert_eq!(
                tracker.max_concurrent, 1,
                "Connection {} should have max 1 concurrent request (got {})",
                tracker.conn_id, tracker.max_concurrent
            );
        }

        let stats = worker.stats();
        println!(
            "✓ Closed-loop per-connection sent {} requests across 2 connections (max 1 per conn verified)",
            stats.tx_requests()
        );
    }

    #[test]
    #[allow(clippy::excessive_nesting)]
    fn test_closed_loop_global_scheduler() {
        use crate::scheduler::{ClosedLoopSelector, ClosedLoopTiming, UnifiedScheduler};
        use std::sync::{Arc, Mutex};

        // Track global concurrent requests across all connections
        let global_concurrent = Arc::new(Mutex::new((0usize, 0usize))); // (current, max)
        let global_concurrent_clone = global_concurrent.clone();

        // Start server that tracks global concurrency
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            let mut connections = Vec::new();
            // Accept all connections
            for _ in 0..3 {
                if let Ok((socket, _)) = listener.accept() {
                    connections.push(socket);
                }
            }

            // Handle connections in parallel
            let handles: Vec<_> = connections
                .into_iter()
                .map(|mut socket| {
                    let global = global_concurrent_clone.clone();
                    thread::spawn(move || {
                        let mut buf = vec![0u8; 4096];
                        loop {
                            let n = match socket.read(&mut buf) {
                                Ok(0) => break,
                                Ok(n) => n,
                                Err(_) => break,
                            };

                            // Track global concurrent request
                            {
                                let mut g = global.lock().unwrap();
                                g.0 += 1;
                                g.1 = g.1.max(g.0);
                            }

                            // Small delay
                            thread::sleep(Duration::from_micros(500));

                            // Echo response
                            if socket.write_all(&buf[..n]).is_err() {
                                break;
                            }

                            // Decrement global concurrent
                            {
                                let mut g = global.lock().unwrap();
                                g.0 -= 1;
                            }
                        }
                    })
                })
                .collect();

            for h in handles {
                let _ = h.join();
            }
        });

        thread::sleep(Duration::from_millis(100));

        // Create worker with closed-loop global scheduler
        let echo_protocol = xylem_protocols::echo::EchoProtocol::new(64);
        let protocol = ProtocolAdapter::new(echo_protocol);
        let generator =
            RequestGenerator::new(KeyGeneration::sequential(0), RateControl::ClosedLoop, 64);
        let stats = StatsCollector::default();
        let config = PipelinedWorkerConfig {
            target: addr,
            duration: Duration::from_millis(200),
            value_size: 64,
            conn_count: 3,
            max_pending_per_conn: 1,
        };

        // Create closed-loop scheduler with global limit of 1
        let timing = Box::new(ClosedLoopTiming::new());
        let selector = Box::new(ClosedLoopSelector::global(3, 1)); // Global limit: 1 request
        let scheduler = UnifiedScheduler::new(timing, selector);

        let mut worker =
            PipelinedWorker::new(TcpTransport::new, protocol, generator, stats, config, scheduler)
                .unwrap();

        let result = worker.run();
        assert!(result.is_ok(), "Worker should complete: {:?}", result.err());

        // Verify global behavior: max 1 concurrent across ALL connections
        let (_, max_global) = *global_concurrent.lock().unwrap();
        assert_eq!(
            max_global, 1,
            "Should have max 1 concurrent request globally (got {})",
            max_global
        );

        let stats = worker.stats();
        println!(
            "✓ Closed-loop global sent {} requests across 3 connections (max 1 global verified)",
            stats.tx_requests()
        );
    }
}
