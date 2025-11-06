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
}

impl<T: Transport, P: Protocol> PipelinedWorker<T, P> {
    /// Create a new pipelined worker
    pub fn new(
        transport_factory: impl Fn() -> T,
        protocol: P,
        generator: RequestGenerator,
        stats: StatsCollector,
        config: PipelinedWorkerConfig,
    ) -> Result<Self> {
        let pool = ConnectionPool::new(
            transport_factory,
            config.target,
            config.conn_count,
            config.max_pending_per_conn,
        )?;

        Ok(Self { pool, protocol, generator, stats, config })
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

                // Generate request
                let (key, value_size) = self.generator.next_request();
                let conn_id = conn.idx();
                let (request_data, req_id) =
                    self.protocol.generate_request(conn_id, key, value_size);

                // Send request
                conn.send(&request_data, req_id)?;
                self.stats.record_tx_bytes(request_data.len());

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
            }
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

        let mut worker =
            PipelinedWorker::new(TcpTransport::new, protocol, generator, stats, config).unwrap();

        let result = worker.run();
        assert!(result.is_ok(), "Worker should complete: {:?}", result.err());

        let stats = worker.stats();
        assert!(stats.tx_requests() > 0, "Should have sent requests");
    }
}
