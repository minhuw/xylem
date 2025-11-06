//! Worker thread implementation
//!
//! Implements Lancet-style busy-waiting event loop for nanosecond-precision latency measurement.

use crate::stats::StatsCollector;
use crate::timing;
use crate::workload::RequestGenerator;
use crate::Result;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use xylem_transport::Transport;

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

    /// Parse a response and return the request ID it corresponds to
    /// Returns Ok((bytes_consumed, Some(request_id))) if complete response found
    /// Returns Ok((0, None)) if response is incomplete
    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> anyhow::Result<(usize, Option<Self::RequestId>)>;

    /// Protocol name
    fn name(&self) -> &'static str;

    /// Reset protocol state
    fn reset(&mut self);
}

/// Worker configuration
pub struct WorkerConfig {
    pub target: SocketAddr,
    pub duration: Duration,
    pub value_size: usize,
}

/// Worker that runs measurement loop
pub struct Worker<T: Transport, P: Protocol> {
    transport: T,
    protocol: P,
    generator: RequestGenerator,
    stats: StatsCollector,
    config: WorkerConfig,
}

impl<T: Transport, P: Protocol> Worker<T, P> {
    /// Create a new worker
    pub fn new(
        transport: T,
        protocol: P,
        generator: RequestGenerator,
        stats: StatsCollector,
        config: WorkerConfig,
    ) -> Self {
        Self {
            transport,
            protocol,
            generator,
            stats,
            config,
        }
    }

    /// Run the measurement loop using Lancet-style busy-waiting
    pub fn run(&mut self) -> Result<()> {
        // Connect to target
        self.transport.connect(&self.config.target)?;

        let _start = Instant::now();
        let start_ns = timing::time_ns();
        let duration = self.config.duration;
        let duration_ns = duration.as_nanos() as u64;

        // Calculate next send time based on rate control
        let mut next_send_ns = start_ns;
        let mut last_send_ts = None;

        // Main measurement loop - Lancet style
        loop {
            let now_ns = timing::time_ns();

            // Check if we've exceeded the duration
            if now_ns - start_ns >= duration_ns {
                break;
            }

            // Send phase - busy wait until next send time
            if now_ns >= next_send_ns && last_send_ts.is_none() {
                // Generate request (single connection, so conn_id = 0)
                let (key, value_size) = self.generator.next_request();
                let (request_data, _req_id) = self.protocol.generate_request(0, key, value_size);

                // Send request and capture timestamp
                let send_ts = self.transport.send(&request_data)?;
                self.stats.record_tx_bytes(request_data.len());

                // Store send timestamp for latency calculation
                last_send_ts = Some(send_ts);

                // Mark request as sent for rate control
                self.generator.mark_request_sent();

                // Update next send time based on rate control
                if let Some(delay) = self.generator.delay_until_next() {
                    next_send_ns = timing::time_ns() + delay.as_nanos() as u64;
                } else {
                    next_send_ns = timing::time_ns();
                }
            }

            // Receive phase - non-blocking poll
            if self.transport.poll_readable()? {
                let (response_data, recv_ts) = self.transport.recv()?;

                if response_data.is_empty() {
                    continue;
                }

                // Parse response (single connection, so conn_id = 0)
                let (bytes_consumed, _req_id_opt) =
                    self.protocol.parse_response(0, &response_data)?;

                if bytes_consumed == 0 {
                    continue; // Incomplete response
                }

                self.stats.record_rx_bytes(bytes_consumed);

                // Calculate and record latency
                let Some(send_ts) = last_send_ts.take() else {
                    continue;
                };
                let latency = recv_ts.duration_since(&send_ts);
                self.stats.record_latency(latency);
            }

            // Yield to OS scheduler without sleeping
            std::hint::spin_loop();
        }

        // Drain any remaining responses
        for _ in 0..100 {
            if !self.transport.poll_readable()? {
                break;
            }

            let Ok((response_data, _)) = self.transport.recv() else {
                continue;
            };

            if response_data.is_empty() {
                continue;
            }

            if let Ok((bytes_consumed, _)) = self.protocol.parse_response(0, &response_data) {
                if bytes_consumed > 0 {
                    self.stats.record_rx_bytes(bytes_consumed);
                }
            }

            std::thread::sleep(Duration::from_micros(100));
        }

        // Close connection
        self.transport.close()?;

        Ok(())
    }

    /// Get the stats collector
    pub fn stats(&self) -> &StatsCollector {
        &self.stats
    }

    /// Consume the worker and return the stats collector
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

    // Adapter to make xylem_protocols::Protocol work with our Protocol trait
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
    fn test_worker_basic() {
        // Start a simple echo server
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

                // Echo back
                if socket.write_all(&buf[..n]).is_err() {
                    break;
                }
            }
        });

        // Wait for server to start
        thread::sleep(Duration::from_millis(50));

        // Create worker components
        let transport = TcpTransport::new();
        let echo_protocol = xylem_protocols::echo::EchoProtocol::new(64);
        let protocol = ProtocolAdapter::new(echo_protocol);
        let generator =
            RequestGenerator::new(KeyGeneration::sequential(0), RateControl::ClosedLoop, 64);
        let stats = StatsCollector::default();
        let config = WorkerConfig {
            target: addr,
            duration: Duration::from_millis(100),
            value_size: 64,
        };

        let mut worker = Worker::new(transport, protocol, generator, stats, config);

        // Run worker
        let result = worker.run();

        // Worker should complete successfully
        if let Err(e) = &result {
            eprintln!("Worker error: {e:?}");
        }
        assert!(result.is_ok());

        // Should have collected some stats
        let stats = worker.stats();
        assert!(stats.tx_requests() > 0);
        assert!(stats.rx_requests() > 0);
        assert!(!stats.samples().is_empty());
    }
}
