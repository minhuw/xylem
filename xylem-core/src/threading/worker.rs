//! Worker thread implementation

use crate::stats::StatsCollector;
use crate::transport::Transport;
use crate::workload::RequestGenerator;
use crate::Result;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

/// Protocol trait for generating requests and parsing responses
pub trait Protocol: Send + Sync {
    /// Generate a request
    fn generate_request(&mut self, key: u64, value_size: usize) -> Vec<u8>;

    /// Parse a response (returns Ok(()) if valid)
    fn parse_response(&mut self, data: &[u8]) -> anyhow::Result<()>;

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

    /// Run the measurement loop
    pub async fn run(&mut self) -> Result<()> {
        // Connect to target
        self.transport.connect(&self.config.target).await?;

        let start = tokio::time::Instant::now();
        let duration = self.config.duration;

        // Main measurement loop
        while start.elapsed() < duration {
            // Check if we need to delay before next request
            if let Some(delay) = self.generator.delay_until_next() {
                if delay > Duration::ZERO {
                    sleep(delay).await;
                }
            }

            // Generate request
            let (key, value_size) = self.generator.next_request();
            let request_data = self.protocol.generate_request(key, value_size);

            // Send request and capture timestamp
            let send_ts = self.transport.send(&request_data).await?;
            self.stats.record_tx_bytes(request_data.len());

            // Mark request as sent for rate control
            self.generator.mark_request_sent();

            // Receive response and capture timestamp
            let (response_data, recv_ts) = self.transport.recv().await?;
            self.stats.record_rx_bytes(response_data.len());

            // Validate response
            self.protocol.parse_response(&response_data)?;

            // Calculate and record latency
            let latency = recv_ts.duration_since(&send_ts);
            self.stats.record_latency(latency);
        }

        // Close connection
        self.transport.close().await?;

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
    use crate::transport::tcp::TcpTransport;
    use crate::workload::{KeyGeneration, RateControl};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

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
        fn generate_request(&mut self, key: u64, value_size: usize) -> Vec<u8> {
            self.inner.generate_request(key, value_size)
        }

        fn parse_response(&mut self, data: &[u8]) -> anyhow::Result<()> {
            self.inner.parse_response(data)
        }

        fn name(&self) -> &'static str {
            self.inner.name()
        }

        fn reset(&mut self) {
            self.inner.reset()
        }
    }

    #[tokio::test]
    async fn test_worker_basic() {
        // Start a simple echo server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];

            loop {
                let n = match socket.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };

                // Echo back
                #[allow(clippy::excessive_nesting)]
                if socket.write_all(&buf[..n]).await.is_err() {
                    break;
                }
            }
        });

        // Wait for server to start
        tokio::time::sleep(Duration::from_millis(50)).await;

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
        let result = worker.run().await;

        // Worker should complete successfully
        if let Err(e) = &result {
            eprintln!("Worker error: {:?}", e);
        }
        assert!(result.is_ok());

        // Should have collected some stats
        let stats = worker.stats();
        assert!(stats.tx_requests() > 0);
        assert!(stats.rx_requests() > 0);
        assert!(!stats.samples().is_empty());
    }
}
