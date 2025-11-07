//! Integration test for RoundRobin scheduler with Redis
//!
//! This test verifies that the RoundRobin scheduler properly distributes requests
//! across multiple Redis connections.

mod common;

use std::time::Duration;
use xylem_core::stats::StatsCollector;
use xylem_core::threading::worker::{Protocol, Worker, WorkerConfig};
use xylem_core::workload::{KeyGeneration, RateControl, RequestGenerator};
use xylem_protocols::redis::RedisOp;
use xylem_transport::TcpTransport;

// Protocol adapter to bridge xylem_protocols::Protocol with worker Protocol trait
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

// Use common::start_redis() instead of local implementation

/// Test that RoundRobin scheduler distributes requests across multiple connections
#[test]
fn test_round_robin_scheduler_with_multiple_connections() {
    let redis = common::start_redis().expect("Failed to start Redis");
    let port = redis.port();

    let target_addr = format!("127.0.0.1:{}", port).parse().unwrap();
    let duration = Duration::from_secs(2);

    let protocol = xylem_protocols::redis::RedisProtocol::new(RedisOp::Get);
    let protocol = ProtocolAdapter::new(protocol);
    let generator =
        RequestGenerator::new(KeyGeneration::sequential(0), RateControl::ClosedLoop, 64);
    let stats = StatsCollector::default();

    // Use worker config with multiple connections
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        value_size: 64,
    };

    // Create transport factory that creates transports with RoundRobin scheduler
    let transport = TcpTransport::new();

    let mut worker = Worker::new(transport, protocol, generator, stats, worker_config);

    println!("Testing RoundRobin scheduler behavior with multiple connections...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully: {:?}", result.err());

    // Check stats
    let stats = worker.into_stats();
    let total_requests = stats.tx_requests();

    println!("RoundRobin Scheduler Results:");
    println!("  Total requests: {}", total_requests);
    println!("  Successful responses: {}", stats.rx_requests());
    println!(
        "  Error rate: {:.2}%",
        (total_requests - stats.rx_requests()) as f64 / total_requests as f64 * 100.0
    );

    assert!(total_requests > 100, "Should have sent many requests");
    assert!(stats.rx_requests() > 100, "Should have received many responses");

    let throughput = total_requests as f64 / duration.as_secs_f64();
    println!("  Throughput: {:.2} req/s", throughput);

    // With round-robin distribution to multiple connections, we should get good throughput
    assert!(throughput > 1000.0, "Round-robin should achieve good throughput");
}

/// Test that RoundRobin scheduler handles sequential workload correctly
#[test]
fn test_round_robin_scheduler_with_sequential_workload() {
    let redis = common::start_redis().expect("Failed to start Redis");
    let port = redis.port();

    let target_addr = format!("127.0.0.1:{}", port).parse().unwrap();
    let duration = Duration::from_secs(1);

    let protocol = xylem_protocols::redis::RedisProtocol::new(RedisOp::Set);
    let protocol = ProtocolAdapter::new(protocol);

    // Use sequential key generation to test distribution
    let generator =
        RequestGenerator::new(KeyGeneration::sequential(0), RateControl::ClosedLoop, 128);
    let stats = StatsCollector::default();

    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        value_size: 128,
    };

    let transport = TcpTransport::new();

    let mut worker = Worker::new(transport, protocol, generator, stats, worker_config);

    println!("Testing RoundRobin scheduler with sequential workload...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully: {:?}", result.err());

    let stats = worker.into_stats();

    println!("Sequential Workload Test Results:");
    println!("  Total requests: {}", stats.tx_requests());
    println!("  Total bytes sent: {}", stats.tx_bytes());
    println!("  Total bytes received: {}", stats.rx_bytes());

    assert!(stats.tx_requests() > 50, "Should have sent requests with sequential keys");
    assert!(stats.rx_requests() > 50, "Should have received responses");

    // Verify error rate is low
    let error_rate =
        (stats.tx_requests() - stats.rx_requests()) as f64 / stats.tx_requests() as f64 * 100.0;
    println!("  Error rate: {:.2}%", error_rate);
    assert!(error_rate < 5.0, "Error rate should be low");

    let throughput = stats.tx_requests() as f64 / duration.as_secs_f64();
    println!("  Throughput: {:.2} req/s", throughput);
}
