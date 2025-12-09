//! Pipelining integration tests
//!
//! Tests pipelining behavior with Redis - multiple outstanding requests per connection.

use std::time::Duration;
use xylem_core::threading::{Worker, WorkerConfig};

use xylem_transport::TcpTransportFactory;

mod common;

// Protocol adapter
struct ProtocolAdapter<P: xylem_protocols::Protocol> {
    inner: P,
}

impl<P: xylem_protocols::Protocol> ProtocolAdapter<P> {
    fn new(protocol: P) -> Self {
        Self { inner: protocol }
    }
}

impl<P: xylem_protocols::Protocol> xylem_core::threading::worker::Protocol for ProtocolAdapter<P> {
    type RequestId = P::RequestId;

    fn next_request(
        &mut self,
        conn_id: usize,
    ) -> (Vec<u8>, Self::RequestId, xylem_core::threading::RequestMeta) {
        let (data, req_id, proto_meta) = self.inner.next_request(conn_id);
        (
            data,
            req_id,
            xylem_core::threading::RequestMeta { is_warmup: proto_meta.is_warmup },
        )
    }

    fn regenerate_request(
        &mut self,
        conn_id: usize,
        original_request_id: Self::RequestId,
    ) -> (Vec<u8>, Self::RequestId, xylem_core::threading::RequestMeta) {
        let (data, req_id, proto_meta) =
            self.inner.regenerate_request(conn_id, original_request_id);
        (
            data,
            req_id,
            xylem_core::threading::RequestMeta { is_warmup: proto_meta.is_warmup },
        )
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
fn test_redis_pipelined_single_connection() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Running pipelined test with 1 connection...");

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(2);

    let protocol = xylem_protocols::redis::RedisProtocol::new(Box::new(
        xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get),
    ));
    let protocol = ProtocolAdapter::new(protocol);
    let stats = common::create_test_stats();
    let config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count: 1,
        max_pending_per_conn: 16, // Allow 16 pipelined requests
    };

    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, config).unwrap();

    println!("Starting pipelined test (1 conn, 16 max pending)...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete: {:?}", result.err());

    let stats = worker.stats().global();
    let basic_stats = stats.calculate_basic_stats();

    println!("Results:");
    println!("  Total requests: {}", stats.tx_requests());
    println!("  Throughput: {:.2} req/s", stats.tx_requests() as f64 / duration.as_secs_f64());
    println!("  Min latency: {:.2} μs", basic_stats.min.as_micros());
    println!("  Mean latency: {:.2} μs", basic_stats.mean.as_micros());
    println!("  Max latency: {:.2} μs", basic_stats.max.as_micros());

    assert!(stats.tx_requests() > 0, "Should have sent requests");
    assert!(stats.rx_requests() > 0, "Should have received responses");

    // With pipelining, we should get much higher throughput
    let throughput = stats.tx_requests() as f64 / duration.as_secs_f64();
    println!("✓ Pipelined throughput: {throughput:.0} req/s");
}

#[test]
fn test_redis_pipelined_multiple_connections() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Running pipelined test with 4 connections...");

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(2);

    let protocol = xylem_protocols::redis::RedisProtocol::new(Box::new(
        xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get),
    ));
    let protocol = ProtocolAdapter::new(protocol);
    let stats = common::create_test_stats();
    let config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count: 4,            // 4 connections
        max_pending_per_conn: 16, // 16 pipelined per connection
    };

    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, config).unwrap();

    println!("Starting pipelined test (4 conns, 16 max pending each)...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete: {:?}", result.err());

    let stats = worker.stats().global();
    let basic_stats = stats.calculate_basic_stats();

    println!("Results:");
    println!("  Total requests: {}", stats.tx_requests());
    println!("  Throughput: {:.2} req/s", stats.tx_requests() as f64 / duration.as_secs_f64());
    println!("  Min latency: {:.2} μs", basic_stats.min.as_micros());
    println!("  Mean latency: {:.2} μs", basic_stats.mean.as_micros());
    println!("  Max latency: {:.2} μs", basic_stats.max.as_micros());

    assert!(stats.tx_requests() > 0, "Should have sent requests");
    assert!(stats.rx_requests() > 0, "Should have received responses");

    // With 4 connections and pipelining, throughput should be very high
    let throughput = stats.tx_requests() as f64 / duration.as_secs_f64();
    println!("✓ Multi-connection pipelined throughput: {throughput:.0} req/s");
}

/// Test closed-loop pipelined performance with multiple connections
///
/// Note: This is a closed-loop test (maximum throughput), not rate-limited.
/// The worker will send requests as fast as possible with the configured
/// pipelining depth.
#[test]
fn test_redis_pipelined_closed_loop() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Running pipelined closed-loop test...");

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(2);

    let protocol = xylem_protocols::redis::RedisProtocol::new(Box::new(
        xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get),
    ));
    let protocol = ProtocolAdapter::new(protocol);
    let stats = common::create_test_stats();
    let config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count: 2,
        max_pending_per_conn: 8,
    };

    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, config).unwrap();

    println!("Starting closed-loop test...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete: {:?}", result.err());

    let stats = worker.stats().global();
    let actual_rate = stats.tx_requests() as f64 / duration.as_secs_f64();

    println!("Results:");
    println!("  Actual rate: {actual_rate:.2} req/s");
    println!("  Total requests: {}", stats.tx_requests());

    // Closed-loop should achieve reasonable throughput (at least 1000 req/s with pipelining)
    assert!(
        actual_rate > 1000.0,
        "Closed-loop pipelined should achieve at least 1000 req/s, got {actual_rate:.2}"
    );

    println!("✓ Closed-loop pipelining working ({actual_rate:.0} req/s)");
}
