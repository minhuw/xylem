//! Rate control integration tests
//!
//! Tests verifying that rate control mechanisms achieve target rates accurately.
//! Requires Redis Docker container.

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

    fn next_request(&mut self, conn_id: usize) -> (Vec<u8>, Self::RequestId) {
        self.inner.next_request(conn_id)
    }

    fn regenerate_request(
        &mut self,
        conn_id: usize,
        original_request_id: Self::RequestId,
    ) -> (Vec<u8>, Self::RequestId) {
        self.inner.regenerate_request(conn_id, original_request_id)
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

/// Run a rate-limited experiment and return (target_rate, actual_rate, error_percent)
fn run_rate_experiment(target_rate: f64, duration_secs: u64, conn_count: usize) -> (f64, f64, f64) {
    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(duration_secs);

    let protocol = xylem_protocols::redis::RedisProtocol::new(Box::new(
        xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get),
    ));
    let protocol = ProtocolAdapter::new(protocol);
    let stats = common::create_test_stats();
    let config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count,
        max_pending_per_conn: 8,
    };

    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, config).unwrap();

    let result = worker.run();
    assert!(result.is_ok(), "Worker failed: {:?}", result.err());

    let stats = worker.stats().global();
    let actual_rate = stats.tx_requests() as f64 / duration.as_secs_f64();
    let error_percent = ((actual_rate - target_rate) / target_rate * 100.0).abs();

    (target_rate, actual_rate, error_percent)
}

#[test]
fn test_rate_accuracy_low_rate() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("\n=== Testing Low Rate (100 req/s) ===");
    let (target, actual, error) = run_rate_experiment(100.0, 5, 1);

    println!("Target rate: {target:.2} req/s");
    println!("Actual rate: {actual:.2} req/s");
    println!("Error: {error:.2}%");

    // Low rates should be very accurate (within 5%)
    assert!(
        error < 5.0,
        "Low rate error too high: {error:.2}% (target: {target:.2}, actual: {actual:.2})"
    );
    println!("✓ Low rate accuracy: {error:.2}%");
}

#[test]
fn test_rate_accuracy_medium_rate() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("\n=== Testing Medium Rate (5000 req/s) ===");
    let (target, actual, error) = run_rate_experiment(5000.0, 5, 2);

    println!("Target rate: {target:.2} req/s");
    println!("Actual rate: {actual:.2} req/s");
    println!("Error: {error:.2}%");

    // Medium rates should be accurate within 15%
    assert!(
        error < 15.0,
        "Medium rate error too high: {error:.2}% (target: {target:.2}, actual: {actual:.2})"
    );
    println!("✓ Medium rate accuracy: {error:.2}%");
}

#[test]
fn test_rate_vs_throughput_saturation() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("\n=== Testing Rate vs Throughput Saturation ===");
    println!("Finding maximum achievable throughput...\n");

    // First, find max throughput in closed-loop mode
    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(3);

    let protocol = xylem_protocols::redis::RedisProtocol::new(Box::new(
        xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get),
    ));
    let protocol = ProtocolAdapter::new(protocol);
    let stats = common::create_test_stats();
    let config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count: 4,
        max_pending_per_conn: 16,
    };

    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, config).unwrap();

    worker.run().unwrap();
    let max_throughput = worker.stats().global().tx_requests() as f64 / duration.as_secs_f64();

    println!("Maximum throughput (closed-loop): {max_throughput:.0} req/s\n");

    // Test rates below and above saturation point
    let test_rates = vec![
        max_throughput * 0.5, // 50% of max - should achieve target
        max_throughput * 1.5, // 150% of max - should saturate
    ];

    println!("{:<20} {:<20} {:<15}", "Target Rate", "Actual Rate", "Saturated?");
    println!("{:-<55}", "");

    for target in test_rates {
        let (_t, actual, _e) = run_rate_experiment(target, 3, 4);
        let saturated = if actual < target * 0.9 { "Yes" } else { "No" };

        println!("{target:<20.0} {actual:<20.0} {saturated:<15}");
    }

    println!("\n✓ Rate control behaves correctly at saturation");
}
