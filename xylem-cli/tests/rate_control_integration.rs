//! Rate control and throughput integration tests.
//!
//! Tests verifying:
//! - Closed-loop throughput behavior with different configurations
//! - Rate-limited behavior with fixed-rate and Poisson policies
//!
//! Requires Redis Docker container.

use std::time::Duration;
use xylem_core::scheduler::UniformPolicyScheduler;
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

/// Run a closed-loop throughput experiment and return the actual rate
fn run_throughput_experiment(duration_secs: u64, conn_count: usize, pipeline_depth: usize) -> f64 {
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
        max_pending_per_conn: pipeline_depth,
    };

    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, config).unwrap();

    let result = worker.run();
    assert!(result.is_ok(), "Worker failed: {:?}", result.err());

    let stats = worker.stats().global();
    stats.tx_requests() as f64 / duration.as_secs_f64()
}

/// Test closed-loop throughput with single connection
#[test]
fn test_throughput_single_connection() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("\n=== Testing Single Connection Throughput ===");
    let rate = run_throughput_experiment(3, 1, 8);

    println!("Actual rate: {rate:.2} req/s");

    // Single connection with pipelining should achieve reasonable throughput
    assert!(
        rate > 1000.0,
        "Single connection should achieve at least 1000 req/s, got {rate:.2}"
    );
    println!("✓ Single connection throughput: {rate:.0} req/s");
}

/// Test closed-loop throughput with multiple connections
#[test]
fn test_throughput_multiple_connections() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("\n=== Testing Multiple Connection Throughput ===");
    let rate = run_throughput_experiment(3, 4, 8);

    println!("Actual rate: {rate:.2} req/s");

    // Multiple connections should achieve higher throughput
    assert!(
        rate > 2000.0,
        "Multiple connections should achieve at least 2000 req/s, got {rate:.2}"
    );
    println!("✓ Multiple connection throughput: {rate:.0} req/s");
}

/// Test throughput scaling with pipeline depth
#[test]
fn test_throughput_scaling_with_pipeline_depth() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("\n=== Testing Throughput Scaling with Pipeline Depth ===");

    // Test with different pipeline depths
    let depths = [1, 4, 16];
    let mut rates = Vec::new();

    println!("{:<20} {:<20}", "Pipeline Depth", "Throughput (req/s)");
    println!("{:-<40}", "");

    for depth in depths {
        let rate = run_throughput_experiment(3, 2, depth);
        println!("{depth:<20} {rate:<20.0}");
        rates.push(rate);
    }

    // Higher pipeline depth should generally achieve higher or equal throughput
    // (up to saturation point)
    assert!(
        rates[1] >= rates[0] * 0.9,
        "Pipeline depth 4 should not be much worse than depth 1"
    );

    println!("\n✓ Throughput scales with pipeline depth");
}

// =============================================================================
// Rate-Limited Tests
// =============================================================================

/// Run a rate-limited experiment and return (target_rate, actual_rate, error_percent)
fn run_rate_limited_experiment(
    target_rate: f64,
    duration_secs: u64,
    conn_count: usize,
) -> (f64, f64, f64) {
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
        max_pending_per_conn: 1, // Single pending for rate-limited mode
    };

    // Use fixed-rate policy scheduler
    let policy_scheduler = Box::new(UniformPolicyScheduler::fixed_rate(target_rate));

    let mut worker =
        Worker::new(&TcpTransportFactory::default(), protocol, stats, config, policy_scheduler)
            .unwrap();

    let result = worker.run();
    assert!(result.is_ok(), "Worker failed: {:?}", result.err());

    let stats = worker.stats().global();
    let actual_rate = stats.tx_requests() as f64 / duration.as_secs_f64();
    let error_percent = ((actual_rate - target_rate) / target_rate * 100.0).abs();

    (target_rate, actual_rate, error_percent)
}

/// Test rate accuracy with low rate (100 req/s)
#[test]
fn test_rate_accuracy_low_rate() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("\n=== Testing Low Rate (100 req/s) ===");
    let (target, actual, error) = run_rate_limited_experiment(100.0, 5, 1);

    println!("Target rate: {target:.2} req/s");
    println!("Actual rate: {actual:.2} req/s");
    println!("Error: {error:.2}%");

    // Low rates should be accurate (within 10%)
    assert!(
        error < 10.0,
        "Low rate error too high: {error:.2}% (target: {target:.2}, actual: {actual:.2})"
    );
    println!("✓ Low rate accuracy: {error:.2}%");
}

/// Test rate accuracy with medium rate (1000 req/s)
#[test]
fn test_rate_accuracy_medium_rate() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("\n=== Testing Medium Rate (1000 req/s) ===");
    // Use 1 connection since rate limiting is per-connection
    let (target, actual, error) = run_rate_limited_experiment(1000.0, 5, 1);

    println!("Target rate: {target:.2} req/s");
    println!("Actual rate: {actual:.2} req/s");
    println!("Error: {error:.2}%");

    // Medium rates should be accurate within 10%
    assert!(
        error < 10.0,
        "Medium rate error too high: {error:.2}% (target: {target:.2}, actual: {actual:.2})"
    );
    println!("✓ Medium rate accuracy: {error:.2}%");
}

/// Test Poisson rate limiting produces correct average rate
#[test]
fn test_poisson_rate_limiting() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("\n=== Testing Poisson Rate Limiting (500 req/s) ===");

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(5);
    let target_rate = 500.0;

    let protocol = xylem_protocols::redis::RedisProtocol::new(Box::new(
        xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get),
    ));
    let protocol = ProtocolAdapter::new(protocol);
    let stats = common::create_test_stats();
    let config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count: 1,
        max_pending_per_conn: 1,
    };

    // Use Poisson policy scheduler
    let policy_scheduler =
        Box::new(UniformPolicyScheduler::poisson(target_rate).expect("Failed to create Poisson"));

    let mut worker =
        Worker::new(&TcpTransportFactory::default(), protocol, stats, config, policy_scheduler)
            .unwrap();

    let result = worker.run();
    assert!(result.is_ok(), "Worker failed: {:?}", result.err());

    let stats = worker.stats().global();
    let actual_rate = stats.tx_requests() as f64 / duration.as_secs_f64();
    let error = ((actual_rate - target_rate) / target_rate * 100.0).abs();

    println!("Target rate: {target_rate:.2} req/s");
    println!("Actual rate: {actual_rate:.2} req/s");
    println!("Error: {error:.2}%");

    // Poisson should be accurate within 10%
    assert!(
        error < 10.0,
        "Poisson rate error too high: {error:.2}% (target: {target_rate:.2}, actual: {actual_rate:.2})"
    );
    println!("✓ Poisson rate accuracy: {error:.2}%");
}
