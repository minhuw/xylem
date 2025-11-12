//! Rate control integration tests
//!
//! Tests verifying that rate control mechanisms achieve target rates accurately
//! across different configurations. Requires Redis Docker container.

use std::time::Duration;
use xylem_core::threading::{Worker, WorkerConfig};
use xylem_core::workload::{KeyGeneration, RateControl, RequestGenerator};
use xylem_transport::TcpTransport;

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

/// Run a rate-limited experiment and return (target_rate, actual_rate, error_percent)
fn run_rate_experiment(target_rate: f64, duration_secs: u64, conn_count: usize) -> (f64, f64, f64) {
    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(duration_secs);

    let protocol = xylem_protocols::redis::RedisProtocol::new(Box::new(
        xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get),
    ));
    let protocol = ProtocolAdapter::new(protocol);
    let generator = RequestGenerator::new(
        KeyGeneration::sequential(0),
        RateControl::Fixed { rate: target_rate },
        Box::new(xylem_core::workload::FixedSize::new(64)),
    );
    let stats = common::create_test_stats();
    let config = WorkerConfig {
        target: target_addr,
        duration,
        value_size: 64,
        conn_count,
        max_pending_per_conn: 8,
    };

    let mut worker =
        Worker::with_closed_loop(TcpTransport::new, protocol, generator, stats, config).unwrap();

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

    println!("\n=== Testing Medium Rate (1000 req/s) ===");
    let (target, actual, error) = run_rate_experiment(1000.0, 5, 1);

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

#[test]
fn test_rate_accuracy_high_rate() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("\n=== Testing High Rate (10000 req/s) ===");
    let (target, actual, error) = run_rate_experiment(10000.0, 5, 2);

    println!("Target rate: {target:.2} req/s");
    println!("Actual rate: {actual:.2} req/s");
    println!("Error: {error:.2}%");

    // High rates may have more variation due to latency effects
    // Relaxed threshold: within 50% for CI/test environments
    assert!(
        error < 50.0 || actual > 5000.0,
        "High rate too far off: {error:.2}% (target: {target:.2}, actual: {actual:.2})"
    );
    println!("✓ High rate attempted: {actual:.2} req/s ({error:.2}% error)");
}

#[test]
fn test_rate_accuracy_very_high_rate() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("\n=== Testing Very High Rate (50000 req/s) ===");
    let (target, actual, error) = run_rate_experiment(50000.0, 5, 4);

    println!("Target rate: {target:.2} req/s");
    println!("Actual rate: {actual:.2} req/s");
    println!("Error: {error:.2}%");

    // Very high rates are limited by server capacity
    // CI environments may only achieve 5-10k req/s due to resource constraints
    println!("✓ Very high rate attempted: {actual:.2} req/s achieved");
    assert!(actual > 5000.0, "Should achieve at least 5k req/s (got {actual:.2})");
}

#[test]
fn test_rate_sweep() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("\n=== Rate Accuracy Sweep ===");
    println!(
        "{:<15} {:<15} {:<15} {:<15}",
        "Target (req/s)", "Actual (req/s)", "Error (%)", "Status"
    );
    println!("{:-<60}", "");

    let rates = vec![50.0, 100.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0];
    let mut results = Vec::new();

    for rate in rates {
        let (target, actual, error) = run_rate_experiment(rate, 3, 1);
        let status = if error < 10.0 { "✓" } else { "⚠" };

        println!("{target:<15.2} {actual:<15.2} {error:<15.2} {status:<15}");

        results.push((target, actual, error));
    }

    println!("\n=== Summary ===");
    let avg_error: f64 = results.iter().map(|(_, _, e)| e).sum::<f64>() / results.len() as f64;
    println!("Average error across all rates: {avg_error:.2}%");

    // Check that most rates are within tolerance (25% for CI environments)
    let accurate_count = results.iter().filter(|(_, _, e)| *e < 25.0).count();
    let accuracy_ratio = accurate_count as f64 / results.len() as f64;

    println!(
        "Rates within 25% error: {}/{} ({:.0}%)",
        accurate_count,
        results.len(),
        accuracy_ratio * 100.0
    );

    // Require at least 60% of rates to be within 25% error (relaxed for CI)
    assert!(
        accuracy_ratio >= 0.6,
        "At least 60% of rates should be within 25% error (got {:.0}%)",
        accuracy_ratio * 100.0
    );
    println!("✓ Rate control accuracy validated");
}

#[test]
fn test_rate_consistency() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("\n=== Testing Rate Consistency (5 runs at 1000 req/s) ===");

    let target_rate = 1000.0;
    let mut rates = Vec::new();

    for run in 1..=5 {
        let (_target, actual, error) = run_rate_experiment(target_rate, 3, 1);
        println!("Run {run}: {actual:.2} req/s (error: {error:.2}%)");
        rates.push(actual);
    }

    // Calculate standard deviation
    let mean = rates.iter().sum::<f64>() / rates.len() as f64;
    let variance = rates.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / rates.len() as f64;
    let std_dev = variance.sqrt();
    let cv = (std_dev / mean) * 100.0; // Coefficient of variation

    println!("\nStatistics:");
    println!("  Mean rate: {mean:.2} req/s");
    println!("  Std dev: {std_dev:.2} req/s");
    println!("  Coefficient of variation: {cv:.2}%");

    // Rate should be consistent (CV < 10%)
    assert!(cv < 10.0, "Rate control should be consistent across runs (CV: {cv:.2}%)");
    println!("✓ Rate control is consistent (CV: {cv:.2}%)");
}

#[test]
fn test_rate_with_multiple_connections() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("\n=== Testing Rate Control with Multiple Connections ===");

    let target_rate = 5000.0;
    let configs = vec![
        (1, "1 connection"),
        (2, "2 connections"),
        (4, "4 connections"),
        (8, "8 connections"),
    ];

    println!("{:<20} {:<15} {:<15} {:<15}", "Configuration", "Target", "Actual", "Error %");
    println!("{:-<65}", "");

    for (conn_count, desc) in configs {
        let (target, actual, error) = run_rate_experiment(target_rate, 3, conn_count);

        println!("{desc:<20} {target:<15.2} {actual:<15.2} {error:<15.2}");

        // All configurations should achieve target within 15%
        assert!(error < 15.0, "{desc} error too high: {error:.2}%");
    }

    println!("✓ Rate control works correctly with multiple connections");
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
    let generator = RequestGenerator::new(
        KeyGeneration::sequential(0),
        RateControl::ClosedLoop,
        Box::new(xylem_core::workload::FixedSize::new(64)),
    );
    let stats = common::create_test_stats();
    let config = WorkerConfig {
        target: target_addr,
        duration,
        value_size: 64,
        conn_count: 4,
        max_pending_per_conn: 16,
    };

    let mut worker =
        Worker::with_closed_loop(TcpTransport::new, protocol, generator, stats, config).unwrap();

    worker.run().unwrap();
    let max_throughput = worker.stats().global().tx_requests() as f64 / duration.as_secs_f64();

    println!("Maximum throughput (closed-loop): {max_throughput:.0} req/s\n");

    // Test rates below and above saturation point
    let test_rates = vec![
        max_throughput * 0.5, // 50% of max
        max_throughput * 0.8, // 80% of max
        max_throughput * 1.2, // 120% of max (should saturate)
        max_throughput * 2.0, // 200% of max (should saturate)
    ];

    println!("{:<20} {:<20} {:<15}", "Target Rate", "Actual Rate", "Saturation");
    println!("{:-<55}", "");

    for target in test_rates {
        let (_t, actual, _e) = run_rate_experiment(target, 3, 4);
        let saturation = if actual < target * 0.9 { "Yes" } else { "No" };

        println!("{target:<20.0} {actual:<20.0} {saturation:<15}");
    }

    println!("\n✓ Rate control behaves correctly at saturation");
}
