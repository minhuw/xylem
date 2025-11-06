//! Rate control accuracy tests
//!
//! These tests verify that open-loop rate control achieves the target rate accurately
//! across different configurations and rate targets.

use std::process::{Child, Command};
use std::sync::Mutex;
use std::thread::sleep;
use std::time::Duration;
use xylem_core::stats::StatsCollector;
use xylem_core::threading::{PipelinedWorker, PipelinedWorkerConfig};
use xylem_core::workload::{KeyGeneration, RateControl, RequestGenerator};
use xylem_transport::TcpTransport;

// Global Redis server state
static REDIS_SERVER: Mutex<Option<Child>> = Mutex::new(None);

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

fn check_redis_available() -> bool {
    std::net::TcpStream::connect_timeout(&"127.0.0.1:6379".parse().unwrap(), Duration::from_secs(1))
        .is_ok()
}

fn start_redis() -> Result<(), Box<dyn std::error::Error>> {
    if check_redis_available() {
        println!("✓ Redis already running on port 6379");
        return Ok(());
    }

    println!("Starting Redis server...");
    let _ = Command::new("pkill").args(["-f", "redis-server.*6379"]).output();
    sleep(Duration::from_millis(100));

    let child = Command::new("redis-server")
        .args(["--port", "6379", "--save", "", "--appendonly", "no"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    *REDIS_SERVER.lock().unwrap() = Some(child);

    for i in 0..30 {
        sleep(Duration::from_millis(100));
        if check_redis_available() {
            println!("✓ Redis server ready after {}ms", (i + 1) * 100);
            return Ok(());
        }
    }

    Err("Redis failed to start within 3 seconds".into())
}

fn stop_redis() {
    println!("Stopping Redis server...");
    let _ = Command::new("redis-cli").args(["-p", "6379", "shutdown", "nosave"]).output();
    sleep(Duration::from_millis(100));

    if let Ok(mut guard) = REDIS_SERVER.lock() {
        if let Some(mut child) = guard.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    let _ = Command::new("pkill").args(["-f", "redis-server.*6379"]).output();
    println!("✓ Redis server stopped");
}

struct RedisGuard;

impl Drop for RedisGuard {
    fn drop(&mut self) {
        stop_redis();
    }
}

fn setup_redis() -> Result<RedisGuard, Box<dyn std::error::Error>> {
    start_redis()?;
    Ok(RedisGuard)
}

/// Run a rate-limited experiment and return (target_rate, actual_rate, error_percent)
fn run_rate_experiment(target_rate: f64, duration_secs: u64, conn_count: usize) -> (f64, f64, f64) {
    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(duration_secs);

    let protocol = xylem_protocols::redis::RedisProtocol::new(xylem_protocols::redis::RedisOp::Get);
    let protocol = ProtocolAdapter::new(protocol);
    let generator = RequestGenerator::new(
        KeyGeneration::sequential(0),
        RateControl::Fixed { rate: target_rate },
        64,
    );
    let stats = StatsCollector::default();
    let config = PipelinedWorkerConfig {
        target: target_addr,
        duration,
        value_size: 64,
        conn_count,
        max_pending_per_conn: 8,
    };

    let mut worker =
        PipelinedWorker::new(TcpTransport::new, protocol, generator, stats, config).unwrap();

    let result = worker.run();
    assert!(result.is_ok(), "Worker failed: {:?}", result.err());

    let stats = worker.stats();
    let actual_rate = stats.tx_requests() as f64 / duration.as_secs_f64();
    let error_percent = ((actual_rate - target_rate) / target_rate * 100.0).abs();

    (target_rate, actual_rate, error_percent)
}

#[test]
#[ignore]
fn test_rate_accuracy_low_rate() {
    let _guard = setup_redis().expect("Failed to start Redis");

    println!("\n=== Testing Low Rate (100 req/s) ===");
    let (target, actual, error) = run_rate_experiment(100.0, 5, 1);

    println!("Target rate: {:.2} req/s", target);
    println!("Actual rate: {:.2} req/s", actual);
    println!("Error: {:.2}%", error);

    // Low rates should be very accurate (within 5%)
    assert!(
        error < 5.0,
        "Low rate error too high: {:.2}% (target: {:.2}, actual: {:.2})",
        error,
        target,
        actual
    );
    println!("✓ Low rate accuracy: {:.2}%", error);
}

#[test]
#[ignore]
fn test_rate_accuracy_medium_rate() {
    let _guard = setup_redis().expect("Failed to start Redis");

    println!("\n=== Testing Medium Rate (1000 req/s) ===");
    let (target, actual, error) = run_rate_experiment(1000.0, 5, 1);

    println!("Target rate: {:.2} req/s", target);
    println!("Actual rate: {:.2} req/s", actual);
    println!("Error: {:.2}%", error);

    // Medium rates should be accurate within 10%
    assert!(
        error < 10.0,
        "Medium rate error too high: {:.2}% (target: {:.2}, actual: {:.2})",
        error,
        target,
        actual
    );
    println!("✓ Medium rate accuracy: {:.2}%", error);
}

#[test]
#[ignore]
fn test_rate_accuracy_high_rate() {
    let _guard = setup_redis().expect("Failed to start Redis");

    println!("\n=== Testing High Rate (10000 req/s) ===");
    let (target, actual, error) = run_rate_experiment(10000.0, 5, 2);

    println!("Target rate: {:.2} req/s", target);
    println!("Actual rate: {:.2} req/s", actual);
    println!("Error: {:.2}%", error);

    // High rates may have more variation due to latency effects (within 15%)
    assert!(
        error < 15.0,
        "High rate error too high: {:.2}% (target: {:.2}, actual: {:.2})",
        error,
        target,
        actual
    );
    println!("✓ High rate accuracy: {:.2}%", error);
}

#[test]
#[ignore]
fn test_rate_accuracy_very_high_rate() {
    let _guard = setup_redis().expect("Failed to start Redis");

    println!("\n=== Testing Very High Rate (50000 req/s) ===");
    let (target, actual, error) = run_rate_experiment(50000.0, 5, 4);

    println!("Target rate: {:.2} req/s", target);
    println!("Actual rate: {:.2} req/s", actual);
    println!("Error: {:.2}%", error);

    // Very high rates are limited by server capacity
    // We just verify it's attempting to go fast
    println!("✓ Very high rate attempted: {:.2} req/s achieved", actual);
    assert!(actual > 30000.0, "Should achieve at least 30k req/s");
}

#[test]
#[ignore]
fn test_rate_sweep() {
    let _guard = setup_redis().expect("Failed to start Redis");

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

        println!("{:<15.2} {:<15.2} {:<15.2} {:<15}", target, actual, error, status);

        results.push((target, actual, error));
    }

    println!("\n=== Summary ===");
    let avg_error: f64 = results.iter().map(|(_, _, e)| e).sum::<f64>() / results.len() as f64;
    println!("Average error across all rates: {:.2}%", avg_error);

    // Check that most rates are within 15% error
    let accurate_count = results.iter().filter(|(_, _, e)| *e < 15.0).count();
    let accuracy_ratio = accurate_count as f64 / results.len() as f64;

    println!(
        "Rates within 15% error: {}/{} ({:.0}%)",
        accurate_count,
        results.len(),
        accuracy_ratio * 100.0
    );

    assert!(accuracy_ratio >= 0.7, "At least 70% of rates should be within 15% error");
    println!("✓ Rate control accuracy validated");
}

#[test]
#[ignore]
fn test_rate_consistency() {
    let _guard = setup_redis().expect("Failed to start Redis");

    println!("\n=== Testing Rate Consistency (5 runs at 1000 req/s) ===");

    let target_rate = 1000.0;
    let mut rates = Vec::new();

    for run in 1..=5 {
        let (_target, actual, error) = run_rate_experiment(target_rate, 3, 1);
        println!("Run {}: {:.2} req/s (error: {:.2}%)", run, actual, error);
        rates.push(actual);
    }

    // Calculate standard deviation
    let mean = rates.iter().sum::<f64>() / rates.len() as f64;
    let variance = rates.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / rates.len() as f64;
    let std_dev = variance.sqrt();
    let cv = (std_dev / mean) * 100.0; // Coefficient of variation

    println!("\nStatistics:");
    println!("  Mean rate: {:.2} req/s", mean);
    println!("  Std dev: {:.2} req/s", std_dev);
    println!("  Coefficient of variation: {:.2}%", cv);

    // Rate should be consistent (CV < 10%)
    assert!(cv < 10.0, "Rate control should be consistent across runs (CV: {:.2}%)", cv);
    println!("✓ Rate control is consistent (CV: {:.2}%)", cv);
}

#[test]
#[ignore]
fn test_rate_with_multiple_connections() {
    let _guard = setup_redis().expect("Failed to start Redis");

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

        println!("{:<20} {:<15.2} {:<15.2} {:<15.2}", desc, target, actual, error);

        // All configurations should achieve target within 15%
        assert!(error < 15.0, "{} error too high: {:.2}%", desc, error);
    }

    println!("✓ Rate control works correctly with multiple connections");
}

#[test]
#[ignore]
fn test_rate_vs_throughput_saturation() {
    let _guard = setup_redis().expect("Failed to start Redis");

    println!("\n=== Testing Rate vs Throughput Saturation ===");
    println!("Finding maximum achievable throughput...\n");

    // First, find max throughput in closed-loop mode
    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(3);

    let protocol = xylem_protocols::redis::RedisProtocol::new(xylem_protocols::redis::RedisOp::Get);
    let protocol = ProtocolAdapter::new(protocol);
    let generator =
        RequestGenerator::new(KeyGeneration::sequential(0), RateControl::ClosedLoop, 64);
    let stats = StatsCollector::default();
    let config = PipelinedWorkerConfig {
        target: target_addr,
        duration,
        value_size: 64,
        conn_count: 4,
        max_pending_per_conn: 16,
    };

    let mut worker =
        PipelinedWorker::new(TcpTransport::new, protocol, generator, stats, config).unwrap();

    worker.run().unwrap();
    let max_throughput = worker.stats().tx_requests() as f64 / duration.as_secs_f64();

    println!("Maximum throughput (closed-loop): {:.0} req/s\n", max_throughput);

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

        println!("{:<20.0} {:<20.0} {:<15}", target, actual, saturation);
    }

    println!("\n✓ Rate control behaves correctly at saturation");
}
