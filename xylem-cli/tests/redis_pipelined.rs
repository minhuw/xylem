//! Integration tests for Redis protocol with pipelining
//!
//! These tests demonstrate the pipelined worker with multiple outstanding requests.

use std::process::{Child, Command};
use std::sync::Mutex;
use std::thread::sleep;
use std::time::Duration;
use xylem_core::stats::StatsCollector;
use xylem_core::threading::{PipelinedWorker, PipelinedWorkerConfig};
use xylem_core::workload::{KeyGeneration, RateControl, RequestGenerator};
use xylem_transport::TcpTransport;

// Global state to track Redis server process
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

#[test]
#[ignore]
fn test_redis_pipelined_single_connection() {
    let _guard = setup_redis().expect("Failed to start Redis");

    println!("Running pipelined test with 1 connection...");

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(2);

    let protocol = xylem_protocols::redis::RedisProtocol::new(xylem_protocols::redis::RedisOp::Get);
    let protocol = ProtocolAdapter::new(protocol);
    let generator =
        RequestGenerator::new(KeyGeneration::sequential(0), RateControl::ClosedLoop, 64);
    let stats = StatsCollector::default();
    let config = PipelinedWorkerConfig {
        target: target_addr,
        duration,
        value_size: 64,
        conn_count: 1,
        max_pending_per_conn: 16, // Allow 16 pipelined requests
    };

    let mut worker =
        PipelinedWorker::with_closed_loop(TcpTransport::new, protocol, generator, stats, config)
            .unwrap();

    println!("Starting pipelined test (1 conn, 16 max pending)...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete: {:?}", result.err());

    let stats = worker.stats();
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
#[ignore]
fn test_redis_pipelined_multiple_connections() {
    let _guard = setup_redis().expect("Failed to start Redis");

    println!("Running pipelined test with 4 connections...");

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(2);

    let protocol = xylem_protocols::redis::RedisProtocol::new(xylem_protocols::redis::RedisOp::Get);
    let protocol = ProtocolAdapter::new(protocol);
    let generator =
        RequestGenerator::new(KeyGeneration::sequential(0), RateControl::ClosedLoop, 64);
    let stats = StatsCollector::default();
    let config = PipelinedWorkerConfig {
        target: target_addr,
        duration,
        value_size: 64,
        conn_count: 4,            // 4 connections
        max_pending_per_conn: 16, // 16 pipelined per connection
    };

    let mut worker =
        PipelinedWorker::with_closed_loop(TcpTransport::new, protocol, generator, stats, config)
            .unwrap();

    println!("Starting pipelined test (4 conns, 16 max pending each)...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete: {:?}", result.err());

    let stats = worker.stats();
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

#[test]
#[ignore]
fn test_redis_pipelined_rate_limited() {
    let _guard = setup_redis().expect("Failed to start Redis");

    println!("Running pipelined rate-limited test...");

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(2);
    let target_rate = 1000.0; // 1000 req/s

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
        conn_count: 2,
        max_pending_per_conn: 8,
    };

    let mut worker =
        PipelinedWorker::with_closed_loop(TcpTransport::new, protocol, generator, stats, config)
            .unwrap();

    println!("Starting rate-limited test (target: {target_rate} req/s)...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete: {:?}", result.err());

    let stats = worker.stats();
    let actual_rate = stats.tx_requests() as f64 / duration.as_secs_f64();

    println!("Results:");
    println!("  Target rate: {target_rate:.2} req/s");
    println!("  Actual rate: {actual_rate:.2} req/s");
    println!("  Total requests: {}", stats.tx_requests());

    // Verify rate is close to target (within 20%)
    let rate_ratio = actual_rate / target_rate;
    assert!(
        rate_ratio > 0.8 && rate_ratio < 1.2,
        "Actual rate should be within 20% of target, got {rate_ratio:.2}x"
    );

    println!("✓ Rate control working ({}% of target)", (rate_ratio * 100.0) as i32);
}
