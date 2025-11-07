//! Integration tests for Memcached protocols
//!
//! These tests automatically start and stop a Memcached server for testing.

use std::process::{Child, Command};
use std::sync::Mutex;
use std::thread::sleep;
use std::time::Duration;
use xylem_core::stats::StatsCollector;
use xylem_core::threading::{ThreadingRuntime, Worker, WorkerConfig};
use xylem_core::workload::{KeyGeneration, RateControl, RequestGenerator};
use xylem_transport::TcpTransport;

// Global state to track Memcached server process
static MEMCACHED_SERVER: Mutex<Option<Child>> = Mutex::new(None);

// Protocol adapter to bridge xylem_protocols::Protocol with worker Protocol trait
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

/// Helper to check if Memcached is available
fn check_memcached_available() -> bool {
    std::net::TcpStream::connect_timeout(
        &"127.0.0.1:11211".parse().unwrap(),
        Duration::from_secs(1),
    )
    .is_ok()
}

/// Start Memcached server if not already running
fn start_memcached() -> Result<(), Box<dyn std::error::Error>> {
    if check_memcached_available() {
        println!("✓ Memcached already running on port 11211");
        return Ok(());
    }

    println!("Starting Memcached server on port 11211...");

    let child = Command::new("memcached")
        .args([
            "-p", "11211", // Port
            "-m", "64", // 64MB memory
            "-c", "1024", // Max connections
            "-vv",  // Very verbose
        ])
        .spawn()?;

    // Store the child process
    let mut server = MEMCACHED_SERVER.lock().unwrap();
    *server = Some(child);

    // Wait for server to be ready
    for _ in 0..30 {
        sleep(Duration::from_millis(100));
        if check_memcached_available() {
            println!("✓ Memcached started successfully");
            return Ok(());
        }
    }

    Err("Memcached failed to start within timeout".into())
}

/// Stop Memcached server if running
fn stop_memcached() {
    let mut server = MEMCACHED_SERVER.lock().unwrap();
    if let Some(mut child) = server.take() {
        let _ = child.kill();
        let _ = child.wait();
        println!("✓ Memcached server stopped");
    }
}

/// Setup function to ensure Memcached is running
fn setup() -> Result<(), Box<dyn std::error::Error>> {
    start_memcached()
}

/// Test Memcached Binary protocol with single thread
#[test]
#[ignore] // Run with: cargo test --test memcached_integration -- --ignored
fn test_memcached_binary_single_thread() {
    setup().expect("Failed to start Memcached");

    let runtime = ThreadingRuntime::new(1);
    let target_addr = "127.0.0.1:11211".parse().unwrap();

    let results = runtime
        .run_workers(move |_thread_id| {
            let protocol = ProtocolAdapter::new(
                xylem_protocols::memcached::binary::MemcachedBinaryProtocol::new(
                    xylem_protocols::memcached::binary::MemcachedOp::Get,
                ),
            );
            let transport = TcpTransport::new();
            let generator =
                RequestGenerator::new(KeyGeneration::sequential(0), RateControl::ClosedLoop, 64);
            let stats = StatsCollector::default();
            let config = WorkerConfig {
                target: target_addr,
                duration: Duration::from_secs(1),
                value_size: 64,
            };

            let mut worker = Worker::new(transport, protocol, generator, stats, config);
            worker.run().unwrap();
            Ok(worker.into_stats())
        })
        .expect("Worker failed");

    let stats = StatsCollector::merge(results);
    let basic_stats = stats.calculate_basic_stats();

    println!("\n=== Memcached Binary Protocol Test Results ===");
    println!("Requests: {}", stats.tx_requests());
    println!("Mean latency: {:.2} μs", basic_stats.mean.as_micros());
    println!("Min latency: {:.2} μs", basic_stats.min.as_micros());
    println!("Max latency: {:.2} μs", basic_stats.max.as_micros());

    // Assertions
    assert!(stats.tx_requests() > 100, "Should process at least 100 requests");
    assert!(basic_stats.mean.as_micros() > 0, "Mean latency should be positive");
    assert!(basic_stats.mean.as_millis() < 10, "Mean latency should be < 10ms");
}

/// Test Memcached ASCII protocol with single thread
#[test]
#[ignore] // Run with: cargo test --test memcached_integration -- --ignored
fn test_memcached_ascii_single_thread() {
    setup().expect("Failed to start Memcached");

    let runtime = ThreadingRuntime::new(1);
    let target_addr = "127.0.0.1:11211".parse().unwrap();

    let results = runtime
        .run_workers(move |_thread_id| {
            let protocol = ProtocolAdapter::new(
                xylem_protocols::memcached::ascii::MemcachedAsciiProtocol::new(
                    xylem_protocols::memcached::ascii::MemcachedOp::Get,
                ),
            );
            let transport = TcpTransport::new();
            let generator =
                RequestGenerator::new(KeyGeneration::sequential(0), RateControl::ClosedLoop, 64);
            let stats = StatsCollector::default();
            let config = WorkerConfig {
                target: target_addr,
                duration: Duration::from_secs(1),
                value_size: 64,
            };

            let mut worker = Worker::new(transport, protocol, generator, stats, config);
            worker.run().unwrap();
            Ok(worker.into_stats())
        })
        .expect("Worker failed");

    let stats = StatsCollector::merge(results);
    let basic_stats = stats.calculate_basic_stats();

    println!("\n=== Memcached ASCII Protocol Test Results ===");
    println!("Requests: {}", stats.tx_requests());
    println!("Mean latency: {:.2} μs", basic_stats.mean.as_micros());
    println!("Min latency: {:.2} μs", basic_stats.min.as_micros());
    println!("Max latency: {:.2} μs", basic_stats.max.as_micros());

    // Assertions
    assert!(stats.tx_requests() > 100, "Should process at least 100 requests");
    assert!(basic_stats.mean.as_micros() > 0, "Mean latency should be positive");
    assert!(basic_stats.mean.as_millis() < 10, "Mean latency should be < 10ms");
}

/// Test Memcached Binary protocol with multiple threads
#[test]
#[ignore] // Run with: cargo test --test memcached_integration -- --ignored
fn test_memcached_binary_multi_thread() {
    setup().expect("Failed to start Memcached");

    let runtime = ThreadingRuntime::new(4);
    let target_addr = "127.0.0.1:11211".parse().unwrap();

    let results = runtime
        .run_workers(move |_thread_id| {
            let protocol = ProtocolAdapter::new(
                xylem_protocols::memcached::binary::MemcachedBinaryProtocol::new(
                    xylem_protocols::memcached::binary::MemcachedOp::Get,
                ),
            );
            let transport = TcpTransport::new();
            let generator =
                RequestGenerator::new(KeyGeneration::random(10000), RateControl::ClosedLoop, 64);
            let stats = StatsCollector::default();
            let config = WorkerConfig {
                target: target_addr,
                duration: Duration::from_secs(2),
                value_size: 64,
            };

            let mut worker = Worker::new(transport, protocol, generator, stats, config);
            worker.run().unwrap();
            Ok(worker.into_stats())
        })
        .expect("Worker failed");

    let stats = StatsCollector::merge(results);
    let basic_stats = stats.calculate_basic_stats();
    let throughput_rps = stats.tx_requests() as f64 / 2.0; // 2 second duration

    println!("\n=== Memcached Binary Multi-Thread Test Results ===");
    println!("Threads: 4");
    println!("Requests: {}", stats.tx_requests());
    println!("Throughput: {throughput_rps:.2} req/s");
    println!("Mean latency: {:.2} μs", basic_stats.mean.as_micros());

    // Assertions
    assert!(
        stats.tx_requests() > 1000,
        "Should process at least 1000 requests with 4 threads"
    );
    assert!(throughput_rps > 100.0, "Should achieve > 100 req/s");
}

/// Test Memcached ASCII protocol with rate limiting
#[test]
#[ignore] // Run with: cargo test --test memcached_integration -- --ignored
fn test_memcached_ascii_rate_limited() {
    setup().expect("Failed to start Memcached");

    let runtime = ThreadingRuntime::new(1);
    let target_addr = "127.0.0.1:11211".parse().unwrap();
    let target_rate = 1000.0; // 1000 req/s

    let results = runtime
        .run_workers(move |_thread_id| {
            let protocol = ProtocolAdapter::new(
                xylem_protocols::memcached::ascii::MemcachedAsciiProtocol::new(
                    xylem_protocols::memcached::ascii::MemcachedOp::Get,
                ),
            );
            let transport = TcpTransport::new();
            let generator = RequestGenerator::new(
                KeyGeneration::sequential(0),
                RateControl::Fixed { rate: target_rate },
                64,
            );
            let stats = StatsCollector::default();
            let config = WorkerConfig {
                target: target_addr,
                duration: Duration::from_secs(2),
                value_size: 64,
            };

            let mut worker = Worker::new(transport, protocol, generator, stats, config);
            worker.run().unwrap();
            Ok(worker.into_stats())
        })
        .expect("Worker failed");

    let stats = StatsCollector::merge(results);
    let basic_stats = stats.calculate_basic_stats();
    let actual_rate = stats.tx_requests() as f64 / 2.0; // 2 second duration

    println!("\n=== Memcached ASCII Rate-Limited Test Results ===");
    println!("Target rate: {target_rate:.2} req/s");
    println!("Actual rate: {actual_rate:.2} req/s");
    println!("Requests: {}", stats.tx_requests());
    println!("Mean latency: {:.2} μs", basic_stats.mean.as_micros());

    // Allow 10% deviation from target rate
    let rate_error = (actual_rate - target_rate).abs() / target_rate;
    assert!(rate_error < 0.10, "Rate error should be < 10%, got {:.1}%", rate_error * 100.0);
}

/// Cleanup function
#[test]
fn cleanup() {
    stop_memcached();
}
