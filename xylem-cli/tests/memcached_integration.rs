//! Integration tests for Memcached protocols
//!
//! These tests use Docker Compose to manage a Memcached server for testing.
//! Docker is required to run these tests.

use std::time::Duration;
use xylem_core::stats::GroupStatsCollector;
use xylem_core::threading::{ThreadingRuntime, Worker, WorkerConfig};

use xylem_transport::TcpTransportFactory;

mod common;

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

/// Test Memcached Binary protocol with single thread
#[test]
fn test_memcached_binary_single_thread() {
    let _memcached = common::memcached::MemcachedGuard::new().expect("Failed to start Memcached");

    let runtime = ThreadingRuntime::new(1);
    let target_addr = "127.0.0.1:11211".parse().unwrap();

    let results = runtime
        .run_workers_generic(move |_thread_id| {
            let protocol = ProtocolAdapter::new(
                xylem_protocols::memcached::binary::MemcachedBinaryProtocol::new(
                    xylem_protocols::memcached::binary::MemcachedOp::Get,
                ),
            );
            let _transport = TcpTransportFactory::default();
            let stats = common::create_test_stats();
            let config = WorkerConfig {
                target: target_addr,
                duration: Duration::from_secs(1),
                conn_count: 1,
                max_pending_per_conn: 1,
            };

            let mut worker =
                Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, config)?;
            worker.run()?;
            Ok(worker.into_stats())
        })
        .expect("Worker failed");

    let stats = GroupStatsCollector::merge(results);
    let basic_stats = stats.global().calculate_basic_stats();

    println!("\n=== Memcached Binary Protocol Test Results ===");
    println!("Requests: {}", stats.global().tx_requests());
    println!("Mean latency: {:.2} μs", basic_stats.mean.as_micros());
    println!("Min latency: {:.2} μs", basic_stats.min.as_micros());
    println!("Max latency: {:.2} μs", basic_stats.max.as_micros());

    // Assertions
    assert!(stats.global().tx_requests() > 100, "Should process at least 100 requests");
    assert!(basic_stats.mean.as_micros() > 0, "Mean latency should be positive");
    assert!(basic_stats.mean.as_millis() < 10, "Mean latency should be < 10ms");
}

/// Test Memcached ASCII protocol with single thread
#[test]
fn test_memcached_ascii_single_thread() {
    let _memcached = common::memcached::MemcachedGuard::new().expect("Failed to start Memcached");

    let runtime = ThreadingRuntime::new(1);
    let target_addr = "127.0.0.1:11211".parse().unwrap();

    let results = runtime
        .run_workers_generic(move |_thread_id| {
            let protocol = ProtocolAdapter::new(
                xylem_protocols::memcached::ascii::MemcachedAsciiProtocol::new(
                    xylem_protocols::memcached::ascii::MemcachedOp::Get,
                ),
            );
            let _transport = TcpTransportFactory::default();
            let stats = common::create_test_stats();
            let config = WorkerConfig {
                target: target_addr,
                duration: Duration::from_secs(1),
                conn_count: 1,
                max_pending_per_conn: 1,
            };

            let mut worker =
                Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, config)?;
            worker.run()?;
            Ok(worker.into_stats())
        })
        .expect("Worker failed");

    let stats = GroupStatsCollector::merge(results);
    let basic_stats = stats.global().calculate_basic_stats();

    println!("\n=== Memcached ASCII Protocol Test Results ===");
    println!("Requests: {}", stats.global().tx_requests());
    println!("Mean latency: {:.2} μs", basic_stats.mean.as_micros());
    println!("Min latency: {:.2} μs", basic_stats.min.as_micros());
    println!("Max latency: {:.2} μs", basic_stats.max.as_micros());

    // Assertions
    assert!(stats.global().tx_requests() > 100, "Should process at least 100 requests");
    assert!(basic_stats.mean.as_micros() > 0, "Mean latency should be positive");
    assert!(basic_stats.mean.as_millis() < 10, "Mean latency should be < 10ms");
}

/// Test Memcached Binary protocol with multiple threads
#[test]
fn test_memcached_binary_multi_thread() {
    let _memcached = common::memcached::MemcachedGuard::new().expect("Failed to start Memcached");

    let runtime = ThreadingRuntime::new(4);
    let target_addr = "127.0.0.1:11211".parse().unwrap();

    let results = runtime
        .run_workers_generic(move |_thread_id| {
            let protocol = ProtocolAdapter::new(
                xylem_protocols::memcached::binary::MemcachedBinaryProtocol::new(
                    xylem_protocols::memcached::binary::MemcachedOp::Get,
                ),
            );
            let _transport = TcpTransportFactory::default();
            let stats = common::create_test_stats();
            let config = WorkerConfig {
                target: target_addr,
                duration: Duration::from_secs(2),
                conn_count: 1,
                max_pending_per_conn: 1,
            };

            let mut worker =
                Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, config)?;
            worker.run()?;
            Ok(worker.into_stats())
        })
        .expect("Worker failed");

    let stats = GroupStatsCollector::merge(results);
    let basic_stats = stats.global().calculate_basic_stats();
    let throughput_rps = stats.global().tx_requests() as f64 / 2.0; // 2 second duration

    println!("\n=== Memcached Binary Multi-Thread Test Results ===");
    println!("Threads: 4");
    println!("Requests: {}", stats.global().tx_requests());
    println!("Throughput: {throughput_rps:.2} req/s");
    println!("Mean latency: {:.2} μs", basic_stats.mean.as_micros());

    // Assertions
    assert!(
        stats.global().tx_requests() > 1000,
        "Should process at least 1000 requests with 4 threads"
    );
    assert!(throughput_rps > 100.0, "Should achieve > 100 req/s");
}

/// Test Memcached ASCII protocol with rate limiting
#[test]
fn test_memcached_ascii_rate_limited() {
    let _memcached = common::memcached::MemcachedGuard::new().expect("Failed to start Memcached");

    let runtime = ThreadingRuntime::new(1);
    let target_addr = "127.0.0.1:11211".parse().unwrap();
    let target_rate = 1000.0; // 1000 req/s

    let results = runtime
        .run_workers_generic(move |_thread_id| {
            let protocol = ProtocolAdapter::new(
                xylem_protocols::memcached::ascii::MemcachedAsciiProtocol::new(
                    xylem_protocols::memcached::ascii::MemcachedOp::Get,
                ),
            );
            let _transport = TcpTransportFactory::default();
            let stats = common::create_test_stats();
            let config = WorkerConfig {
                target: target_addr,
                duration: Duration::from_secs(2),
                conn_count: 1,
                max_pending_per_conn: 1,
            };

            // Use fixed-rate policy for rate limiting
            let policy_scheduler =
                Box::new(xylem_core::scheduler::UniformPolicyScheduler::fixed_rate(target_rate));

            let mut worker = Worker::new(
                &TcpTransportFactory::default(),
                protocol,
                stats,
                config,
                policy_scheduler,
            )?;
            worker.run()?;
            Ok(worker.into_stats())
        })
        .expect("Worker failed");

    let stats = GroupStatsCollector::merge(results);
    let basic_stats = stats.global().calculate_basic_stats();
    let actual_rate = stats.global().tx_requests() as f64 / 2.0; // 2 second duration

    println!("\n=== Memcached ASCII Rate-Limited Test Results ===");
    println!("Target rate: {target_rate:.2} req/s");
    println!("Actual rate: {actual_rate:.2} req/s");
    println!("Requests: {}", stats.global().tx_requests());
    println!("Mean latency: {:.2} μs", basic_stats.mean.as_micros());

    // Allow 10% deviation from target rate
    let rate_error = (actual_rate - target_rate).abs() / target_rate;
    assert!(rate_error < 0.10, "Rate error should be < 10%, got {:.1}%", rate_error * 100.0);
}
