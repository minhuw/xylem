//! Integration tests for Memcached protocols
//!
//! These tests use Docker Compose to manage a Memcached server for testing.
//! Docker is required to run these tests.

use std::time::Duration;
use xylem_core::stats::TupleStatsCollector;
use xylem_core::threading::{ThreadingRuntime, Worker, WorkerConfig};

use xylem_transport::TcpTransportFactory;

mod common;

/// Test Memcached Binary protocol with single thread
#[test]
fn test_memcached_binary_single_thread() {
    let memcached = common::memcached::MemcachedGuard::new().expect("Failed to start Memcached");

    let runtime = ThreadingRuntime::new(1);
    let target_addr = memcached.addr();

    let results = runtime
        .run_workers_generic(move |_thread_id| {
            let protocol = xylem_protocols::memcached::binary::MemcachedBinaryProtocol::new(
                xylem_protocols::memcached::binary::MemcachedOp::Get,
            );
            let _transport = TcpTransportFactory::default();
            let stats = common::create_test_stats();
            let config = WorkerConfig {
                target: target_addr,
                duration: Duration::from_secs(1),
                conn_count: 1,
                max_pending_per_conn: 1,
                drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
            };

            let mut worker =
                Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, config)?;
            worker.run()?;
            Ok(worker.into_stats())
        })
        .expect("Worker failed");

    let stats = TupleStatsCollector::merge(results);
    let global = stats.aggregate_global(Duration::from_secs(1));

    println!("\n=== Memcached Binary Protocol Test Results ===");
    println!("Requests: {}", global.total_requests);
    println!("Mean latency: {:.2} μs", global.mean_latency.as_micros());
    println!("P50 latency: {:.2} μs", global.latency_p50.as_micros());
    println!("P99 latency: {:.2} μs", global.latency_p99.as_micros());

    // Assertions
    assert!(global.total_requests > 100, "Should process at least 100 requests");
    assert!(global.mean_latency.as_micros() > 0, "Mean latency should be positive");
    assert!(global.mean_latency.as_millis() < 10, "Mean latency should be < 10ms");
}

/// Test Memcached ASCII protocol with single thread
#[test]
fn test_memcached_ascii_single_thread() {
    let memcached = common::memcached::MemcachedGuard::new().expect("Failed to start Memcached");

    let runtime = ThreadingRuntime::new(1);
    let target_addr = memcached.addr();

    let results = runtime
        .run_workers_generic(move |_thread_id| {
            let protocol = xylem_protocols::memcached::ascii::MemcachedAsciiProtocol::new(
                xylem_protocols::memcached::ascii::MemcachedOp::Get,
            );
            let _transport = TcpTransportFactory::default();
            let stats = common::create_test_stats();
            let config = WorkerConfig {
                target: target_addr,
                duration: Duration::from_secs(1),
                conn_count: 1,
                max_pending_per_conn: 1,
                drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
            };

            let mut worker =
                Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, config)?;
            worker.run()?;
            Ok(worker.into_stats())
        })
        .expect("Worker failed");

    let stats = TupleStatsCollector::merge(results);
    let global = stats.aggregate_global(Duration::from_secs(1));

    println!("\n=== Memcached ASCII Protocol Test Results ===");
    println!("Requests: {}", global.total_requests);
    println!("Mean latency: {:.2} μs", global.mean_latency.as_micros());
    println!("P50 latency: {:.2} μs", global.latency_p50.as_micros());
    println!("P99 latency: {:.2} μs", global.latency_p99.as_micros());

    // Assertions
    assert!(global.total_requests > 100, "Should process at least 100 requests");
    assert!(global.mean_latency.as_micros() > 0, "Mean latency should be positive");
    assert!(global.mean_latency.as_millis() < 10, "Mean latency should be < 10ms");
}

/// Test Memcached Binary protocol with multiple threads
#[test]
fn test_memcached_binary_multi_thread() {
    let memcached = common::memcached::MemcachedGuard::new().expect("Failed to start Memcached");

    let runtime = ThreadingRuntime::new(4);
    let target_addr = memcached.addr();

    let results = runtime
        .run_workers_generic(move |_thread_id| {
            let protocol = xylem_protocols::memcached::binary::MemcachedBinaryProtocol::new(
                xylem_protocols::memcached::binary::MemcachedOp::Get,
            );
            let _transport = TcpTransportFactory::default();
            let stats = common::create_test_stats();
            let config = WorkerConfig {
                target: target_addr,
                duration: Duration::from_secs(2),
                conn_count: 1,
                max_pending_per_conn: 1,
                drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
            };

            let mut worker =
                Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, config)?;
            worker.run()?;
            Ok(worker.into_stats())
        })
        .expect("Worker failed");

    let stats = TupleStatsCollector::merge(results);
    let global = stats.aggregate_global(Duration::from_secs(2));
    let throughput_rps = global.total_requests as f64 / 2.0; // 2 second duration

    println!("\n=== Memcached Binary Multi-Thread Test Results ===");
    println!("Threads: 4");
    println!("Requests: {}", global.total_requests);
    println!("Throughput: {throughput_rps:.2} req/s");
    println!("Mean latency: {:.2} μs", global.mean_latency.as_micros());

    // Assertions
    assert!(
        global.total_requests > 1000,
        "Should process at least 1000 requests with 4 threads"
    );
    assert!(throughput_rps > 100.0, "Should achieve > 100 req/s");
}

/// Test Memcached ASCII protocol with rate limiting
#[test]
fn test_memcached_ascii_rate_limited() {
    let memcached = common::memcached::MemcachedGuard::new().expect("Failed to start Memcached");

    let runtime = ThreadingRuntime::new(1);
    let target_addr = memcached.addr();
    let target_rate = 1000.0; // 1000 req/s

    let results = runtime
        .run_workers_generic(move |_thread_id| {
            let protocol = xylem_protocols::memcached::ascii::MemcachedAsciiProtocol::new(
                xylem_protocols::memcached::ascii::MemcachedOp::Get,
            );
            let _transport = TcpTransportFactory::default();
            let stats = common::create_test_stats();
            let config = WorkerConfig {
                target: target_addr,
                duration: Duration::from_secs(2),
                conn_count: 1,
                max_pending_per_conn: 1,
                drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
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

    let stats = TupleStatsCollector::merge(results);
    let global = stats.aggregate_global(Duration::from_secs(2));
    let actual_rate = global.total_requests as f64 / 2.0; // 2 second duration

    println!("\n=== Memcached ASCII Rate-Limited Test Results ===");
    println!("Target rate: {target_rate:.2} req/s");
    println!("Actual rate: {actual_rate:.2} req/s");
    println!("Requests: {}", global.total_requests);
    println!("Mean latency: {:.2} μs", global.mean_latency.as_micros());

    // Allow 10% deviation from target rate
    let rate_error = (actual_rate - target_rate).abs() / target_rate;
    assert!(rate_error < 0.10, "Rate error should be < 10%, got {:.1}%", rate_error * 100.0);
}
