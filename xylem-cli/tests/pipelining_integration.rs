//! Pipelining integration tests
//!
//! Tests pipelining behavior with Redis - multiple outstanding requests per connection.
//!
//! ## Closed-loop vs Open-loop Semantics
//!
//! - **Closed-loop**: Send next request when a response arrives (feedback-driven).
//!   `max_pending_per_connection` controls max concurrent in-flight requests.
//!   Throughput is naturally limited by RTT and pipeline depth.
//!
//! - **Open-loop (rate-limited)**: Send requests at a fixed rate regardless of responses.
//!   `max_pending_per_connection` acts as backpressure to avoid overload.
//!   Throughput is controlled by the configured rate.

use std::time::Duration;
use xylem_core::threading::{Worker, WorkerConfig};

use xylem_transport::TcpTransportFactory;

mod common;

#[test]
fn test_redis_pipelined_single_connection() {
    let _guard = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Running pipelined test with 1 connection...");

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(2);

    let protocol = xylem_protocols::redis::RedisProtocol::new(Box::new(
        xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get),
    ));
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

    let global = worker.stats().aggregate_global(duration);

    println!("Results:");
    println!("  Total requests: {}", global.total_requests);
    println!(
        "  Throughput: {:.2} req/s",
        global.total_requests as f64 / duration.as_secs_f64()
    );
    println!("  P50 latency: {:.2} μs", global.latency_p50.as_micros());
    println!("  Mean latency: {:.2} μs", global.mean_latency.as_micros());
    println!("  P99 latency: {:.2} μs", global.latency_p99.as_micros());

    assert!(global.total_requests > 0, "Should have completed requests");

    // With pipelining, we should get much higher throughput
    let throughput = global.total_requests as f64 / duration.as_secs_f64();
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

    let global = worker.stats().aggregate_global(duration);

    println!("Results:");
    println!("  Total requests: {}", global.total_requests);
    println!(
        "  Throughput: {:.2} req/s",
        global.total_requests as f64 / duration.as_secs_f64()
    );
    println!("  P50 latency: {:.2} μs", global.latency_p50.as_micros());
    println!("  Mean latency: {:.2} μs", global.mean_latency.as_micros());
    println!("  P99 latency: {:.2} μs", global.latency_p99.as_micros());

    assert!(global.total_requests > 0, "Should have completed requests");

    // With 4 connections and pipelining, throughput should be very high
    let throughput = global.total_requests as f64 / duration.as_secs_f64();
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

    let global = worker.stats().aggregate_global(duration);
    let actual_rate = global.total_requests as f64 / duration.as_secs_f64();

    println!("Results:");
    println!("  Actual rate: {actual_rate:.2} req/s");
    println!("  Total requests: {}", global.total_requests);

    // Closed-loop should achieve reasonable throughput (at least 1000 req/s with pipelining)
    assert!(
        actual_rate > 1000.0,
        "Closed-loop pipelined should achieve at least 1000 req/s, got {actual_rate:.2}"
    );

    println!("✓ Closed-loop pipelining working ({actual_rate:.0} req/s)");
}
