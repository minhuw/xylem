//! Integration tests for Masstree protocol
//!
//! These tests use Docker Compose to manage a Masstree server for testing.
//! Docker is required to run these tests.

use std::time::Duration;
use xylem_core::stats::TupleStatsCollector;
use xylem_core::threading::{ThreadingRuntime, Worker, WorkerConfig};

use xylem_transport::TcpTransportFactory;

mod common;

#[test]
fn test_masstree_get_single_thread() {
    // Start Masstree server (will auto-cleanup on drop)
    let _masstree = common::masstree::MasstreeGuard::new().expect("Failed to start Masstree");

    println!("Running Masstree GET single-threaded test...");

    let target_addr = "127.0.0.1:2117".parse().unwrap();
    let duration = Duration::from_secs(3);

    // Create Masstree protocol with GET operations
    let protocol = xylem_protocols::masstree::MasstreeProtocol::new(
        xylem_protocols::masstree::MasstreeOp::Get,
    );

    // Create worker components
    let stats = common::create_test_stats();
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count: 1,
        max_pending_per_conn: 1,
    };

    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, worker_config)
            .expect("Failed to create worker");

    // Run the worker
    println!("Starting single-threaded Masstree GET test...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully: {:?}", result.err());

    // Check stats
    let stats = worker.into_stats();
    let global = stats.aggregate_global(duration);

    println!("Results:");
    println!("  Total requests: {}", global.total_requests);
    println!(
        "  Throughput: {:.2} req/s",
        global.total_requests as f64 / duration.as_secs_f64()
    );
    println!("  P50 latency: {:.2} us", global.latency_p50.as_micros());
    println!("  Mean latency: {:.2} us", global.mean_latency.as_micros());
    println!("  P99 latency: {:.2} us", global.latency_p99.as_micros());

    // Verify we got some requests through
    assert!(global.total_requests > 0, "Should have completed some requests");
    assert!(
        stats.entries().values().any(|entry| entry.latency.count() > 0),
        "Should have collected latency samples"
    );

    // Sanity check on latency (should be < 50ms for local Masstree)
    assert!(
        global.mean_latency < Duration::from_millis(50),
        "Mean latency should be < 50ms for local Masstree, got {:?}",
        global.mean_latency
    );

    println!("Test passed!");
}

#[test]
fn test_masstree_set_single_thread() {
    // Start Masstree server (will auto-cleanup on drop)
    let _masstree = common::masstree::MasstreeGuard::new().expect("Failed to start Masstree");

    println!("Running Masstree SET single-threaded test...");

    let target_addr = "127.0.0.1:2117".parse().unwrap();
    let duration = Duration::from_secs(3);

    // Create Masstree protocol with SET operations
    let protocol = xylem_protocols::masstree::MasstreeProtocol::new(
        xylem_protocols::masstree::MasstreeOp::Set,
    );

    // Create worker components
    let stats = common::create_test_stats();
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count: 1,
        max_pending_per_conn: 1,
    };

    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, worker_config)
            .expect("Failed to create worker");

    // Run the worker
    println!("Starting single-threaded Masstree SET test...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully: {:?}", result.err());

    // Check stats
    let stats = worker.into_stats();
    let global = stats.aggregate_global(duration);

    println!("Results:");
    println!("  Total requests: {}", global.total_requests);
    println!(
        "  Throughput: {:.2} req/s",
        global.total_requests as f64 / duration.as_secs_f64()
    );
    println!("  P50 latency: {:.2} us", global.latency_p50.as_micros());
    println!("  Mean latency: {:.2} us", global.mean_latency.as_micros());
    println!("  P99 latency: {:.2} us", global.latency_p99.as_micros());

    // Verify we got some requests through
    assert!(global.total_requests > 0, "Should have completed some requests");

    println!("Test passed!");
}

#[test]
fn test_masstree_multi_thread() {
    // Start Masstree server (will auto-cleanup on drop)
    let _masstree = common::masstree::MasstreeGuard::new().expect("Failed to start Masstree");

    println!("Running Masstree multi-threaded test...");

    let target_addr = "127.0.0.1:2117".parse().unwrap();
    let duration = Duration::from_secs(3);
    let num_threads = 4;

    let runtime = ThreadingRuntime::new(num_threads);

    println!("Starting {num_threads}-threaded Masstree test...");

    let results = runtime.run_workers_generic(move |_thread_id| {
        let protocol = xylem_protocols::masstree::MasstreeProtocol::new(
            xylem_protocols::masstree::MasstreeOp::Get,
        );
        let stats = common::create_test_stats();
        let worker_config = WorkerConfig {
            target: target_addr,
            duration,
            conn_count: 1,
            max_pending_per_conn: 1,
        };
        let mut worker = Worker::with_closed_loop(
            &TcpTransportFactory::default(),
            protocol,
            stats,
            worker_config,
        )
        .expect("Failed to create worker");

        worker.run()?;
        Ok(worker.into_stats())
    });

    assert!(
        results.is_ok(),
        "Multi-threaded workers should complete successfully: {:?}",
        results.err()
    );

    let results = results.unwrap();
    assert_eq!(results.len(), num_threads);

    // Merge stats from all threads
    let merged_stats = TupleStatsCollector::merge(results);
    let global = merged_stats.aggregate_global(duration);

    println!("Results ({num_threads} threads):");
    println!("  Total requests: {}", global.total_requests);
    println!(
        "  Throughput: {:.2} req/s",
        global.total_requests as f64 / duration.as_secs_f64()
    );
    println!("  P50 latency: {:.2} us", global.latency_p50.as_micros());
    println!("  Mean latency: {:.2} us", global.mean_latency.as_micros());
    println!("  P99 latency: {:.2} us", global.latency_p99.as_micros());

    // Verify we got some requests through
    assert!(global.total_requests > 0, "Should have completed some requests");

    // With multiple threads, we should get reasonable throughput
    assert!(
        global.total_requests > 100,
        "Should have reasonable throughput with {num_threads} threads"
    );

    println!("Test passed!");
}

#[test]
fn test_masstree_remove_operation() {
    // Start Masstree server (will auto-cleanup on drop)
    let _masstree = common::masstree::MasstreeGuard::new().expect("Failed to start Masstree");

    println!("Running Masstree REMOVE single-threaded test...");

    let target_addr = "127.0.0.1:2117".parse().unwrap();
    let duration = Duration::from_secs(2);

    // Create Masstree protocol with REMOVE operations
    let protocol = xylem_protocols::masstree::MasstreeProtocol::new(
        xylem_protocols::masstree::MasstreeOp::Remove,
    );

    // Create worker components
    let stats = common::create_test_stats();
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count: 1,
        max_pending_per_conn: 1,
    };

    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, worker_config)
            .expect("Failed to create worker");

    // Run the worker
    println!("Starting single-threaded Masstree REMOVE test...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully: {:?}", result.err());

    // Check stats
    let global = worker.into_stats().aggregate_global(duration);

    println!("Results:");
    println!("  Total requests: {}", global.total_requests);
    println!(
        "  Throughput: {:.2} req/s",
        global.total_requests as f64 / duration.as_secs_f64()
    );

    // Verify we got some requests through
    assert!(global.total_requests > 0, "Should have completed some requests");

    println!("Test passed!");
}

#[test]
fn test_masstree_rate_limited() {
    // Start Masstree server (will auto-cleanup on drop)
    let _masstree = common::masstree::MasstreeGuard::new().expect("Failed to start Masstree");

    println!("Running Masstree rate-limited test...");

    let target_addr = "127.0.0.1:2117".parse().unwrap();
    let duration = Duration::from_secs(3);
    let target_rate = 300.0; // 300 req/s

    let protocol = xylem_protocols::masstree::MasstreeProtocol::new(
        xylem_protocols::masstree::MasstreeOp::Get,
    );
    let stats = common::create_test_stats();
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
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
        worker_config,
        policy_scheduler,
    )
    .expect("Failed to create worker");

    println!("Starting rate-limited test (target: {target_rate} req/s)...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully");

    let global = worker.into_stats().aggregate_global(duration);
    let actual_rate = global.total_requests as f64 / duration.as_secs_f64();

    println!("Results:");
    println!("  Target rate: {target_rate:.2} req/s");
    println!("  Actual rate: {actual_rate:.2} req/s");
    println!("  Total requests: {}", global.total_requests);

    // Verify we're close to target rate (within 50% tolerance for slower Masstree)
    let rate_ratio = actual_rate / target_rate;
    assert!(
        rate_ratio > 0.5 && rate_ratio < 1.5,
        "Actual rate should be within 50% of target rate, got {rate_ratio:.2}x"
    );

    println!("Test passed!");
}

#[test]
fn test_masstree_pipelined() {
    // Start Masstree server (will auto-cleanup on drop)
    let _masstree = common::masstree::MasstreeGuard::new().expect("Failed to start Masstree");

    println!("Running Masstree pipelined test (pipeline depth=4)...");

    let target_addr = "127.0.0.1:2117".parse().unwrap();
    let duration = Duration::from_secs(3);

    // Create Masstree protocol with GET operations
    let protocol = xylem_protocols::masstree::MasstreeProtocol::new(
        xylem_protocols::masstree::MasstreeOp::Get,
    );

    // Create worker components with pipelining enabled
    let stats = common::create_test_stats();
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count: 1,
        max_pending_per_conn: 4, // Enable pipelining with depth=4
    };

    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, worker_config)
            .expect("Failed to create worker");

    // Run the worker
    println!("Starting pipelined Masstree test (max_pending=4)...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully: {:?}", result.err());

    // Check stats
    let stats = worker.into_stats();
    let global = stats.aggregate_global(duration);

    println!("Results:");
    println!("  Total requests: {}", global.total_requests);
    println!(
        "  Throughput: {:.2} req/s",
        global.total_requests as f64 / duration.as_secs_f64()
    );
    println!("  P50 latency: {:.2} us", global.latency_p50.as_micros());
    println!("  Mean latency: {:.2} us", global.mean_latency.as_micros());
    println!("  P99 latency: {:.2} us", global.latency_p99.as_micros());

    // Verify we got some requests through
    assert!(global.total_requests > 0, "Should have completed some requests");
    assert!(
        stats.entries().values().any(|entry| entry.latency.count() > 0),
        "Should have collected latency samples"
    );

    // With pipelining, we should get higher throughput than without
    let throughput = global.total_requests as f64 / duration.as_secs_f64();
    println!("✓ Pipelined throughput: {throughput:.0} req/s");

    // Basic sanity check - should be more than 100 req/s with pipelining
    assert!(
        throughput > 100.0,
        "Pipelined throughput should be > 100 req/s, got {throughput:.0}"
    );

    println!("Test passed!");
}
