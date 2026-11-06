//! Integration tests for Redis protocol
//!
//! These tests use Docker Compose to manage a Redis server for testing.
//! Docker is required to run these tests.

use std::process::Command;
use std::time::Duration;
use xylem_core::stats::{SamplingPolicy, TupleStatsCollector};
use xylem_core::threading::{ThreadingRuntime, Worker, WorkerConfig};

use xylem_transport::TcpTransportFactory;

mod common;

/// Get Redis stats using INFO command
fn get_redis_stats() -> Result<RedisStats, Box<dyn std::error::Error>> {
    let output = Command::new("redis-cli").args(["-p", "6379", "INFO", "stats"]).output()?;

    let info = String::from_utf8(output.stdout)?;

    // Parse total_commands_processed from INFO output
    let mut total_commands = 0u64;
    let mut total_connections = 0u64;

    for line in info.lines() {
        if line.starts_with("total_commands_processed:") {
            total_commands =
                line.split(':').nth(1).and_then(|s| s.trim().parse().ok()).unwrap_or(0);
        } else if line.starts_with("total_connections_received:") {
            total_connections =
                line.split(':').nth(1).and_then(|s| s.trim().parse().ok()).unwrap_or(0);
        }
    }

    Ok(RedisStats {
        total_commands_processed: total_commands,
        total_connections_received: total_connections,
    })
}

/// Redis server statistics
#[derive(Debug, Clone)]
struct RedisStats {
    total_commands_processed: u64,
    #[allow(dead_code)]
    total_connections_received: u64,
}

#[test]
fn test_redis_single_thread() {
    // Start Redis server (will auto-cleanup on drop)
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Running single-threaded test...");

    // Get initial Redis stats
    let stats_before = get_redis_stats().expect("Failed to get initial Redis stats");
    println!(
        "Redis stats before test: {} commands processed",
        stats_before.total_commands_processed
    );

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(2);

    // Create Redis protocol with GET operations
    let protocol = xylem_protocols::redis::RedisProtocol::new(Box::new(
        xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get),
    ));

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
    println!("Starting single-threaded Redis test...");
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
    println!("  P50 latency: {:.2} μs", global.latency_p50.as_micros());
    println!("  Mean latency: {:.2} μs", global.mean_latency.as_micros());
    println!("  P99 latency: {:.2} μs", global.latency_p99.as_micros());

    // Get Redis stats after test
    let stats_after = get_redis_stats().expect("Failed to get final Redis stats");
    let redis_commands_processed =
        stats_after.total_commands_processed - stats_before.total_commands_processed;

    println!("\nRedis server validation:");
    println!("  Commands before: {}", stats_before.total_commands_processed);
    println!("  Commands after: {}", stats_after.total_commands_processed);
    println!("  Commands processed: {redis_commands_processed}");
    println!("  Client requests: {}", global.total_requests);

    // Verify we got some requests through
    assert!(global.total_requests > 0, "Should have completed some requests");
    assert!(
        stats.entries().values().any(|entry| entry.latency.count() > 0),
        "Should have collected latency samples"
    );

    // STRICT VALIDATION: Verify Redis actually processed the requests
    // Redis counts each GET command, so it should match our completed requests
    assert!(redis_commands_processed > 0, "Redis should have processed some commands");

    // Allow small discrepancy due to INFO command itself and potential connection setup commands
    let discrepancy = (redis_commands_processed as i64 - global.total_requests as i64).abs();
    assert!(
        discrepancy < 10,
        "Redis commands processed ({}) should match client requests ({}) within small margin (discrepancy: {})",
        redis_commands_processed,
        global.total_requests,
        discrepancy
    );

    println!("✓ Redis stats validation passed (discrepancy: {discrepancy} commands)");

    // Sanity check on latency (should be < 10ms for local Redis)
    assert!(
        global.mean_latency < Duration::from_millis(10),
        "Mean latency should be < 10ms for local Redis, got {:?}",
        global.mean_latency
    );
}

#[test]
fn test_global_sampling_none_disables_latency() {
    // Start Redis server (will auto-cleanup on drop)
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Testing SamplingPolicy::None (no latency tracking)...");

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(2);

    // Create Redis protocol with GET operations
    let protocol = xylem_protocols::redis::RedisProtocol::new(Box::new(
        xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get),
    ));

    // Create stats collector with SamplingPolicy::None
    let mut stats = TupleStatsCollector::new(SamplingPolicy::None, Duration::from_secs(1));
    stats.register_group_policy(0, SamplingPolicy::None);

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
    println!("Starting test with SamplingPolicy::None...");
    let result = worker.run();

    assert!(
        result.is_ok(),
        "Worker should complete successfully even with SamplingPolicy::None: {:?}",
        result.err()
    );

    // Check stats
    let stats = worker.into_stats();
    let global = stats.aggregate_global(duration);
    let latency_samples: u64 = stats.entries().values().map(|entry| entry.latency.count()).sum();

    println!("Results:");
    println!("  Total requests: {}", global.total_requests);
    println!(
        "  Throughput: {:.2} req/s",
        global.total_requests as f64 / duration.as_secs_f64()
    );
    println!("  Latency samples collected: {latency_samples}");

    // Verify we got some requests through
    assert!(global.total_requests > 0, "Should have completed some requests");

    // With SamplingPolicy::None, no latency samples should be collected
    assert_eq!(
        latency_samples, 0,
        "SamplingPolicy::None should not collect any latency samples"
    );
    assert_eq!(
        global.latency_p99,
        Duration::ZERO,
        "SamplingPolicy::None should not report global latency"
    );

    println!("✓ SamplingPolicy::None validation passed - no latency samples collected as expected");
}

#[test]
fn test_redis_multi_thread() {
    // Start Redis server (will auto-cleanup on drop)
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Running multi-threaded test...");

    // Get initial Redis stats
    let stats_before = get_redis_stats().expect("Failed to get initial Redis stats");

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(2);
    let num_threads = 4;

    let runtime = ThreadingRuntime::new(num_threads);

    println!("Starting {num_threads}-threaded Redis test...");

    let results = runtime.run_workers_generic(move |_thread_id| {
        let protocol = xylem_protocols::redis::RedisProtocol::new(Box::new(
            xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get),
        ));
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
    println!("  P50 latency: {:.2} μs", global.latency_p50.as_micros());
    println!("  Mean latency: {:.2} μs", global.mean_latency.as_micros());
    println!("  P99 latency: {:.2} μs", global.latency_p99.as_micros());

    // Get Redis stats after test
    let stats_after = get_redis_stats().expect("Failed to get final Redis stats");
    let redis_commands_processed =
        stats_after.total_commands_processed - stats_before.total_commands_processed;

    println!("\nRedis server validation:");
    println!("  Commands processed by Redis: {redis_commands_processed}");
    println!("  Client requests: {}", global.total_requests);

    // Verify we got some requests through
    assert!(global.total_requests > 0, "Should have completed some requests");

    // STRICT VALIDATION: Verify Redis actually processed the requests
    assert!(redis_commands_processed > 0, "Redis should have processed some commands");

    let discrepancy = (redis_commands_processed as i64 - global.total_requests as i64).abs();
    assert!(
        discrepancy < 50, // Allow slightly more discrepancy for multi-threaded (connection setup per thread)
        "Redis commands processed ({}) should match client requests ({}) within margin (discrepancy: {})",
        redis_commands_processed,
        global.total_requests,
        discrepancy
    );

    println!("✓ Redis stats validation passed (discrepancy: {discrepancy} commands)");

    // With 4 threads, we should get more throughput than single-threaded
    // (though this depends on Redis and system capacity)
    assert!(
        global.total_requests > 1000,
        "Should have reasonable throughput with {num_threads} threads"
    );
}

/// Test closed-loop throughput with minimal pipelining (single outstanding request)
#[test]
fn test_redis_closed_loop_minimal() {
    // Start Redis server (will auto-cleanup on drop)
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Running closed-loop minimal pipelining test...");

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(2);

    let protocol = xylem_protocols::redis::RedisProtocol::new(Box::new(
        xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get),
    ));
    let stats = common::create_test_stats();
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count: 1,
        max_pending_per_conn: 1, // Minimal pipelining - one request at a time
    };

    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, worker_config)
            .expect("Failed to create worker");

    println!("Starting closed-loop test with minimal pipelining...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully");

    let global = worker.into_stats().aggregate_global(duration);
    let actual_rate = global.total_requests as f64 / duration.as_secs_f64();

    println!("Results:");
    println!("  Actual rate: {actual_rate:.2} req/s");
    println!("  Total requests: {}", global.total_requests);

    // With minimal pipelining (1 outstanding request), we should still achieve some throughput
    assert!(
        actual_rate > 100.0,
        "Even minimal pipelining should achieve at least 100 req/s, got {actual_rate:.2}"
    );
}
