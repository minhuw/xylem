//! Integration tests for Redis protocol
//!
//! These tests use Docker Compose to manage a Redis server for testing.
//! Docker is required to run these tests.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;
use xylem_core::stats::{SamplingPolicy, TupleStatsCollector};
use xylem_core::threading::{ThreadingRuntime, Worker, WorkerConfig};

use xylem_transport::TcpTransportFactory;

mod common;

/// Get Redis stats using INFO command
fn get_redis_commands(addr: SocketAddr) -> Result<u64, Box<dyn std::error::Error>> {
    redis_info_stat(addr, "total_commands_processed")
}

/// The counter `name` of the server's `INFO stats`
fn redis_info_stat(addr: SocketAddr, name: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(addr)?;
    stream.write_all(b"*2\r\n$4\r\nINFO\r\n$5\r\nstats\r\n")?;
    let mut reader = BufReader::new(stream);
    let mut header = String::new();
    reader.read_line(&mut header)?;
    let length: usize = header
        .strip_prefix('$')
        .and_then(|s| s.strip_suffix("\r\n"))
        .ok_or("invalid INFO bulk reply")?
        .parse()?;
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    let mut terminator = [0; 2];
    reader.read_exact(&mut terminator)?;
    if terminator != *b"\r\n" {
        return Err("invalid INFO bulk terminator".into());
    }
    let info = String::from_utf8(body)?;
    let value = info
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(':'))
        .ok_or_else(|| format!("INFO stats missing {name}"))?;
    Ok(value.trim().parse()?)
}

#[test]
fn test_redis_single_thread() {
    // Start Redis server (will auto-cleanup on drop)
    let redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Running single-threaded test...");

    // Get initial Redis stats
    let stats_before = get_redis_commands(redis.addr()).expect("Failed to get initial Redis stats");
    println!("Redis stats before test: {} commands processed", stats_before);

    let target_addr = redis.addr();
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
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
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
    let stats_after = get_redis_commands(redis.addr()).expect("Failed to get final Redis stats");
    let redis_commands_processed = stats_after - stats_before;

    println!("\nRedis server validation:");
    println!("  Commands before: {}", stats_before);
    println!("  Commands after: {}", stats_after);
    println!("  Commands processed: {redis_commands_processed}");
    println!("  Client requests: {}", global.total_requests);

    // Verify we got some requests through
    assert!(global.total_requests > 0, "Should have completed some requests");
    assert!(
        stats.entries().map(|(_, entry)| entry).any(|entry| entry.latency.count() > 0),
        "Should have collected latency samples"
    );

    // INFO reports its count before that INFO itself is counted. The delta
    // therefore includes the initial INFO and all completed GETs, including
    // responses collected during drain. This workload sends no setup commands.
    let counts = stats.request_counts_global();
    assert_eq!(counts.timed_out, 0, "{counts:?}");
    assert_eq!(counts.failed, 0, "{counts:?}");
    assert_eq!(redis_commands_processed, counts.completed + 1);

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
    let redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Testing SamplingPolicy::None (no latency tracking)...");

    let target_addr = redis.addr();
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
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
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
    let latency_samples: u64 =
        stats.entries().map(|(_, entry)| entry).map(|entry| entry.latency.count()).sum();

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
    let redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Running multi-threaded test...");

    // Get initial Redis stats
    let stats_before = get_redis_commands(redis.addr()).expect("Failed to get initial Redis stats");

    let target_addr = redis.addr();
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
            drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
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
    let stats_after = get_redis_commands(redis.addr()).expect("Failed to get final Redis stats");
    let redis_commands_processed = stats_after - stats_before;

    println!("\nRedis server validation:");
    println!("  Commands processed by Redis: {redis_commands_processed}");
    println!("  Client requests: {}", global.total_requests);

    // Verify we got some requests through
    assert!(global.total_requests > 0, "Should have completed some requests");

    // The initial INFO contributes one command to the delta; completed
    // includes responses collected during drain across all workers.
    let counts = merged_stats.request_counts_global();
    assert_eq!(counts.timed_out, 0, "{counts:?}");
    assert_eq!(counts.failed, 0, "{counts:?}");
    assert_eq!(redis_commands_processed, counts.completed + 1);

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
    let redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    println!("Running closed-loop minimal pipelining test...");

    let target_addr = redis.addr();
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
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
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

/// Measured requests start only once every connection of every thread has
/// its insert-phase reply: none misses a key the insert phase set
///
/// Several connections share a thread's insert phase, and the keys are fewer
/// than the requests in flight, so other connections' first requests go out
/// with the insert SETs. Each SET is a megabyte that Redis reads over several
/// passes of its event loop, while it answers a small request at once: one
/// that reached a key before its SET would miss.
#[test]
fn redis_measures_only_after_every_insert_reply() {
    let redis = common::redis::RedisGuard::new().expect("Failed to start Redis");
    let before = redis_info_stat(redis.addr(), "keyspace_misses").unwrap();
    let json = common::run_profile(serde_json::json!({
        "experiment": { "name": "redis-insert-barrier", "duration": "1s", "seed": 7 },
        "traffic_groups": [{
            "name": "strlen", "threads": [0, 1], "connections_per_thread": 4,
            "max_pending_per_connection": 16, "target": redis.addr().to_string(),
            "protocol": "redis", "traffic_policy": { "type": "closed-loop" },
            "protocol_config": {
                "keys": { "strategy": "round-robin", "max": 12, "value_size": 16,
                    "prefix": "barrier:" },
                "insert_phase": { "value_size": 1_000_000 },
                "operations": { "strategy": "weighted", "commands": [{ "name": "custom",
                    "weight": 1.0, "params": { "template": "STRLEN __key__" } }] },
            },
        }],
    }));
    let requests = &json["global"]["requests"];
    assert!(requests["completed"].as_u64().unwrap() > 100, "{requests}");
    assert_eq!(requests["errors"], 0, "{requests}");
    let misses = redis_info_stat(redis.addr(), "keyspace_misses").unwrap() - before;
    assert_eq!(misses, 0, "requests reached keys the insert phase had not set yet");
}
