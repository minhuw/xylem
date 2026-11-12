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

/// A one-second closed-loop profile of `protocol` against `target` with
/// `protocol_config`
fn memcached_profile(
    protocol: &str,
    target: String,
    transport: &str,
    protocol_config: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "experiment": { "name": "memcached-mix", "duration": "1s", "seed": 3 },
        "traffic_groups": [{
            "name": "mix", "threads": [0], "connections_per_thread": 2,
            "max_pending_per_connection": 4, "target": target, "transport": transport,
            "protocol": protocol, "traffic_policy": { "type": "closed-loop" },
            "protocol_config": protocol_config,
        }],
    })
}

/// Every operation in one weighted mix, with weighted value sizes: the
/// server runs each of them and no reply is an error. Incr and decr draw
/// their keys from a range nothing stores, so they miss instead of hitting
/// the non-numeric values SET writes.
fn run_every_operation(protocol: &str, prefix: &str) {
    let memcached = common::memcached::MemcachedGuard::new().expect("Failed to start Memcached");
    let before = memcached.stats();
    let counters = ["incr_misses", "decr_misses"];
    let unstored = serde_json::json!({
        "strategy": "sequential", "start": 1_000_000_000u64, "value_size": 32, "prefix": prefix
    });
    let config = serde_json::json!({
        "keys": { "strategy": "random", "max": 200, "value_size": 32, "prefix": prefix },
        "insert_phase": { "key_count": 200 },
        "value_size": { "strategy": "weighted",
            "sizes": [{ "size": 16, "weight": 3.0 }, { "size": 3000, "weight": 1.0 }] },
        "operations": { "strategy": "weighted", "commands": [
            { "name": "get", "weight": 3.0 },
            { "name": "set", "weight": 1.0 },
            { "name": "add", "weight": 1.0 },
            { "name": "replace", "weight": 1.0 },
            { "name": "delete", "weight": 1.0 },
            { "name": "touch", "weight": 1.0, "params": { "exptime": 600 } },
            { "name": "mget", "weight": 1.0, "params": { "count": 4 } },
            { "name": "incr", "weight": 0.5, "params": { "delta": 3 }, "keys": unstored },
            { "name": "decr", "weight": 0.5, "keys": unstored },
        ] },
    });
    let target = memcached.addr().to_string();
    let json = common::run_profile(memcached_profile(protocol, target, "tcp", config));
    let requests = &json["global"]["requests"];
    let completed = requests["completed"].as_u64().unwrap();
    assert!(completed > 1000, "{requests}");
    assert_eq!(requests["errors"], 0, "{requests}");
    assert_eq!(requests["failed"], 0, "{requests}");

    let after = memcached.stats();
    let delta = |name: &str| after[name] - before.get(name).copied().unwrap_or(0);
    // Each operation reached the server, with hits and misses
    for name in [
        "get_hits",
        "get_misses",
        "cmd_set",
        "delete_hits",
        "delete_misses",
        "cmd_touch",
        "touch_hits",
        "touch_misses",
    ]
    .into_iter()
    .chain(counters)
    {
        assert!(delta(name) > 0, "{name} did not grow");
    }
    assert_eq!(delta("incr_hits") + delta("decr_hits"), 0);
    // A multi-get's keys count one get each: gets outnumber the get requests
    let gets = delta("get_hits") + delta("get_misses");
    assert!(gets > completed / 2, "{gets} gets of {completed} requests");
}

#[test]
fn memcached_ascii_runs_every_operation_in_a_mix() {
    run_every_operation("memcached-ascii", "mix-ascii:");
}

#[test]
fn memcached_binary_runs_every_operation_in_a_mix() {
    run_every_operation("memcached-binary", "mix-binary:");
}

/// Incr on the stored non-numeric values is an error reply of each codec's kind
///
/// One connection: the insert phase is shared by a thread's connections, so
/// with two, one connection's incr can reach a key before the other's SET of
/// it and miss. On one connection every SET is ahead of every incr.
#[test]
fn memcached_incr_of_a_non_numeric_value_is_an_error_reply() {
    let memcached = common::memcached::MemcachedGuard::new().expect("Failed to start Memcached");
    for (protocol, kind) in
        [("memcached-ascii", "CLIENT_ERROR"), ("memcached-binary", "DELTA_BADVAL")]
    {
        let before = memcached.stats();
        let config = serde_json::json!({
            "keys": { "strategy": "round-robin", "max": 10, "value_size": 4,
                "prefix": format!("{protocol}:") },
            "insert_phase": {},
            "operation": "incr",
        });
        let target = memcached.addr().to_string();
        let mut profile = memcached_profile(protocol, target, "tcp", config);
        profile["traffic_groups"][0]["connections_per_thread"] = 1.into();
        let json = common::run_profile(profile);
        let requests = &json["global"]["requests"];
        let completed = requests["completed"].as_u64().unwrap();
        assert!(completed > 100, "{requests}");
        assert_eq!(requests["errors"], completed, "{requests}");
        assert_eq!(requests["errors_by_kind"], serde_json::json!({ kind: completed }));
        let after = memcached.stats();
        let misses = after["incr_misses"] - before.get("incr_misses").copied().unwrap_or(0);
        assert_eq!(misses, 0, "{protocol}: incr reached keys the insert phase had not set");
    }
}

/// The single-key operations over UDP: memcached answers a multi-get over
/// UDP with a datagram per value, so mget is TCP-only
#[test]
fn memcached_operations_over_udp() {
    let memcached = common::memcached::MemcachedGuard::new().expect("Failed to start Memcached");
    for protocol in ["memcached-ascii", "memcached-binary"] {
        let config = serde_json::json!({
            "keys": { "strategy": "round-robin", "max": 50, "value_size": 3000,
                "prefix": format!("udp-{protocol}:") },
            "insert_phase": {},
            "operations": { "strategy": "weighted", "commands": [
                { "name": "get", "weight": 4.0 },
                { "name": "replace", "weight": 1.0 },
                { "name": "add", "weight": 1.0 },
                { "name": "touch", "weight": 1.0 },
                { "name": "incr", "weight": 1.0, "keys": { "strategy": "sequential",
                    "start": 1_000_000_000u64, "value_size": 3000,
                    "prefix": format!("udp-{protocol}:") } },
            ] },
        });
        let target = memcached.addr().to_string();
        let json = common::run_profile(memcached_profile(protocol, target, "udp", config));
        let requests = &json["global"]["requests"];
        let count = |key: &str| requests[key].as_u64().unwrap();
        assert!(count("completed") > 100, "{protocol}: {requests}");
        assert_eq!(count("errors"), 0, "{protocol}: {requests}");
        assert_eq!(count("failed"), 0, "{protocol}: {requests}");
        assert!(count("timed_out") * 100 <= count("sent"), "{protocol}: {requests}");
    }
}
