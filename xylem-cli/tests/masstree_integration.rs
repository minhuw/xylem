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
    let masstree = common::masstree::MasstreeGuard::new().expect("Failed to start Masstree");

    println!("Running Masstree GET single-threaded test...");

    let target_addr = masstree.addr();
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
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
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
        stats.entries().map(|(_, entry)| entry).any(|entry| entry.latency.count() > 0),
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
    let masstree = common::masstree::MasstreeGuard::new().expect("Failed to start Masstree");

    println!("Running Masstree SET single-threaded test...");

    let target_addr = masstree.addr();
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
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
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
    let masstree = common::masstree::MasstreeGuard::new().expect("Failed to start Masstree");

    println!("Running Masstree multi-threaded test...");

    let target_addr = masstree.addr();
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
    let masstree = common::masstree::MasstreeGuard::new().expect("Failed to start Masstree");

    println!("Running Masstree REMOVE single-threaded test...");

    let target_addr = masstree.addr();
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
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
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
    let masstree = common::masstree::MasstreeGuard::new().expect("Failed to start Masstree");

    println!("Running Masstree rate-limited test...");

    let target_addr = masstree.addr();
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
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
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
    let masstree = common::masstree::MasstreeGuard::new().expect("Failed to start Masstree");

    println!("Running Masstree pipelined test (pipeline depth=4)...");

    let target_addr = masstree.addr();
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
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
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
        stats.entries().map(|(_, entry)| entry).any(|entry| entry.latency.count() > 0),
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

/// Every operation in one weighted mix, with multi-column puts so GETs and
/// scans meet rows of several columns, scans from generated keys and
/// weighted value sizes. Returns the run's global request counts.
fn run_every_operation(target: String) -> serde_json::Value {
    let profile = serde_json::json!({
        "experiment": { "name": "masstree-mix", "duration": "2s", "seed": 5 },
        "traffic_groups": [{
            "name": "mix", "threads": [0], "connections_per_thread": 2,
            "max_pending_per_connection": 4, "target": target, "protocol": "masstree",
            "traffic_policy": { "type": "closed-loop" },
            "protocol_config": {
                "keys": { "strategy": "random", "max": 500, "value_size": 16 },
                "insert_phase": {},
                "value_size": { "strategy": "weighted",
                    "sizes": [{ "size": 8, "weight": 3.0 }, { "size": 2000, "weight": 1.0 }] },
                "scan": { "count": 5 },
                "operations": { "strategy": "weighted", "commands": [
                    { "name": "get", "weight": 4.0 },
                    { "name": "set", "weight": 1.0 },
                    { "name": "put", "weight": 2.0, "params": { "columns": 3 } },
                    { "name": "scan", "weight": 1.0 },
                    { "name": "scan", "weight": 1.0, "params": { "count": 20, "fields": [0, 2] } },
                    { "name": "remove", "weight": 1.0 },
                ] },
            },
        }],
    });
    let json = common::run_profile(profile);
    let requests = json["global"]["requests"].clone();
    let count = |key: &str| requests[key].as_u64().unwrap();
    assert!(count("completed") > 1000, "{requests}");
    assert_eq!(count("errors"), 0, "{requests}");
    assert_eq!(count("failed"), 0, "{requests}");
    assert_eq!(count("timed_out"), 0, "{requests}");
    // Scans of up to 20 rows of up to three 2000-byte columns: responses
    // average far more than a value
    let mbps = json["global"]["throughput_mbps"].as_f64().unwrap();
    let rps = json["global"]["throughput_rps"].as_f64().unwrap();
    assert!(mbps * 1e6 / 8.0 / rps > 1000.0, "{mbps} Mbps at {rps} rps");
    requests
}

#[test]
fn masstree_runs_every_operation_in_a_mix() {
    let masstree = common::masstree::MasstreeGuard::new().expect("Failed to start Masstree");
    run_every_operation(masstree.get_addr().to_string());
}

/// Stackbench's `mtd` (a fork of the masstree-beta the Docker image builds),
/// killed on drop
struct StackbenchMtd {
    child: std::process::Child,
    port: u16,
}

impl StackbenchMtd {
    /// Start the `mtd` at `path` without logging on a free port and wait
    /// until it accepts connections
    fn start(path: &str) -> Self {
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let child = std::process::Command::new(path)
            .args(["-n", &format!("--port={port}"), "--threads=2"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("cannot run {path}: {e}"));
        let mtd = Self { child, port };
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
            assert!(std::time::Instant::now() < deadline, "{path} did not listen on {port}");
            std::thread::sleep(Duration::from_millis(50));
        }
        mtd
    }
}

impl Drop for StackbenchMtd {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The mix against the `mtd` Stackbench builds, from
/// `nix build <stackbench>#benchmarks.masstree.default`; its `bin/mtd` path
/// goes in XYLEM_STACKBENCH_MTD. A checkpoint is the one command the two
/// servers may answer differently: Stackbench's answers `[seq, -1]` on
/// backends without blocking calls, which the Linux build has.
#[test]
#[ignore = "needs XYLEM_STACKBENCH_MTD=<stackbench masstree build>/bin/mtd"]
fn stackbench_mtd_runs_every_operation_in_a_mix() {
    let path = std::env::var("XYLEM_STACKBENCH_MTD")
        .expect("XYLEM_STACKBENCH_MTD names the Stackbench mtd binary");
    let mtd = StackbenchMtd::start(&path);
    run_every_operation(format!("127.0.0.1:{}", mtd.port));

    let profile = serde_json::json!({
        "experiment": { "name": "masstree-checkpoint", "duration": "1s", "seed": 5 },
        "traffic_groups": [{
            "name": "checkpoint", "threads": [0], "connections_per_thread": 1,
            "target": format!("127.0.0.1:{}", mtd.port), "protocol": "masstree",
            "traffic_policy": { "type": "closed-loop" },
            "protocol_config": { "operation": "checkpoint" },
        }],
    });
    let json = common::run_profile(profile);
    let requests = &json["global"]["requests"];
    assert!(requests["completed"].as_u64().unwrap() > 10, "{requests}");
    assert_eq!(requests["errors"], 0, "{requests}");
}
