//! Integration tests for HTTP protocol
//!
//! These tests use Docker Compose to manage an Nginx server for testing.
//! Docker is required to run these tests.
//!
//! ## Running the tests
//!
//! Run all HTTP integration tests:
//! ```bash
//! just test-http
//! ```
//!
//! Run a specific test:
//! ```bash
//! cargo nextest run --profile integration -E 'binary(http_integration) and test(test_http_get_single_thread)'
//! ```

use std::time::Duration;
use xylem_core::stats::TupleStatsCollector;
use xylem_core::threading::{ThreadingRuntime, Worker, WorkerConfig};

use xylem_transport::TcpTransportFactory;

mod common;

#[test]
fn test_http_get_single_thread() {
    let _nginx = common::nginx::NginxGuard::new().expect("Failed to start nginx");

    println!("Running HTTP GET single-threaded test...");

    let target_addr = "127.0.0.1:8080".parse().unwrap();
    let duration = Duration::from_secs(2);

    let protocol = xylem_protocols::http::HttpProtocol::new(
        xylem_protocols::HttpMethod::Get,
        "/".to_string(),
        "localhost".to_string(),
    );
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

    println!("Starting HTTP GET test...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully: {:?}", result.err());

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

    // Verify we got some requests through
    assert!(global.total_requests > 0, "Should have completed some requests");
    assert!(
        stats.entries().values().any(|entry| entry.latency.count() > 0),
        "Should have collected latency samples"
    );

    // Sanity check on latency (should be < 10ms for local nginx)
    assert!(
        global.mean_latency < Duration::from_millis(10),
        "Mean latency should be < 10ms for local nginx, got {:?}",
        global.mean_latency
    );
}

#[test]
fn test_http_post_single_thread() {
    let _nginx = common::nginx::NginxGuard::new().expect("Failed to start nginx");

    println!("Running HTTP POST single-threaded test...");

    let target_addr = "127.0.0.1:8080".parse().unwrap();
    let duration = Duration::from_secs(2);

    let protocol = xylem_protocols::http::HttpProtocol::new(
        xylem_protocols::HttpMethod::Post,
        "/".to_string(),
        "localhost".to_string(),
    );
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

    println!("Starting HTTP POST test...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully");

    let stats = worker.into_stats();
    let global = stats.aggregate_global(duration);

    println!("Results:");
    println!("  Total requests: {}", global.total_requests);
    println!(
        "  Throughput: {:.2} req/s",
        global.total_requests as f64 / duration.as_secs_f64()
    );
    println!("  Mean latency: {:.2} μs", global.mean_latency.as_micros());

    assert!(global.total_requests > 0, "Should have completed POST requests");
}

#[test]
fn test_http_multi_thread() {
    let _nginx = common::nginx::NginxGuard::new().expect("Failed to start nginx");

    println!("Running HTTP multi-threaded test...");

    let target_addr = "127.0.0.1:8080".parse().unwrap();
    let duration = Duration::from_secs(2);
    let num_threads = 4;

    let runtime = ThreadingRuntime::new(num_threads);

    println!("Starting {num_threads}-threaded HTTP test...");

    let results = runtime.run_workers_generic(move |_thread_id| {
        let protocol = xylem_protocols::http::HttpProtocol::new(
            xylem_protocols::HttpMethod::Get,
            "/".to_string(),
            "localhost".to_string(),
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
    println!("  P50 latency: {:.2} μs", global.latency_p50.as_micros());
    println!("  Mean latency: {:.2} μs", global.mean_latency.as_micros());
    println!("  P99 latency: {:.2} μs", global.latency_p99.as_micros());

    // Verify we got some requests through
    assert!(global.total_requests > 0, "Should have completed some requests");

    // With 4 threads, we should get reasonable throughput
    assert!(
        global.total_requests > 100,
        "Should have reasonable throughput with {num_threads} threads"
    );
}

#[test]
fn test_http_rate_limited() {
    let _nginx = common::nginx::NginxGuard::new().expect("Failed to start nginx");

    println!("Running HTTP rate-limited test...");

    let target_addr = "127.0.0.1:8080".parse().unwrap();
    let duration = Duration::from_secs(2);
    let target_rate = 500.0; // 500 req/s

    let protocol = xylem_protocols::http::HttpProtocol::new(
        xylem_protocols::HttpMethod::Get,
        "/".to_string(),
        "localhost".to_string(),
    );
    let stats = common::create_test_stats();
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count: 1,
        max_pending_per_conn: 1,
    };

    let scheduler =
        Box::new(xylem_core::scheduler::UniformPolicyScheduler::fixed_rate(target_rate));
    let mut worker =
        Worker::new(&TcpTransportFactory::default(), protocol, stats, worker_config, scheduler)
            .expect("Failed to create worker");

    println!("Starting rate-limited test (target: {target_rate} req/s)...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully");

    let stats = worker.into_stats();
    let global = stats.aggregate_global(duration);
    let actual_rate = global.total_requests as f64 / duration.as_secs_f64();

    println!("Results:");
    println!("  Target rate: {target_rate:.2} req/s");
    println!("  Actual rate: {actual_rate:.2} req/s");
    println!("  Total requests: {}", global.total_requests);

    // Verify we're close to target rate (within 40% tolerance)
    let rate_ratio = actual_rate / target_rate;
    assert!(
        rate_ratio > 0.6 && rate_ratio < 1.4,
        "Actual rate should be within 40% of target rate, got {rate_ratio:.2}x"
    );
}

#[test]
fn test_http_put_request() {
    let _nginx = common::nginx::NginxGuard::new().expect("Failed to start nginx");

    println!("Running HTTP PUT test...");

    let target_addr = "127.0.0.1:8080".parse().unwrap();
    let duration = Duration::from_secs(2);

    let protocol = xylem_protocols::http::HttpProtocol::new(
        xylem_protocols::HttpMethod::Put,
        "/".to_string(),
        "localhost".to_string(),
    );
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

    println!("Starting HTTP PUT test...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully");

    let stats = worker.into_stats();
    let global = stats.aggregate_global(duration);

    println!("Results:");
    println!("  Total requests: {}", global.total_requests);
    println!(
        "  Total bytes sent: {}",
        stats.entries().values().map(|entry| entry.tx_bytes).sum::<u64>()
    );

    assert!(global.total_requests > 0, "Should have completed PUT requests");
}

#[test]
fn test_http_pipelined() {
    let _nginx = common::nginx::NginxGuard::new().expect("Failed to start nginx");

    println!("Running HTTP pipelined test...");

    let target_addr = "127.0.0.1:8080".parse().unwrap();
    let duration = Duration::from_secs(2);

    let protocol = xylem_protocols::http::HttpProtocol::new(
        xylem_protocols::HttpMethod::Get,
        "/".to_string(),
        "localhost".to_string(),
    );
    let stats = common::create_test_stats();
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count: 2,
        max_pending_per_conn: 16, // Allow HTTP pipelining
    };

    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, worker_config)
            .expect("Failed to create worker");

    println!("Starting HTTP pipelined test (2 conns, 16 max pending each)...");
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

    // With pipelining, we should get higher throughput
    let throughput = global.total_requests as f64 / duration.as_secs_f64();
    println!("✓ HTTP pipelined throughput: {throughput:.0} req/s");
}
