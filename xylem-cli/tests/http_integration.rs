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

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;
use xylem_core::stats::TupleStatsCollector;
use xylem_core::threading::{ThreadingRuntime, Worker, WorkerConfig};
use xylem_protocols::{Parsed, Protocol};

use xylem_transport::TcpTransportFactory;

mod common;

#[test]
fn test_http_get_single_thread() {
    let nginx = common::nginx::NginxGuard::new().expect("Failed to start nginx");

    println!("Running HTTP GET single-threaded test...");

    let target_addr = nginx.addr();
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
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
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
        stats.entries().map(|(_, entry)| entry).any(|entry| entry.latency.count() > 0),
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
    let nginx = common::nginx::NginxGuard::new().expect("Failed to start nginx");

    println!("Running HTTP POST single-threaded test...");

    let target_addr = nginx.addr();
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
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
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
    let nginx = common::nginx::NginxGuard::new().expect("Failed to start nginx");

    println!("Running HTTP multi-threaded test...");

    let target_addr = nginx.addr();
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
    let nginx = common::nginx::NginxGuard::new().expect("Failed to start nginx");

    println!("Running HTTP rate-limited test...");

    let target_addr = nginx.addr();
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
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
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
    let nginx = common::nginx::NginxGuard::new().expect("Failed to start nginx");

    println!("Running HTTP PUT test...");

    let target_addr = nginx.addr();
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
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
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
        stats.entries().map(|(_, entry)| entry).map(|entry| entry.tx_bytes).sum::<u64>()
    );

    assert!(global.total_requests > 0, "Should have completed PUT requests");
}

#[test]
fn test_http_pipelined() {
    let nginx = common::nginx::NginxGuard::new().expect("Failed to start nginx");

    println!("Running HTTP pipelined test...");

    let target_addr = nginx.addr();
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
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
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

/// Fetch one response for `path` from nginx and return it, framed by xylem's parser
fn fetch_one(target_addr: SocketAddr, path: &str) -> Vec<u8> {
    let mut protocol = xylem_protocols::http::HttpProtocol::new(
        xylem_protocols::HttpMethod::Get,
        path.to_string(),
        "localhost".to_string(),
    );
    let mut stream = TcpStream::connect(target_addr).expect("Failed to connect to nginx");
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.write_all(&protocol.next_request(0).data).unwrap();
    let mut response = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        let n = stream.read(&mut buf).expect("Failed to read the nginx response");
        assert!(n > 0, "nginx closed the connection mid-response");
        response.extend_from_slice(&buf[..n]);
        if let Parsed::Complete { consumed: len, .. } =
            protocol.parse_response(0, &response).unwrap()
        {
            assert_eq!(len, response.len(), "nginx sent bytes past the response");
            return response;
        }
    }
}

/// Run GET `/chunked` against nginx, closed loop, and check every response's
/// full wire bytes were counted
fn run_chunked(max_pending_per_conn: usize) {
    let nginx = common::nginx::NginxGuard::new().expect("Failed to start nginx");
    let target_addr = nginx.addr();
    let duration = Duration::from_secs(2);

    // nginx frames this keep-alive response with chunked encoding, in several
    // chunks; every response has the same length (the Date header is fixed width)
    let sample = fetch_one(target_addr, "/chunked");
    let text = String::from_utf8_lossy(&sample);
    println!("Sample response:\n{text}");
    assert!(text.contains("Transfer-Encoding: chunked"), "Expected a chunked response");
    assert!(text.contains("Connection: keep-alive"), "Expected a keep-alive response");
    assert!(!text.contains("Content-Length"), "Expected no Content-Length");
    assert!(text.ends_with("\r\n0\r\n\r\n"), "Expected a last-chunk");

    let protocol = xylem_protocols::http::HttpProtocol::new(
        xylem_protocols::HttpMethod::Get,
        "/chunked".to_string(),
        "localhost".to_string(),
    );
    let stats = common::create_test_stats();
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count: 2,
        max_pending_per_conn,
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
    };
    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, worker_config)
            .expect("Failed to create worker");
    let result = worker.run();
    assert!(result.is_ok(), "Worker should complete: {:?}", result.err());

    let stats = worker.into_stats();
    let global = stats.aggregate_global(duration);
    let requests: u64 =
        stats.entries().map(|(_, entry)| entry).map(|entry| entry.request_count).sum();
    let rx_bytes: u64 = stats.entries().map(|(_, entry)| entry).map(|entry| entry.rx_bytes).sum();
    println!("Results (max pending {max_pending_per_conn}):");
    println!("  Total requests: {}", global.total_requests);
    println!(
        "  Throughput: {:.2} req/s",
        global.total_requests as f64 / duration.as_secs_f64()
    );
    println!("  P50 latency: {:.2} μs", global.latency_p50.as_micros());
    println!("  P99 latency: {:.2} μs", global.latency_p99.as_micros());
    println!("  Rx bytes: {rx_bytes}");

    assert!(requests > 0, "Should have completed chunked requests");
    assert_eq!(rx_bytes, requests * sample.len() as u64, "Rx bytes should cover every response");
}

#[test]
fn test_http_chunked_keep_alive() {
    run_chunked(1);
}

#[test]
fn test_http_chunked_pipelined() {
    run_chunked(16);
}
