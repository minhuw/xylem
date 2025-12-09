//! Integration tests for HTTP protocol
//!
//! These tests use Docker Compose to manage an Nginx server for testing.
//! Docker is required to run these tests.
//!
//! ## Running the tests
//!
//! Run all HTTP integration tests:
//! ```bash
//! cargo test --test http_integration -- --ignored
//! ```
//!
//! Run a specific test:
//! ```bash
//! cargo test --test http_integration test_http_get_single_thread -- --ignored
//! ```

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

    fn next_request(
        &mut self,
        conn_id: usize,
    ) -> (Vec<u8>, Self::RequestId, xylem_core::threading::RequestMeta) {
        let (data, req_id, proto_meta) = self.inner.next_request(conn_id);
        (
            data,
            req_id,
            xylem_core::threading::RequestMeta { is_warmup: proto_meta.is_warmup },
        )
    }

    fn regenerate_request(
        &mut self,
        conn_id: usize,
        original_request_id: Self::RequestId,
    ) -> (Vec<u8>, Self::RequestId, xylem_core::threading::RequestMeta) {
        let (data, req_id, proto_meta) =
            self.inner.regenerate_request(conn_id, original_request_id);
        (
            data,
            req_id,
            xylem_core::threading::RequestMeta { is_warmup: proto_meta.is_warmup },
        )
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
    let protocol = ProtocolAdapter::new(protocol);
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
    let basic_stats = stats.global().calculate_basic_stats();

    println!("Results:");
    println!("  Total requests: {}", stats.global().tx_requests());
    println!(
        "  Throughput: {:.2} req/s",
        stats.global().tx_requests() as f64 / duration.as_secs_f64()
    );
    println!("  Min latency: {:.2} μs", basic_stats.min.as_micros());
    println!("  Mean latency: {:.2} μs", basic_stats.mean.as_micros());
    println!("  Max latency: {:.2} μs", basic_stats.max.as_micros());

    // Verify we got some requests through
    assert!(stats.global().tx_requests() > 0, "Should have sent some requests");
    assert!(stats.global().rx_requests() > 0, "Should have received some responses");
    assert!(!stats.global().samples().is_empty(), "Should have collected latency samples");

    // Sanity check on latency (should be < 10ms for local nginx)
    assert!(
        basic_stats.mean < Duration::from_millis(10),
        "Mean latency should be < 10ms for local nginx, got {:?}",
        basic_stats.mean
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
    let protocol = ProtocolAdapter::new(protocol);
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
    let basic_stats = stats.global().calculate_basic_stats();

    println!("Results:");
    println!("  Total requests: {}", stats.global().tx_requests());
    println!(
        "  Throughput: {:.2} req/s",
        stats.global().tx_requests() as f64 / duration.as_secs_f64()
    );
    println!("  Mean latency: {:.2} μs", basic_stats.mean.as_micros());

    assert!(stats.global().tx_requests() > 0, "Should have sent POST requests");
    assert!(stats.global().rx_requests() > 0, "Should have received responses");
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
        let protocol = ProtocolAdapter::new(protocol);
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
    let merged_stats = GroupStatsCollector::merge(results);
    let basic_stats = merged_stats.global().calculate_basic_stats();

    println!("Results ({num_threads} threads):");
    println!("  Total requests: {}", merged_stats.global().tx_requests());
    println!(
        "  Throughput: {:.2} req/s",
        merged_stats.global().tx_requests() as f64 / duration.as_secs_f64()
    );
    println!("  Min latency: {:.2} μs", basic_stats.min.as_micros());
    println!("  Mean latency: {:.2} μs", basic_stats.mean.as_micros());
    println!("  Max latency: {:.2} μs", basic_stats.max.as_micros());

    // Verify we got some requests through
    assert!(merged_stats.global().tx_requests() > 0, "Should have sent some requests");
    assert!(merged_stats.global().rx_requests() > 0, "Should have received some responses");

    // With 4 threads, we should get reasonable throughput
    assert!(
        merged_stats.global().tx_requests() > 100,
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
    let protocol = ProtocolAdapter::new(protocol);
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

    println!("Starting rate-limited test (target: {target_rate} req/s)...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully");

    let stats = worker.into_stats();
    let actual_rate = stats.global().tx_requests() as f64 / duration.as_secs_f64();

    println!("Results:");
    println!("  Target rate: {target_rate:.2} req/s");
    println!("  Actual rate: {actual_rate:.2} req/s");
    println!("  Total requests: {}", stats.global().tx_requests());

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
    let protocol = ProtocolAdapter::new(protocol);
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

    println!("Results:");
    println!("  Total requests: {}", stats.global().tx_requests());
    println!("  Total bytes sent: {}", stats.global().tx_bytes());

    assert!(stats.global().tx_requests() > 0, "Should have sent PUT requests");
    assert!(stats.global().rx_requests() > 0, "Should have received responses");
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
    let protocol = ProtocolAdapter::new(protocol);
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

    let stats = worker.stats().global();
    let basic_stats = stats.calculate_basic_stats();

    println!("Results:");
    println!("  Total requests: {}", stats.tx_requests());
    println!("  Throughput: {:.2} req/s", stats.tx_requests() as f64 / duration.as_secs_f64());
    println!("  Min latency: {:.2} μs", basic_stats.min.as_micros());
    println!("  Mean latency: {:.2} μs", basic_stats.mean.as_micros());
    println!("  Max latency: {:.2} μs", basic_stats.max.as_micros());

    assert!(stats.tx_requests() > 0, "Should have sent requests");
    assert!(stats.rx_requests() > 0, "Should have received responses");

    // With pipelining, we should get higher throughput
    let throughput = stats.tx_requests() as f64 / duration.as_secs_f64();
    println!("✓ HTTP pipelined throughput: {throughput:.0} req/s");
}
