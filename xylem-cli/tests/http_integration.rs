//! Integration tests for HTTP protocol
//!
//! These tests automatically start and stop an nginx server for testing.
//!
//! ## Running the tests
//!
//! Run all HTTP integration tests:
//! ```bash
//! cargo test --test http_integration -- --ignored --test-threads=1
//! ```
//!
//! Run a specific test:
//! ```bash
//! cargo test --test http_integration test_http_get_single_thread -- --ignored
//! ```
//!
//! ## Requirements
//!
//! - nginx must be installed and available in PATH
//! - Port 8080 must be available
//! - Tests must be run with --test-threads=1 to avoid port conflicts

use std::fs;
use std::process::{Child, Command};
use std::sync::Mutex;
use std::thread::sleep;
use std::time::Duration;
use xylem_core::stats::StatsCollector;
use xylem_core::threading::{ThreadingRuntime, Worker, WorkerConfig};
use xylem_core::workload::{KeyGeneration, RateControl, RequestGenerator};
use xylem_transport::TcpTransport;

// Global state to track nginx server process
static NGINX_SERVER: Mutex<Option<Child>> = Mutex::new(None);

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

    fn generate_request(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
    ) -> (Vec<u8>, Self::RequestId) {
        self.inner.generate_request(conn_id, key, value_size)
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

/// Helper to check if nginx is available
fn check_nginx_available() -> bool {
    std::net::TcpStream::connect_timeout(&"127.0.0.1:8080".parse().unwrap(), Duration::from_secs(1))
        .is_ok()
}

/// Create minimal nginx configuration
fn create_nginx_config() -> Result<String, Box<dyn std::error::Error>> {
    let config_dir = "/tmp/xylem-nginx-test";
    fs::create_dir_all(config_dir)?;
    fs::create_dir_all(format!("{}/logs", config_dir))?;

    let config_path = format!("{}/nginx.conf", config_dir);
    let config_content = format!(
        r#"
daemon off;
worker_processes 1;
error_log {}/logs/error.log;
pid {}/nginx.pid;

events {{
    worker_connections 1024;
}}

http {{
    access_log {}/logs/access.log;

    # Enable keep-alive connections
    keepalive_timeout 65;
    keepalive_requests 100000;  # Allow many requests per connection

    server {{
        listen 8080;
        server_name localhost;

        location / {{
            return 200 "OK\n";
            add_header Content-Type text/plain;
        }}

        location /test {{
            return 200 "Test OK\n";
            add_header Content-Type text/plain;
        }}
    }}
}}
"#,
        config_dir, config_dir, config_dir
    );

    fs::write(&config_path, config_content)?;
    Ok(config_path)
}

/// Start nginx server if not already running
fn start_nginx() -> Result<(), Box<dyn std::error::Error>> {
    if check_nginx_available() {
        println!("✓ nginx already running on port 8080");
        return Ok(());
    }

    println!("Starting nginx server on port 8080...");

    // Kill any existing nginx processes on port 8080
    let _ = Command::new("pkill").args(["-f", "nginx.*8080"]).output();
    sleep(Duration::from_millis(100));

    // Create config
    let config_path = create_nginx_config()?;

    // Start nginx server
    let child = Command::new("nginx")
        .args(["-c", &config_path])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    // Store the child process
    let mut server = NGINX_SERVER.lock().unwrap();
    *server = Some(child);

    // Wait for server to be ready
    for i in 0..30 {
        sleep(Duration::from_millis(100));
        if check_nginx_available() {
            println!("✓ nginx started successfully after {}ms", (i + 1) * 100);
            return Ok(());
        }
    }

    Err("nginx failed to start within timeout".into())
}

/// Stop nginx server if running
fn stop_nginx() {
    println!("Stopping nginx server...");

    // Try graceful shutdown
    let _ = Command::new("nginx").args(["-s", "quit"]).output();
    sleep(Duration::from_millis(200));

    let mut server = NGINX_SERVER.lock().unwrap();
    if let Some(mut child) = server.take() {
        let _ = child.kill();
        let _ = child.wait();
    }

    // Cleanup
    let _ = Command::new("pkill").args(["-f", "nginx.*8080"]).output();
    let _ = fs::remove_dir_all("/tmp/xylem-nginx-test");

    println!("✓ nginx server stopped");
}

/// Test cleanup guard - stops nginx when dropped
struct NginxGuard;

impl Drop for NginxGuard {
    fn drop(&mut self) {
        stop_nginx();
    }
}

/// Setup nginx for tests - returns a guard that will cleanup on drop
fn setup_nginx() -> Result<NginxGuard, Box<dyn std::error::Error>> {
    start_nginx()?;
    Ok(NginxGuard)
}

#[test]
#[ignore] // Run with: cargo test --test http_integration -- --ignored
fn test_http_get_single_thread() {
    let _guard = setup_nginx().expect("Failed to start nginx");

    println!("Running HTTP GET single-threaded test...");

    let target_addr = "127.0.0.1:8080".parse().unwrap();
    let duration = Duration::from_secs(2);

    let protocol = xylem_protocols::http::HttpProtocol::new(
        xylem_protocols::HttpMethod::Get,
        "/".to_string(),
        "localhost".to_string(),
    );
    let protocol = ProtocolAdapter::new(protocol);
    let generator =
        RequestGenerator::new(KeyGeneration::sequential(0), RateControl::ClosedLoop, 64);
    let stats = StatsCollector::default();
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        value_size: 64,
        conn_count: 1,
        max_pending_per_conn: 1,
    };

    let mut worker =
        Worker::with_closed_loop(TcpTransport::new, protocol, generator, stats, worker_config)
            .expect("Failed to create worker");

    println!("Starting HTTP GET test...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully: {:?}", result.err());

    let stats = worker.into_stats();
    let basic_stats = stats.calculate_basic_stats();

    println!("Results:");
    println!("  Total requests: {}", stats.tx_requests());
    println!("  Throughput: {:.2} req/s", stats.tx_requests() as f64 / duration.as_secs_f64());
    println!("  Min latency: {:.2} μs", basic_stats.min.as_micros());
    println!("  Mean latency: {:.2} μs", basic_stats.mean.as_micros());
    println!("  Max latency: {:.2} μs", basic_stats.max.as_micros());

    // Verify we got some requests through
    assert!(stats.tx_requests() > 0, "Should have sent some requests");
    assert!(stats.rx_requests() > 0, "Should have received some responses");
    assert!(!stats.samples().is_empty(), "Should have collected latency samples");

    // Sanity check on latency (should be < 10ms for local nginx)
    assert!(
        basic_stats.mean < Duration::from_millis(10),
        "Mean latency should be < 10ms for local nginx, got {:?}",
        basic_stats.mean
    );
}

#[test]
#[ignore] // Run with: cargo test --test http_integration -- --ignored
fn test_http_post_single_thread() {
    let _guard = setup_nginx().expect("Failed to start nginx");

    println!("Running HTTP POST single-threaded test...");

    let target_addr = "127.0.0.1:8080".parse().unwrap();
    let duration = Duration::from_secs(2);

    let protocol = xylem_protocols::http::HttpProtocol::new(
        xylem_protocols::HttpMethod::Post,
        "/".to_string(),
        "localhost".to_string(),
    );
    let protocol = ProtocolAdapter::new(protocol);
    let generator =
        RequestGenerator::new(KeyGeneration::sequential(0), RateControl::ClosedLoop, 128);
    let stats = StatsCollector::default();
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        value_size: 128,
        conn_count: 1,
        max_pending_per_conn: 1,
    };

    let mut worker =
        Worker::with_closed_loop(TcpTransport::new, protocol, generator, stats, worker_config)
            .expect("Failed to create worker");

    println!("Starting HTTP POST test...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully");

    let stats = worker.into_stats();
    let basic_stats = stats.calculate_basic_stats();

    println!("Results:");
    println!("  Total requests: {}", stats.tx_requests());
    println!("  Throughput: {:.2} req/s", stats.tx_requests() as f64 / duration.as_secs_f64());
    println!("  Mean latency: {:.2} μs", basic_stats.mean.as_micros());

    assert!(stats.tx_requests() > 0, "Should have sent POST requests");
    assert!(stats.rx_requests() > 0, "Should have received responses");
}

#[test]
#[ignore] // Run with: cargo test --test http_integration -- --ignored
fn test_http_multi_thread() {
    let _guard = setup_nginx().expect("Failed to start nginx");

    println!("Running HTTP multi-threaded test...");

    let target_addr = "127.0.0.1:8080".parse().unwrap();
    let duration = Duration::from_secs(2);
    let num_threads = 4;

    let runtime = ThreadingRuntime::new(num_threads);

    println!("Starting {num_threads}-threaded HTTP test...");

    let results = runtime.run_workers(move |thread_id| {
        let protocol = xylem_protocols::http::HttpProtocol::new(
            xylem_protocols::HttpMethod::Get,
            "/".to_string(),
            "localhost".to_string(),
        );
        let protocol = ProtocolAdapter::new(protocol);
        let generator = RequestGenerator::new(
            KeyGeneration::sequential(thread_id as u64 * 10000),
            RateControl::ClosedLoop,
            64,
        );
        let stats = StatsCollector::default();
        let worker_config = WorkerConfig {
            target: target_addr,
            duration,
            value_size: 64,
            conn_count: 1,
            max_pending_per_conn: 1,
        };
        let mut worker =
            Worker::with_closed_loop(TcpTransport::new, protocol, generator, stats, worker_config)
                .expect("Failed to create worker");

        {
            worker.run()?;
            Ok(worker.into_stats())
        }
    });

    assert!(
        results.is_ok(),
        "Multi-threaded workers should complete successfully: {:?}",
        results.err()
    );

    let results = results.unwrap();
    assert_eq!(results.len(), num_threads);

    // Merge stats from all threads
    let merged_stats = StatsCollector::merge(results);
    let basic_stats = merged_stats.calculate_basic_stats();

    println!("Results ({num_threads} threads):");
    println!("  Total requests: {}", merged_stats.tx_requests());
    println!(
        "  Throughput: {:.2} req/s",
        merged_stats.tx_requests() as f64 / duration.as_secs_f64()
    );
    println!("  Min latency: {:.2} μs", basic_stats.min.as_micros());
    println!("  Mean latency: {:.2} μs", basic_stats.mean.as_micros());
    println!("  Max latency: {:.2} μs", basic_stats.max.as_micros());

    // Verify we got some requests through
    assert!(merged_stats.tx_requests() > 0, "Should have sent some requests");
    assert!(merged_stats.rx_requests() > 0, "Should have received some responses");

    // With 4 threads, we should get reasonable throughput
    assert!(
        merged_stats.tx_requests() > 100,
        "Should have reasonable throughput with {num_threads} threads"
    );
}

#[test]
#[ignore] // Run with: cargo test --test http_integration -- --ignored
fn test_http_rate_limited() {
    let _guard = setup_nginx().expect("Failed to start nginx");

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
    let generator = RequestGenerator::new(
        KeyGeneration::sequential(0),
        RateControl::Fixed { rate: target_rate },
        64,
    );
    let stats = StatsCollector::default();
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        value_size: 64,
        conn_count: 1,
        max_pending_per_conn: 1,
    };

    let mut worker =
        Worker::with_closed_loop(TcpTransport::new, protocol, generator, stats, worker_config)
            .expect("Failed to create worker");

    println!("Starting rate-limited test (target: {target_rate} req/s)...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully");

    let stats = worker.into_stats();
    let actual_rate = stats.tx_requests() as f64 / duration.as_secs_f64();

    println!("Results:");
    println!("  Target rate: {target_rate:.2} req/s");
    println!("  Actual rate: {actual_rate:.2} req/s");
    println!("  Total requests: {}", stats.tx_requests());

    // Verify we're close to target rate (within 40% tolerance)
    let rate_ratio = actual_rate / target_rate;
    assert!(
        rate_ratio > 0.6 && rate_ratio < 1.4,
        "Actual rate should be within 40% of target rate, got {rate_ratio:.2}x"
    );
}

#[test]
#[ignore] // Run with: cargo test --test http_integration -- --ignored
fn test_http_put_request() {
    let _guard = setup_nginx().expect("Failed to start nginx");

    println!("Running HTTP PUT test...");

    let target_addr = "127.0.0.1:8080".parse().unwrap();
    let duration = Duration::from_secs(2);

    let protocol = xylem_protocols::http::HttpProtocol::new(
        xylem_protocols::HttpMethod::Put,
        "/".to_string(),
        "localhost".to_string(),
    );
    let protocol = ProtocolAdapter::new(protocol);
    let generator =
        RequestGenerator::new(KeyGeneration::sequential(0), RateControl::ClosedLoop, 256);
    let stats = StatsCollector::default();
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        value_size: 256,
        conn_count: 1,
        max_pending_per_conn: 1,
    };

    let mut worker =
        Worker::with_closed_loop(TcpTransport::new, protocol, generator, stats, worker_config)
            .expect("Failed to create worker");

    println!("Starting HTTP PUT test...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully");

    let stats = worker.into_stats();

    println!("Results:");
    println!("  Total requests: {}", stats.tx_requests());
    println!("  Total bytes sent: {}", stats.tx_bytes());

    assert!(stats.tx_requests() > 0, "Should have sent PUT requests");
    assert!(stats.rx_requests() > 0, "Should have received responses");
}

#[test]
#[ignore] // Run with: cargo test --test http_integration -- --ignored
fn test_http_pipelined() {
    let _guard = setup_nginx().expect("Failed to start nginx");

    println!("Running HTTP pipelined test...");

    let target_addr = "127.0.0.1:8080".parse().unwrap();
    let duration = Duration::from_secs(2);

    let protocol = xylem_protocols::http::HttpProtocol::new(
        xylem_protocols::HttpMethod::Get,
        "/".to_string(),
        "localhost".to_string(),
    );
    let protocol = ProtocolAdapter::new(protocol);
    let generator =
        RequestGenerator::new(KeyGeneration::sequential(0), RateControl::ClosedLoop, 64);
    let stats = StatsCollector::default();
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        value_size: 64,
        conn_count: 2,
        max_pending_per_conn: 16, // Allow HTTP pipelining
    };

    let mut worker =
        Worker::with_closed_loop(TcpTransport::new, protocol, generator, stats, worker_config)
            .expect("Failed to create worker");

    println!("Starting HTTP pipelined test (2 conns, 16 max pending each)...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete: {:?}", result.err());

    let stats = worker.stats();
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
