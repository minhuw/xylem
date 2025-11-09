//! Integration tests for Redis protocol
//!
//! These tests automatically start and stop a Redis server for testing.

use std::process::{Child, Command};
use std::sync::Mutex;
use std::thread::sleep;
use std::time::Duration;
use xylem_core::stats::GroupStatsCollector;
use xylem_core::threading::{ThreadingRuntime, Worker, WorkerConfig};
use xylem_core::workload::{KeyGeneration, RateControl, RequestGenerator};
use xylem_transport::TcpTransport;

mod common;

// Global state to track Redis server process
static REDIS_SERVER: Mutex<Option<Child>> = Mutex::new(None);

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

/// Helper to check if Redis is available
fn check_redis_available() -> bool {
    std::net::TcpStream::connect_timeout(&"127.0.0.1:6379".parse().unwrap(), Duration::from_secs(1))
        .is_ok()
}

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

/// Start Redis server if not already running
fn start_redis() -> Result<(), Box<dyn std::error::Error>> {
    if check_redis_available() {
        println!("✓ Redis already running on port 6379");
        return Ok(());
    }

    println!("Starting Redis server...");

    // Kill any existing Redis processes on port 6379
    let _ = Command::new("pkill").args(["-f", "redis-server.*6379"]).output();

    sleep(Duration::from_millis(100));

    // Start Redis server in background
    let child = Command::new("redis-server")
        .args(["--port", "6379", "--save", "", "--appendonly", "no"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    // Store the process handle
    *REDIS_SERVER.lock().unwrap() = Some(child);

    // Wait for Redis to be ready
    for i in 0..30 {
        sleep(Duration::from_millis(100));
        if check_redis_available() {
            println!("✓ Redis server ready after {}ms", (i + 1) * 100);
            return Ok(());
        }
    }

    Err("Redis failed to start within 3 seconds".into())
}

/// Stop Redis server
fn stop_redis() {
    println!("Stopping Redis server...");

    // Try to stop via redis-cli
    let _ = Command::new("redis-cli").args(["-p", "6379", "shutdown", "nosave"]).output();

    sleep(Duration::from_millis(100));

    // If that didn't work, kill the process
    if let Ok(mut guard) = REDIS_SERVER.lock() {
        if let Some(mut child) = guard.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    // Final cleanup - kill any remaining redis-server processes
    let _ = Command::new("pkill").args(["-f", "redis-server.*6379"]).output();

    println!("✓ Redis server stopped");
}

#[test]
#[ignore] // Run with: cargo test --test redis_integration -- --ignored --test-threads=1
fn test_redis_single_thread() {
    // Start Redis server (will auto-cleanup on drop)
    let _guard = setup_redis().expect("Failed to start Redis");

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
    let protocol = xylem_protocols::redis::RedisProtocol::new(xylem_protocols::redis::RedisOp::Get);
    let protocol = ProtocolAdapter::new(protocol);

    // Create worker components
    let generator =
        RequestGenerator::new(KeyGeneration::sequential(0), RateControl::ClosedLoop, 64);
    let stats = common::create_test_stats();
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

    // Run the worker
    println!("Starting single-threaded Redis test...");
    let result = worker.run();

    assert!(result.is_ok(), "Worker should complete successfully: {:?}", result.err());

    // Check stats
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

    // Get Redis stats after test
    let stats_after = get_redis_stats().expect("Failed to get final Redis stats");
    let redis_commands_processed =
        stats_after.total_commands_processed - stats_before.total_commands_processed;

    println!("\nRedis server validation:");
    println!("  Commands before: {}", stats_before.total_commands_processed);
    println!("  Commands after: {}", stats_after.total_commands_processed);
    println!("  Commands processed: {redis_commands_processed}");
    println!("  Client tx_requests: {}", stats.global().tx_requests());

    // Verify we got some requests through
    assert!(stats.global().tx_requests() > 0, "Should have sent some requests");
    assert!(stats.global().rx_requests() > 0, "Should have received some responses");
    assert!(!stats.global().samples().is_empty(), "Should have collected latency samples");

    // STRICT VALIDATION: Verify Redis actually processed the requests
    // Redis counts each GET command, so it should match our tx_requests
    assert!(redis_commands_processed > 0, "Redis should have processed some commands");

    // Allow small discrepancy due to INFO command itself and potential connection setup commands
    let discrepancy = (redis_commands_processed as i64 - stats.global().tx_requests() as i64).abs();
    assert!(
        discrepancy < 10,
        "Redis commands processed ({}) should match client requests ({}) within small margin (discrepancy: {})",
        redis_commands_processed,
        stats.global().tx_requests(),
        discrepancy
    );

    println!("✓ Redis stats validation passed (discrepancy: {discrepancy} commands)");

    // Sanity check on latency (should be < 10ms for local Redis)
    assert!(
        basic_stats.mean < Duration::from_millis(10),
        "Mean latency should be < 10ms for local Redis, got {:?}",
        basic_stats.mean
    );
}

#[test]
#[ignore] // Run with: cargo test --test redis_integration -- --ignored --test-threads=1
fn test_redis_multi_thread() {
    // Start Redis server (will auto-cleanup on drop)
    let _guard = setup_redis().expect("Failed to start Redis");

    println!("Running multi-threaded test...");

    // Get initial Redis stats
    let stats_before = get_redis_stats().expect("Failed to get initial Redis stats");

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(2);
    let num_threads = 4;

    let runtime = ThreadingRuntime::new(num_threads);

    println!("Starting {num_threads}-threaded Redis test...");

    let results = runtime.run_workers_generic(move |thread_id| {
        let protocol =
            xylem_protocols::redis::RedisProtocol::new(xylem_protocols::redis::RedisOp::Get);
        let protocol = ProtocolAdapter::new(protocol);
        let generator = RequestGenerator::new(
            KeyGeneration::sequential(thread_id as u64 * 10000),
            RateControl::ClosedLoop,
            64,
        );
        let stats = common::create_test_stats();
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

    // Get Redis stats after test
    let stats_after = get_redis_stats().expect("Failed to get final Redis stats");
    let redis_commands_processed =
        stats_after.total_commands_processed - stats_before.total_commands_processed;

    println!("\nRedis server validation:");
    println!("  Commands processed by Redis: {redis_commands_processed}");
    println!("  Client tx_requests: {}", merged_stats.global().tx_requests());

    // Verify we got some requests through
    assert!(merged_stats.global().tx_requests() > 0, "Should have sent some requests");
    assert!(merged_stats.global().rx_requests() > 0, "Should have received some responses");

    // STRICT VALIDATION: Verify Redis actually processed the requests
    assert!(redis_commands_processed > 0, "Redis should have processed some commands");

    let discrepancy =
        (redis_commands_processed as i64 - merged_stats.global().tx_requests() as i64).abs();
    assert!(
        discrepancy < 50, // Allow slightly more discrepancy for multi-threaded (connection setup per thread)
        "Redis commands processed ({}) should match client requests ({}) within margin (discrepancy: {})",
        redis_commands_processed,
        merged_stats.global().tx_requests(),
        discrepancy
    );

    println!("✓ Redis stats validation passed (discrepancy: {discrepancy} commands)");

    // With 4 threads, we should get more throughput than single-threaded
    // (though this depends on Redis and system capacity)
    assert!(
        merged_stats.global().tx_requests() > 1000,
        "Should have reasonable throughput with {num_threads} threads"
    );
}

#[test]
#[ignore] // Run with: cargo test --test redis_integration -- --ignored --test-threads=1
fn test_redis_rate_limited() {
    // Start Redis server (will auto-cleanup on drop)
    let _guard = setup_redis().expect("Failed to start Redis");

    println!("Running rate-limited test...");

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(2);
    let target_rate = 500.0; // 500 req/s (adjusted for closed-loop latency)

    let protocol = xylem_protocols::redis::RedisProtocol::new(xylem_protocols::redis::RedisOp::Get);
    let protocol = ProtocolAdapter::new(protocol);
    let generator = RequestGenerator::new(
        KeyGeneration::sequential(0),
        RateControl::Fixed { rate: target_rate },
        64,
    );
    let stats = common::create_test_stats();
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
    let actual_rate = stats.global().tx_requests() as f64 / duration.as_secs_f64();

    println!("Results:");
    println!("  Target rate: {target_rate:.2} req/s");
    println!("  Actual rate: {actual_rate:.2} req/s");
    println!("  Total requests: {}", stats.global().tx_requests());

    // Verify we're close to target rate (within 40% tolerance)
    // Note: Rate limiting is affected by network latency, so we need a generous tolerance
    let rate_ratio = actual_rate / target_rate;
    assert!(
        rate_ratio > 0.6 && rate_ratio < 1.4,
        "Actual rate should be within 40% of target rate, got {rate_ratio:.2}x"
    );
}

/// Test cleanup guard - stops Redis when dropped
struct RedisGuard;

impl Drop for RedisGuard {
    fn drop(&mut self) {
        stop_redis();
    }
}

/// Setup Redis for tests - returns a guard that will cleanup on drop
fn setup_redis() -> Result<RedisGuard, Box<dyn std::error::Error>> {
    start_redis()?;
    Ok(RedisGuard)
}
