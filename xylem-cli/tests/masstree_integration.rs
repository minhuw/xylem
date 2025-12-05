//! Integration tests for Masstree protocol
//!
//! These tests use Docker Compose to manage a Masstree server for testing.
//! Docker is required to run these tests.

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

    fn next_request(&mut self, conn_id: usize) -> (Vec<u8>, Self::RequestId) {
        self.inner.next_request(conn_id)
    }

    fn regenerate_request(
        &mut self,
        conn_id: usize,
        original_request_id: Self::RequestId,
    ) -> (Vec<u8>, Self::RequestId) {
        self.inner.regenerate_request(conn_id, original_request_id)
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
    let protocol = ProtocolAdapter::new(protocol);

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
    let basic_stats = stats.global().calculate_basic_stats();

    println!("Results:");
    println!("  Total requests: {}", stats.global().tx_requests());
    println!(
        "  Throughput: {:.2} req/s",
        stats.global().tx_requests() as f64 / duration.as_secs_f64()
    );
    println!("  Min latency: {:.2} us", basic_stats.min.as_micros());
    println!("  Mean latency: {:.2} us", basic_stats.mean.as_micros());
    println!("  Max latency: {:.2} us", basic_stats.max.as_micros());

    // Verify we got some requests through
    assert!(stats.global().tx_requests() > 0, "Should have sent some requests");
    assert!(stats.global().rx_requests() > 0, "Should have received some responses");
    assert!(!stats.global().samples().is_empty(), "Should have collected latency samples");

    // Sanity check on latency (should be < 50ms for local Masstree)
    assert!(
        basic_stats.mean < Duration::from_millis(50),
        "Mean latency should be < 50ms for local Masstree, got {:?}",
        basic_stats.mean
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
    let protocol = ProtocolAdapter::new(protocol);

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
    let basic_stats = stats.global().calculate_basic_stats();

    println!("Results:");
    println!("  Total requests: {}", stats.global().tx_requests());
    println!(
        "  Throughput: {:.2} req/s",
        stats.global().tx_requests() as f64 / duration.as_secs_f64()
    );
    println!("  Min latency: {:.2} us", basic_stats.min.as_micros());
    println!("  Mean latency: {:.2} us", basic_stats.mean.as_micros());
    println!("  Max latency: {:.2} us", basic_stats.max.as_micros());

    // Verify we got some requests through
    assert!(stats.global().tx_requests() > 0, "Should have sent some requests");
    assert!(stats.global().rx_requests() > 0, "Should have received some responses");

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
    println!("  Min latency: {:.2} us", basic_stats.min.as_micros());
    println!("  Mean latency: {:.2} us", basic_stats.mean.as_micros());
    println!("  Max latency: {:.2} us", basic_stats.max.as_micros());

    // Verify we got some requests through
    assert!(merged_stats.global().tx_requests() > 0, "Should have sent some requests");
    assert!(merged_stats.global().rx_requests() > 0, "Should have received some responses");

    // With multiple threads, we should get reasonable throughput
    assert!(
        merged_stats.global().tx_requests() > 100,
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
    let protocol = ProtocolAdapter::new(protocol);

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
    let stats = worker.into_stats();

    println!("Results:");
    println!("  Total requests: {}", stats.global().tx_requests());
    println!(
        "  Throughput: {:.2} req/s",
        stats.global().tx_requests() as f64 / duration.as_secs_f64()
    );

    // Verify we got some requests through
    assert!(stats.global().tx_requests() > 0, "Should have sent some requests");
    assert!(stats.global().rx_requests() > 0, "Should have received some responses");

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
    let protocol = ProtocolAdapter::new(protocol);
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

    let stats = worker.into_stats();
    let actual_rate = stats.global().tx_requests() as f64 / duration.as_secs_f64();

    println!("Results:");
    println!("  Target rate: {target_rate:.2} req/s");
    println!("  Actual rate: {actual_rate:.2} req/s");
    println!("  Total requests: {}", stats.global().tx_requests());

    // Verify we're close to target rate (within 50% tolerance for slower Masstree)
    let rate_ratio = actual_rate / target_rate;
    assert!(
        rate_ratio > 0.5 && rate_ratio < 1.5,
        "Actual rate should be within 50% of target rate, got {rate_ratio:.2}x"
    );

    println!("Test passed!");
}
