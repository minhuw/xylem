//! Integration tests for tuple-keyed statistics collection
//!
//! Tests the TupleStatsCollector with actual experiments to verify:
//! - Per-connection statistics tracking
//! - Time-series data generation
//! - Stats merging across threads
//! - Output generation with new fields

use std::time::Duration;
use xylem_core::stats::{SamplingPolicy, TupleStatsCollector};
use xylem_core::threading::{ThreadingRuntime, Worker, WorkerConfig};
use xylem_transport::TcpTransportFactory;

mod common;

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
    ) -> xylem_core::threading::worker::Request<Self::RequestId> {
        let request = self.inner.next_request(conn_id);
        xylem_core::threading::worker::Request::new(
            request.data,
            request.request_id,
            xylem_core::threading::worker::RequestMeta { is_warmup: request.metadata.is_warmup },
        )
    }

    fn regenerate_request(
        &mut self,
        conn_id: usize,
        original_request_id: Self::RequestId,
    ) -> xylem_core::threading::worker::Request<Self::RequestId> {
        let request = self.inner.regenerate_request(conn_id, original_request_id);
        xylem_core::threading::worker::Request::new(
            request.data,
            request.request_id,
            xylem_core::threading::worker::RequestMeta { is_warmup: request.metadata.is_warmup },
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

/// Create a TupleStatsCollector for testing
fn create_tuple_stats() -> TupleStatsCollector {
    let policy = SamplingPolicy::Limited { max_samples: 100_000, rate: 1.0 };
    TupleStatsCollector::new(policy, Duration::from_secs(1))
}

#[test]
fn test_tuple_stats_single_thread_redis() {
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(3);
    let conn_count = 4;

    let protocol = xylem_protocols::redis::RedisProtocol::new(Box::new(
        xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get),
    ));
    let protocol = ProtocolAdapter::new(protocol);

    let stats = create_tuple_stats();
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count,
        max_pending_per_conn: 1,
    };

    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, worker_config)
            .expect("Failed to create worker");

    let result = worker.run();
    assert!(result.is_ok(), "Worker should complete successfully: {:?}", result.err());

    let stats = worker.into_stats();

    // Verify global aggregation
    let global = stats.aggregate_global(duration);
    println!("Global stats:");
    println!("  Total requests: {}", global.total_requests);
    println!("  Throughput: {:.2} req/s", global.throughput_rps);
    println!("  p50 latency: {:.2} μs", global.latency_p50.as_micros());
    println!("  p99 latency: {:.2} μs", global.latency_p99.as_micros());

    assert!(global.total_requests > 0, "Should have processed requests");
    assert!(global.throughput_rps > 0.0, "Should have positive throughput");

    // Verify per-connection stats
    let conn_stats = stats.aggregate_by_connection(0, duration);
    println!("\nPer-connection stats ({} connections):", conn_stats.len());
    for (conn_id, cs) in &conn_stats {
        println!(
            "  Connection {}: {} requests, p99={:.2}μs",
            conn_id,
            cs.request_count,
            cs.latency.latency_p99.as_micros()
        );
    }

    assert_eq!(conn_stats.len(), conn_count, "Should have stats for all connections");
    let total_from_connections: u64 = conn_stats.values().map(|c| c.request_count).sum();
    assert_eq!(
        total_from_connections, global.total_requests,
        "Sum of per-connection requests should equal global total"
    );

    // Verify time series data
    let time_series = stats.time_series_global();
    println!("\nTime series ({} buckets):", time_series.len());
    for ts in &time_series {
        println!(
            "  t={}: {} requests, {:.0} rps, p99={:.2}μs",
            ts.time_bucket,
            ts.request_count,
            ts.throughput_rps,
            ts.latency_p99_ns as f64 / 1000.0
        );
    }

    assert!(!time_series.is_empty(), "Should have time series data");
    // With 3 second duration and 1 second buckets, we should have ~3 buckets
    assert!(time_series.len() >= 2, "Should have at least 2 time buckets");
}

#[test]
fn test_tuple_stats_multi_thread_redis() {
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    let target_addr: std::net::SocketAddr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(3);
    let conn_count = 2;
    let num_threads = 2;

    let runtime = ThreadingRuntime::new(num_threads);

    let results: Vec<TupleStatsCollector> = runtime
        .run_workers_generic(move |thread_idx| {
            let protocol = xylem_protocols::redis::RedisProtocol::new(Box::new(
                xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get),
            ));
            let protocol = ProtocolAdapter::new(protocol);

            let stats = create_tuple_stats();
            let worker_config = WorkerConfig {
                target: target_addr,
                duration,
                conn_count,
                max_pending_per_conn: 1,
            };

            let mut worker = Worker::with_closed_loop(
                &TcpTransportFactory::default(),
                protocol,
                stats,
                worker_config,
            )
            .expect("Failed to create worker");

            println!("Thread {} starting...", thread_idx);
            worker.run().expect("Worker failed");
            println!("Thread {} completed", thread_idx);

            Ok(worker.into_stats())
        })
        .expect("Failed to run workers");

    // Merge stats from all threads
    let merged = TupleStatsCollector::merge(results);

    // Verify merged global stats
    let global = merged.aggregate_global(duration);
    println!("\nMerged global stats:");
    println!("  Total requests: {}", global.total_requests);
    println!("  Throughput: {:.2} req/s", global.throughput_rps);

    assert!(global.total_requests > 0, "Should have processed requests");

    // Verify per-connection stats after merge
    let conn_stats = merged.aggregate_by_connection(0, duration);
    println!("\nMerged per-connection stats ({} connections):", conn_stats.len());

    // Each thread has conn_count connections, all in group 0
    // Connection IDs are local to each thread, so we might have duplicates
    // that get merged
    assert!(!conn_stats.is_empty(), "Should have per-connection stats");

    // Verify time series is merged correctly
    let time_series = merged.time_series_global();
    println!("\nMerged time series ({} buckets):", time_series.len());
    for ts in &time_series {
        println!(
            "  t={}: {} requests, {:.0} rps",
            ts.time_bucket, ts.request_count, ts.throughput_rps
        );
    }

    assert!(!time_series.is_empty(), "Should have merged time series");
}

#[test]
fn test_tuple_stats_output_generation() {
    use xylem_cli::output::DetailedExperimentResults;
    use xylem_core::traffic_group::{PolicyConfig, TrafficGroupConfig};

    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(2);
    let conn_count = 3;

    let protocol = xylem_protocols::redis::RedisProtocol::new(Box::new(
        xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get),
    ));
    let protocol = ProtocolAdapter::new(protocol);

    let stats = create_tuple_stats();
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count,
        max_pending_per_conn: 1,
    };

    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, worker_config)
            .expect("Failed to create worker");

    worker.run().expect("Worker failed");
    let stats = worker.into_stats();

    // Create traffic group config for output generation
    let policy = SamplingPolicy::Limited { max_samples: 100_000, rate: 1.0 };
    let traffic_groups = vec![TrafficGroupConfig {
        name: "redis-test".to_string(),
        threads: vec![0],
        connections_per_thread: conn_count,
        max_pending_per_connection: 1,
        protocol: "redis".to_string(),
        target: "127.0.0.1:6379".to_string(),
        transport: "tcp".to_string(),
        traffic_policy: PolicyConfig::ClosedLoop,
        sampling_policy: policy,
        protocol_config: None,
    }];

    // Generate detailed results with records included
    let results = DetailedExperimentResults::from_tuple_stats(
        "Tuple Stats Test".to_string(),
        Some("Integration test for tuple-keyed stats".to_string()),
        Some(42),
        "127.0.0.1:6379".to_string(),
        "redis".to_string(),
        "tcp".to_string(),
        duration,
        &stats,
        &traffic_groups,
        true, // include_records
    );

    // Verify output structure
    println!("\nDetailedExperimentResults:");
    println!("  Experiment: {}", results.experiment.name);
    println!("  Global requests: {}", results.global.total_requests);
    println!("  Traffic groups: {}", results.traffic_groups.len());
    println!("  Records: {}", results.records.len());

    assert_eq!(results.experiment.name, "Tuple Stats Test");
    assert!(results.global.total_requests > 0);
    assert_eq!(results.traffic_groups.len(), 1);
    assert_eq!(results.traffic_groups[0].name, "redis-test");

    // Verify records have per-connection data
    assert!(!results.records.is_empty(), "Should have records");

    // Count unique connections in records
    let unique_conns: std::collections::HashSet<_> =
        results.records.iter().map(|r| (r.group_id, r.connection_id)).collect();
    println!("\n  Unique connections in records: {}", unique_conns.len());
    assert_eq!(
        unique_conns.len(),
        conn_count,
        "Should have records for all {} connections",
        conn_count
    );

    println!("\n  Records (per-connection):");
    for r in results.records.iter().take(15) {
        println!(
            "    t={:.1}s g={} c={}: {} reqs, {:.0} rps, p99={:.2}μs",
            r.time_secs,
            r.group_id,
            r.connection_id,
            r.request_count,
            r.throughput_rps,
            r.latency_p99_us
        );
    }

    // Verify JSON serialization works
    let json = serde_json::to_string_pretty(&results).expect("Failed to serialize to JSON");
    assert!(json.contains("records"), "JSON should contain records field");
    assert!(json.contains("group_id"), "JSON should contain group_id in records");
    assert!(json.contains("connection_id"), "JSON should contain connection_id in records");

    // Print human-readable output
    println!("\n--- Human-readable output ---");
    results.print_human();
}

#[test]
fn test_tuple_stats_time_buckets() {
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    // Use shorter bucket duration to get more granular data
    let policy = SamplingPolicy::Limited { max_samples: 100_000, rate: 1.0 };
    let stats = TupleStatsCollector::new(policy, Duration::from_millis(500)); // 500ms buckets

    let target_addr = "127.0.0.1:6379".parse().unwrap();
    let duration = Duration::from_secs(3);

    let protocol = xylem_protocols::redis::RedisProtocol::new(Box::new(
        xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get),
    ));
    let protocol = ProtocolAdapter::new(protocol);

    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count: 2,
        max_pending_per_conn: 1,
    };

    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, worker_config)
            .expect("Failed to create worker");

    worker.run().expect("Worker failed");
    let stats = worker.into_stats();

    let time_series = stats.time_series_global();
    println!("Time series with 500ms buckets ({} buckets):", time_series.len());
    for ts in &time_series {
        println!(
            "  bucket {}: {} requests, {:.0} rps",
            ts.time_bucket, ts.request_count, ts.throughput_rps
        );
    }

    // With 3 second duration and 500ms buckets, should have ~6 buckets
    assert!(
        time_series.len() >= 4,
        "Should have at least 4 buckets for 3s duration with 500ms buckets, got {}",
        time_series.len()
    );

    // Verify bucket duration is reflected correctly
    assert_eq!(
        stats.bucket_duration(),
        Duration::from_millis(500),
        "Bucket duration should be 500ms"
    );
}
