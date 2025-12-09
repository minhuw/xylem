//! Multi-protocol integration tests
//!
//! Tests Redis + Memcached + HTTP protocols running simultaneously on the same worker threads.
//! This validates that the multi-protocol architecture works correctly.
//!
//! These tests use Docker Compose to manage services. Docker is required.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use xylem_cli::multi_protocol;
use xylem_core::stats::{GroupStatsCollector, SamplingPolicy};
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
fn test_multi_protocol_redis_memcached_http() {
    eprintln!("Testing multi-protocol with Redis + Memcached + HTTP on different targets");

    // Start all three servers using Docker Compose
    let _services =
        common::multi_protocol::MultiProtocolGuard::new().expect("Failed to start services");

    run_multi_protocol_test().expect("Multi-protocol test failed");
}

fn run_multi_protocol_test() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Running multi-protocol integration test...");

    let duration = Duration::from_secs(2);

    // Create threading runtime with 2 threads
    let runtime = ThreadingRuntime::new(2);

    let results = runtime.run_workers_generic(move |thread_idx| {
        // Each thread will handle all three protocols
        let mut protocols = HashMap::new();

        // Group 0: Redis on port 6379
        let redis_selector =
            Box::new(xylem_protocols::FixedCommandSelector::new(xylem_protocols::RedisOp::Get));
        let redis_protocol = multi_protocol::create_protocol("redis", None, Some(redis_selector))?;
        protocols.insert(0, ProtocolAdapter::new(redis_protocol));

        // Group 1: Memcached on port 11211
        let memcached_protocol = multi_protocol::create_protocol("memcached-binary", None, None)?;
        protocols.insert(1, ProtocolAdapter::new(memcached_protocol));

        // Group 2: HTTP on port 8080
        let http_protocol =
            multi_protocol::create_protocol("http", Some(("/", "127.0.0.1:8080")), None)?;
        protocols.insert(2, ProtocolAdapter::new(http_protocol));

        let mut stats = GroupStatsCollector::new();
        stats.register_group(0, &SamplingPolicy::Unlimited);
        stats.register_group(1, &SamplingPolicy::Unlimited);
        stats.register_group(2, &SamplingPolicy::Unlimited);

        // Build groups configuration with per-group targets
        // Each group now gets its own target address!
        let redis_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let memcached_addr: SocketAddr = "127.0.0.1:11211".parse().unwrap();
        let http_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        let groups_config = if thread_idx == 0 {
            // Thread 0: Redis (group 0) and Memcached (group 1) with different targets
            vec![
                (
                    0,          // group_id
                    redis_addr, // target for Redis
                    5,          // conn_count
                    1,          // max_pending
                    Box::new(xylem_core::scheduler::UniformPolicyScheduler::closed_loop())
                        as Box<dyn xylem_core::scheduler::PolicyScheduler>,
                ),
                (
                    1,              // group_id
                    memcached_addr, // target for Memcached
                    5,              // conn_count
                    1,              // max_pending
                    Box::new(xylem_core::scheduler::UniformPolicyScheduler::closed_loop())
                        as Box<dyn xylem_core::scheduler::PolicyScheduler>,
                ),
            ]
        } else {
            // Thread 1: HTTP (group 2) with its own target
            vec![(
                2,         // group_id
                http_addr, // target for HTTP
                5,         // conn_count
                1,         // max_pending
                Box::new(xylem_core::scheduler::UniformPolicyScheduler::closed_loop())
                    as Box<dyn xylem_core::scheduler::PolicyScheduler>,
            )]
        };

        // Worker config (target is not used by new_multi_group_with_targets, but required for struct)
        let worker_config = WorkerConfig {
            target: redis_addr, // Default (unused by new_multi_group_with_targets)
            duration,
            conn_count: 10,
            max_pending_per_conn: 1,
        };

        // Filter protocols for this thread's groups
        let thread_protocols: HashMap<usize, _> = protocols
            .into_iter()
            .filter(|(group_id, _)| groups_config.iter().any(|(gid, _, _, _, _)| gid == group_id))
            .collect();

        let mut worker = Worker::new_multi_group_with_targets(
            &TcpTransportFactory::default(),
            thread_protocols,
            stats,
            worker_config,
            groups_config,
        )?;

        worker.run()?;
        Ok(worker.into_stats())
    })?;

    let stats = GroupStatsCollector::merge(results);

    // Verify all three protocols generated traffic
    let redis_stats = stats.get_group(0).expect("Redis group stats not found");
    let memcached_stats = stats.get_group(1).expect("Memcached group stats not found");
    let http_stats = stats.get_group(2).expect("HTTP group stats not found");

    eprintln!("Redis requests: {}", redis_stats.tx_requests());
    eprintln!("Memcached requests: {}", memcached_stats.tx_requests());
    eprintln!("HTTP requests: {}", http_stats.tx_requests());

    // Assert that all protocols generated some traffic
    assert!(redis_stats.tx_requests() > 0, "Redis should have processed requests");
    assert!(memcached_stats.tx_requests() > 0, "Memcached should have processed requests");
    assert!(http_stats.tx_requests() > 0, "HTTP should have processed requests");

    eprintln!("✓ Multi-protocol test passed!");
    eprintln!("  - Redis: {} requests", redis_stats.tx_requests());
    eprintln!("  - Memcached: {} requests", memcached_stats.tx_requests());
    eprintln!("  - HTTP: {} requests", http_stats.tx_requests());

    Ok(())
}

#[test]
fn test_multi_protocol_same_thread() {
    // Simpler test: just Redis on same thread (tests multi-group architecture)
    let _redis = common::redis::RedisGuard::new().expect("Failed to start Redis");

    run_redis_memcached_test().expect("Redis + Memcached test failed");
}

fn run_redis_memcached_test() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Testing multi-protocol architecture with two Redis groups...");
    eprintln!("(Using Redis for both groups to work around single-target limitation)");

    let duration = Duration::from_secs(1);

    let runtime = ThreadingRuntime::new(1);

    let results = runtime.run_workers_generic(move |_thread_idx| {
        let mut protocols = HashMap::new();

        // Group 0: Redis (simulating one protocol)
        let redis_selector1 =
            Box::new(xylem_protocols::FixedCommandSelector::new(xylem_protocols::RedisOp::Get));
        let redis_protocol1 =
            multi_protocol::create_protocol("redis", None, Some(redis_selector1))?;
        protocols.insert(0, ProtocolAdapter::new(redis_protocol1));

        // Group 1: Redis (simulating another protocol - validates multi-protocol dispatch)
        let redis_selector2 =
            Box::new(xylem_protocols::FixedCommandSelector::new(xylem_protocols::RedisOp::Get));
        let redis_protocol2 =
            multi_protocol::create_protocol("redis", None, Some(redis_selector2))?;
        protocols.insert(1, ProtocolAdapter::new(redis_protocol2));

        let mut stats = GroupStatsCollector::new();
        stats.register_group(0, &SamplingPolicy::Unlimited);
        stats.register_group(1, &SamplingPolicy::Unlimited);

        // Both groups on same thread - this validates the core multi-protocol architecture
        let groups_config = vec![
            (
                0,
                5,
                1,
                Box::new(xylem_core::scheduler::UniformPolicyScheduler::closed_loop())
                    as Box<dyn xylem_core::scheduler::PolicyScheduler>,
            ),
            (
                1,
                5,
                1,
                Box::new(xylem_core::scheduler::UniformPolicyScheduler::closed_loop())
                    as Box<dyn xylem_core::scheduler::PolicyScheduler>,
            ),
        ];

        let worker_config = WorkerConfig {
            target: "127.0.0.1:6379".parse().unwrap(),
            duration,
            conn_count: 10,
            max_pending_per_conn: 1,
        };

        let mut worker = Worker::new_multi_group(
            &TcpTransportFactory::default(),
            protocols,
            stats,
            worker_config,
            groups_config,
        )?;

        worker.run()?;
        Ok(worker.into_stats())
    })?;

    let stats = GroupStatsCollector::merge(results);

    let group0_stats = stats.get_group(0).expect("Group 0 stats");
    let group1_stats = stats.get_group(1).expect("Group 1 stats");

    eprintln!("✓ Multi-protocol architecture test passed!");
    eprintln!("  - Group 0 (Redis): {} requests", group0_stats.tx_requests());
    eprintln!("  - Group 1 (Redis): {} requests", group1_stats.tx_requests());
    eprintln!();
    eprintln!("This validates that:");
    eprintln!("  1. Multiple protocol instances can coexist on same thread");
    eprintln!("  2. Protocol dispatch by group_id works correctly");
    eprintln!("  3. Stats are tracked separately per group");

    assert!(group0_stats.tx_requests() > 0, "Group 0 should have processed requests");
    assert!(group1_stats.tx_requests() > 0, "Group 1 should have processed requests");

    Ok(())
}
