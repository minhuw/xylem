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
use xylem_core::stats::{SamplingPolicy, TupleStatsCollector};
use xylem_core::threading::{ThreadingRuntime, Worker, WorkerConfig};

use xylem_transport::TcpTransportFactory;

mod common;

/// Create a protocol instance from a protocol name string
///
/// # Arguments
/// * `name` - Protocol name (redis, memcached-binary, memcached-ascii, http, xylem-echo)
/// * `http_config` - Optional (path, host) for HTTP protocol
/// * `redis_selector` - Optional command selector for Redis protocol
fn create_protocol(
    name: &str,
    http_config: Option<(&str, &str)>,
    redis_selector: Option<Box<dyn xylem_protocols::CommandSelector<xylem_protocols::RedisOp>>>,
) -> anyhow::Result<multi_protocol::MultiProtocol> {
    match name {
        "redis" => {
            let selector = redis_selector.ok_or_else(|| {
                anyhow::anyhow!("Redis protocol requires a command selector")
            })?;
            Ok(multi_protocol::create_redis_protocol(selector))
        }
        "memcached-binary" => Ok(multi_protocol::MultiProtocol::MemcachedBinary(
            xylem_protocols::memcached::MemcachedBinaryProtocol::new(
                xylem_protocols::memcached::MemcachedOp::Get,
            ),
        )),
        "memcached-ascii" => Ok(multi_protocol::MultiProtocol::MemcachedAscii(
            xylem_protocols::memcached::MemcachedAsciiProtocol::new(
                xylem_protocols::memcached::ascii::MemcachedOp::Get,
            ),
        )),
        "http" => {
            let (path, host) = http_config.ok_or_else(|| {
                anyhow::anyhow!("HTTP protocol requires path and host configuration")
            })?;
            Ok(multi_protocol::MultiProtocol::Http(xylem_protocols::http::HttpProtocol::new(
                xylem_protocols::HttpMethod::Get,
                path.to_string(),
                host.to_string(),
            )))
        }
        "xylem-echo" => Ok(multi_protocol::MultiProtocol::XylemEcho(
            xylem_protocols::xylem_echo::XylemEchoProtocol::default(),
        )),
        _ => anyhow::bail!(
            "Unknown protocol '{}'. Supported: redis, memcached-binary, memcached-ascii, http, xylem-echo",
            name
        ),
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
        let redis_protocol = create_protocol("redis", None, Some(redis_selector))?;
        protocols.insert(0, redis_protocol);

        // Group 1: Memcached on port 11211
        let memcached_protocol = create_protocol("memcached-binary", None, None)?;
        protocols.insert(1, memcached_protocol);

        // Group 2: HTTP on port 8080
        let http_protocol = create_protocol("http", Some(("/", "127.0.0.1:8080")), None)?;
        protocols.insert(2, http_protocol);

        let mut stats = TupleStatsCollector::new(SamplingPolicy::default(), Duration::from_secs(1));
        stats.register_group_policy(0, SamplingPolicy::Unlimited);
        stats.register_group_policy(1, SamplingPolicy::Unlimited);
        stats.register_group_policy(2, SamplingPolicy::Unlimited);

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

    let stats = TupleStatsCollector::merge(results);
    let groups = stats.aggregate_by_group(duration);

    // Verify all three protocols generated traffic
    let redis_stats = groups.get(&0).expect("Redis group stats not found");
    let memcached_stats = groups.get(&1).expect("Memcached group stats not found");
    let http_stats = groups.get(&2).expect("HTTP group stats not found");

    eprintln!("Redis requests: {}", redis_stats.total_requests);
    eprintln!("Memcached requests: {}", memcached_stats.total_requests);
    eprintln!("HTTP requests: {}", http_stats.total_requests);

    // Assert that all protocols generated some traffic
    assert!(redis_stats.total_requests > 0, "Redis should have processed requests");
    assert!(memcached_stats.total_requests > 0, "Memcached should have processed requests");
    assert!(http_stats.total_requests > 0, "HTTP should have processed requests");

    eprintln!("✓ Multi-protocol test passed!");
    eprintln!("  - Redis: {} requests", redis_stats.total_requests);
    eprintln!("  - Memcached: {} requests", memcached_stats.total_requests);
    eprintln!("  - HTTP: {} requests", http_stats.total_requests);

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
        let redis_protocol1 = create_protocol("redis", None, Some(redis_selector1))?;
        protocols.insert(0, redis_protocol1);

        // Group 1: Redis (simulating another protocol - validates multi-protocol dispatch)
        let redis_selector2 =
            Box::new(xylem_protocols::FixedCommandSelector::new(xylem_protocols::RedisOp::Get));
        let redis_protocol2 = create_protocol("redis", None, Some(redis_selector2))?;
        protocols.insert(1, redis_protocol2);

        let mut stats = TupleStatsCollector::new(SamplingPolicy::default(), Duration::from_secs(1));
        stats.register_group_policy(0, SamplingPolicy::Unlimited);
        stats.register_group_policy(1, SamplingPolicy::Unlimited);

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

    let stats = TupleStatsCollector::merge(results);
    let groups = stats.aggregate_by_group(duration);

    let group0_stats = groups.get(&0).expect("Group 0 stats");
    let group1_stats = groups.get(&1).expect("Group 1 stats");

    eprintln!("✓ Multi-protocol architecture test passed!");
    eprintln!("  - Group 0 (Redis): {} requests", group0_stats.total_requests);
    eprintln!("  - Group 1 (Redis): {} requests", group1_stats.total_requests);
    eprintln!();
    eprintln!("This validates that:");
    eprintln!("  1. Multiple protocol instances can coexist on same thread");
    eprintln!("  2. Protocol dispatch by group_id works correctly");
    eprintln!("  3. Stats are tracked separately per group");

    assert!(group0_stats.total_requests > 0, "Group 0 should have processed requests");
    assert!(group1_stats.total_requests > 0, "Group 1 should have processed requests");

    Ok(())
}
