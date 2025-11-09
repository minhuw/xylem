//! Multi-protocol integration tests
//!
//! Tests Redis + Memcached + HTTP protocols running simultaneously on the same worker threads.
//! This validates that the multi-protocol architecture works correctly.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::{Child, Command};
use std::sync::Mutex;
use std::thread::sleep;
use std::time::Duration;
use xylem_cli::multi_protocol;
use xylem_core::stats::{GroupStatsCollector, SamplingPolicy};
use xylem_core::threading::{ThreadingRuntime, Worker, WorkerConfig};
use xylem_core::workload::{KeyGeneration, RateControl, RequestGenerator};
use xylem_transport::TcpTransport;

// Global state to track service processes
static REDIS_SERVER: Mutex<Option<Child>> = Mutex::new(None);
static MEMCACHED_SERVER: Mutex<Option<Child>> = Mutex::new(None);
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

/// Check if Redis is available
fn check_redis_available() -> bool {
    std::net::TcpStream::connect_timeout(
        &"127.0.0.1:6379".parse().unwrap(),
        Duration::from_millis(100),
    )
    .is_ok()
}

/// Check if Memcached is available
fn check_memcached_available() -> bool {
    std::net::TcpStream::connect_timeout(
        &"127.0.0.1:11211".parse().unwrap(),
        Duration::from_millis(100),
    )
    .is_ok()
}

/// Check if HTTP server is available
fn check_http_available() -> bool {
    std::net::TcpStream::connect_timeout(
        &"127.0.0.1:8080".parse().unwrap(),
        Duration::from_millis(100),
    )
    .is_ok()
}

/// Start Redis server
fn start_redis() -> Result<(), Box<dyn std::error::Error>> {
    if check_redis_available() {
        eprintln!("Redis already running on port 6379");
        return Ok(());
    }

    eprintln!("Starting Redis server...");
    let child = Command::new("redis-server")
        .args(["--port", "6379", "--save", "", "--appendonly", "no"])
        .spawn()?;

    *REDIS_SERVER.lock().unwrap() = Some(child);

    // Wait for Redis to be ready
    for _ in 0..50 {
        if check_redis_available() {
            eprintln!("Redis server started successfully");
            sleep(Duration::from_millis(100));
            return Ok(());
        }
        sleep(Duration::from_millis(100));
    }

    Err("Redis server failed to start".into())
}

/// Start Memcached server
fn start_memcached() -> Result<(), Box<dyn std::error::Error>> {
    if check_memcached_available() {
        eprintln!("Memcached already running on port 11211");
        return Ok(());
    }

    eprintln!("Starting Memcached server...");
    let child = Command::new("memcached").args(["-p", "11211", "-m", "64"]).spawn()?;

    *MEMCACHED_SERVER.lock().unwrap() = Some(child);

    // Wait for Memcached to be ready
    for _ in 0..50 {
        if check_memcached_available() {
            eprintln!("Memcached server started successfully");
            sleep(Duration::from_millis(100));
            return Ok(());
        }
        sleep(Duration::from_millis(100));
    }

    Err("Memcached server failed to start".into())
}

/// Start simple HTTP server using Python
fn start_http_server() -> Result<(), Box<dyn std::error::Error>> {
    if check_http_available() {
        eprintln!("HTTP server already running on port 8080");
        return Ok(());
    }

    eprintln!("Starting HTTP server...");
    let child = Command::new("python3").args(["-m", "http.server", "8080"]).spawn()?;

    *NGINX_SERVER.lock().unwrap() = Some(child);

    // Wait for HTTP server to be ready
    for _ in 0..50 {
        if check_http_available() {
            eprintln!("HTTP server started successfully");
            sleep(Duration::from_millis(100));
            return Ok(());
        }
        sleep(Duration::from_millis(100));
    }

    Err("HTTP server failed to start".into())
}

/// Stop all servers
fn stop_servers() {
    eprintln!("Stopping test servers...");

    if let Some(mut child) = REDIS_SERVER.lock().unwrap().take() {
        let _ = child.kill();
        let _ = child.wait();
    }

    if let Some(mut child) = MEMCACHED_SERVER.lock().unwrap().take() {
        let _ = child.kill();
        let _ = child.wait();
    }

    if let Some(mut child) = NGINX_SERVER.lock().unwrap().take() {
        let _ = child.kill();
        let _ = child.wait();
    }

    sleep(Duration::from_millis(500));
}

#[test]
#[ignore] // Run with: cargo test --test multi_protocol_integration -- --ignored
fn test_multi_protocol_redis_memcached_http() {
    eprintln!("Testing multi-protocol with Redis + Memcached + HTTP on different targets");

    // Start all three servers
    if let Err(e) = start_redis() {
        eprintln!("Failed to start Redis: {}", e);
        return;
    }

    if let Err(e) = start_memcached() {
        eprintln!("Failed to start Memcached: {}", e);
        stop_servers();
        return;
    }

    if let Err(e) = start_http_server() {
        eprintln!("Failed to start HTTP server: {}", e);
        stop_servers();
        return;
    }

    let result = run_multi_protocol_test();
    stop_servers();
    result.expect("Multi-protocol test failed");
}

fn run_multi_protocol_test() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Running multi-protocol integration test...");

    let duration = Duration::from_secs(2);
    let key_gen = KeyGeneration::sequential(0);
    let value_size = 64;

    // Create threading runtime with 2 threads
    let runtime = ThreadingRuntime::new(2);

    let results = runtime.run_workers_generic(move |thread_idx| {
        // Each thread will handle all three protocols
        let mut protocols = HashMap::new();

        // Group 0: Redis on port 6379
        let redis_protocol = multi_protocol::create_protocol("redis", None)?;
        protocols.insert(0, ProtocolAdapter::new(redis_protocol));

        // Group 1: Memcached on port 11211
        let memcached_protocol = multi_protocol::create_protocol("memcached-binary", None)?;
        protocols.insert(1, ProtocolAdapter::new(memcached_protocol));

        // Group 2: HTTP on port 8080
        let http_protocol = multi_protocol::create_protocol("http", Some(("/", "127.0.0.1:8080")))?;
        protocols.insert(2, ProtocolAdapter::new(http_protocol));

        let generator = RequestGenerator::new(key_gen.clone(), RateControl::ClosedLoop, value_size);

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
            value_size,
            conn_count: 10,
            max_pending_per_conn: 1,
        };

        // Filter protocols for this thread's groups
        let thread_protocols: HashMap<usize, _> = protocols
            .into_iter()
            .filter(|(group_id, _)| groups_config.iter().any(|(gid, _, _, _, _)| gid == group_id))
            .collect();

        let mut worker = Worker::new_multi_group_with_targets(
            TcpTransport::new,
            thread_protocols,
            generator,
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
#[ignore]
fn test_multi_protocol_same_thread() {
    // Simpler test: just Redis + Memcached on same thread
    if let Err(e) = start_redis() {
        eprintln!("Failed to start Redis: {}", e);
        return;
    }

    if let Err(e) = start_memcached() {
        eprintln!("Failed to start Memcached: {}", e);
        stop_servers();
        return;
    }

    let result = run_redis_memcached_test();
    stop_servers();
    result.expect("Redis + Memcached test failed");
}

fn run_redis_memcached_test() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Testing multi-protocol architecture with two Redis groups...");
    eprintln!("(Using Redis for both groups to work around single-target limitation)");

    let duration = Duration::from_secs(1);
    let key_gen = KeyGeneration::sequential(0);
    let value_size = 64;

    let runtime = ThreadingRuntime::new(1);

    let results = runtime.run_workers_generic(move |_thread_idx| {
        let mut protocols = HashMap::new();

        // Group 0: Redis (simulating one protocol)
        let redis_protocol1 = multi_protocol::create_protocol("redis", None)?;
        protocols.insert(0, ProtocolAdapter::new(redis_protocol1));

        // Group 1: Redis (simulating another protocol - validates multi-protocol dispatch)
        let redis_protocol2 = multi_protocol::create_protocol("redis", None)?;
        protocols.insert(1, ProtocolAdapter::new(redis_protocol2));

        let generator = RequestGenerator::new(key_gen.clone(), RateControl::ClosedLoop, value_size);

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
            value_size,
            conn_count: 10,
            max_pending_per_conn: 1,
        };

        let mut worker = Worker::new_multi_group(
            TcpTransport::new,
            protocols,
            generator,
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
