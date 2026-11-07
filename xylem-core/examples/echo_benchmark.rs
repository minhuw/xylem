//! Benchmark using actual xylem Worker with echo protocol
//!
//! This benchmark uses the real xylem worker implementation to profile
//! the actual scheduling, connection management, and I/O paths.
//!
//! Quick start with just:
//!   just flamegraph-echo

use std::net::SocketAddr;
use std::thread;
use std::time::Duration;
use xylem_common::Request;
use xylem_core::stats::{SamplingPolicy, TupleStatsCollector};
use xylem_core::threading::{Worker, WorkerConfig};
use xylem_transport::TcpTransportFactory;

const MESSAGE_SIZE: usize = 16;
const DEFAULT_DURATION_SECS: u64 = 30;
const DEFAULT_CONNECTIONS: usize = 1024;
const DEFAULT_PIPELINE: usize = 1;

// =============================================================================
// Echo Protocol Implementation
// =============================================================================

/// Simple echo protocol for benchmarking
///
/// Request format: [request_id: u64][padding: 8 bytes] = 16 bytes
/// Response format: same as request (echoed back)
struct EchoProtocol {
    next_req_id: u64,
}

impl EchoProtocol {
    fn new() -> Self {
        Self { next_req_id: 0 }
    }
}

impl xylem_core::threading::Protocol for EchoProtocol {
    type RequestId = u64;

    fn next_request(&mut self, _conn_id: usize) -> Request<Self::RequestId> {
        let req_id = self.next_req_id;
        self.next_req_id += 1;

        let mut buf = vec![0u8; MESSAGE_SIZE];
        buf[0..8].copy_from_slice(&req_id.to_le_bytes());
        // bytes 8-15 are delay (0 for max speed)

        Request::measurement(buf, req_id)
    }

    fn parse_response(
        &mut self,
        _conn_id: usize,
        data: &[u8],
    ) -> anyhow::Result<(usize, Option<Self::RequestId>)> {
        if data.len() < MESSAGE_SIZE {
            return Ok((0, None)); // Incomplete
        }

        let req_id = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);

        Ok((MESSAGE_SIZE, Some(req_id)))
    }

    fn name(&self) -> &'static str {
        "echo"
    }

    fn reset(&mut self) {
        self.next_req_id = 0;
    }
}

// =============================================================================
// Benchmark Worker
// =============================================================================

fn run_worker(
    target: SocketAddr,
    duration: Duration,
    num_connections: usize,
    max_pending: usize,
    core_id: usize,
) -> (u64, Duration, Duration) {
    // Pin to core
    if let Some(core_ids) = core_affinity::get_core_ids() {
        if core_id < core_ids.len() {
            core_affinity::set_for_current(core_ids[core_id]);
        }
    }

    let protocol = EchoProtocol::new();

    // Record latency in one HDR histogram per time bucket for the global view.
    // Group 0 skips latency storage: a histogram (about 270 KB) per connection
    // and time bucket is too much memory with the default 1024 connections.
    let mut stats = TupleStatsCollector::new(
        SamplingPolicy::HdrHistogram { sigfigs: 3, max_value_us: None },
        Duration::from_secs(1),
    );
    stats.register_group_policy(0, SamplingPolicy::None);

    let config = WorkerConfig {
        target,
        duration,
        conn_count: num_connections,
        max_pending_per_conn: max_pending,
    };

    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, config)
            .expect("Failed to create worker");

    // Run the actual xylem worker loop
    worker.run().expect("Worker failed");

    // Extract stats
    let stats = worker.into_stats();
    let global = stats.aggregate_global(duration);

    (global.total_requests, global.mean_latency, global.latency_p999)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let target: SocketAddr = args
        .iter()
        .position(|s| s == "--target")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            eprintln!("Usage: echo_benchmark --target <addr:port> [--threads N] [--connections N] [--pipeline N] [--duration N]");
            eprintln!("Example: echo_benchmark --target 127.0.0.1:9999 --threads 1 --connections 1024 --duration 30");
            std::process::exit(1);
        });

    let num_threads = args
        .iter()
        .position(|s| s == "--threads")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let num_connections = args
        .iter()
        .position(|s| s == "--connections")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CONNECTIONS);

    let max_pending = args
        .iter()
        .position(|s| s == "--pipeline")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PIPELINE);

    let duration_secs = args
        .iter()
        .position(|s| s == "--duration")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_DURATION_SECS);

    let duration = Duration::from_secs(duration_secs);

    println!("Xylem Echo Benchmark");
    println!("===================================");
    println!("Target: {}", target);
    println!("Worker threads: {}", num_threads);
    println!("Connections per thread: {}", num_connections);
    println!("Pipeline depth: {}", max_pending);
    println!("Duration: {}s", duration_secs);
    println!();

    // Warmup
    println!("Warming up (2s)...");
    let _ = run_worker(target, Duration::from_secs(2), num_connections, max_pending, 0);

    // Run benchmark
    println!("Running benchmark for {}s with {} threads...", duration_secs, num_threads);

    let start = std::time::Instant::now();

    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            thread::spawn(move || run_worker(target, duration, num_connections, max_pending, i))
        })
        .collect();

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    let total_elapsed = start.elapsed();
    let total_requests: u64 = results.iter().map(|(requests, _, _)| requests).sum();

    println!();
    println!("Results:");
    println!("  Total: {} requests", total_requests);
    println!("  Throughput: {:.0} req/s", total_requests as f64 / total_elapsed.as_secs_f64());
    println!(
        "  Avg latency: {:.2} μs",
        results.iter().map(|(_, mean, _)| mean.as_micros()).sum::<u128>() as f64
            / num_threads as f64
    );
    println!(
        "  Max p99.9 latency: {:.2} μs",
        results.iter().map(|(_, _, p999)| p999.as_micros()).max().unwrap_or(0)
    );

    if num_threads > 1 {
        println!("\nPer-thread results:");
        for (i, (requests, mean, p999)) in results.iter().enumerate() {
            println!(
                "  Thread {}: requests={}, mean={:.2}μs, p99.9={:.2}μs",
                i,
                requests,
                mean.as_micros(),
                p999.as_micros()
            );
        }
    }
}
