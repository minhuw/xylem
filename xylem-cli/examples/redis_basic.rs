//! Basic Redis benchmarking example
//!
//! This example demonstrates how to use Xylem programmatically to benchmark Redis.
//! It shows:
//! - Setting up a Redis protocol worker
//! - Running a single-threaded benchmark
//! - Collecting and displaying latency statistics
//!
//! ## Prerequisites
//!
//! - Redis server running on localhost:6379
//!   Start with Docker: `docker run -d -p 6379:6379 redis:latest`
//!   Or use: `docker compose -f tests/redis/docker-compose.yml up -d`
//!
//! ## Running
//!
//! ```bash
//! # Make sure Redis is running first
//! docker run -d -p 6379:6379 redis:latest
//!
//! # Run the example
//! cargo run --example redis_basic
//! ```

use std::time::Duration;
use xylem_core::stats::{SamplingPolicy, TupleStatsCollector};
use xylem_core::threading::{Worker, WorkerConfig};
use xylem_transport::TcpTransportFactory;

fn main() -> anyhow::Result<()> {
    println!("╔════════════════════════════════════════════════╗");
    println!("║       Xylem Redis Benchmark Example           ║");
    println!("╚════════════════════════════════════════════════╝\n");

    // Check if Redis is available
    println!("🔌 Checking Redis connection...");
    let target_addr = "127.0.0.1:6379".parse()?;

    match std::net::TcpStream::connect_timeout(&target_addr, Duration::from_secs(1)) {
        Ok(_) => println!("✓ Redis is available on 127.0.0.1:6379\n"),
        Err(_) => {
            eprintln!("❌ Error: Redis is not running on 127.0.0.1:6379");
            eprintln!("\nPlease start Redis with Docker:");
            eprintln!("  docker run -d -p 6379:6379 redis:latest");
            eprintln!("\nOr use the just command:");
            eprintln!("  just redis-start");
            std::process::exit(1);
        }
    }

    // Configuration
    let duration = Duration::from_secs(5);
    let value_size = 64;
    let conn_count = 1;
    let max_pending_per_conn = 1;

    println!("📊 Benchmark Configuration:");
    println!("  • Target: 127.0.0.1:6379");
    println!("  • Protocol: Redis GET");
    println!("  • Duration: {} seconds", duration.as_secs());
    println!("  • Value size: {} bytes", value_size);
    println!("  • Connections: {}", conn_count);
    println!("  • Max pending per connection: {}", max_pending_per_conn);
    println!("  • Mode: Closed-loop (max throughput)\n");

    // Create Redis protocol with GET operations and embedded workload
    let selector =
        Box::new(xylem_protocols::FixedCommandSelector::new(xylem_protocols::redis::RedisOp::Get));
    let key_gen = xylem_protocols::workload::KeyGeneration::sequential(0);
    let protocol = xylem_protocols::redis::RedisProtocol::with_workload_and_options(
        selector,
        key_gen,
        value_size,
        "key:".to_string(),
        false,
        None,
    );

    // Create stats collector with unlimited sampling (collect all latencies)
    let stats = TupleStatsCollector::new(SamplingPolicy::Unlimited, Duration::from_secs(1));

    // Create worker configuration
    let worker_config = WorkerConfig {
        target: target_addr,
        duration,
        conn_count,
        max_pending_per_conn,
        drain_timeout: xylem_core::threading::DEFAULT_DRAIN_TIMEOUT,
    };

    // Create and configure the worker
    let mut worker =
        Worker::with_closed_loop(&TcpTransportFactory::default(), protocol, stats, worker_config)?;

    // Run the benchmark
    println!("🚀 Starting benchmark...");
    let start = std::time::Instant::now();

    worker.run()?;

    let elapsed = start.elapsed();

    // Extract and display results
    let stats = worker.into_stats();

    // Calculate aggregated stats with percentiles
    let agg_stats = stats.aggregate_global(elapsed);

    println!("\n╔════════════════════════════════════════════════╗");
    println!("║              Benchmark Results                 ║");
    println!("╚════════════════════════════════════════════════╝\n");

    // Throughput metrics
    println!("📈 Throughput:");
    println!("  • Total requests: {}", agg_stats.total_requests);
    println!("  • Requests/sec: {:.2}", agg_stats.throughput_rps);
    println!("  • Throughput: {:.2} MB/s", agg_stats.throughput_mbps);

    // Latency metrics
    println!("\n⏱️  Latency (microseconds):");
    println!("  • Mean:   {:>10.2} μs", agg_stats.mean_latency.as_micros());
    println!("  • Median: {:>10.2} μs", agg_stats.latency_p50.as_micros());
    println!("  • p95:    {:>10.2} μs", agg_stats.latency_p95.as_micros());
    println!("  • p99:    {:>10.2} μs", agg_stats.latency_p99.as_micros());
    println!("  • p99.9:  {:>10.2} μs", agg_stats.latency_p999.as_micros());

    println!("\n✅ Benchmark completed successfully!\n");

    // Display some helpful tips
    println!("💡 Next steps:");
    println!("  • Try increasing connections: Change conn_count to 4 or 8");
    println!("  • Test pipelining: Set max_pending_per_conn to 16");
    println!("  • Test rate limiting: Use RateControl::Fixed {{ rate: 1000.0 }}");
    println!("  • Run multi-threaded: Check out the multi-threaded examples");

    Ok(())
}
