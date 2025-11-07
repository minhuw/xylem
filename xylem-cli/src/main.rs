use clap::Parser;
use std::time::Duration;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use xylem_core::stats::StatsCollector;
use xylem_core::threading::{CpuPinning, ThreadingRuntime, Worker, WorkerConfig};
use xylem_core::workload::{KeyGeneration, RateControl, RequestGenerator};
use xylem_transport::TcpTransport;

mod config;
mod output;

use output::ExperimentResults;

#[derive(Parser)]
#[command(name = "xylem")]
#[command(version, about = "Latency measurement tool", long_about = None)]
struct Cli {
    /// Target server address (e.g., 192.168.1.100:6379)
    #[arg(short, long)]
    target: String,

    /// Protocol to use (echo, redis, memcached-binary, memcached-ascii, http, synthetic, masstree, xylem-echo)
    #[arg(short, long)]
    protocol: String,

    /// Number of worker threads
    #[arg(long, default_value = "1")]
    threads: usize,

    /// Connections per thread (currently only 1 connection per thread supported)
    #[arg(long, default_value = "1")]
    connections: usize,

    /// Target request rate (requests per second). If not specified, runs in closed-loop mode
    #[arg(long)]
    rate: Option<f64>,

    /// Experiment duration (e.g., 30s, 1m, 1h)
    #[arg(long, default_value = "10s")]
    duration: String,

    /// Transport protocol (currently only tcp supported)
    #[arg(long, default_value = "tcp")]
    transport: String,

    /// Key distribution (sequential, random, round-robin, zipfian)
    #[arg(long, default_value = "sequential")]
    key_dist: String,

    /// Value size in bytes
    #[arg(long, default_value = "64")]
    value_size: usize,

    /// Output file for results (JSON format)
    #[arg(short, long)]
    output: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,

    /// Pin worker threads to CPU cores (thread N -> core N)
    #[arg(long)]
    pin_cpus: bool,

    /// Starting CPU core offset for pinning (used with --pin-cpus)
    #[arg(long, default_value = "0")]
    cpu_start: usize,
}

// Protocol adapter to bridge xylem_protocols::Protocol with xylem_core Protocol trait
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

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| cli.log_level.clone().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Xylem latency measurement tool");
    tracing::info!("Target: {}", cli.target);
    tracing::info!("Protocol: {}", cli.protocol);
    tracing::info!("Transport: {}", cli.transport);
    tracing::info!("Threads: {}", cli.threads);

    if cli.connections != 1 {
        tracing::warn!(
            "Multiple connections per thread not yet implemented, using 1 connection per thread"
        );
    }

    if let Some(rate) = cli.rate {
        tracing::info!("Target rate: {} req/s (total across all threads)", rate);
    } else {
        tracing::info!("Running in closed-loop mode (max throughput)");
    }
    tracing::info!("Duration: {}", cli.duration);

    // Parse duration
    let duration: Duration = humantime::parse_duration(&cli.duration)?;

    // Parse target address
    let target_addr: std::net::SocketAddr = cli.target.parse()?;
    let target_string = cli.target.clone();

    // Create key generation strategy
    let key_gen = match cli.key_dist.as_str() {
        "sequential" => KeyGeneration::sequential(0),
        "random" => KeyGeneration::random(10000),
        "round-robin" => KeyGeneration::round_robin(10000),
        "zipfian" => KeyGeneration::zipfian(10000, 0.99)?,
        _ => {
            anyhow::bail!("Unsupported key distribution: {}", cli.key_dist);
        }
    };

    // Create rate control
    let rate_control = if let Some(rate) = cli.rate {
        RateControl::Fixed { rate }
    } else {
        RateControl::ClosedLoop
    };

    // Configure CPU pinning
    let cpu_pinning = if cli.pin_cpus {
        if cli.cpu_start > 0 {
            tracing::info!("CPU pinning enabled with offset {}", cli.cpu_start);
            CpuPinning::Offset(cli.cpu_start)
        } else {
            tracing::info!("CPU pinning enabled (auto mode)");
            CpuPinning::Auto
        }
    } else {
        CpuPinning::None
    };

    // Validate CPU pinning configuration
    if cli.pin_cpus {
        if let Some(core_count) = xylem_core::threading::get_core_count() {
            let max_core_needed = cli.cpu_start + cli.threads - 1;
            if max_core_needed >= core_count {
                tracing::warn!(
                    "CPU pinning may fail: need {} cores but only {} available",
                    max_core_needed + 1,
                    core_count
                );
            }
        }
    }

    tracing::info!("Starting experiment...");

    // Helper macro to run workers for a specific protocol
    macro_rules! run_protocol {
        ($protocol_expr:expr) => {{
            let runtime = ThreadingRuntime::with_cpu_pinning(cli.threads, cpu_pinning.clone());

            // Calculate per-thread rate if rate limiting is enabled
            let thread_rate_control = match rate_control {
                RateControl::Fixed { rate } => {
                    RateControl::Fixed { rate: rate / cli.threads as f64 }
                }
                RateControl::ClosedLoop => RateControl::ClosedLoop,
            };

            // Clone values that will be moved into closures
            let value_size = cli.value_size;

            let results = runtime.run_workers(move |_thread_id| {
                let protocol = ProtocolAdapter::new($protocol_expr);
                let transport = TcpTransport::new();
                let generator =
                    RequestGenerator::new(key_gen.clone(), thread_rate_control.clone(), value_size);
                let stats = StatsCollector::default();
                let worker_config = WorkerConfig {
                    target: target_addr,
                    duration,
                    value_size,
                };
                let mut worker = Worker::new(transport, protocol, generator, stats, worker_config);

                worker.run()?;
                Ok(worker.into_stats())
            })?;

            tracing::info!("Experiment completed successfully");
            StatsCollector::merge(results)
        }};
    }

    // Run experiment based on protocol
    let stats = match cli.protocol.as_str() {
        "echo" => {
            run_protocol!(xylem_protocols::echo::EchoProtocol::new(cli.value_size))
        }
        "redis" => {
            run_protocol!(xylem_protocols::redis::RedisProtocol::new(
                xylem_protocols::redis::RedisOp::Get
            ))
        }
        "synthetic" => {
            run_protocol!(xylem_protocols::synthetic::SyntheticProtocol::new(1000))
        }
        "http" => {
            run_protocol!(xylem_protocols::http::HttpProtocol::new(
                "GET".to_string(),
                "/".to_string(),
                target_string.clone()
            ))
        }
        "memcached-binary" => {
            run_protocol!(xylem_protocols::memcached::MemcachedBinaryProtocol::new(
                xylem_protocols::memcached::MemcachedOp::Get
            ))
        }
        "memcached-ascii" => {
            run_protocol!(xylem_protocols::memcached::MemcachedAsciiProtocol::new(
                xylem_protocols::memcached::ascii::MemcachedOp::Get
            ))
        }
        "masstree" => {
            run_protocol!(xylem_protocols::masstree::MasstreeProtocol::new(
                xylem_protocols::masstree::MasstreeOp::Get
            ))
        }
        "xylem-echo" => {
            run_protocol!(xylem_protocols::xylem_echo::XylemEchoProtocol::new(0))
        }
        _ => {
            anyhow::bail!(
                "Unsupported protocol: {}. Supported: echo, redis, synthetic, http, memcached-binary, memcached-ascii, masstree, xylem-echo",
                cli.protocol
            );
        }
    };

    // Aggregate statistics with percentiles and confidence intervals
    let aggregated_stats = xylem_core::stats::aggregate_stats(&stats, duration, 0.95);

    // Create results
    let results = ExperimentResults::from_aggregated_stats(
        cli.protocol.clone(),
        cli.target.clone(),
        duration,
        aggregated_stats,
    );

    // Output results
    results.print_human();

    // Write JSON if requested
    if let Some(output_path) = cli.output {
        results.write_json(&output_path)?;
    }

    Ok(())
}
