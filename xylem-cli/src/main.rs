use clap::Parser;
use std::path::PathBuf;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use xylem_core::stats::StatsCollector;
use xylem_core::threading::{CpuPinning, ThreadingRuntime, Worker, WorkerConfig};
use xylem_core::workload::{RateControl, RequestGenerator};
use xylem_transport::TcpTransport;

mod config;
mod output;

use config::ProfileConfig;
use output::ExperimentResults;

/// Xylem: Reproducible latency measurement tool
///
/// Xylem uses TOML configuration files (profiles) to define experiments.
/// This ensures reproducibility and simplifies complex workload specifications.
///
/// Example usage:
///   xylem -P profiles/redis-get-zipfian.toml
///   xylem -P profiles/http-spike.toml --target 192.168.1.100:8080
///   xylem -P profiles/memcached-ramp.toml --duration 120s --seed 12345
///
/// See profiles/ directory for example configurations.
#[derive(Parser)]
#[command(name = "xylem")]
#[command(version, about = "Latency measurement tool with config-first design", long_about = None)]
struct Cli {
    /// Path to TOML profile configuration file (REQUIRED)
    #[arg(short = 'P', long, required = true)]
    profile: PathBuf,

    /// Override target server address (e.g., 192.168.1.100:6379)
    #[arg(short, long)]
    target: Option<String>,

    /// Override experiment duration (e.g., 30s, 1m, 1h)
    #[arg(short, long)]
    duration: Option<String>,

    /// Override output JSON file path
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Override random seed for reproducibility
    #[arg(short, long)]
    seed: Option<u64>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short = 'l', long, default_value = "info")]
    log_level: String,
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

impl<P: xylem_protocols::Protocol> xylem_core::threading::Protocol for ProtocolAdapter<P> {
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

    tracing::info!("Xylem latency measurement tool (config-first mode)");
    tracing::info!("Loading profile: {}", cli.profile.display());

    // Load and parse profile configuration
    let config = ProfileConfig::from_file(&cli.profile)?;

    // Apply CLI overrides
    let config = config.with_overrides(cli.target, cli.duration, cli.output, cli.seed)?;

    // Display experiment configuration
    tracing::info!("=== Experiment Configuration ===");
    tracing::info!("Name: {}", config.experiment.name);
    if let Some(desc) = &config.experiment.description {
        tracing::info!("Description: {}", desc);
    }
    if let Some(seed) = config.experiment.seed {
        tracing::info!("Seed: {} (reproducible mode)", seed);
    }
    tracing::info!("Duration: {:?}", config.experiment.duration);
    let target_address = config
        .target
        .address
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Target address must be specified"))?;

    tracing::info!("Target: {} ({})", target_address, config.target.protocol);
    tracing::info!("Transport: {}", config.target.transport);

    // Display traffic groups information
    tracing::info!("Traffic groups: {}", config.traffic_groups.len());
    for (i, group) in config.traffic_groups.iter().enumerate() {
        tracing::info!(
            "  Group {}: '{}' - threads: {:?}, conns/thread: {}, sampling: {}",
            i,
            group.name,
            group.threads,
            group.connections_per_thread,
            group.sampling_rate
        );
    }

    tracing::info!("Key strategy: {:?}", config.workload.keys);
    tracing::info!("Load pattern: {:?}", config.workload.pattern);
    tracing::info!("================================");

    // Parse target address
    let target_addr: std::net::SocketAddr = target_address.parse()?;
    let duration = config.experiment.duration;

    // Extract thread assignment from traffic groups
    let thread_assignment =
        xylem_core::traffic_group::ThreadGroupAssignment::from_configs(&config.traffic_groups);
    let thread_ids = thread_assignment.thread_ids();
    let num_threads = thread_ids.len();

    // Configure CPU pinning based on thread IDs
    let cpu_pinning = if thread_ids.is_empty() {
        CpuPinning::None
    } else {
        let min_thread_id = *thread_ids.iter().min().unwrap();
        if min_thread_id > 0 {
            tracing::info!("CPU pinning enabled with offset {}", min_thread_id);
            CpuPinning::Offset(min_thread_id)
        } else {
            tracing::info!("CPU pinning enabled (auto mode)");
            CpuPinning::Auto
        }
    };

    // Validate CPU pinning configuration
    if let CpuPinning::Offset(offset) = cpu_pinning {
        if let Some(core_count) = xylem_core::threading::get_core_count() {
            let max_core_needed = offset + num_threads - 1;
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

    // Create key generation strategy with seed support
    let key_gen = config.workload.keys.to_key_generation(config.experiment.seed)?;

    // Get value size from keys config
    let value_size = config.workload.keys.value_size();

    // Clone values needed for results output (already have target_address from above)
    let protocol_name = config.target.protocol.clone();
    let target_address_string = target_address.to_string();
    let target_address_for_http = target_address_string.clone();

    // Helper macro to run workers for a specific protocol
    macro_rules! run_protocol {
        ($protocol_expr:expr) => {{
            let runtime = ThreadingRuntime::with_cpu_pinning(num_threads, cpu_pinning.clone());

            // For now, use a simplified approach: assume one group per thread
            // This works for most cases and we can enhance later
            let results = runtime.run_workers(move |thread_idx| {
                // Find which traffic group this thread belongs to
                let groups_for_thread =
                    thread_assignment.get_groups_for_thread(thread_idx).ok_or_else(|| {
                        anyhow::anyhow!("No groups assigned to thread {}", thread_idx)
                    })?;

                // For simplicity, if a thread has multiple groups, we'll just use the first one
                // TODO: Support multiple groups per thread in the future
                let (group_id, group_meta) = groups_for_thread
                    .first()
                    .ok_or_else(|| anyhow::anyhow!("No group assigned to thread {}", thread_idx))?;

                let group_config = &config.traffic_groups[*group_id];

                let protocol = ProtocolAdapter::new($protocol_expr);

                // Calculate per-thread rate based on group policy
                let thread_rate_control = match &group_config.policy {
                    xylem_core::traffic_group::PolicyConfig::ClosedLoop => RateControl::ClosedLoop,
                    xylem_core::traffic_group::PolicyConfig::FixedRate { rate } => {
                        RateControl::Fixed { rate: *rate }
                    }
                    xylem_core::traffic_group::PolicyConfig::Poisson { rate } => {
                        // Poisson per connection, so total rate for thread is rate * connections
                        RateControl::Fixed {
                            rate: rate * group_meta.connections_per_thread as f64,
                        }
                    }
                    xylem_core::traffic_group::PolicyConfig::Adaptive { .. } => {
                        // For now, treat adaptive as closed-loop
                        // TODO: Implement adaptive rate control
                        tracing::warn!(
                            "Adaptive policy not yet fully implemented, using closed-loop"
                        );
                        RateControl::ClosedLoop
                    }
                };

                let generator =
                    RequestGenerator::new(key_gen.clone(), thread_rate_control.clone(), value_size);
                let stats = StatsCollector::default();
                let worker_config = WorkerConfig {
                    target: target_addr,
                    duration,
                    value_size,
                    conn_count: group_meta.connections_per_thread,
                    max_pending_per_conn: group_meta.max_pending_per_connection,
                };

                let mut worker = Worker::with_closed_loop(
                    TcpTransport::new,
                    protocol,
                    generator,
                    stats,
                    worker_config,
                )?;

                worker.run()?;
                Ok(worker.into_stats())
            })?;

            tracing::info!("Experiment completed successfully");
            StatsCollector::merge(results)
        }};
    }

    // Run experiment based on protocol
    let stats = match protocol_name.as_str() {
        "redis" => {
            run_protocol!(xylem_protocols::redis::RedisProtocol::new(
                xylem_protocols::redis::RedisOp::Get
            ))
        }
        "http" => {
            run_protocol!(xylem_protocols::http::HttpProtocol::new(
                xylem_protocols::HttpMethod::Get,
                "/".to_string(),
                target_address_for_http.clone()
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
                xylem_protocols::MasstreeOp::Get
            ))
        }
        "xylem-echo" => {
            run_protocol!(xylem_protocols::xylem_echo::XylemEchoProtocol::new(0))
        }
        _ => {
            anyhow::bail!(
                "Unsupported protocol: {}. Supported: redis, http, memcached-binary, memcached-ascii, masstree, xylem-echo",
                protocol_name
            );
        }
    };

    // Aggregate statistics with percentiles and confidence intervals
    let aggregated_stats = xylem_core::stats::aggregate_stats(&stats, duration, 0.95);

    // Create results
    let results = ExperimentResults::from_aggregated_stats(
        protocol_name,
        target_address_string,
        duration,
        aggregated_stats,
    );

    // Output results
    results.print_human();

    // Write output file if configured
    if config.output.format == "json" {
        let path_str = config
            .output
            .file
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid output file path"))?;
        results.write_json(path_str)?;
        tracing::info!("Results written to: {}", config.output.file.display());
    }

    Ok(())
}
