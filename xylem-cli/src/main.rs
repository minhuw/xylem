use anyhow::Context;
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{generate, Shell};
use schemars::schema_for;
use std::io;
use std::path::PathBuf;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use xylem_core::threading::{CpuPinning, ThreadingRuntime, Worker, WorkerConfig};
use xylem_core::workload::{RateControl, RequestGenerator};
use xylem_transport::{TcpTransportFactory, UdpTransportFactory};

mod completions;
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
///   xylem -P tests/redis/redis-get-zipfian.toml
///   xylem -P profiles/http-spike.toml --set target.address=192.168.1.100:8080
///   xylem -P profiles/memcached-ramp.toml --set experiment.duration=120s --set experiment.seed=12345
///   xylem -P tests/redis/redis-bench.toml --set traffic_groups.0.sampling_rate=0.5
///   xylem completions bash > ~/.local/share/bash-completion/completions/xylem
///
/// Override any config value using dot notation:
///   --set target.protocol=redis
///   --set workload.keys.n=1000000
///   --set traffic_groups.0.threads=[0,1,2,3]
///   --set 'traffic_groups.+={name="new-group",threads=[4,5]}'
///
/// See profiles/ directory for example configurations.
#[derive(Parser)]
#[command(name = "xylem")]
#[command(version, about = "Latency measurement tool with config-first design", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Path to TOML profile configuration file
    #[arg(short = 'P', long)]
    profile: Option<PathBuf>,

    /// Override any configuration value using dot notation (can be specified multiple times)
    ///
    /// Examples:
    ///   --set target.address=127.0.0.1:6379
    ///   --set experiment.duration=60s
    ///   --set experiment.seed=999
    ///   --set target.protocol=memcached-binary
    ///   --set workload.keys.n=1000000
    ///   --set traffic_groups.0.threads=[0,1,2,3]
    ///   --set output.file=/tmp/results.json
    #[arg(long = "set", value_name = "KEY=VALUE")]
    set: Vec<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short = 'l', long, default_value = "info", global = true)]
    log_level: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate shell completions
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: Shell,
    },

    /// Generate JSON Schema for configuration files
    Schema,

    /// List all valid config paths for --set flag (used by shell completions)
    #[command(hide = true)]
    CompletePaths,
}

// Protocol adapter to bridge xylem_protocols::Protocol with xylem_core Protocol trait
// For simplicity, we use a common RequestId type for protocols that support it
struct ProtocolAdapter<P: xylem_protocols::Protocol<RequestId = (usize, u64)> + 'static> {
    inner: P,
}

impl<P: xylem_protocols::Protocol<RequestId = (usize, u64)> + 'static> ProtocolAdapter<P> {
    fn new(protocol: P) -> Self {
        Self { inner: protocol }
    }
}

impl<P: xylem_protocols::Protocol<RequestId = (usize, u64)> + 'static>
    xylem_core::threading::Protocol for ProtocolAdapter<P>
{
    type RequestId = (usize, u64);

    fn generate_request(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
    ) -> (Vec<u8>, Self::RequestId) {
        self.inner.generate_request(conn_id, key, value_size)
    }

    fn generate_set_with_imported_data(
        &mut self,
        conn_id: usize,
        key: &str,
        value: &[u8],
    ) -> (Vec<u8>, Self::RequestId) {
        // Check if P is MultiProtocol (which can dispatch to Redis)
        use std::any::TypeId;
        let p_type = TypeId::of::<P>();
        let multi_type = TypeId::of::<xylem_cli::multi_protocol::MultiProtocol>();

        if p_type == multi_type {
            // SAFETY: We just checked that P is MultiProtocol
            let multi = unsafe {
                &mut *(&mut self.inner as *mut P as *mut xylem_cli::multi_protocol::MultiProtocol)
            };
            multi.generate_set_with_imported_data(conn_id, key, value)
        } else {
            // If not MultiProtocol, it's not supported
            panic!("generate_set_with_imported_data only supported for MultiProtocol (Redis), got type: {:?}", std::any::type_name::<P>())
        }
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> anyhow::Result<(usize, Option<Self::RequestId>)> {
        self.inner.parse_response(conn_id, data)
    }

    fn parse_response_extended(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> anyhow::Result<xylem_core::threading::ParseResult<Self::RequestId>> {
        let result = self.inner.parse_response_extended(conn_id, data)?;

        // Convert xylem_protocols::ParseResult to xylem_core::threading::ParseResult
        match result {
            xylem_protocols::ParseResult::Complete { bytes_consumed, request_id } => {
                Ok(xylem_core::threading::ParseResult::Complete { bytes_consumed, request_id })
            }
            xylem_protocols::ParseResult::Incomplete => {
                Ok(xylem_core::threading::ParseResult::Incomplete)
            }
            xylem_protocols::ParseResult::Retry(retry) => {
                Ok(xylem_core::threading::ParseResult::Retry(xylem_core::threading::RetryRequest {
                    bytes_consumed: retry.bytes_consumed,
                    original_request_id: retry.original_request_id,
                    key: retry.key,
                    value_size: retry.value_size,
                    target_conn_id: retry.target_conn_id,
                    prepare_commands: retry.prepare_commands,
                    attempt: retry.attempt,
                }))
            }
        }
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn reset(&mut self) {
        self.inner.reset()
    }
}

fn main() {
    // Disable anyhow backtrace capture for cleaner error messages
    std::env::set_var("RUST_LIB_BACKTRACE", "0");

    let cli = Cli::parse();

    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| cli.log_level.clone().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let result = match cli.command {
        Some(Commands::Completions { shell }) => {
            let bin_name = "xylem";
            match shell {
                Shell::Bash => {
                    println!("{}", completions::generate_bash_completion(bin_name));
                }
                Shell::Zsh => {
                    println!("{}", completions::generate_zsh_completion(bin_name));
                }
                _ => {
                    // For other shells, fall back to clap's default generator
                    let mut cmd = Cli::command();
                    generate(shell, &mut cmd, bin_name.to_string(), &mut io::stdout());
                }
            }
            Ok(())
        }
        Some(Commands::Schema) => {
            let schema = schema_for!(ProfileConfig);
            match serde_json::to_string_pretty(&schema) {
                Ok(schema_json) => {
                    println!("{}", schema_json);
                    Ok(())
                }
                Err(e) => Err(e.into()),
            }
        }
        Some(Commands::CompletePaths) => {
            let paths = completions::get_config_paths();
            for path in paths {
                println!("{}", path);
            }
            Ok(())
        }
        None => {
            // No subcommand provided - check if profile flag is set
            if let Some(profile) = cli.profile {
                // Run the experiment
                run_experiment(profile, cli.set)
            } else {
                // No profile provided - show help
                let mut cmd = Cli::command();
                if let Err(e) = cmd.print_help() {
                    eprintln!("Error printing help: {}", e);
                }
                std::process::exit(1);
            }
        }
    };

    // Handle errors with clean, user-friendly messages
    if let Err(err) = result {
        eprintln!("Error: {}", err);

        // Show error chain if available
        let mut source = err.source();
        while let Some(cause) = source {
            eprintln!("  Caused by: {}", cause);
            source = cause.source();
        }

        std::process::exit(1);
    }
}

fn run_experiment(profile: PathBuf, set: Vec<String>) -> anyhow::Result<()> {
    tracing::info!("Xylem latency measurement tool (config-first mode)");
    tracing::info!("Loading profile: {}", profile.display());

    // Load and parse profile configuration with overrides
    let config = if set.is_empty() {
        // No overrides, just load and validate
        let config = ProfileConfig::from_file(&profile)?;
        config.validate()?;
        config
    } else {
        // Apply --set overrides
        ProfileConfig::from_file_with_overrides(&profile, &set)?
    };

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

    tracing::info!("Target: {} (protocol: per-group)", target_address);
    tracing::info!("Transport: {}", config.target.transport);

    // Display traffic groups information
    tracing::info!("Traffic groups: {}", config.traffic_groups.len());
    for (i, group) in config.traffic_groups.iter().enumerate() {
        tracing::info!(
            "  Group {}: '{}' - threads: {:?}, conns/thread: {}, sampling: {:?}",
            i,
            group.name,
            group.threads,
            group.connections_per_thread,
            group.sampling_policy
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

    // Clone config for use in worker closure
    let config_for_worker = config.clone();

    // Clone values needed for results output (already have target_address from above)
    let target_address_string = target_address.to_string();
    let target_address_for_http = target_address_string.clone();

    // Determine the main protocol name for output (use first group's protocol)
    let output_protocol = config.traffic_groups[0]
        .protocol
        .as_ref()
        .or(config.target.protocol.as_ref())
        .unwrap_or(&"unknown".to_string())
        .clone();

    let runtime = ThreadingRuntime::with_cpu_pinning(num_threads, cpu_pinning.clone());

    // Support multiple traffic groups per thread with per-group protocols
    let results = runtime.run_workers_generic(move |thread_idx| {
        // Find which traffic groups this thread belongs to
        let groups_for_thread = thread_assignment
            .get_groups_for_thread(thread_idx)
            .ok_or_else(|| anyhow::anyhow!("No groups assigned to thread {}", thread_idx))?;

        // Create value size generator from config
        let value_size_gen: Box<dyn xylem_core::workload::ValueSizeGenerator> =
            if let Some(ref value_size_config) = config_for_worker.workload.value_size {
                value_size_config.to_generator(config_for_worker.experiment.seed)?
            } else {
                Box::new(xylem_core::workload::FixedSize::new(
                    config_for_worker.workload.keys.value_size(),
                ))
            };

        // Build protocol map: one protocol per group_id
        let mut protocols = std::collections::HashMap::new();

        for (group_id, _group_meta) in groups_for_thread.iter() {
            let group_config = &config_for_worker.traffic_groups[*group_id];

            // Get protocol name: prefer group's protocol, fall back to target.protocol
            let protocol_name = group_config
                .protocol
                .as_ref()
                .or(config_for_worker.target.protocol.as_ref())
                .expect("Protocol must be specified (validated in config)");

            // Create protocol using multi_protocol factory
            let protocol_adapted = match protocol_name.as_str() {
                "http" => {
                    let p = xylem_cli::multi_protocol::create_http_protocol(
                        "/",
                        &target_address_for_http,
                    )?;
                    ProtocolAdapter::new(p)
                }
                "redis" => {
                    // Create a new selector for this group
                    let group_redis_selector =
                        if let Some(ref ops_config) = config_for_worker.workload.operations {
                            ops_config.to_redis_selector(config_for_worker.experiment.seed)?
                        } else {
                            // Default to GET if no operations config specified
                            Box::new(xylem_protocols::FixedCommandSelector::new(
                                xylem_protocols::RedisOp::Get,
                            ))
                        };
                    let p = xylem_cli::multi_protocol::create_redis_protocol(group_redis_selector);
                    ProtocolAdapter::new(p)
                }
                "redis-cluster" => {
                    // Create a new selector for this group
                    let group_redis_selector =
                        if let Some(ref ops_config) = config_for_worker.workload.operations {
                            ops_config.to_redis_selector(config_for_worker.experiment.seed)?
                        } else {
                            // Default to GET if no operations config specified
                            Box::new(xylem_protocols::FixedCommandSelector::new(
                                xylem_protocols::RedisOp::Get,
                            ))
                        };

                    // Get cluster config from target
                    let cluster_config =
                        config_for_worker.target.redis_cluster.as_ref().ok_or_else(|| {
                            anyhow::anyhow!(
                                "redis-cluster protocol requires target.redis_cluster configuration"
                            )
                        })?;

                    // Convert config to multi_protocol format
                    let cluster_proto_config = xylem_cli::multi_protocol::RedisClusterConfig {
                        nodes: cluster_config
                            .nodes
                            .iter()
                            .map(|n| xylem_cli::multi_protocol::RedisClusterNode {
                                address: n.address.clone(),
                                slot_start: n.slot_start,
                                slot_end: n.slot_end,
                            })
                            .collect(),
                    };

                    let p = xylem_cli::multi_protocol::create_redis_cluster_protocol(
                        group_redis_selector,
                        cluster_proto_config,
                    )?;
                    ProtocolAdapter::new(p)
                }
                "memcached-binary" => {
                    let p = xylem_cli::multi_protocol::create_memcached_binary_protocol();
                    ProtocolAdapter::new(p)
                }
                "memcached-ascii" => {
                    let p = xylem_cli::multi_protocol::create_memcached_ascii_protocol();
                    ProtocolAdapter::new(p)
                }
                "xylem-echo" => {
                    let p = xylem_cli::multi_protocol::create_xylem_echo_protocol();
                    ProtocolAdapter::new(p)
                }
                other => return Err(anyhow::anyhow!("Unknown protocol: {}", other).into()),
            };

            protocols.insert(*group_id, protocol_adapted);
        }

        // Create a shared request generator for this thread
        // All groups share the same key generation but may have different traffic policies
        let generator = if let Some(ref data_import_config) = config_for_worker.workload.data_import
        {
            // Use imported data instead of generated keys
            tracing::info!(
                "Thread {}: Loading imported data from {}",
                thread_idx,
                data_import_config.file.display()
            );

            let data_importer = xylem_core::workload::DataImporter::from_csv_with_seed(
                &data_import_config.file,
                config_for_worker.experiment.seed,
            )?;

            tracing::info!(
                "Thread {}: Loaded {} entries from CSV",
                thread_idx,
                data_importer.len()
            );

            RequestGenerator::with_imported_data(
                data_importer,
                RateControl::ClosedLoop,
                value_size_gen,
            )
        } else {
            // Normal mode: generate keys synthetically
            RequestGenerator::new(key_gen.clone(), RateControl::ClosedLoop, value_size_gen)
        };

        // Initialize group stats collector and register all groups
        let mut stats = xylem_core::stats::GroupStatsCollector::new();

        // Build group configurations for multi-group worker
        let mut groups_config = Vec::new();

        for (group_id, group_meta) in groups_for_thread.iter() {
            let group_config = &config_for_worker.traffic_groups[*group_id];

            // Register group in stats collector
            stats.register_group(*group_id, &group_config.sampling_policy);

            // Create policy scheduler for this group
            let policy_scheduler: Box<dyn xylem_core::scheduler::PolicyScheduler> =
                match &group_config.policy {
                    xylem_core::traffic_group::PolicyConfig::ClosedLoop => {
                        Box::new(xylem_core::scheduler::UniformPolicyScheduler::closed_loop())
                    }
                    xylem_core::traffic_group::PolicyConfig::FixedRate { rate } => {
                        Box::new(xylem_core::scheduler::UniformPolicyScheduler::fixed_rate(*rate))
                    }
                    xylem_core::traffic_group::PolicyConfig::Poisson { rate } => {
                        Box::new(xylem_core::scheduler::UniformPolicyScheduler::poisson(*rate)?)
                    }
                    xylem_core::traffic_group::PolicyConfig::Adaptive { .. } => {
                        tracing::warn!(
                            "Adaptive policy not yet fully implemented, using closed-loop"
                        );
                        Box::new(xylem_core::scheduler::UniformPolicyScheduler::closed_loop())
                    }
                };

            // Determine target for this group (group-specific or fallback to global)
            let group_target = if let Some(ref target_str) = group_config.target {
                target_str.parse().with_context(|| {
                    format!(
                        "Failed to parse target address '{}' for group {}",
                        target_str, group_config.name
                    )
                })?
            } else {
                target_addr
            };

            // Add to groups config: (group_id, target, conn_count, max_pending, policy_scheduler)
            groups_config.push((
                *group_id,
                group_target,
                group_meta.connections_per_thread,
                group_meta.max_pending_per_connection,
                policy_scheduler,
            ));
        }

        // Calculate total connections for worker config
        let total_conn_count: usize = groups_config.iter().map(|(_, _, count, _, _)| count).sum();

        let worker_config = WorkerConfig {
            target: target_addr,
            duration,
            value_size: config_for_worker.workload.keys.value_size(), // Not used, RequestGenerator handles this
            conn_count: total_conn_count,
            max_pending_per_conn: 1, // This will be overridden per-group
        };

        // Select transport factory based on config
        match config_for_worker.target.transport.as_str() {
            "tcp" => {
                let factory = TcpTransportFactory::default();
                let mut worker = Worker::new_multi_group_with_targets(
                    &factory,
                    protocols,
                    generator,
                    stats,
                    worker_config,
                    groups_config,
                )?;
                worker.run()?;
                Ok(worker.into_stats())
            }
            "udp" => {
                let factory = UdpTransportFactory::default();
                let mut worker = Worker::new_multi_group_with_targets(
                    &factory,
                    protocols,
                    generator,
                    stats,
                    worker_config,
                    groups_config,
                )?;
                worker.run()?;
                Ok(worker.into_stats())
            }
            other => Err(xylem_core::Error::Config(format!("Unsupported transport: {}", other)))?,
        }
    })?;

    tracing::info!("Experiment completed successfully");
    let stats = xylem_core::stats::GroupStatsCollector::merge(results);

    // Aggregate statistics with percentiles and confidence intervals
    // Use global stats which aggregates all traffic groups
    let aggregated_stats = xylem_core::stats::aggregate_stats(stats.global(), duration, 0.95);

    // Write output file if configured
    use config::OutputFormat;
    match config.output.format {
        OutputFormat::Json => {
            // Simple JSON format (backward compatible)
            let results = ExperimentResults::from_aggregated_stats(
                output_protocol.clone(),
                target_address_string.clone(),
                duration,
                aggregated_stats,
            );
            results.print_human();

            let path_str = config
                .output
                .file
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("Invalid output file path"))?;
            results.write_json(path_str)?;
            tracing::info!("Results written to: {}", config.output.file.display());
        }
        OutputFormat::Human => {
            // Human format - console output only, no file
            let results = ExperimentResults::from_aggregated_stats(
                output_protocol,
                target_address_string,
                duration,
                aggregated_stats,
            );
            results.print_human();
            tracing::debug!("Human format selected - output printed to console");
        }
        OutputFormat::DetailedJson | OutputFormat::Both | OutputFormat::Html => {
            // Detailed format with per-group statistics
            let detailed_results = output::DetailedExperimentResults::from_group_stats(
                config.experiment.name.clone(),
                config.experiment.description.clone(),
                config.experiment.seed,
                target_address_string,
                output_protocol,
                config.target.transport.clone(),
                duration,
                &stats,
                &config.traffic_groups,
            );

            detailed_results.print_human();

            let base_path = config.output.file.with_extension("");

            match config.output.format {
                OutputFormat::DetailedJson => {
                    let json_path = base_path.with_extension("json");
                    let path_str = json_path
                        .to_str()
                        .ok_or_else(|| anyhow::anyhow!("Invalid output file path"))?;
                    detailed_results.write_json(path_str)?;
                }
                OutputFormat::Html => {
                    let html_path = base_path.with_extension("html");
                    let path_str = html_path
                        .to_str()
                        .ok_or_else(|| anyhow::anyhow!("Invalid output file path"))?;
                    output::html::generate_html_report(&detailed_results, path_str)?;
                }
                OutputFormat::Both => {
                    let json_path = base_path.with_extension("json");
                    let html_path = base_path.with_extension("html");

                    let json_str =
                        json_path.to_str().ok_or_else(|| anyhow::anyhow!("Invalid JSON path"))?;
                    let html_str =
                        html_path.to_str().ok_or_else(|| anyhow::anyhow!("Invalid HTML path"))?;

                    detailed_results.write_json(json_str)?;
                    output::html::generate_html_report(&detailed_results, html_str)?;
                }
                _ => unreachable!(),
            }
        }
    }

    Ok(())
}
