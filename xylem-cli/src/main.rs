use anyhow::Context;
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{generate, Shell};
use schemars::schema_for;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use xylem_core::threading::{CpuPinning, ThreadingRuntime, Worker, WorkerConfig};
use xylem_transport::{TcpTransportFactory, UdpTransportFactory};

use xylem_cli::{completions, config, output};

use config::ProfileConfig;

/// Xylem: Reproducible latency measurement tool
///
/// Xylem uses TOML configuration files (profiles) to define experiments.
/// This ensures reproducibility and simplifies complex workload specifications.
///
/// Example usage:
///   xylem -P tests/redis/redis-get-zipfian.toml
///   xylem -P profiles/http-spike.toml --set traffic_groups.0.target=192.168.1.100:8080
///   xylem -P profiles/memcached-ramp.toml --set experiment.duration=120s --set experiment.seed=12345
///   xylem -P tests/redis/redis-bench.toml --set traffic_groups.0.connections_per_thread=50
///   xylem completions bash > ~/.local/share/bash-completion/completions/xylem
///
/// Override any config value using dot notation:
///   --set traffic_groups.0.target=127.0.0.1:6379
///   --set traffic_groups.0.protocol=redis
///   --set traffic_groups.0.protocol_config.keys.n=1000000
///   --set traffic_groups.0.threads=[0,1,2,3]
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
    ///   --set traffic_groups.0.target=127.0.0.1:6379
    ///   --set traffic_groups.0.protocol=redis
    ///   --set experiment.duration=60s
    ///   --set experiment.seed=999
    ///   --set traffic_groups.0.protocol_config.keys.n=1000000
    ///   --set traffic_groups.0.protocol_config.keys.strategy=zipfian
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
            let mut cmd = Cli::command();
            match shell {
                Shell::Bash => {
                    println!("{}", completions::generate_bash_completion(&mut cmd, bin_name));
                }
                Shell::Zsh => {
                    println!("{}", completions::generate_zsh_completion(&mut cmd, bin_name));
                }
                _ => {
                    // For other shells, fall back to clap's default generator
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
        let mut config = ProfileConfig::from_file(&profile)?;
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

    // Display traffic groups information
    tracing::info!("Traffic groups: {}", config.traffic_groups.len());
    for (i, group) in config.traffic_groups.iter().enumerate() {
        tracing::info!(
            "  Group {}: '{}' -> {} ({}) - threads: {:?}, conns/thread: {}, sampling: {:?}",
            i,
            group.name,
            group.target,
            group.protocol,
            group.threads,
            group.connections_per_thread,
            group.sampling_policy
        );
    }

    // Log per-group protocol configs
    for (i, group) in config.traffic_groups.iter().enumerate() {
        if let Some(ref protocol_config) = group.protocol_config {
            tracing::info!(
                "Traffic group {} '{}' protocol_config: {}",
                i,
                group.name,
                protocol_config
            );
        }
    }
    tracing::info!("================================");

    let duration = config.experiment.duration;

    // Extract thread assignment from traffic groups
    let thread_assignment =
        xylem_core::traffic_group::ThreadGroupAssignment::from_configs(&config.traffic_groups);
    let thread_ids = thread_assignment.thread_ids();
    let _num_threads = thread_ids.len();

    // Configure CPU pinning based on thread IDs
    // Thread IDs are used directly as CPU core IDs for pinning
    let cpu_pinning = if thread_ids.is_empty() {
        CpuPinning::None
    } else {
        // Thread IDs map directly to CPU cores
        // e.g., threads = [2, 3, 4] pins to cores 2, 3, 4
        tracing::info!("CPU pinning enabled for cores: {:?}", thread_ids);
        CpuPinning::Auto
    };

    // Validate CPU pinning configuration
    if !thread_ids.is_empty() {
        if let Some(core_count) = xylem_core::threading::get_core_count() {
            let max_core_needed = *thread_ids.iter().max().unwrap();
            if max_core_needed >= core_count {
                tracing::warn!(
                    "CPU pinning may fail: thread ID {} exceeds available cores ({})",
                    max_core_needed,
                    core_count
                );
            }
        }
    }

    // Initialize evalsync if enabled and environment variables are set
    #[cfg(feature = "evalsync")]
    let evalsync_enabled = {
        match evalsync::init_env() {
            Ok(()) => {
                tracing::info!("evalsync initialized, signaling ready...");
                evalsync::ready().expect("Failed to signal evalsync ready");
                tracing::info!("Waiting for evalsync start signal...");
                evalsync::wait_for_start().expect("Failed to wait for evalsync start");
                tracing::info!("Received evalsync start signal");
                // xylem has no warmup phase, so measurement starts with the workload;
                // end() below closes the measurement window together with the workload.
                evalsync::measure_start().expect("Failed to signal evalsync measurement start");
                true
            }
            Err(e) => {
                tracing::debug!("evalsync not enabled: {}", e);
                false
            }
        }
    };

    tracing::info!("Starting experiment...");

    // Create request dumper if configured
    let request_dumper = if let Some(ref dump_config) = config.request_dump {
        let dumper = xylem_core::request_dump::RequestDumper::new(dump_config.clone())
            .context("Failed to create request dumper")?;
        tracing::info!(
            "Request dumping enabled: directory={:?}, prefix={}, encoding={:?}, rotation={:?}",
            dump_config.directory,
            dump_config.prefix,
            dump_config.encoding,
            dump_config.rotation
        );
        Some(dumper)
    } else {
        None
    };

    // All workers must use the same policy so their global collectors can merge.
    // Validation guarantees there is at least one traffic group.
    let global_sampling_policy = config
        .stats
        .sampling_policy
        .clone()
        .unwrap_or_else(|| config.traffic_groups[0].sampling_policy.clone());

    // Clone config for use in worker closure
    let config_for_worker = config.clone();

    // For results output, derive a summary of targets/protocols
    // Single group: use that group's values
    // Multiple groups: indicate multi-target/protocol
    let (target_address_string, output_protocol) = if config.traffic_groups.len() == 1 {
        (
            config.traffic_groups[0].target.clone(),
            config.traffic_groups[0].protocol.clone(),
        )
    } else {
        // Collect unique targets and protocols
        let unique_targets: std::collections::HashSet<_> =
            config.traffic_groups.iter().map(|g| g.target.as_str()).collect();
        let unique_protocols: std::collections::HashSet<_> =
            config.traffic_groups.iter().map(|g| g.protocol.as_str()).collect();

        let target_summary = if unique_targets.len() == 1 {
            config.traffic_groups[0].target.clone()
        } else {
            format!("{} targets", unique_targets.len())
        };

        let protocol_summary = if unique_protocols.len() == 1 {
            config.traffic_groups[0].protocol.clone()
        } else {
            format!("{} protocols", unique_protocols.len())
        };

        (target_summary, protocol_summary)
    };

    let runtime = ThreadingRuntime::with_thread_ids(thread_ids.clone(), cpu_pinning.clone());
    // One epoch for every thread's time buckets, so equal bucket numbers
    // cover the same interval when the collectors are merged.
    let stats_epoch_ns = xylem_core::timing::time_ns();

    // Support multiple traffic groups per thread with per-group protocols
    let results = runtime.run_workers_generic(move |thread_idx| {
        // Clone the request dumper for this worker thread
        let request_dumper_for_worker = request_dumper.clone();
        // Find which traffic groups this thread belongs to
        let groups_for_thread = thread_assignment
            .get_groups_for_thread(thread_idx)
            .ok_or_else(|| anyhow::anyhow!("No groups assigned to thread {}", thread_idx))?;

        // Build protocol map: one per group_id
        let mut protocols = std::collections::HashMap::new();

        for (group_id, _group_meta) in groups_for_thread.iter() {
            let group_config = &config_for_worker.traffic_groups[*group_id];

            // Validation parsed each group's protocol_config, in group order.
            // The group's target is the HTTP Host header unless the config sets one.
            let protocol = xylem_cli::multi_protocol::MultiProtocol::from_config(
                &config_for_worker.protocol_configs[*group_id],
                &group_config.target,
                &group_config.transport,
                config_for_worker.experiment.seed,
            )?;

            protocols.insert(*group_id, protocol);
        }

        // Initialize tuple stats collector
        // Global sampling policy: used for global aggregation (can be "none" to skip global latency)
        // Per-group sampling policies: each group aggregates using its own policy
        let mut stats = xylem_core::stats::TupleStatsCollector::with_start_time(
            global_sampling_policy.clone(),
            config_for_worker.stats.bucket_duration,
            stats_epoch_ns,
        );
        // Connection IDs restart at 0 on every thread; the thread ID keeps
        // per-connection statistics apart when the collectors are merged.
        stats.set_thread_id(thread_idx);

        // Register per-group sampling policies for independent aggregation
        for (group_id, _) in groups_for_thread.iter() {
            let group_policy = &config_for_worker.traffic_groups[*group_id].sampling_policy;
            stats.register_group_policy(*group_id, group_policy.clone());
        }

        // Build group configurations for multi-group worker
        let mut groups_config = Vec::new();
        // Get transport from first group on this thread (validation ensures all groups use same transport)
        let thread_transport = groups_for_thread
            .first()
            .map(|(group_id, _)| config_for_worker.traffic_groups[*group_id].transport.as_str())
            .unwrap_or("tcp");
        let is_unix_transport = thread_transport == "unix";

        for (group_id, group_meta) in groups_for_thread.iter() {
            let group_config = &config_for_worker.traffic_groups[*group_id];

            // Unix transport handles targets as paths, not SocketAddr
            // Skip groups_config building - Unix branch handles this separately
            if is_unix_transport {
                continue;
            }

            // Calculate total connections for rate distribution
            let total_connections =
                group_config.connections_per_thread * group_config.threads.len();
            let policy_scheduler: Box<dyn xylem_core::scheduler::PolicyScheduler> =
                group_config.traffic_policy.create_scheduler(total_connections)?;

            let group_target: std::net::SocketAddr =
                group_config.target.parse().with_context(|| {
                    format!(
                        "Failed to parse target address '{}' for group {}",
                        group_config.target, group_config.name
                    )
                })?;

            groups_config.push((
                *group_id,
                group_target,
                group_meta.connections_per_thread,
                group_meta.max_pending_per_connection,
                policy_scheduler,
            ));
        }

        // Calculate total connections for worker config
        // For Unix transport, groups_config is empty - we'll compute from groups_for_thread instead
        let (total_conn_count, default_target): (usize, std::net::SocketAddr) = if is_unix_transport
        {
            // Unix transport: compute from groups_for_thread, use placeholder address
            let count: usize =
                groups_for_thread.iter().map(|(_, meta)| meta.connections_per_thread).sum();
            (count, "0.0.0.0:0".parse().unwrap())
        } else {
            // TCP/UDP: compute from groups_config
            let count = groups_config.iter().map(|(_, _, c, _, _)| c).sum();
            let target = groups_config
                .first()
                .map(|(_, addr, _, _, _)| *addr)
                .expect("At least one group must be configured for TCP/UDP transport");
            (count, target)
        };

        let worker_config = WorkerConfig {
            target: default_target,
            duration,
            conn_count: total_conn_count,
            max_pending_per_conn: 1, // This will be overridden per-group
            drain_timeout: config_for_worker.experiment.drain_timeout,
        };

        // Each group's request timeout (UDP groups only)
        let request_timeouts: Vec<_> = groups_for_thread
            .iter()
            .map(|(group_id, _)| {
                let group_config = &config_for_worker.traffic_groups[*group_id];
                (*group_id, group_config.effective_request_timeout())
            })
            .collect();

        // Helper function to wire the request dumper and request timeouts into a
        // created worker, run it, and return its stats.
        fn run_worker<G, S>(
            mut worker: Worker<G, xylem_cli::multi_protocol::MultiProtocol, S>,
            request_dumper: Option<&Arc<xylem_core::request_dump::RequestDumper>>,
            thread_idx: usize,
            request_timeouts: &[(usize, Option<std::time::Duration>)],
        ) -> xylem_core::Result<S>
        where
            G: xylem_transport::ConnectionGroup,
            S: xylem_core::stats::StatsRecorder,
        {
            // Wire request dumper if enabled
            if let Some(dumper) = request_dumper {
                worker.set_request_dumper(Arc::clone(dumper), thread_idx);
            }
            for &(group_id, timeout) in request_timeouts {
                worker.set_request_timeout(group_id, timeout);
            }

            worker.run()?;
            Ok(worker.into_stats())
        }

        // Select transport factory based on thread's transport
        match thread_transport {
            "tcp" => {
                let factory = TcpTransportFactory::default();
                let worker = Worker::new_multi_group_with_targets(
                    &factory,
                    protocols,
                    stats,
                    worker_config,
                    groups_config,
                )?;
                run_worker(
                    worker,
                    request_dumper_for_worker.as_ref(),
                    thread_idx,
                    &request_timeouts,
                )
            }
            "udp" => {
                let factory = UdpTransportFactory::default();
                let worker = Worker::new_multi_group_with_targets(
                    &factory,
                    protocols,
                    stats,
                    worker_config,
                    groups_config,
                )?;
                run_worker(
                    worker,
                    request_dumper_for_worker.as_ref(),
                    thread_idx,
                    &request_timeouts,
                )
            }
            "unix" => {
                // Unix transport requires special handling - targets are paths, not SocketAddr
                // Build Unix-specific groups config with paths
                let mut unix_groups_config: Vec<xylem_core::connection::UnixTrafficGroupConfig> =
                    Vec::new();

                for (group_id, group_meta) in groups_for_thread.iter() {
                    let group_config = &config_for_worker.traffic_groups[*group_id];
                    // Calculate total connections for rate distribution
                    let total_connections =
                        group_config.connections_per_thread * group_config.threads.len();
                    let policy_scheduler =
                        group_config.traffic_policy.create_scheduler(total_connections)?;
                    unix_groups_config.push((
                        *group_id,
                        group_config.target.clone(),
                        group_meta.connections_per_thread,
                        group_meta.max_pending_per_connection,
                        policy_scheduler,
                    ));
                }

                let factory = xylem_transport::UnixTransportFactory::default();
                let worker = Worker::new_multi_group_unix(
                    &factory,
                    protocols,
                    stats,
                    worker_config,
                    unix_groups_config,
                )?;
                run_worker(
                    worker,
                    request_dumper_for_worker.as_ref(),
                    thread_idx,
                    &request_timeouts,
                )
            }
            #[cfg(all(target_os = "linux", feature = "ntcp"))]
            "ntcp" => {
                // Validation guarantees the section and one endpoint per ntcp thread.
                // The factory opens the TUN and builds the stack on this thread.
                let ntcp = config_for_worker.ntcp.as_ref().expect("validated [ntcp] section");
                let endpoint = ntcp
                    .endpoint_for_thread(&config_for_worker.traffic_groups, thread_idx)
                    .expect("validated ntcp endpoint for this thread");
                let factory = xylem_transport::NtcpTunFactory::new(
                    endpoint.ifname.clone(),
                    endpoint.local_ip,
                    ntcp.mtu,
                )
                // Each drive costs time per connection slot: size it to this thread
                .with_max_connections(total_conn_count);
                let worker = Worker::new_multi_group_with_targets(
                    &factory,
                    protocols,
                    stats,
                    worker_config,
                    groups_config,
                )?;
                run_worker(
                    worker,
                    request_dumper_for_worker.as_ref(),
                    thread_idx,
                    &request_timeouts,
                )
            }
            other => Err(xylem_core::Error::Config(format!("Unsupported transport: {}", other)))?,
        }
    })?;

    // Signal evalsync end if it was enabled
    #[cfg(feature = "evalsync")]
    if evalsync_enabled {
        if let Err(e) = evalsync::end() {
            tracing::warn!("Failed to signal evalsync end: {}", e);
        } else {
            tracing::info!("Signaled evalsync end");
        }
    }

    tracing::info!("Experiment completed successfully");
    let stats = xylem_core::stats::TupleStatsCollector::merge(results);

    // Get transport summary from traffic groups
    let unique_transports: std::collections::HashSet<_> =
        config.traffic_groups.iter().map(|g| g.transport.as_str()).collect();
    let output_transport = if unique_transports.len() == 1 {
        config.traffic_groups[0].transport.clone()
    } else {
        format!("{} transports", unique_transports.len())
    };

    // Build detailed results (used for all formats except Human)
    let detailed_results = output::DetailedExperimentResults::from_tuple_stats(
        config.experiment.name.clone(),
        config.experiment.description.clone(),
        config.experiment.seed,
        target_address_string,
        output_protocol,
        output_transport,
        duration,
        &stats,
        &config.traffic_groups,
        config.stats.include_records,
    );

    tracing::debug!(
        "Output format: {:?}, path: {}",
        config.output.format,
        config.output.file.display()
    );

    // Print the summary and write output files if configured
    output::write_results(&detailed_results, &config.output.format, &config.output.file)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_scripts_list_every_flag() {
        let bash = completions::generate_bash_completion(&mut Cli::command(), "xylem");
        let zsh = completions::generate_zsh_completion(&mut Cli::command(), "xylem");

        // Building adds clap's --help and --version flags and help subcommand
        let mut cmd = Cli::command();
        cmd.build();

        let mut expected = Vec::new();
        for arg in cmd.get_arguments() {
            if arg.is_hide_set() {
                continue;
            }
            if let Some(long) = arg.get_long() {
                expected.push(format!("--{}", long));
            }
        }
        for subcommand in cmd.get_subcommands() {
            if !subcommand.is_hide_set() {
                expected.push(subcommand.get_name().to_string());
            }
        }
        assert!(expected.contains(&"--help".to_string()));
        assert!(expected.contains(&"completions".to_string()));

        for word in &expected {
            assert!(bash.contains(word.as_str()), "bash completion is missing {}", word);
            assert!(zsh.contains(word.as_str()), "zsh completion is missing {}", word);
        }
    }
}
