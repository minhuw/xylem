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

mod completions;
mod config;
mod output;

use config::ProfileConfig;

/// Parsed protocol configuration for a traffic group.
/// This enum holds the typed configuration for each supported protocol.
enum ParsedProtocolConfig {
    Redis(xylem_protocols::RedisConfig),
    Http(xylem_protocols::HttpConfig),
    Memcached(xylem_protocols::MemcachedConfig),
    XylemEcho(xylem_protocols::XylemEchoConfig),
    Masstree(xylem_protocols::MasstreeConfig),
}

impl ParsedProtocolConfig {
    /// Parse protocol_config JSON value into the appropriate typed struct based on protocol name
    fn parse(
        protocol_name: &str,
        protocol_config: Option<&serde_json::Value>,
    ) -> anyhow::Result<Self> {
        match protocol_name {
            "redis" | "redis-cluster" => {
                let config: xylem_protocols::RedisConfig = protocol_config
                    .map(|pc| serde_json::from_value(pc.clone()))
                    .transpose()
                    .map_err(|e| anyhow::anyhow!("Failed to parse Redis protocol_config: {}", e))?
                    .unwrap_or_default();
                Ok(Self::Redis(config))
            }
            "http" => {
                let config: xylem_protocols::HttpConfig = protocol_config
                    .map(|pc| serde_json::from_value(pc.clone()))
                    .transpose()
                    .map_err(|e| anyhow::anyhow!("Failed to parse HTTP protocol_config: {}", e))?
                    .unwrap_or_default();
                Ok(Self::Http(config))
            }
            "memcached-binary" | "memcached-ascii" => {
                let config: xylem_protocols::MemcachedConfig = protocol_config
                    .map(|pc| serde_json::from_value(pc.clone()))
                    .transpose()
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to parse Memcached protocol_config: {}", e)
                    })?
                    .unwrap_or_default();
                Ok(Self::Memcached(config))
            }
            "xylem-echo" => {
                let config: xylem_protocols::XylemEchoConfig = protocol_config
                    .map(|pc| serde_json::from_value(pc.clone()))
                    .transpose()
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to parse XylemEcho protocol_config: {}", e)
                    })?
                    .unwrap_or_default();
                Ok(Self::XylemEcho(config))
            }
            "masstree" => {
                let config: xylem_protocols::MasstreeConfig = protocol_config
                    .map(|pc| serde_json::from_value(pc.clone()))
                    .transpose()
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to parse Masstree protocol_config: {}", e)
                    })?
                    .unwrap_or_default();
                Ok(Self::Masstree(config))
            }
            other => anyhow::bail!("Unknown protocol: {}", other),
        }
    }

    /// Get the keys configuration (for protocols that support keys)
    fn keys_config(&self) -> Option<xylem_protocols::KeysConfig> {
        match self {
            Self::Redis(c) => Some(c.keys.clone()),
            Self::Memcached(c) => Some(c.keys.clone()),
            Self::Masstree(c) => Some(c.keys.clone()),
            // HTTP and XylemEcho don't use key-based workloads
            Self::Http(_) | Self::XylemEcho(_) => None,
        }
    }
}

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

    fn next_request(
        &mut self,
        conn_id: usize,
    ) -> xylem_core::threading::worker::Request<Self::RequestId> {
        let request = self.inner.next_request(conn_id);
        // Convert xylem_protocols::Request to xylem_core::threading::Request
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
                    is_warmup: retry.is_warmup,
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

    fn can_send(&self, conn_id: usize) -> bool {
        self.inner.can_send(conn_id)
    }
}

impl<P: xylem_protocols::Protocol<RequestId = (usize, u64)> + 'static> ProtocolAdapter<P> {
    /// Register cluster connections from actual pool mappings
    ///
    /// Only applicable for RedisCluster protocol. For other protocols, this is a no-op.
    fn register_cluster_connections(&mut self, connections: &[(usize, std::net::SocketAddr)]) {
        use std::any::TypeId;
        let p_type = TypeId::of::<P>();
        let multi_type = TypeId::of::<xylem_cli::multi_protocol::MultiProtocol>();

        if p_type == multi_type {
            // SAFETY: We just checked that P is MultiProtocol
            let multi = unsafe {
                &mut *(&mut self.inner as *mut P as *mut xylem_cli::multi_protocol::MultiProtocol)
            };
            multi.register_cluster_connections(connections);
        }
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

            // Protocol and target are now required on each group
            let protocol_name = &group_config.protocol;

            // Parse the protocol_config into a typed struct based on protocol name
            let parsed_config =
                ParsedProtocolConfig::parse(protocol_name, group_config.protocol_config.as_ref())?;

            // Use group's target for HTTP Host header (or explicit host from config)
            let group_target_for_host = &group_config.target;

            // Create protocol using multi_protocol factory
            let protocol_adapted = match (&parsed_config, protocol_name.as_str()) {
                (ParsedProtocolConfig::Http(http_config), "http") => {
                    let method = parse_http_method(&http_config.method)?;
                    // Use explicit host from config, or fall back to group's target address
                    let host =
                        http_config.host.clone().unwrap_or_else(|| group_target_for_host.clone());

                    let p = xylem_cli::multi_protocol::create_http_protocol_full(
                        method,
                        &http_config.path,
                        &host,
                    );
                    ProtocolAdapter::new(p)
                }
                (ParsedProtocolConfig::Redis(redis_config), "redis") => {
                    let selector =
                        create_redis_selector(redis_config, config_for_worker.experiment.seed)?;
                    // Get key generator and value size for embedded workload
                    let keys_config = parsed_config.keys_config().ok_or_else(|| {
                        anyhow::anyhow!("Redis protocol requires keys configuration")
                    })?;
                    let key_gen = keys_config.to_key_gen(config_for_worker.experiment.seed)?;
                    let p = xylem_cli::multi_protocol::create_redis_protocol_from_config(
                        redis_config,
                        selector,
                        key_gen,
                        config_for_worker.experiment.seed,
                    )?;
                    ProtocolAdapter::new(p)
                }
                (ParsedProtocolConfig::Redis(redis_config), "redis-cluster") => {
                    let selector =
                        create_redis_selector(redis_config, config_for_worker.experiment.seed)?;

                    // Get cluster config from protocol_config.redis_cluster
                    let cluster_config =
                        redis_config.redis_cluster.as_ref().ok_or_else(|| {
                            anyhow::anyhow!(
                                "redis-cluster protocol requires protocol_config.redis_cluster configuration"
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

                    // Get key generator and value size for embedded workload
                    let keys_config = parsed_config.keys_config().ok_or_else(|| {
                        anyhow::anyhow!("Redis-cluster protocol requires keys configuration")
                    })?;
                    let key_gen = keys_config.to_key_gen(config_for_worker.experiment.seed)?;
                    let p = xylem_cli::multi_protocol::create_redis_cluster_protocol_from_config(
                        redis_config,
                        selector,
                        cluster_proto_config,
                        key_gen,
                        config_for_worker.experiment.seed,
                    )?;
                    ProtocolAdapter::new(p)
                }
                (ParsedProtocolConfig::Memcached(mc_config), "memcached-binary") => {
                    let operation = parse_memcached_op_binary(&mc_config.operation)?;
                    let key_gen = mc_config.keys.to_key_gen(config_for_worker.experiment.seed)?;
                    let p = xylem_cli::multi_protocol::create_memcached_binary_protocol_from_config(
                        mc_config,
                        operation,
                        key_gen,
                    )?;
                    ProtocolAdapter::new(p)
                }
                (ParsedProtocolConfig::Memcached(mc_config), "memcached-ascii") => {
                    let operation = parse_memcached_op_ascii(&mc_config.operation)?;
                    let key_gen = mc_config.keys.to_key_gen(config_for_worker.experiment.seed)?;
                    let p = xylem_cli::multi_protocol::create_memcached_ascii_protocol_from_config(
                        mc_config,
                        operation,
                        key_gen,
                    )?;
                    ProtocolAdapter::new(p)
                }
                (ParsedProtocolConfig::XylemEcho(_echo_config), "xylem-echo") => {
                    // XylemEcho message_size is handled via the value_size in the generator
                    let p = xylem_cli::multi_protocol::create_xylem_echo_protocol();
                    ProtocolAdapter::new(p)
                }
                (ParsedProtocolConfig::Masstree(mt_config), "masstree") => {
                    let operation = parse_masstree_op(mt_config)?;
                    let key_gen = mt_config.keys.to_key_gen(config_for_worker.experiment.seed)?;
                    let p = xylem_cli::multi_protocol::create_masstree_protocol_from_config(
                        mt_config,
                        operation,
                        key_gen,
                        config_for_worker.experiment.seed,
                    )?;
                    ProtocolAdapter::new(p)
                }
                _ => return Err(anyhow::anyhow!("Unknown protocol: {}", protocol_name).into()),
            };

            protocols.insert(*group_id, protocol_adapted);
        }

        // Initialize tuple stats collector (use sampling policy from first group)
        // Note: All groups on the same thread share a single sampling policy for the stats collector.
        // If groups have different sampling policies, the first group's policy is used.
        let first_group_id = groups_for_thread.first().map(|(gid, _)| *gid).unwrap_or(0);
        let sampling_policy = config_for_worker.traffic_groups[first_group_id].sampling_policy.clone();

        // Warn if multiple groups on this thread have different sampling policies
        for (group_id, _) in groups_for_thread.iter().skip(1) {
            let other_policy = &config_for_worker.traffic_groups[*group_id].sampling_policy;
            if std::mem::discriminant(other_policy) != std::mem::discriminant(&sampling_policy) {
                tracing::warn!(
                    "Thread {}: Group {} uses a different sampling policy than group {}. \
                     All groups on this thread will use the sampling policy from group {}.",
                    thread_idx,
                    group_id,
                    first_group_id,
                    first_group_id
                );
            }
        }

        let stats = xylem_core::stats::TupleStatsCollector::new(
            sampling_policy,
            config_for_worker.stats.bucket_duration,
        );

        // Build group configurations for multi-group worker
        let mut groups_config = Vec::new();
        // Track which groups are redis-cluster for post-creation wiring
        let mut redis_cluster_groups: Vec<usize> = Vec::new();
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

            // Non-cluster protocol: single target
            if group_config.protocol != "redis-cluster" {
                // Calculate total connections for rate distribution
                let total_connections = group_config.connections_per_thread * group_config.threads.len();
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
                continue;
            }

            // Redis-cluster: create connections to ALL cluster nodes
            redis_cluster_groups.push(*group_id);

            // Parse protocol_config to get redis_cluster configuration
            let redis_config: xylem_protocols::RedisConfig = group_config
                .protocol_config
                .as_ref()
                .map(|pc| serde_json::from_value(pc.clone()))
                .transpose()
                .map_err(|e| anyhow::anyhow!("Failed to parse redis-cluster protocol_config: {}", e))?
                .unwrap_or_default();

            let cluster_config =
                redis_config.redis_cluster.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "redis-cluster protocol requires protocol_config.redis_cluster configuration"
                    )
                })?;

            // Distribute connections across cluster nodes round-robin
            let num_nodes = cluster_config.nodes.len();
            let conns_per_node = group_meta.connections_per_thread / num_nodes;
            let extra_conns = group_meta.connections_per_thread % num_nodes;

            for (node_idx, node) in cluster_config.nodes.iter().enumerate() {
                let node_addr: std::net::SocketAddr = node.address.parse().with_context(|| {
                    format!(
                        "Failed to parse cluster node address '{}' for group {}",
                        node.address, group_config.name
                    )
                })?;

                // Distribute extra connections to first nodes
                let node_conn_count = conns_per_node + usize::from(node_idx < extra_conns);
                if node_conn_count == 0 {
                    continue;
                }

                // Calculate total connections for rate distribution (across all cluster nodes)
                let total_connections = group_config.connections_per_thread * group_config.threads.len();
                let policy_scheduler: Box<dyn xylem_core::scheduler::PolicyScheduler> =
                    group_config.traffic_policy.create_scheduler(total_connections)?;

                groups_config.push((
                    *group_id,
                    node_addr,
                    node_conn_count,
                    group_meta.max_pending_per_connection,
                    policy_scheduler,
                ));
            }
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
        };

        // Helper function to wire redis-cluster connections after worker creation
        fn wire_cluster_connections<G, S>(
            worker: &mut Worker<G, ProtocolAdapter<xylem_cli::multi_protocol::MultiProtocol>, S>,
            cluster_groups: &[usize],
        ) where
            G: xylem_transport::ConnectionGroup,
            S: xylem_core::stats::StatsRecorder,
        {
            // Wire connections to each redis-cluster protocol, filtered by group_id
            // to avoid cross-group socket confusion (e.g., routing Redis to HTTP socket)
            for &group_id in cluster_groups {
                let conn_targets = worker.connection_targets_for_group(group_id);
                if let Some(protocol) = worker.protocols_mut().get_mut(&group_id) {
                    protocol.register_cluster_connections(&conn_targets);
                }
            }
        }

        // Select transport factory based on thread's transport
        match thread_transport {
            "tcp" => {
                let factory = TcpTransportFactory::default();
                let mut worker = Worker::new_multi_group_with_targets(
                    &factory,
                    protocols,
                    stats,
                    worker_config,
                    groups_config,
                )?;

                // Wire redis-cluster connections
                wire_cluster_connections(&mut worker, &redis_cluster_groups);

                // Wire request dumper if enabled
                if let Some(ref dumper) = request_dumper_for_worker {
                    worker.set_request_dumper(Arc::clone(dumper), thread_idx);
                }

                worker.run()?;
                Ok(worker.into_stats())
            }
            "udp" => {
                let factory = UdpTransportFactory::default();
                let mut worker = Worker::new_multi_group_with_targets(
                    &factory,
                    protocols,
                    stats,
                    worker_config,
                    groups_config,
                )?;

                // Wire redis-cluster connections
                wire_cluster_connections(&mut worker, &redis_cluster_groups);

                // Wire request dumper if enabled
                if let Some(ref dumper) = request_dumper_for_worker {
                    worker.set_request_dumper(Arc::clone(dumper), thread_idx);
                }

                worker.run()?;
                Ok(worker.into_stats())
            }
            "unix" => {
                // Unix transport requires special handling - targets are paths, not SocketAddr
                // Build Unix-specific groups config with paths
                let mut unix_groups_config: Vec<xylem_core::connection::UnixTrafficGroupConfig> =
                    Vec::new();

                for (group_id, group_meta) in groups_for_thread.iter() {
                    let group_config = &config_for_worker.traffic_groups[*group_id];
                    // Calculate total connections for rate distribution
                    let total_connections = group_config.connections_per_thread * group_config.threads.len();
                    let policy_scheduler = group_config.traffic_policy.create_scheduler(total_connections)?;
                    unix_groups_config.push((
                        *group_id,
                        group_config.target.clone(),
                        group_meta.connections_per_thread,
                        group_meta.max_pending_per_connection,
                        policy_scheduler,
                    ));
                }

                let factory = xylem_transport::UnixTransportFactory::default();
                let mut worker = Worker::new_multi_group_unix(
                    &factory,
                    protocols,
                    stats,
                    worker_config,
                    unix_groups_config,
                )?;

                // Wire redis-cluster connections (unlikely for Unix but for consistency)
                wire_cluster_connections(&mut worker, &redis_cluster_groups);

                // Wire request dumper if enabled
                if let Some(ref dumper) = request_dumper_for_worker {
                    worker.set_request_dumper(Arc::clone(dumper), thread_idx);
                }

                worker.run()?;
                Ok(worker.into_stats())
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

    // Write output file if configured
    use config::OutputFormat;

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

    match config.output.format {
        OutputFormat::Human => {
            // Human format - console output only, no file
            detailed_results.print_human();
            tracing::debug!("Human format selected - output printed to console");
        }
        OutputFormat::Json => {
            detailed_results.print_human();

            let json_path = config.output.file.with_extension("json");
            let path_str =
                json_path.to_str().ok_or_else(|| anyhow::anyhow!("Invalid output file path"))?;
            tracing::debug!("Writing JSON results to {}", json_path.display());
            match detailed_results.write_json(path_str) {
                Ok(_) => tracing::info!("Results written to: {}", json_path.display()),
                Err(e) => {
                    tracing::error!(
                        "Failed to write JSON results to {}: {}",
                        json_path.display(),
                        e
                    );
                    return Err(e);
                }
            }
        }
        OutputFormat::Html => {
            detailed_results.print_human();

            let html_path = config.output.file.with_extension("html");
            let path_str =
                html_path.to_str().ok_or_else(|| anyhow::anyhow!("Invalid output file path"))?;
            tracing::debug!("Writing HTML report to {}", html_path.display());
            if let Err(e) = output::html::generate_html_report(&detailed_results, path_str) {
                tracing::error!("Failed to write HTML report to {}: {}", html_path.display(), e);
                return Err(e);
            }
            tracing::info!("HTML report written to: {}", html_path.display());
        }
        OutputFormat::Both => {
            detailed_results.print_human();

            let base_path = config.output.file.with_extension("");
            let json_path = base_path.with_extension("json");
            let html_path = base_path.with_extension("html");

            let json_str =
                json_path.to_str().ok_or_else(|| anyhow::anyhow!("Invalid JSON path"))?;
            let html_str =
                html_path.to_str().ok_or_else(|| anyhow::anyhow!("Invalid HTML path"))?;

            tracing::debug!(
                "Writing JSON and HTML results to {} and {}",
                json_path.display(),
                html_path.display()
            );
            if let Err(e) = detailed_results.write_json(json_str) {
                tracing::error!("Failed to write JSON results to {}: {}", json_path.display(), e);
                return Err(e);
            }
            if let Err(e) = output::html::generate_html_report(&detailed_results, html_str) {
                tracing::error!("Failed to write HTML report to {}: {}", html_path.display(), e);
                return Err(e);
            }
            tracing::info!(
                "Results written to: {} and {}",
                json_path.display(),
                html_path.display()
            );
        }
    }

    Ok(())
}

/// Create a Redis command selector from a typed RedisConfig
///
/// When weighted commands have per-command key overrides, commands without explicit
/// keys will use the group's main key distribution (from `config.keys`).
fn create_redis_selector(
    config: &xylem_protocols::RedisConfig,
    seed: Option<u64>,
) -> anyhow::Result<Box<dyn xylem_protocols::CommandSelector<xylem_protocols::RedisOp>>> {
    if let Some(ref ops) = config.operations {
        match ops {
            xylem_protocols::RedisOperationsConfig::Fixed { operation } => {
                let op = parse_redis_op(operation, None)?;
                Ok(Box::new(xylem_protocols::FixedCommandSelector::new(op)))
            }
            xylem_protocols::RedisOperationsConfig::Weighted { commands } => {
                let mut commands_weights = Vec::new();
                let mut per_command_keys: Vec<Option<Box<dyn xylem_protocols::KeyGenerator>>> =
                    Vec::new();
                let mut has_any_per_command_keys = false;

                for cmd in commands {
                    let op = parse_redis_op(&cmd.name, cmd.params.as_ref())?;
                    commands_weights.push((op, cmd.weight));

                    // Check if this command has per-command key distribution
                    if let Some(ref keys_config) = cmd.keys {
                        let key_gen = keys_config.to_key_gen(seed)?;
                        per_command_keys.push(Some(
                            Box::new(key_gen) as Box<dyn xylem_protocols::KeyGenerator>
                        ));
                        has_any_per_command_keys = true;
                    } else {
                        per_command_keys.push(None);
                    }
                }

                if has_any_per_command_keys {
                    // Build per-command key generators
                    // Commands without explicit keys config use the group's main key distribution
                    let default_keys = &config.keys;
                    let key_gens: Vec<Box<dyn xylem_protocols::KeyGenerator>> = per_command_keys
                        .into_iter()
                        .map(|opt| match opt {
                            Some(kg) => kg,
                            None => {
                                let kg = default_keys.to_key_gen(seed).unwrap();
                                Box::new(kg) as Box<dyn xylem_protocols::KeyGenerator>
                            }
                        })
                        .collect();

                    Ok(Box::new(xylem_protocols::WeightedCommandSelector::with_per_command_keys(
                        commands_weights,
                        key_gens,
                        seed,
                    )?))
                } else {
                    Ok(Box::new(xylem_protocols::WeightedCommandSelector::with_seed(
                        commands_weights,
                        seed,
                    )?))
                }
            }
        }
    } else {
        // Default to GET
        Ok(Box::new(xylem_protocols::FixedCommandSelector::new(
            xylem_protocols::RedisOp::Get,
        )))
    }
}

/// Parse HTTP method string to HttpMethod enum
fn parse_http_method(method: &str) -> anyhow::Result<xylem_protocols::HttpMethod> {
    match method.to_uppercase().as_str() {
        "GET" => Ok(xylem_protocols::HttpMethod::Get),
        "POST" => Ok(xylem_protocols::HttpMethod::Post),
        "PUT" => Ok(xylem_protocols::HttpMethod::Put),
        other => anyhow::bail!("Unknown HTTP method: {}. Supported: GET, POST, PUT", other),
    }
}

/// Parse Memcached operation string to binary protocol MemcachedOp enum
fn parse_memcached_op_binary(
    operation: &str,
) -> anyhow::Result<xylem_protocols::memcached::MemcachedOp> {
    match operation.to_uppercase().as_str() {
        "GET" => Ok(xylem_protocols::memcached::MemcachedOp::Get),
        "SET" => Ok(xylem_protocols::memcached::MemcachedOp::Set),
        other => anyhow::bail!("Unknown Memcached operation: {}. Supported: GET, SET", other),
    }
}

/// Parse Memcached operation string to ASCII protocol MemcachedOp enum
fn parse_memcached_op_ascii(
    operation: &str,
) -> anyhow::Result<xylem_protocols::memcached::ascii::MemcachedOp> {
    match operation.to_uppercase().as_str() {
        "GET" => Ok(xylem_protocols::memcached::ascii::MemcachedOp::Get),
        "SET" => Ok(xylem_protocols::memcached::ascii::MemcachedOp::Set),
        other => anyhow::bail!("Unknown Memcached operation: {}. Supported: GET, SET", other),
    }
}

/// Parse a Redis operation from string with optional parameters
fn parse_redis_op(
    cmd: &str,
    params: Option<&xylem_protocols::RedisCommandParams>,
) -> anyhow::Result<xylem_protocols::RedisOp> {
    match cmd.to_uppercase().as_str() {
        "GET" => Ok(xylem_protocols::RedisOp::Get),
        "SET" => Ok(xylem_protocols::RedisOp::Set),
        "INCR" => Ok(xylem_protocols::RedisOp::Incr),
        "MGET" => {
            let count = match params {
                Some(xylem_protocols::RedisCommandParams::MGet { count }) => *count,
                _ => 10, // Default to 10 keys if not specified
            };
            Ok(xylem_protocols::RedisOp::MGet { count })
        }
        "WAIT" => {
            let (num_replicas, timeout_ms) = match params {
                Some(xylem_protocols::RedisCommandParams::Wait { num_replicas, timeout_ms }) => {
                    (*num_replicas, *timeout_ms)
                }
                _ => (1, 1000), // Default: 1 replica, 1000ms timeout
            };
            Ok(xylem_protocols::RedisOp::Wait { num_replicas, timeout_ms })
        }
        "SETEX" => Ok(xylem_protocols::RedisOp::SetEx { ttl_seconds: 3600 }),
        "SETRANGE" => Ok(xylem_protocols::RedisOp::SetRange { offset: 0 }),
        "GETRANGE" => Ok(xylem_protocols::RedisOp::GetRange { offset: 0, end: -1 }),
        "SCAN" => {
            let (cursor, count, pattern) = match params {
                Some(xylem_protocols::RedisCommandParams::Scan { cursor, count, pattern }) => {
                    (*cursor, *count, pattern.clone())
                }
                _ => (0, None, None),
            };
            Ok(xylem_protocols::RedisOp::Scan { cursor, count, pattern })
        }
        "MULTI" => Ok(xylem_protocols::RedisOp::Multi),
        "EXEC" => Ok(xylem_protocols::RedisOp::Exec),
        "DISCARD" => Ok(xylem_protocols::RedisOp::Discard),
        "CUSTOM" => {
            // Custom command requires a template in params
            let template_str = match params {
                Some(xylem_protocols::RedisCommandParams::Custom { template }) => template.clone(),
                _ => {
                    anyhow::bail!(
                        "Custom command requires params.template. Example: \
                         {{ name = \"custom\", params = {{ template = \"HSET myhash __key__ __data__\" }} }}"
                    )
                }
            };
            let template = xylem_protocols::CommandTemplate::parse(&template_str)?;
            Ok(xylem_protocols::RedisOp::Custom(template))
        }
        _ => anyhow::bail!(
            "Unknown Redis command: {}. Supported: GET, SET, INCR, MGET, WAIT, \
             SETEX, SETRANGE, GETRANGE, SCAN, MULTI, EXEC, DISCARD, CUSTOM",
            cmd
        ),
    }
}

/// Parse Masstree operation from config
fn parse_masstree_op(
    config: &xylem_protocols::MasstreeConfig,
) -> anyhow::Result<xylem_protocols::MasstreeOp> {
    match config.operation.to_lowercase().as_str() {
        "get" => Ok(xylem_protocols::MasstreeOp::Get),
        "set" => Ok(xylem_protocols::MasstreeOp::Set),
        "put" => {
            // For PUT, create column updates. Default to single column (0) with empty placeholder.
            // The actual value will be filled in during request generation.
            let num_columns = config.put_columns.unwrap_or(1);
            let columns: Vec<(u32, Vec<u8>)> =
                (0..num_columns as u32).map(|i| (i, Vec::new())).collect();
            Ok(xylem_protocols::MasstreeOp::Put { columns })
        }
        "remove" => Ok(xylem_protocols::MasstreeOp::Remove),
        "scan" => {
            let scan_config = config.scan.as_ref();
            let count = scan_config.map(|s| s.count as u32).unwrap_or(10);
            // Convert string field names to u32 indices (assume numeric field names or use index)
            let fields: Vec<u32> = scan_config
                .map(|s| s.fields.iter().filter_map(|f| f.parse().ok()).collect())
                .unwrap_or_default();
            Ok(xylem_protocols::MasstreeOp::Scan {
                firstkey: String::new(), // Will be set during request generation
                count,
                fields,
            })
        }
        "checkpoint" => Ok(xylem_protocols::MasstreeOp::Checkpoint),
        other => anyhow::bail!(
            "Unknown Masstree operation: {}. Supported: get, set, put, remove, scan, checkpoint",
            other
        ),
    }
}
