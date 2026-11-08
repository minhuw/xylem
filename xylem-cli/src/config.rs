//! Configuration file support for Xylem
//!
//! This module provides TOML-based configuration for reproducible load testing experiments.
//! Configuration files are the primary interface for Xylem, with CLI arguments available
//! for quick overrides.

use crate::thresholds::{Threshold, ValidityConfig};
use anyhow::{anyhow, bail, Context, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use xylem_core::traffic_group::{PolicyConfig, TimestampSourceConfig};

mod keys;
pub mod ntcp;
pub use ntcp::{NtcpBackend, NtcpConfig, NtcpEndpoint};

/// Top-level profile configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProfileConfig {
    pub experiment: ExperimentConfig,
    /// Traffic groups configuration
    pub traffic_groups: Vec<xylem_core::traffic_group::TrafficGroupConfig>,
    pub output: OutputConfig,
    /// Optional request dump configuration for recording all requests
    #[serde(default)]
    pub request_dump: Option<xylem_core::request_dump::DumpConfig>,
    /// Statistics collection configuration
    #[serde(default)]
    pub stats: StatsConfig,
    /// User-space TCP settings, required when a group uses transport "ntcp"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ntcp: Option<NtcpConfig>,
    /// Pass/fail limits on the results: a metric, optionally filtered to a
    /// traffic group, and its expressions, as in
    /// `"latency{group=api}" = ["p99 < 250us"]`
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub thresholds: BTreeMap<String, Vec<String>>,
    /// Checks that the run applied its configured load
    #[serde(default)]
    pub validity: ValidityConfig,
    /// Typed protocol_config of each traffic group, in group order.
    /// Empty until `validate` succeeds; not part of the profile format.
    #[serde(skip)]
    pub protocol_configs: Vec<ProtocolConfig>,
    /// The parsed `thresholds`, in key order. Empty until `validate`
    /// succeeds; not part of the profile format.
    #[serde(skip)]
    pub threshold_checks: Vec<Threshold>,
}

/// Experiment metadata
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExperimentConfig {
    /// Experiment name
    pub name: String,
    /// Optional description
    #[serde(default)]
    pub description: Option<String>,
    /// Random seed for reproducibility (None = use entropy)
    #[serde(default)]
    pub seed: Option<u64>,
    /// Experiment duration
    #[serde(with = "humantime_serde")]
    #[schemars(with = "String")]
    pub duration: Duration,
    /// Longest wait for pending responses after the measurement window
    /// (default 1s). Requests still pending then count as timed out.
    #[serde(with = "humantime_serde", default = "default_drain_timeout")]
    #[schemars(with = "String")]
    pub drain_timeout: Duration,
}

fn default_drain_timeout() -> Duration {
    xylem_core::threading::DEFAULT_DRAIN_TIMEOUT
}

/// Re-export KeysConfig from xylem-protocols for use in config files
pub use xylem_protocols::KeysConfig;

/// Output configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OutputConfig {
    /// Output format: json, html, both, or human
    #[serde(default = "default_output_format")]
    pub format: OutputFormat,
    /// Output file path (for json/html, extensions will be added automatically)
    pub file: PathBuf,
    /// Reserved for real-time updates; must be false (not supported by the CLI).
    #[serde(default)]
    pub real_time: bool,
    /// HTML chart configuration
    #[serde(default)]
    pub html: HtmlOutputConfig,
}

/// Output format options
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// JSON format with per-group breakdown.
    /// Use `[stats] include_records = true` to include per-second per-connection records.
    #[default]
    Json,
    /// HTML report with visualizations
    Html,
    /// Both JSON and HTML
    Both,
    /// Human-readable console output only (no file)
    Human,
}

fn default_output_format() -> OutputFormat {
    OutputFormat::Json
}

/// HTML output configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HtmlOutputConfig {
    /// Chart library; only "chartjs" is currently supported.
    #[serde(default = "default_chart_library")]
    pub library: String,
    /// Theme; only "light" is currently supported.
    #[serde(default = "default_theme")]
    pub theme: String,
}

impl Default for HtmlOutputConfig {
    fn default() -> Self {
        Self {
            library: default_chart_library(),
            theme: default_theme(),
        }
    }
}

fn default_chart_library() -> String {
    "chartjs".to_string()
}

fn default_theme() -> String {
    "light".to_string()
}

/// Statistics collection configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StatsConfig {
    /// Time bucket duration for per-second per-connection records
    #[serde(default = "default_bucket_duration", with = "humantime_serde")]
    #[schemars(with = "String")]
    pub bucket_duration: Duration,
    /// Include per-second per-connection records in JSON output
    #[serde(default)]
    pub include_records: bool,
    /// Global latency sampling policy for the aggregated collector.
    /// If unset, the first traffic group's policy is used (current behavior).
    /// Set to "none" to disable global latency recording.
    #[serde(default)]
    pub sampling_policy: Option<xylem_core::stats::SamplingPolicy>,
}

impl Default for StatsConfig {
    fn default() -> Self {
        Self {
            bucket_duration: default_bucket_duration(),
            include_records: false,
            sampling_policy: None,
        }
    }
}

fn default_bucket_duration() -> Duration {
    Duration::from_secs(1)
}

/// Protocols a traffic group can name
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProtocolKind {
    Redis,
    MemcachedBinary,
    MemcachedAscii,
    Http,
    Masstree,
    XylemEcho,
}

/// Every accepted traffic group protocol name. The invalid-protocol error lists
/// the names in this order.
const PROTOCOLS: [(&str, ProtocolKind); 6] = [
    ("redis", ProtocolKind::Redis),
    ("memcached-binary", ProtocolKind::MemcachedBinary),
    ("memcached-ascii", ProtocolKind::MemcachedAscii),
    ("http", ProtocolKind::Http),
    ("masstree", ProtocolKind::Masstree),
    ("xylem-echo", ProtocolKind::XylemEcho), // Test protocol
];

/// A traffic group's protocol_config parsed as the typed config of its protocol.
/// `ProfileConfig::validate` parses each group once and workers build protocols from it.
#[derive(Debug, Clone)]
pub enum ProtocolConfig {
    Redis(xylem_protocols::RedisConfig),
    MemcachedBinary(xylem_protocols::MemcachedConfig),
    MemcachedAscii(xylem_protocols::MemcachedConfig),
    Http(xylem_protocols::HttpConfig),
    Masstree(xylem_protocols::MasstreeConfig),
    XylemEcho(xylem_protocols::XylemEchoConfig),
}

impl ProfileConfig {
    /// Load profile from TOML file. Every unknown or removed key is reported
    /// at once; they stop loading before any other check.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        let context = || format!("Failed to parse config file: {}", path.display());

        let value: toml::Value = toml::from_str(&content).with_context(context)?;
        check_keys(&value).with_context(context)?;
        // Parse the text again, not the value, so that errors point at a line
        let config: ProfileConfig = toml::from_str(&content).with_context(context)?;

        // Validate explicitly or use from_file_with_overrides() to validate after overrides.
        Ok(config)
    }

    /// Load profile from TOML file with --set style overrides
    pub fn from_file_with_overrides<P: AsRef<Path>>(path: P, overrides: &[String]) -> Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        // Parse TOML to Value for manipulation
        let mut value: toml::Value = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        // Apply each override
        for override_str in overrides {
            let (key, val) = parse_key_value(override_str)
                .with_context(|| format!("Invalid override format: {}", override_str))?;

            set_toml_path(&mut value, &key, &val)
                .with_context(|| format!("Invalid --set {}", override_str))?;
        }
        check_keys(&value).with_context(|| {
            format!("Failed to parse config file: {} (with --set overrides)", path.display())
        })?;

        // Deserialize modified TOML to ProfileConfig
        let mut config: ProfileConfig = value
            .try_into()
            .with_context(|| "Failed to deserialize modified configuration")?;

        // Validate the final configuration
        config.validate()?;
        Ok(config)
    }

    /// Validate configuration and parse each group's protocol_config into `protocol_configs`
    ///
    /// Every independent problem is reported at once, one per line. Checks
    /// that a problem makes meaningless are skipped: an invalid transport
    /// skips the group's timestamps and mixed-transport and mixed-timestamps
    /// checks, an unknown protocol skips
    /// the group's UDP and protocol_config checks, unknown keys in a
    /// protocol_config skip parsing it, a protocol_config that fails to parse
    /// skips its UDP size check, and the thread assignment is only checked
    /// when nothing else failed, since it can only repeat the group errors.
    pub fn validate(&mut self) -> Result<()> {
        let mut errors = Vec::new();
        if self.output.real_time {
            errors.push(anyhow!("output.real_time is not supported by the CLI; use false"));
        }
        if self.output.html.library != "chartjs" {
            errors.push(anyhow!("output.html.library only supports 'chartjs'"));
        }
        if self.output.html.theme != "light" {
            errors.push(anyhow!("output.html.theme only supports 'light'"));
        }

        // Experiment validation
        if self.experiment.name.is_empty() {
            errors.push(anyhow!("Experiment name cannot be empty"));
        }
        if self.experiment.duration.as_secs() == 0 {
            errors.push(anyhow!("Experiment duration must be > 0"));
        }

        let valid_transports: &[&str] = if ntcp::SUPPORTED {
            &["tcp", "udp", "unix", ntcp::TRANSPORT]
        } else {
            &["tcp", "udp", "unix"]
        };

        // Traffic groups validation
        if self.traffic_groups.is_empty() {
            errors.push(anyhow!("At least one traffic group must be defined"));
        }

        // Track transport per thread for validation
        let mut thread_transports: std::collections::HashMap<usize, (&str, usize, &str)> =
            std::collections::HashMap::new();
        // And the timestamp source, which a thread's connection group shares
        let mut thread_timestamps: std::collections::HashMap<
            usize,
            (&TimestampSourceConfig, usize, &str),
        > = std::collections::HashMap::new();

        let mut protocol_configs = Vec::with_capacity(self.traffic_groups.len());

        for (i, group) in self.traffic_groups.iter().enumerate() {
            // Validate target address
            if group.target.is_empty() {
                errors.push(anyhow!(
                    "Traffic group {} '{}' must have a target address",
                    i,
                    group.name
                ));
            }

            // Validate transport
            if let Err(error) = ntcp::check_supported(group, i) {
                errors.push(error);
            }
            let valid_transport = valid_transports.contains(&group.transport.as_str());
            if !valid_transport {
                errors.push(anyhow!(
                    "Invalid transport '{}' for traffic group {} '{}'. Valid options: {}",
                    group.transport,
                    i,
                    group.name,
                    valid_transports.join(", ")
                ));
            }

            // Validate protocol
            let protocol = PROTOCOLS.iter().find(|(name, _)| *name == group.protocol);
            if protocol.is_none() {
                let names: Vec<&str> = PROTOCOLS.iter().map(|(name, _)| *name).collect();
                let mut error = format!(
                    "Invalid protocol '{}' for traffic group {} '{}'. Valid options: {}",
                    group.protocol,
                    i,
                    group.name,
                    names.join(", ")
                );
                if let Some(message) = keys::removed_protocol(&group.protocol) {
                    error = format!("{error}. {message}");
                }
                errors.push(anyhow!(error));
            }

            if valid_transport {
                validate_timestamps(group, i, &mut errors);
            }

            if group.threads.is_empty() {
                errors.push(anyhow!(
                    "Traffic group {} '{}' must have at least one thread",
                    i,
                    group.name
                ));
            }
            if group.connections_per_thread == 0 {
                errors.push(anyhow!(
                    "Traffic group {} '{}' connections_per_thread must be > 0",
                    i,
                    group.name
                ));
            }
            if group.max_pending_per_connection == 0 {
                errors.push(anyhow!(
                    "Traffic group {} '{}' max_pending_per_connection must be > 0",
                    i,
                    group.name
                ));
            }

            // Rate-based policies need a positive rate. Pattern policies accept
            // zero, which pauses sending by design.
            let rates: &[(f64, &str)] = match &group.traffic_policy {
                PolicyConfig::FixedRate { rate } | PolicyConfig::Poisson { rate } => {
                    &[(*rate, "rate")]
                }
                PolicyConfig::Adaptive { initial_rate, min_rate, max_rate, .. } => &[
                    (*initial_rate, "initial_rate"),
                    (*min_rate, "min_rate"),
                    (*max_rate, "max_rate"),
                ],
                PolicyConfig::ClosedLoop
                | PolicyConfig::Sinusoidal { .. }
                | PolicyConfig::Ramp { .. }
                | PolicyConfig::Spike { .. }
                | PolicyConfig::Sawtooth { .. } => &[],
            };
            for &(rate, field) in rates {
                if let Err(error) = validate_positive_rate(rate, field, i, &group.name) {
                    errors.push(error);
                }
            }

            // Validate all groups on the same thread use the same transport
            let threads = if valid_transport {
                &group.threads[..]
            } else {
                &[]
            };
            for &thread_id in threads {
                match thread_transports.get(&thread_id) {
                    Some((existing_transport, existing_group_idx, existing_group_name))
                        if *existing_transport != group.transport.as_str() =>
                    {
                        errors.push(anyhow!(
                            "Thread {} has mixed transports: group {} '{}' uses '{}' but group {} '{}' uses '{}'. \
                             All traffic groups on the same thread must use the same transport.",
                            thread_id,
                            existing_group_idx,
                            existing_group_name,
                            existing_transport,
                            i,
                            group.name,
                            group.transport
                        ));
                    }
                    None => {
                        thread_transports
                            .insert(thread_id, (group.transport.as_str(), i, group.name.as_str()));
                    }
                    _ => {} // Same transport, nothing to do
                }
                match thread_timestamps.get(&thread_id) {
                    Some((existing, existing_group_idx, existing_group_name))
                        if **existing != group.timestamps =>
                    {
                        errors.push(anyhow!(
                            "Thread {} has mixed timestamp sources: group {} '{}' uses {} but \
                             group {} '{}' uses {}. All traffic groups on the same thread share \
                             their sockets' timestamping and must use the same timestamps.",
                            thread_id,
                            existing_group_idx,
                            existing_group_name,
                            existing,
                            i,
                            group.name,
                            group.timestamps
                        ));
                    }
                    None => {
                        thread_timestamps.insert(thread_id, (&group.timestamps, i, &group.name));
                    }
                    _ => {}
                }
            }

            // The remaining checks need the group's protocol
            let Some(&(_, protocol)) = protocol else {
                continue;
            };
            validate_udp(group, protocol, i, &mut errors);

            // Key-value protocols (Redis, Memcached) require protocol_config with keys
            // HTTP and XylemEcho have sensible defaults and don't require protocol_config
            let requires_protocol_config = matches!(
                protocol,
                ProtocolKind::Redis | ProtocolKind::MemcachedBinary | ProtocolKind::MemcachedAscii
            );

            if requires_protocol_config && group.protocol_config.is_none() {
                errors.push(anyhow!(
                    "Traffic group {} '{}' has no protocol_config. \
                     For {} protocol, set traffic_groups[{}].protocol_config with keys configuration.",
                    i,
                    group.name,
                    group.protocol,
                    i
                ));
                continue;
            }

            // Reject unknown and removed keys, every one of them, before parsing
            if let Some(pc) = &group.protocol_config {
                let problems = keys::check_protocol_config(protocol, pc, i, &group.name);
                if !problems.is_empty() {
                    errors.extend(problems.into_iter().map(anyhow::Error::msg));
                    continue;
                }
            }

            // Parse protocol_config once as the appropriate typed struct.
            // This validates the structure, provides better error messages,
            // and gives workers the config they build protocols from.
            let protocol_config = match parse_protocol_config(
                protocol,
                group.protocol_config.as_ref(),
                i,
                &group.name,
            ) {
                Ok(protocol_config) => protocol_config,
                Err(error) => {
                    errors.push(error);
                    continue;
                }
            };
            if group.transport == "udp" {
                if let Err(error) = validate_udp_request_size(&protocol_config, i, &group.name) {
                    errors.push(error);
                }
            }
            protocol_configs.push(protocol_config);
        }

        // Validate thread assignment
        if errors.is_empty() {
            let assignment = xylem_core::traffic_group::ThreadGroupAssignment::from_configs(
                &self.traffic_groups,
            );
            if let Err(error) = assignment.validate() {
                errors.push(error.into());
            }
        }

        if let Err(error) = ntcp::validate(self.ntcp.as_ref(), &self.traffic_groups) {
            errors.push(error);
        }

        // Output validation - format is now an enum, so always valid

        self.validity.validate(&mut errors);
        let threshold_checks =
            match crate::thresholds::parse_thresholds(&self.thresholds, &self.traffic_groups) {
                Ok(threshold_checks) => threshold_checks,
                Err(error) => {
                    errors.push(error);
                    Vec::new()
                }
            };

        combine(errors)?;
        self.protocol_configs = protocol_configs;
        self.threshold_checks = threshold_checks;
        Ok(())
    }
}

/// Reject the unknown and removed keys of a whole profile, all of them at once
fn check_keys(profile: &toml::Value) -> Result<()> {
    let profile = serde_json::to_value(profile).context("Failed to read the profile's keys")?;
    combine(keys::check_profile(&profile).into_iter().map(anyhow::Error::msg).collect())
}

/// Fail with every error in `errors`: a single error as it is, several as
/// a list with one error per line, and the further lines of an error that
/// has several (such as the invalid thresholds) indented below it
fn combine(mut errors: Vec<anyhow::Error>) -> Result<()> {
    match errors.len() {
        0 => Ok(()),
        1 => Err(errors.remove(0)),
        count => {
            let lines: Vec<String> = errors
                .iter()
                .map(|error| format!("  - {}", format!("{error:#}").replace('\n', "\n    ")))
                .collect();
            bail!("The profile has {} errors:\n{}", count, lines.join("\n"))
        }
    }
}

/// Protocols that work over transport "udp": each frames its requests and
/// responses as datagrams and matches responses by a request ID
const UDP_PROTOCOLS: [ProtocolKind; 3] = [
    ProtocolKind::MemcachedAscii,
    ProtocolKind::MemcachedBinary,
    ProtocolKind::XylemEcho,
];

/// Check a group's transport "udp" and request_timeout settings
fn validate_udp(
    group: &xylem_core::traffic_group::TrafficGroupConfig,
    protocol: ProtocolKind,
    index: usize,
    errors: &mut Vec<anyhow::Error>,
) {
    if group.transport == "udp" && !UDP_PROTOCOLS.contains(&protocol) {
        let names: Vec<&str> = PROTOCOLS
            .iter()
            .filter(|(_, kind)| UDP_PROTOCOLS.contains(kind))
            .map(|(name, _)| *name)
            .collect();
        errors.push(anyhow!(
            "Traffic group {} '{}': protocol '{}' does not support transport 'udp'. \
             Protocols that support UDP: {}",
            index,
            group.name,
            group.protocol,
            names.join(", ")
        ));
    }
    if let Some(timeout) = group.request_timeout {
        if group.transport != "udp" {
            errors.push(anyhow!(
                "Traffic group {} '{}' sets request_timeout, which only transport 'udp' \
                 supports; '{}' retransmits lost data and never times out requests",
                index,
                group.name,
                group.transport
            ));
        } else if timeout.is_zero() {
            errors.push(anyhow!(
                "Traffic group {} '{}' request_timeout must be > 0",
                index,
                group.name
            ));
        }
    }
}

/// Transports that can take kernel or NIC timestamps
const TIMESTAMPING_TRANSPORTS: [&str; 2] = ["tcp", "udp"];

/// Longest interface name the kernel accepts (IFNAMSIZ less the NUL)
const MAX_INTERFACE_NAME: usize = 15;

/// Check a group's timestamps setting: "nic" needs transport "ntcp" (whose
/// backend `ntcp::validate` checks), anything else but "user" needs Linux and
/// transport "tcp" or "udp". Whether a hardware interface has timestamping
/// switched on is checked when the run starts, not here.
fn validate_timestamps(
    group: &xylem_core::traffic_group::TrafficGroupConfig,
    index: usize,
    errors: &mut Vec<anyhow::Error>,
) {
    if group.timestamps.is_user() {
        return;
    }
    if group.timestamps == TimestampSourceConfig::Nic {
        if group.transport != ntcp::TRANSPORT {
            errors.push(anyhow!(
                "Traffic group {} '{}' sets timestamps = \"nic\", which only transport 'ntcp' \
                 with backend \"dpdk\" supports; transport '{}' takes {{ hardware = \
                 \"INTERFACE\" }} for NIC timestamps",
                index,
                group.name,
                group.transport
            ));
        }
        return;
    }
    if !TIMESTAMPING_TRANSPORTS.contains(&group.transport.as_str()) {
        let ntcp_hint = if group.transport == ntcp::TRANSPORT {
            " (or \"nic\" with ntcp.backend = \"dpdk\")"
        } else {
            ""
        };
        errors.push(anyhow!(
            "Traffic group {} '{}' sets timestamps = {}, which only transports 'tcp' and 'udp' \
             support; '{}' has no kernel timestamps. Remove timestamps or set it to \
             \"user\"{}.",
            index,
            group.name,
            group.timestamps,
            group.transport,
            ntcp_hint
        ));
        return;
    }
    if !cfg!(target_os = "linux") {
        errors.push(anyhow!(
            "Traffic group {} '{}' sets timestamps = {}, which needs Linux (SO_TIMESTAMPING)",
            index,
            group.name,
            group.timestamps
        ));
        return;
    }
    if let TimestampSourceConfig::Hardware(interface) = &group.timestamps {
        if interface.is_empty()
            || interface.len() > MAX_INTERFACE_NAME
            || interface.contains(['\0', '/'])
            || interface.chars().any(char::is_whitespace)
        {
            errors.push(anyhow!(
                "Traffic group {} '{}' timestamps.hardware {:?} is not a network interface name \
                 (1 to {} bytes, no '/', NUL or whitespace)",
                index,
                group.name,
                interface,
                MAX_INTERFACE_NAME
            ));
        }
    }
}

/// Over UDP a memcached request must fit one datagram, which bounds its value size
fn validate_udp_request_size(config: &ProtocolConfig, index: usize, name: &str) -> Result<()> {
    let (ProtocolConfig::MemcachedAscii(config) | ProtocolConfig::MemcachedBinary(config)) = config
    else {
        return Ok(());
    };
    let max = xylem_protocols::memcached::MAX_UDP_VALUE_SIZE;
    let insert_size = config.insert_phase.as_ref().and_then(|insert| insert.value_size);
    for size in [Some(config.keys.value_size()), insert_size].into_iter().flatten() {
        if size > max {
            bail!(
                "Traffic group {} '{}': value size {} does not fit one memcached UDP \
                 datagram; over UDP the largest value size is {}",
                index,
                name,
                size,
                max
            );
        }
    }
    Ok(())
}

/// Reject a traffic policy rate that is not > 0 (NaN fails too)
fn validate_positive_rate(
    rate: f64,
    field: &str,
    group_idx: usize,
    group_name: &str,
) -> Result<()> {
    if rate > 0.0 {
        return Ok(());
    }
    bail!(
        "Traffic group {} '{}' traffic_policy.{} must be > 0",
        group_idx,
        group_name,
        field
    );
}

/// Parse protocol_config as the typed struct of the group's protocol
fn parse_protocol_config(
    protocol: ProtocolKind,
    pc: Option<&serde_json::Value>,
    group_idx: usize,
    group_name: &str,
) -> Result<ProtocolConfig> {
    match protocol {
        ProtocolKind::Redis => {
            let config: xylem_protocols::RedisConfig =
                parse_typed_config(pc, "Redis", group_idx, group_name)
                    .map_err(|e| anyhow::anyhow!(redact_auth(pc, &e.to_string())))?;
            // Check auth, db and resp now, including that password_env is set
            config.connection_setup().with_context(|| {
                format!(
                    "Traffic group {} '{}' has invalid Redis protocol_config",
                    group_idx, group_name
                )
            })?;
            Ok(ProtocolConfig::Redis(config))
        }
        ProtocolKind::MemcachedBinary => {
            let config = parse_typed_config(pc, "Memcached", group_idx, group_name)?;
            Ok(ProtocolConfig::MemcachedBinary(config))
        }
        ProtocolKind::MemcachedAscii => {
            let config = parse_typed_config(pc, "Memcached", group_idx, group_name)?;
            Ok(ProtocolConfig::MemcachedAscii(config))
        }
        ProtocolKind::Http => {
            let config = parse_typed_config(pc, "HTTP", group_idx, group_name)?;
            Ok(ProtocolConfig::Http(config))
        }
        ProtocolKind::Masstree => {
            let config = parse_typed_config(pc, "Masstree", group_idx, group_name)?;
            Ok(ProtocolConfig::Masstree(config))
        }
        ProtocolKind::XylemEcho => {
            let config = parse_typed_config(pc, "XylemEcho", group_idx, group_name)?;
            Ok(ProtocolConfig::XylemEcho(config))
        }
    }
}

/// The secret values under a protocol_config's `auth`: every string in it,
/// except a username
fn auth_secrets(pc: Option<&serde_json::Value>) -> Vec<String> {
    fn collect(value: &serde_json::Value, secrets: &mut Vec<String>) {
        match value {
            serde_json::Value::String(text) if !text.is_empty() => secrets.push(text.clone()),
            serde_json::Value::Array(items) => items.iter().for_each(|item| collect(item, secrets)),
            serde_json::Value::Object(fields) => fields
                .iter()
                .filter(|(name, _)| !matches!(name.as_str(), "username" | "password_env"))
                .for_each(|(_, field)| collect(field, secrets)),
            _ => {}
        }
    }
    let mut secrets = Vec::new();
    if let Some(auth) = pc.and_then(|pc| pc.get("auth")) {
        collect(auth, &mut secrets);
    }
    secrets
}

/// `message` without the secret values of `pc`'s `auth`. Parse errors can
/// quote a value, for example when `auth` is a string instead of a table.
fn redact_auth(pc: Option<&serde_json::Value>, message: &str) -> String {
    auth_secrets(pc).iter().fold(message.to_string(), |message, secret| {
        message.replace(secret.as_str(), "<redacted>")
    })
}

/// A copy of a protocol_config that is safe to log: the secret values of its
/// `auth` are replaced
pub fn redacted_protocol_config(pc: &serde_json::Value) -> serde_json::Value {
    let mut pc = pc.clone();
    if let Some(auth) = pc.get_mut("auth") {
        *auth = match auth {
            serde_json::Value::Object(fields) => serde_json::Value::Object(
                fields
                    .iter()
                    .map(|(name, value)| {
                        let value = if matches!(name.as_str(), "username" | "password_env") {
                            value.clone()
                        } else {
                            serde_json::Value::String("<redacted>".to_string())
                        };
                        (name.clone(), value)
                    })
                    .collect(),
            ),
            _ => serde_json::Value::String("<redacted>".to_string()),
        };
    }
    pc
}

/// Parse protocol_config as `T`; an omitted protocol_config uses `T`'s defaults
fn parse_typed_config<T>(
    pc: Option<&serde_json::Value>,
    label: &str,
    group_idx: usize,
    group_name: &str,
) -> Result<T>
where
    T: serde::de::DeserializeOwned + Default,
{
    let Some(pc) = pc else {
        return Ok(T::default());
    };
    serde_json::from_value(pc.clone()).map_err(|e| {
        anyhow::anyhow!(
            "Traffic group {} '{}' has invalid {} protocol_config: {}",
            group_idx,
            group_name,
            label,
            e
        )
    })
}

/// Parse a "key=value" string into (key, value) tuple
fn parse_key_value(override_str: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = override_str.splitn(2, '=').collect();
    if parts.len() != 2 {
        bail!("Invalid override format '{}'. Expected 'key=value'", override_str);
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Set a value in TOML using dot-notation path
fn set_toml_path(root: &mut toml::Value, path: &str, value_str: &str) -> Result<()> {
    let parts = parse_path(path)?;

    if parts.is_empty() {
        bail!("Empty path");
    }
    // Every key must exist in the profile format, so a typo is no silent no-op
    let profile = serde_json::to_value(&*root).context("Failed to read the profile's keys")?;
    keys::check_set_path(&profile, &parts)?;

    // Navigate to the parent of the target field
    let mut current = root;
    for (i, part) in parts.iter().enumerate() {
        let is_last = i == parts.len() - 1;

        match part {
            PathSegment::Key(key) => {
                if is_last {
                    // Set the value
                    let parsed_value = parse_value(value_str)?;
                    if let toml::Value::Table(table) = current {
                        table.insert(key.clone(), parsed_value);
                    } else {
                        bail!("Cannot set key '{}' on non-table value", key);
                    }
                    return Ok(());
                } else {
                    // Navigate deeper
                    let toml::Value::Table(table) = current else {
                        bail!("Cannot navigate through non-table value at key '{}'", key);
                    };

                    if !table.contains_key(key) {
                        // Create intermediate table if it doesn't exist
                        table.insert(key.clone(), toml::Value::Table(Default::default()));
                    }
                    current = table.get_mut(key).unwrap();
                }
            }
            PathSegment::Index(idx) => {
                if let toml::Value::Array(arr) = current {
                    if *idx >= arr.len() {
                        bail!("Array index {} out of bounds (length: {})", idx, arr.len());
                    }
                    if is_last {
                        // Set array element
                        let parsed_value = parse_value(value_str)?;
                        arr[*idx] = parsed_value;
                        return Ok(());
                    } else {
                        current = &mut arr[*idx];
                    }
                } else {
                    bail!("Cannot index non-array value");
                }
            }
            PathSegment::Append => {
                if is_last {
                    if let toml::Value::Array(arr) = current {
                        let parsed_value = parse_value(value_str)?;
                        arr.push(parsed_value);
                        return Ok(());
                    } else {
                        bail!("Cannot append to non-array value");
                    }
                } else {
                    bail!("Append operation '+' can only be at the end of path");
                }
            }
        }
    }

    Ok(())
}

/// Parse a path string into segments (handles "key", "0", "+")
fn parse_path(path: &str) -> Result<Vec<PathSegment>> {
    let mut segments = Vec::new();

    for part in path.split('.') {
        if part.is_empty() {
            continue;
        }

        if part == "+" {
            segments.push(PathSegment::Append);
        } else if let Ok(idx) = part.parse::<usize>() {
            segments.push(PathSegment::Index(idx));
        } else {
            segments.push(PathSegment::Key(part.to_string()));
        }
    }

    Ok(segments)
}

/// Path segment types
enum PathSegment {
    Key(String),
    Index(usize),
    Append,
}

/// Parse a string value with type inference
fn parse_value(value_str: &str) -> Result<toml::Value> {
    let trimmed = value_str.trim();

    // Boolean
    if trimmed == "true" {
        return Ok(toml::Value::Boolean(true));
    }
    if trimmed == "false" {
        return Ok(toml::Value::Boolean(false));
    }

    // Integer (no decimal point, no scientific notation)
    if let Ok(int_val) = trimmed.parse::<i64>() {
        return Ok(toml::Value::Integer(int_val));
    }

    // Float (has decimal point or scientific notation)
    if let Ok(float_val) = trimmed.parse::<f64>() {
        return Ok(toml::Value::Float(float_val));
    }

    // Array (starts with '[' and ends with ']')
    if trimmed.starts_with('[') && trimmed.ends_with(']') {
        // Try to parse as TOML array
        let array_toml = format!("value = {}", trimmed);
        if let Ok(toml::Value::Table(mut table)) = toml::from_str::<toml::Value>(&array_toml) {
            if let Some(value) = table.remove("value") {
                return Ok(value);
            }
        }
        bail!("Failed to parse array: {}", trimmed);
    }

    // Inline table (starts with '{' and ends with '}')
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        // Try to parse as TOML inline table
        let table_toml = format!("value = {}", trimmed);
        if let Ok(toml::Value::Table(mut table)) = toml::from_str::<toml::Value>(&table_toml) {
            if let Some(value) = table.remove("value") {
                return Ok(value);
            }
        }
        bail!("Failed to parse inline table: {}", trimmed);
    }

    // String (everything else, strip quotes if present)
    let string_val = if (trimmed.starts_with('"') && trimmed.ends_with('"'))
        || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
    {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    };

    Ok(toml::Value::String(string_val.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use xylem_core::stats::SamplingPolicy;
    use xylem_protocols::workload::KeyGeneration;

    /// Helper to extract KeysConfig from protocol_config
    fn get_keys_config(
        config: &xylem_core::traffic_group::TrafficGroupConfig,
    ) -> Option<KeysConfig> {
        config
            .protocol_config
            .as_ref()
            .and_then(|pc| pc.get("keys").and_then(|v| serde_json::from_value(v.clone()).ok()))
    }

    #[test]
    fn unsupported_options_are_rejected_after_overrides() {
        let path = "../tests/redis/redis-get-zipfian.toml";
        for (option, expected) in [
            // Parquet streaming was removed; its section gets a migration message.
            ("stats.streaming.path=stats.parquet", "stats.streaming was removed"),
            ("output.real_time=true", "output.real_time"),
            ("output.html.library=plotly", "output.html.library"),
            ("output.html.theme=dark", "output.html.theme"),
        ] {
            let error =
                ProfileConfig::from_file_with_overrides(path, &[option.into()]).expect_err(option);
            assert!(format!("{error:#}").contains(expected), "{option}: {error:#}");
        }
        ProfileConfig::from_file_with_overrides(
            path,
            &[
                "output.real_time=false".into(),
                "output.html.library=chartjs".into(),
                "output.html.theme=light".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn test_load_redis_zipfian_profile() {
        let config = ProfileConfig::from_file("../tests/redis/redis-get-zipfian.toml")
            .expect("Failed to load redis-get-zipfian profile");

        assert_eq!(config.experiment.name, "redis-get-zipfian");
        assert_eq!(config.experiment.seed, Some(42));
        assert_eq!(config.experiment.duration, Duration::from_secs(30));

        assert_eq!(config.traffic_groups.len(), 1);
        assert_eq!(config.traffic_groups[0].name, "main");
        assert_eq!(config.traffic_groups[0].protocol, "redis");
        assert_eq!(config.traffic_groups[0].target, "127.0.0.1:6379");
        assert_eq!(config.traffic_groups[0].transport, "tcp");
        assert_eq!(config.traffic_groups[0].threads, vec![0, 1, 2, 3]);
        assert_eq!(config.traffic_groups[0].connections_per_thread, 25);
    }

    #[test]
    fn test_load_memcached_ramp_profile() {
        let config = ProfileConfig::from_file("../profiles/memcached-ramp.toml")
            .expect("Failed to load memcached-ramp profile");

        assert_eq!(config.experiment.name, "memcached-ramp-load");
        assert_eq!(config.experiment.seed, Some(12345));
        assert_eq!(config.experiment.duration, Duration::from_secs(60));

        assert_eq!(config.traffic_groups.len(), 1);
        assert_eq!(config.traffic_groups[0].protocol, "memcached-binary");
        let threads: Vec<usize> = (0..8).collect();
        assert_eq!(config.traffic_groups[0].threads, threads);
    }

    #[test]
    fn test_load_http_spike_profile() {
        let config = ProfileConfig::from_file("../profiles/http-spike.toml")
            .expect("Failed to load http-spike profile");

        assert_eq!(config.experiment.name, "http-spike-test");
        assert_eq!(config.experiment.seed, Some(99999));
        assert_eq!(config.experiment.duration, Duration::from_secs(120));

        assert_eq!(config.traffic_groups.len(), 1);
        assert_eq!(config.traffic_groups[0].protocol, "http");
        let threads: Vec<usize> = (0..16).collect();
        assert_eq!(config.traffic_groups[0].threads, threads);
    }

    #[test]
    fn test_load_poisson_heterogeneous_profile() {
        let config = ProfileConfig::from_file("../profiles/poisson-heterogeneous.toml")
            .expect("Failed to load poisson-heterogeneous profile");

        assert_eq!(config.experiment.name, "poisson-heterogeneous");
        assert_eq!(config.experiment.seed, Some(7777));

        assert_eq!(config.traffic_groups.len(), 2);
        assert_eq!(config.traffic_groups[0].name, "slow-poisson");
        assert_eq!(config.traffic_groups[1].name, "fast-poisson");
    }

    #[test]
    fn test_load_closed_loop_profile() {
        let config = ProfileConfig::from_file("../profiles/closed-loop-max-throughput.toml")
            .expect("Failed to load closed-loop-max-throughput profile");

        assert_eq!(config.experiment.name, "closed-loop-max-throughput");
        assert_eq!(config.experiment.seed, Some(1234567890));
        assert_eq!(config.experiment.duration, Duration::from_secs(30));

        assert_eq!(config.traffic_groups.len(), 1);
        assert_eq!(config.traffic_groups[0].protocol, "memcached-binary");
        let threads: Vec<usize> = (0..16).collect();
        assert_eq!(config.traffic_groups[0].threads, threads);
    }

    #[test]
    fn test_load_latency_agent_profile() {
        let config = ProfileConfig::from_file("../profiles/latency-agent.toml")
            .expect("Failed to load latency-agent profile");

        assert_eq!(config.experiment.name, "latency-agent-separation");
        assert_eq!(config.experiment.seed, Some(42));
        assert_eq!(config.experiment.duration, Duration::from_secs(10));

        assert_eq!(config.traffic_groups[0].protocol, "redis");

        assert_eq!(config.traffic_groups.len(), 2);

        let latency_agent = &config.traffic_groups[0];
        assert_eq!(latency_agent.name, "latency-agent");
        assert_eq!(latency_agent.threads, vec![0, 1]);
        assert_eq!(latency_agent.connections_per_thread, 10);
        assert_eq!(latency_agent.max_pending_per_connection, 1);
        match &latency_agent.sampling_policy {
            SamplingPolicy::Unlimited => {}
            _ => panic!("Expected Unlimited sampling policy for latency agent"),
        }

        let throughput_agent = &config.traffic_groups[1];
        assert_eq!(throughput_agent.name, "throughput-agent");
        assert_eq!(throughput_agent.threads, vec![2, 3, 4, 5, 6, 7]);
        assert_eq!(throughput_agent.connections_per_thread, 25);
        assert_eq!(throughput_agent.max_pending_per_connection, 32);
        match &throughput_agent.sampling_policy {
            SamplingPolicy::Limited { rate, .. } => {
                assert!((rate - 0.01).abs() < 0.001, "Expected 1% sampling rate");
            }
            _ => panic!("Expected Limited sampling policy for throughput agent"),
        }
    }

    #[test]
    fn test_load_redis_bench_profile() {
        let config = ProfileConfig::from_file("../tests/redis/redis-bench.toml")
            .expect("Failed to load redis-bench profile");

        assert_eq!(config.experiment.name, "redis-bench");
        assert_eq!(config.experiment.seed, Some(42));
        assert_eq!(config.experiment.duration, Duration::from_secs(30));

        assert_eq!(config.traffic_groups.len(), 2);
        assert_eq!(config.traffic_groups[0].protocol, "redis");

        let latency_agent = &config.traffic_groups[0];
        assert_eq!(latency_agent.name, "latency-agent");
        assert_eq!(latency_agent.threads, vec![0]);
        assert_eq!(latency_agent.connections_per_thread, 10);
        assert_eq!(latency_agent.max_pending_per_connection, 1);
        match &latency_agent.sampling_policy {
            SamplingPolicy::Unlimited => {}
            _ => panic!("Expected Unlimited sampling policy for latency agent"),
        }

        let throughput_agent = &config.traffic_groups[1];
        assert_eq!(throughput_agent.name, "throughput-agent");
        assert_eq!(throughput_agent.threads, vec![1, 2, 3]);
        assert_eq!(throughput_agent.connections_per_thread, 25);
        assert_eq!(throughput_agent.max_pending_per_connection, 32);
        match &throughput_agent.sampling_policy {
            SamplingPolicy::Limited { rate, .. } => {
                assert!((rate - 0.01).abs() < 0.001, "Expected 1% sampling rate");
            }
            _ => panic!("Expected Limited sampling policy for throughput agent"),
        }
    }

    #[test]
    fn test_config_with_overrides() {
        let config = ProfileConfig::from_file_with_overrides(
            "../tests/redis/redis-get-zipfian.toml",
            &[
                "traffic_groups.0.target=192.168.1.100:6379".to_string(),
                "experiment.duration=60s".to_string(),
                "experiment.seed=999".to_string(),
            ],
        )
        .expect("Failed to apply overrides");

        assert_eq!(config.traffic_groups[0].target, "192.168.1.100:6379");
        assert_eq!(config.experiment.duration, Duration::from_secs(60));
        assert_eq!(config.experiment.seed, Some(999));

        assert_eq!(config.experiment.name, "redis-get-zipfian");
        assert_eq!(config.traffic_groups[0].protocol, "redis");
    }

    #[test]
    fn test_config_with_cli_target_override() {
        let profile = r#"
[experiment]
name = "cli-target-test"
duration = "10s"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1

[traffic_groups.protocol_config.keys]
strategy = "sequential"
start = 0
value_size = 64

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "results/test.json"
"#;

        use std::io::Write;
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        tmpfile.write_all(profile.as_bytes()).unwrap();
        tmpfile.flush().unwrap();

        let config = ProfileConfig::from_file(tmpfile.path()).unwrap();
        assert_eq!(config.traffic_groups[0].target, "127.0.0.1:6379");

        let config = ProfileConfig::from_file_with_overrides(
            tmpfile.path(),
            &["traffic_groups.0.target=192.168.1.100:6379".to_string()],
        )
        .expect("Failed to apply target override");

        assert_eq!(config.traffic_groups[0].target, "192.168.1.100:6379");
    }

    /// A one-group profile with `group` (protocol, transport, extra lines)
    fn udp_profile(protocol: &str, transport: &str, extra: &str) -> Result<ProfileConfig> {
        // xylem-echo and http take no keys
        let keys = match protocol {
            "xylem-echo" | "http" => "",
            _ => {
                "[traffic_groups.protocol_config.keys]\nstrategy = \"sequential\"\nvalue_size = 64"
            }
        };
        let profile = format!(
            r#"
[experiment]
name = "udp"
duration = "1s"

[[traffic_groups]]
name = "main"
protocol = "{protocol}"
target = "127.0.0.1:11211"
transport = "{transport}"
threads = [0]
connections_per_thread = 1
{extra}

{keys}

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "results/test.json"
"#
        );
        let mut config: ProfileConfig = toml::from_str(&profile)?;
        config.validate()?;
        Ok(config)
    }

    #[test]
    fn udp_is_accepted_only_for_protocols_that_support_it() {
        for protocol in ["memcached-ascii", "memcached-binary", "xylem-echo"] {
            let config = udp_profile(protocol, "udp", "").unwrap();
            let group = &config.traffic_groups[0];
            assert_eq!(group.request_timeout, None);
            assert_eq!(group.effective_request_timeout(), Some(Duration::from_secs(1)));
        }
        for protocol in ["redis", "http", "masstree"] {
            let error = udp_profile(protocol, "udp", "").unwrap_err().to_string();
            assert!(
                error.contains(&format!("protocol '{protocol}' does not support transport 'udp'"))
            );
            assert!(error.contains("memcached-binary, memcached-ascii, xylem-echo"), "{error}");
        }
        // Stream transports have no request timeout
        let config = udp_profile("memcached-ascii", "tcp", "").unwrap();
        assert_eq!(config.traffic_groups[0].effective_request_timeout(), None);
    }

    #[test]
    fn request_timeout_is_parsed_and_checked() {
        let config = udp_profile("xylem-echo", "udp", r#"request_timeout = "250ms""#).unwrap();
        let group = &config.traffic_groups[0];
        assert_eq!(group.effective_request_timeout(), Some(Duration::from_millis(250)));
        let error = udp_profile("xylem-echo", "tcp", r#"request_timeout = "250ms""#).unwrap_err();
        assert!(error.to_string().contains("only transport 'udp'"), "{error}");
        let error = udp_profile("xylem-echo", "udp", r#"request_timeout = "0s""#).unwrap_err();
        assert!(error.to_string().contains("request_timeout must be > 0"), "{error}");
    }

    const KERNEL: &str = r#"timestamps = "kernel""#;
    const HARDWARE: &str = r#"timestamps = { hardware = "data0" }"#;

    #[test]
    fn timestamps_parse_for_tcp_and_udp_and_default_to_user() {
        use TimestampSourceConfig::{Hardware, Kernel, User};
        for transport in ["tcp", "udp"] {
            let config = udp_profile("xylem-echo", transport, "").unwrap();
            assert_eq!(config.traffic_groups[0].timestamps, User);
            for (line, expected) in [
                (r#"timestamps = "user""#, User),
                (KERNEL, Kernel),
                (HARDWARE, Hardware("data0".to_string())),
            ] {
                // Validation does not probe the interface: that is the run's job
                let result = udp_profile("xylem-echo", transport, line);
                if cfg!(target_os = "linux") || expected.is_user() {
                    assert_eq!(result.unwrap().traffic_groups[0].timestamps, expected);
                } else {
                    let error = result.unwrap_err().to_string();
                    assert!(error.contains("needs Linux"), "{error}");
                }
            }
        }
        for line in [
            r#"timestamps = "nic""#,
            r#"timestamps = "hardware""#,
            r#"timestamps = { hardware = 1 }"#,
            r#"timestamps = { software = "lo" }"#,
            "timestamps = true",
        ] {
            let error = udp_profile("xylem-echo", "tcp", line).unwrap_err();
            assert!(format!("{error:#}").contains("timestamps"), "{line}: {error:#}");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn timestamps_round_trip_and_the_default_is_not_written() {
        let config = udp_profile("xylem-echo", "tcp", "").unwrap();
        let value = toml::Value::try_from(&config).unwrap();
        assert!(value["traffic_groups"][0].get("timestamps").is_none());
        for line in [KERNEL, HARDWARE] {
            let config = udp_profile("xylem-echo", "udp", line).unwrap();
            let value = toml::Value::try_from(&config).unwrap();
            let parsed: ProfileConfig = value.try_into().unwrap();
            assert_eq!(parsed.traffic_groups[0].timestamps, config.traffic_groups[0].timestamps);
        }
        assert_eq!(
            config.traffic_groups[0].timestamps.to_string(),
            r#""user""#,
            "written as in a profile"
        );
        let hardware = udp_profile("xylem-echo", "tcp", HARDWARE).unwrap();
        assert_eq!(hardware.traffic_groups[0].timestamps.to_string(), r#"{ hardware = "data0" }"#);
    }

    #[test]
    fn timestamps_are_rejected_for_unix_and_ntcp() {
        for line in [KERNEL, HARDWARE] {
            let error = udp_profile("xylem-echo", "unix", line).unwrap_err().to_string();
            assert!(error.contains("only transports 'tcp' and 'udp' support"), "{error}");
            assert!(error.contains("'unix' has no kernel timestamps"), "{error}");
            // ntcp is a transport only in builds with its feature
            let error = udp_profile("xylem-echo", "ntcp", line).unwrap_err().to_string();
            if ntcp::SUPPORTED {
                assert!(error.contains("'ntcp' has no kernel timestamps"), "{error}");
                assert!(error.contains("\"nic\" with ntcp.backend = \"dpdk\""), "{error}");
            }
        }
        udp_profile("xylem-echo", "unix", r#"timestamps = "user""#).unwrap();
    }

    #[test]
    fn nic_timestamps_are_only_for_ntcp() {
        for transport in ["tcp", "udp", "unix"] {
            let error = udp_profile("xylem-echo", transport, r#"timestamps = "nic""#)
                .unwrap_err()
                .to_string();
            assert!(error.contains("only transport 'ntcp' with backend \"dpdk\""), "{error}");
        }
    }

    #[test]
    fn timestamps_errors_are_reported_with_the_other_errors() {
        let extra = format!("{KERNEL}\nmax_pending_per_connection = 0");
        let error = udp_profile("xylem-echo", "unix", &extra).unwrap_err().to_string();
        assert!(error.starts_with("The profile has 2 errors:\n"), "{error}");
        assert!(error.contains("'unix' has no kernel timestamps"), "{error}");
        assert!(error.contains("max_pending_per_connection must be > 0"), "{error}");
        // An invalid transport is reported alone, without a timestamps error
        let error = udp_profile("xylem-echo", "pigeon", KERNEL).unwrap_err().to_string();
        assert!(error.starts_with("Invalid transport 'pigeon'"), "{error}");
        assert!(!error.contains("timestamps"), "{error}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn hardware_interface_names_are_checked() {
        // As TOML strings
        for name in [r#""""#, r#""sixteen-chars-xx""#, r#""eth/0""#, r#""eth 0""#, r#""a\u0000b""#]
        {
            let line = format!("timestamps = {{ hardware = {name} }}");
            let error = udp_profile("xylem-echo", "tcp", &line).unwrap_err().to_string();
            assert!(error.contains("is not a network interface name"), "{name}: {error}");
        }
        udp_profile("xylem-echo", "tcp", r#"timestamps = { hardware = "fifteen-chars-x" }"#)
            .unwrap();
    }

    #[test]
    fn groups_sharing_a_thread_must_share_timestamps() {
        let profile = |second: &str, threads: &str| {
            let mut config = udp_profile("xylem-echo", "tcp", "").unwrap();
            let mut value = toml::Value::try_from(&config).unwrap();
            let mut group = value["traffic_groups"][0].clone();
            group["name"] = toml::Value::String("second".into());
            group["threads"] =
                toml::from_str::<toml::Value>(&format!("t = {threads}")).unwrap()["t"].clone();
            if let Some(timestamps) =
                toml::from_str::<toml::Value>(second).unwrap().get("timestamps")
            {
                group.as_table_mut().unwrap().insert("timestamps".into(), timestamps.clone());
            }
            value["traffic_groups"].as_array_mut().unwrap().push(group);
            config = value.try_into().unwrap();
            config.validate()
        };
        let error = profile(KERNEL, "[0]").unwrap_err().to_string();
        assert!(error.contains("Thread 0 has mixed timestamp sources"), "{error}");
        assert!(error.contains(r#"group 0 'main' uses "user""#), "{error}");
        assert!(error.contains(r#"group 1 'second' uses "kernel""#), "{error}");
        // On other threads, or with the same source, groups may differ or agree
        let other_thread = profile(KERNEL, "[1]");
        assert_eq!(other_thread.is_ok(), cfg!(target_os = "linux"), "{other_thread:?}");
        profile(r#"timestamps = "user""#, "[0]").unwrap();
    }

    #[test]
    fn on_error_defaults_to_count_and_accepts_abort() {
        use xylem_core::traffic_group::OnError;
        let config = udp_profile("xylem-echo", "tcp", "").unwrap();
        assert_eq!(config.traffic_groups[0].on_error, OnError::Count);
        let config = udp_profile("xylem-echo", "tcp", r#"on_error = "abort""#).unwrap();
        assert_eq!(config.traffic_groups[0].on_error, OnError::Abort);
        let error = udp_profile("xylem-echo", "tcp", r#"on_error = "ignore""#).unwrap_err();
        assert!(error.to_string().contains("unknown variant `ignore`"), "{error}");
    }

    #[test]
    fn memcached_udp_values_must_fit_one_datagram() {
        let max = xylem_protocols::memcached::MAX_UDP_VALUE_SIZE;
        let at = |size: usize| {
            let config = udp_profile("memcached-ascii", "udp", "").unwrap();
            let mut value = toml::Value::try_from(&config).unwrap();
            value["traffic_groups"][0]["protocol_config"]["keys"]["value_size"] =
                toml::Value::Integer(size as i64);
            let mut config: ProfileConfig = value.try_into().unwrap();
            config.validate()
        };
        at(max).unwrap();
        let error = at(max + 1).unwrap_err().to_string();
        assert!(error.contains("does not fit one memcached UDP datagram"), "{error}");
    }

    #[test]
    fn test_config_validation_missing_target() {
        let profile_without_target = r#"
[experiment]
name = "no-target-test"
duration = "10s"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = ""
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1

[traffic_groups.protocol_config.keys]
strategy = "sequential"
start = 0
value_size = 64

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "results/test.json"
"#;

        use std::io::Write;
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        tmpfile.write_all(profile_without_target.as_bytes()).unwrap();
        tmpfile.flush().unwrap();

        let mut config = ProfileConfig::from_file(tmpfile.path()).unwrap();

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must have a target address"));
    }

    #[test]
    fn test_validation_rejects_non_positive_rate() {
        let profile = |traffic_policy: &str| -> ProfileConfig {
            let toml_str = format!(
                r#"
[experiment]
name = "rate-test"
duration = "10s"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0]
connections_per_thread = 1

[traffic_groups.protocol_config.keys]
strategy = "sequential"
start = 0
value_size = 64

[traffic_groups.traffic_policy]
{traffic_policy}

[output]
format = "json"
file = "/tmp/test.json"
"#
            );
            toml::from_str(&toml_str).expect("TOML should parse")
        };

        let rejected = [
            ("type = \"fixed-rate\"\nrate = 0", "traffic_policy.rate"),
            ("type = \"fixed-rate\"\nrate = -1.0", "traffic_policy.rate"),
            ("type = \"fixed-rate\"\nrate = nan", "traffic_policy.rate"),
            ("type = \"poisson\"\nrate = 0", "traffic_policy.rate"),
            (
                "type = \"adaptive\"\ninitial_rate = 0\ntarget_latency_us = 1000",
                "traffic_policy.initial_rate",
            ),
            (
                "type = \"adaptive\"\ninitial_rate = 1000.0\ntarget_latency_us = 1000\nmin_rate = 0",
                "traffic_policy.min_rate",
            ),
            (
                "type = \"adaptive\"\ninitial_rate = 1000.0\ntarget_latency_us = 1000\nmax_rate = 0",
                "traffic_policy.max_rate",
            ),
        ];
        for (traffic_policy, field) in rejected {
            let err = profile(traffic_policy).validate().expect_err(traffic_policy).to_string();
            assert_eq!(err, format!("Traffic group 0 'main' {field} must be > 0"));
        }

        // Positive rates pass, and a pattern policy may pause at zero.
        let accepted = [
            "type = \"fixed-rate\"\nrate = 1000.0",
            "type = \"poisson\"\nrate = 1000.0",
            "type = \"adaptive\"\ninitial_rate = 1000.0\ntarget_latency_us = 1000",
            "type = \"ramp\"\nstart_rate = 0\nend_rate = 1000.0\nduration = \"5s\"",
        ];
        for traffic_policy in accepted {
            profile(traffic_policy).validate().expect(traffic_policy);
        }
    }

    #[test]
    fn test_invalid_config_validation() {
        let config = ProfileConfig::from_file("../tests/redis/redis-get-zipfian.toml")
            .expect("Failed to load profile");

        assert_eq!(config.experiment.name, "redis-get-zipfian");
    }

    #[test]
    fn test_key_generation_conversion() {
        let config = ProfileConfig::from_file("../tests/redis/redis-get-zipfian.toml")
            .expect("Failed to load profile");

        let keys_config = get_keys_config(&config.traffic_groups[0])
            .expect("Expected keys config in protocol_config");

        let key_gen = keys_config
            .to_key_gen(config.experiment.seed)
            .expect("Failed to convert to KeyGeneration");

        match key_gen {
            KeyGeneration::Zipfian(_) => {}
            _ => panic!("Expected Zipfian distribution"),
        }
    }

    #[test]
    fn test_value_size_extraction() {
        let config = ProfileConfig::from_file("../tests/redis/redis-get-zipfian.toml")
            .expect("Failed to load profile");

        let keys_config = get_keys_config(&config.traffic_groups[0])
            .expect("Expected keys config in protocol_config");
        let value_size = keys_config.value_size();
        assert_eq!(value_size, 64);
    }

    #[test]
    fn test_seed_reproducibility() {
        let config = ProfileConfig::from_file("../tests/redis/redis-get-zipfian.toml")
            .expect("Failed to load profile");

        let master_seed = config.experiment.seed;
        assert_eq!(master_seed, Some(42));

        let keys = get_keys_config(&config.traffic_groups[0])
            .expect("Expected keys config in protocol_config");

        let key_gen1 = keys.to_key_gen(master_seed).expect("Failed to create key generation 1");
        let key_gen2 = keys.to_key_gen(master_seed).expect("Failed to create key generation 2");

        match (&key_gen1, &key_gen2) {
            (KeyGeneration::Zipfian(_), KeyGeneration::Zipfian(_)) => {}
            _ => panic!("Expected Zipfian distribution"),
        }
    }

    #[test]
    fn test_all_key_strategies() {
        let sequential_config = r#"
[experiment]
name = "test"
duration = "10s"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1

[traffic_groups.protocol_config.keys]
strategy = "sequential"
start = 100
value_size = 64

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "test.json"
"#;

        use std::io::Write;
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        tmpfile.write_all(sequential_config.as_bytes()).unwrap();
        tmpfile.flush().unwrap();

        let config = ProfileConfig::from_file(tmpfile.path()).unwrap();
        let keys_config = get_keys_config(&config.traffic_groups[0]).unwrap();
        let key_gen = keys_config.to_key_gen(None).unwrap();

        match key_gen {
            KeyGeneration::Sequential { start, .. } => {
                assert_eq!(start, 100);
            }
            _ => panic!("Expected Sequential"),
        }

        let random_config = sequential_config.replace(
            r#"strategy = "sequential"
start = 100"#,
            r#"strategy = "random"
max = 10000"#,
        );

        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        tmpfile.write_all(random_config.as_bytes()).unwrap();
        tmpfile.flush().unwrap();

        let config = ProfileConfig::from_file(tmpfile.path()).unwrap();
        let keys_config = get_keys_config(&config.traffic_groups[0]).unwrap();
        let key_gen = keys_config.to_key_gen(Some(12345)).unwrap();

        match key_gen {
            KeyGeneration::Random { max, .. } => {
                assert_eq!(max, 10000);
            }
            _ => panic!("Expected Random"),
        }

        let rr_config = sequential_config.replace(
            r#"strategy = "sequential"
start = 100"#,
            r#"strategy = "round-robin"
max = 5000"#,
        );

        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        tmpfile.write_all(rr_config.as_bytes()).unwrap();
        tmpfile.flush().unwrap();

        let config = ProfileConfig::from_file(tmpfile.path()).unwrap();
        let keys_config = get_keys_config(&config.traffic_groups[0]).unwrap();
        let key_gen = keys_config.to_key_gen(None).unwrap();

        match key_gen {
            KeyGeneration::RoundRobin { max, .. } => {
                assert_eq!(max, 5000);
            }
            _ => panic!("Expected RoundRobin"),
        }
    }

    #[test]
    fn test_all_protocols_valid() {
        let protocols = vec!["redis", "memcached-binary", "memcached-ascii", "http", "xylem-echo"];

        for protocol in protocols {
            // http and xylem-echo take no keys
            let keys = match protocol {
                "http" | "xylem-echo" => "",
                _ => "[traffic_groups.protocol_config.keys]\nstrategy = \"sequential\"\nvalue_size = 64",
            };
            let config_str = format!(
                r#"
[experiment]
name = "test-{protocol}"
duration = "10s"

[[traffic_groups]]
name = "main"
protocol = "{protocol}"
target = "127.0.0.1:6379"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1

{keys}

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "test.json"
"#
            );

            use std::io::Write;
            let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
            tmpfile.write_all(config_str.as_bytes()).unwrap();
            tmpfile.flush().unwrap();

            let config = ProfileConfig::from_file(tmpfile.path())
                .unwrap_or_else(|_| panic!("Failed to parse config for protocol: {}", protocol));
            assert_eq!(config.traffic_groups[0].protocol.as_str(), protocol);
        }
    }

    #[test]
    fn test_invalid_protocol() {
        let config_str = r#"
[experiment]
name = "test-invalid"
duration = "10s"

[[traffic_groups]]
name = "main"
protocol = "invalid_protocol"
target = "127.0.0.1:6379"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1

[traffic_groups.protocol_config.keys]
strategy = "sequential"
start = 0
value_size = 64

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "test.json"
"#;

        use std::io::Write;
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        tmpfile.write_all(config_str.as_bytes()).unwrap();
        tmpfile.flush().unwrap();

        let mut config = ProfileConfig::from_file(tmpfile.path()).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Invalid") && err_msg.contains("protocol"),
            "Error message should contain 'Invalid' and 'protocol', got: {}",
            err_msg
        );
    }

    #[test]
    fn test_duration_parsing() {
        let config_str = r#"
[experiment]
name = "test-duration"
duration = "2m30s"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1

[traffic_groups.protocol_config.keys]
strategy = "sequential"
start = 0
value_size = 64

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "test.json"
"#;

        use std::io::Write;
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        tmpfile.write_all(config_str.as_bytes()).unwrap();
        tmpfile.flush().unwrap();

        let config = ProfileConfig::from_file(tmpfile.path()).unwrap();
        assert_eq!(config.experiment.duration, Duration::from_secs(150));
    }

    #[test]
    fn test_seed_none_vs_some() {
        let no_seed_config = r#"
[experiment]
name = "no-seed"
duration = "10s"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1

[traffic_groups.protocol_config.keys]
strategy = "random"
max = 1000
value_size = 64

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "test.json"
"#;

        use std::io::Write;
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        tmpfile.write_all(no_seed_config.as_bytes()).unwrap();
        tmpfile.flush().unwrap();

        let config = ProfileConfig::from_file(tmpfile.path()).unwrap();
        assert_eq!(config.experiment.seed, None);

        let with_seed = no_seed_config.replace(
            r#"[experiment]
name = "no-seed"
duration = "10s""#,
            r#"[experiment]
name = "with-seed"
seed = 999
duration = "10s""#,
        );

        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        tmpfile.write_all(with_seed.as_bytes()).unwrap();
        tmpfile.flush().unwrap();

        let config = ProfileConfig::from_file(tmpfile.path()).unwrap();
        assert_eq!(config.experiment.seed, Some(999));

        let config = ProfileConfig::from_file_with_overrides(
            tmpfile.path(),
            &["experiment.seed=12345".to_string()],
        )
        .unwrap();
        assert_eq!(config.experiment.seed, Some(12345));
    }

    #[test]
    fn test_set_simple_field() {
        let config = ProfileConfig::from_file_with_overrides(
            "../tests/redis/redis-get-zipfian.toml",
            &["experiment.seed=42".to_string()],
        )
        .unwrap();
        assert_eq!(config.experiment.seed, Some(42));
    }

    #[test]
    fn test_set_nested_field() {
        let config = ProfileConfig::from_file_with_overrides(
            "../tests/redis/redis-get-zipfian.toml",
            &["traffic_groups.0.protocol_config.keys.n=5000".to_string()],
        )
        .unwrap();

        let keys_config = get_keys_config(&config.traffic_groups[0]);
        match keys_config {
            Some(KeysConfig::Zipfian { n, .. }) => {
                assert_eq!(n, 5000);
            }
            _ => panic!("Expected Zipfian keys config"),
        }
    }

    #[test]
    fn test_set_array_index() {
        let config = ProfileConfig::from_file_with_overrides(
            "../tests/redis/redis-bench.toml",
            &["traffic_groups.0.sampling_policy.type=unlimited".to_string()],
        )
        .unwrap();

        match &config.traffic_groups[0].sampling_policy {
            SamplingPolicy::Unlimited => {}
            _ => panic!("Expected Unlimited sampling policy after override"),
        }
    }

    #[test]
    fn test_set_array_value() {
        let config = ProfileConfig::from_file_with_overrides(
            "../tests/redis/redis-bench.toml",
            &["traffic_groups.0.threads=[0,1,2,3]".to_string()],
        )
        .unwrap();

        assert_eq!(config.traffic_groups[0].threads, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_set_multiple_overrides() {
        let config = ProfileConfig::from_file_with_overrides(
            "../tests/redis/redis-get-zipfian.toml",
            &[
                "traffic_groups.0.target=192.168.1.100:6379".to_string(),
                "experiment.duration=5m".to_string(),
                "experiment.seed=999".to_string(),
                "traffic_groups.0.connections_per_thread=8".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(config.traffic_groups[0].target, "192.168.1.100:6379");
        assert_eq!(config.experiment.duration, Duration::from_secs(300));
        assert_eq!(config.experiment.seed, Some(999));
        assert_eq!(config.traffic_groups[0].connections_per_thread, 8);
    }

    #[test]
    fn test_set_type_inference_integer() {
        let config = ProfileConfig::from_file_with_overrides(
            "../tests/redis/redis-get-zipfian.toml",
            &["experiment.seed=12345".to_string()],
        )
        .unwrap();
        assert_eq!(config.experiment.seed, Some(12345));
    }

    #[test]
    fn test_set_type_inference_float() {
        let config = ProfileConfig::from_file_with_overrides(
            "../tests/redis/redis-bench.toml",
            &["traffic_groups.1.sampling_policy.rate=0.75".to_string()],
        )
        .expect("Failed to load redis-bench.toml with overrides");

        match &config.traffic_groups[1].sampling_policy {
            SamplingPolicy::Limited { rate, .. } => {
                assert!((rate - 0.75).abs() < 0.001, "Expected 75% sampling rate");
            }
            _ => panic!("Expected Limited sampling policy"),
        }
    }

    #[test]
    fn test_set_type_inference_boolean() {
        let config = ProfileConfig::from_file_with_overrides(
            "../tests/redis/redis-get-zipfian.toml",
            &["stats.include_records=true".to_string()],
        )
        .unwrap();
        assert!(config.stats.include_records);
    }

    #[test]
    fn test_set_type_inference_string() {
        // The Redis protocol_config does not fit http, so replace it too
        let config = ProfileConfig::from_file_with_overrides(
            "../tests/redis/redis-get-zipfian.toml",
            &[
                "traffic_groups.0.protocol=http".to_string(),
                "traffic_groups.0.protocol_config={}".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(config.traffic_groups[0].protocol, "http");
    }

    #[test]
    fn test_set_invalid_path() {
        let result = ProfileConfig::from_file_with_overrides(
            "../tests/redis/redis-get-zipfian.toml",
            &["nonexistent.field=value".to_string()],
        );
        let error = format!("{:#}", result.unwrap_err());
        assert!(
            error.starts_with(
                "Invalid --set nonexistent.field=value: unknown field `nonexistent` at the top level"
            ),
            "{error}"
        );
    }

    #[test]
    fn set_paths_must_name_keys_and_indexes_that_exist() {
        let path = "../tests/redis/redis-bench.toml";
        for (set, expected) in [
            (
                "experiment.duraton=1s",
                "Invalid --set experiment.duraton=1s: unknown field `duraton` in experiment \
                 (did you mean `duration`?); expected one of `description`, `drain_timeout`, \
                 `duration`, `name`, `seed`",
            ),
            ("experimnt.seed=1", "unknown field `experimnt` at the top level (did you mean `experiment`?)"),
            ("traffic_groups.1.traffic_policy.rat=5", "in traffic_groups[1].traffic_policy (did you mean `rate`?)"),
            (
                "traffic_groups.0.protocol_config.keys.thetaa=1",
                "unknown field `thetaa` in traffic_groups[0].protocol_config.keys (did you mean `theta`?)",
            ),
            // The keys of the group's protocol, not of another
            ("traffic_groups.0.protocol_config.method=GET", "unknown field `method` in traffic_groups[0].protocol_config"),
            (
                "traffic_groups.2.target=127.0.0.1:1",
                "Invalid --set traffic_groups.2.target=127.0.0.1:1: index 2 is out of bounds for \
                 traffic_groups (length: 2)",
            ),
            ("traffic_groups.0.threads.3=1", "index 3 is out of bounds for traffic_groups[0].threads (length: 1)"),
            ("experiment.name.short=x", "experiment.name is not a table"),
            ("experiment.+=x", "experiment is not an array"),
            ("stats.streaming.path=x", "stats.streaming was removed with Parquet stats streaming"),
            (
                "traffic_groups.0.sampling_rate=0.5",
                "Traffic group 0 'latency-agent': sampling_rate was replaced by sampling_policy",
            ),
        ] {
            let error = ProfileConfig::from_file_with_overrides(path, &[set.into()]).unwrap_err();
            let error = format!("{error:#}");
            assert!(error.starts_with(&format!("Invalid --set {set}: ")), "{set}: {error}");
            assert!(error.contains(expected), "{set}: {error}");
        }

        // Keys the file leaves unset, appends, tables and variant fields still work
        let config = ProfileConfig::from_file_with_overrides(
            path,
            &[
                "traffic_groups.0.threads.+=4".into(),
                "request_dump.directory=/tmp/dumps".into(),
                "traffic_groups.1.sampling_policy={ type = \"dd-sketch\" }".into(),
                "traffic_groups.0.traffic_policy.type=fixed-rate".into(),
                "traffic_groups.0.traffic_policy.rate=1000".into(),
                "traffic_groups.0.protocol_config.insert_phase.key_count=10".into(),
            ],
        )
        .unwrap();
        assert_eq!(config.traffic_groups[0].threads, vec![0, 4]);
        assert!(config.request_dump.is_some());
        assert!(matches!(
            config.traffic_groups[1].sampling_policy,
            SamplingPolicy::DdSketch { .. }
        ));
        assert!(
            matches!(config.traffic_groups[0].traffic_policy, PolicyConfig::FixedRate { rate } if rate == 1000.0)
        );
    }

    /// A valid profile with every table a profile can have, and one group
    /// per protocol
    const EVERY_TABLE: &str = r#"
[experiment]
name = "every-table"
duration = "1s"

[output]
file = "/tmp/every-table.json"
html = { library = "chartjs" }

[stats]
sampling_policy = { type = "hdr-histogram" }

[request_dump]
directory = "/tmp/every-table-dumps"
rotation = { size = 1024 }

[ntcp]
endpoints = [{ ifname = "ntcp0", local_ip = "10.0.0.2" }]

[ntcp.dpdk]
device = "0000:05:00.0"
eal_args = ["--in-memory"]
local_ips = ["10.0.0.3"]
prefix_len = 24
gateway = "10.0.0.1"

[ntcp.af_xdp]
ifname = "eth0"
mode = "generic"
local_ips = ["10.0.0.4"]
prefix_len = 24
gateway = "10.0.0.1"

[thresholds]
latency = ["p99 < 1s"]
"requests.errors{group=redis}" = ["rate < 0.01"]

[validity]
skip = ["late_sends"]

[[traffic_groups]]
name = "redis"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0]
connections_per_thread = 1
traffic_policy = { type = "fixed-rate", rate = 100.0 }
sampling_policy = { type = "limited" }

[traffic_groups.protocol_config]
auth = { password = "pw" }
insert_phase = { key_count = 10 }
keys = { strategy = "zipfian", n = 10, theta = 0.99, value_size = 8 }
operations = { strategy = "weighted", commands = [{ name = "scan", weight = 1.0, params = { count = 5 } }] }

[traffic_groups.protocol_config.value_size]
strategy = "per_command"
commands = { set = { distribution = "fixed", size = 8 } }
default = { strategy = "fixed", size = 8 }

[[traffic_groups]]
name = "memcached"
protocol = "memcached-ascii"
target = "127.0.0.1:11211"
threads = [0]
connections_per_thread = 1
traffic_policy = { type = "closed-loop" }
sampling_policy = { type = "unlimited" }
protocol_config = { keys = { strategy = "round-robin", max = 10, value_size = 8 }, insert_phase = {} }

[[traffic_groups]]
name = "http"
protocol = "http"
target = "127.0.0.1:80"
threads = [0]
connections_per_thread = 1
traffic_policy = { type = "closed-loop" }
protocol_config = { method = "GET" }

[[traffic_groups]]
name = "masstree"
protocol = "masstree"
target = "127.0.0.1:2117"
threads = [0]
connections_per_thread = 1
traffic_policy = { type = "closed-loop" }
protocol_config = { operation = "scan", scan = { count = 5 } }

[[traffic_groups]]
name = "echo"
protocol = "xylem-echo"
target = "127.0.0.1:9999"
threads = [0]
connections_per_thread = 1
traffic_policy = { type = "closed-loop" }
protocol_config = {}
"#;

    /// Set `key = value` in the table of `profile` at the dotted `path`
    fn insert(profile: &mut toml::Value, path: &str, key: &str, value: toml::Value) {
        let mut table = profile;
        for step in path.split('.').filter(|step| !step.is_empty()) {
            table = match step.parse::<usize>() {
                Ok(index) => &mut table[index],
                Err(_) => &mut table[step],
            };
        }
        table.as_table_mut().unwrap().insert(key.into(), value);
    }

    /// `EVERY_TABLE` with `key = value` added to the table at the dotted `path`
    fn every_table_with(path: &str, key: &str, value: toml::Value) -> toml::Value {
        let mut profile: toml::Value = toml::from_str(EVERY_TABLE).unwrap();
        insert(&mut profile, path, key, value);
        profile
    }

    #[test]
    fn unknown_keys_are_rejected_at_every_level() {
        let profile: toml::Value = toml::from_str(EVERY_TABLE).unwrap();
        check_keys(&profile).unwrap();
        let mut config: ProfileConfig = toml::from_str(EVERY_TABLE).unwrap();
        config.validate().unwrap();

        for (path, location) in [
            ("", "at the top level"),
            ("experiment", "in experiment"),
            ("output", "in output"),
            ("output.html", "in output.html"),
            ("stats", "in stats"),
            ("stats.sampling_policy", "in stats.sampling_policy"),
            ("request_dump", "in request_dump"),
            ("request_dump.rotation", "in request_dump.rotation"),
            ("ntcp", "in ntcp"),
            ("ntcp.endpoints.0", "in ntcp.endpoints[0]"),
            ("validity", "in validity"),
            ("traffic_groups.0", "in traffic_groups[0]"),
            // A struct variant and a unit variant of each tagged enum
            ("traffic_groups.0.traffic_policy", "in traffic_groups[0].traffic_policy"),
            ("traffic_groups.1.traffic_policy", "in traffic_groups[1].traffic_policy"),
            ("traffic_groups.0.sampling_policy", "in traffic_groups[0].sampling_policy"),
            ("traffic_groups.1.sampling_policy", "in traffic_groups[1].sampling_policy"),
            ("traffic_groups.0.protocol_config", "in traffic_groups[0].protocol_config"),
            (
                "traffic_groups.0.protocol_config.auth",
                "in traffic_groups[0].protocol_config.auth",
            ),
            (
                "traffic_groups.0.protocol_config.keys",
                "in traffic_groups[0].protocol_config.keys",
            ),
            (
                "traffic_groups.0.protocol_config.insert_phase",
                "in traffic_groups[0].protocol_config.insert_phase",
            ),
            (
                "traffic_groups.0.protocol_config.value_size",
                "in traffic_groups[0].protocol_config.value_size",
            ),
            (
                "traffic_groups.0.protocol_config.value_size.commands.set",
                "in traffic_groups[0].protocol_config.value_size.commands.set",
            ),
            (
                "traffic_groups.0.protocol_config.value_size.default",
                "in traffic_groups[0].protocol_config.value_size.default",
            ),
            (
                "traffic_groups.0.protocol_config.operations",
                "in traffic_groups[0].protocol_config.operations",
            ),
            (
                "traffic_groups.0.protocol_config.operations.commands.0",
                "in traffic_groups[0].protocol_config.operations.commands[0]",
            ),
            (
                "traffic_groups.0.protocol_config.operations.commands.0.params",
                "in traffic_groups[0].protocol_config.operations.commands[0].params",
            ),
            ("traffic_groups.1.protocol_config", "in traffic_groups[1].protocol_config"),
            (
                "traffic_groups.1.protocol_config.keys",
                "in traffic_groups[1].protocol_config.keys",
            ),
            (
                "traffic_groups.1.protocol_config.insert_phase",
                "in traffic_groups[1].protocol_config.insert_phase",
            ),
            ("traffic_groups.2.protocol_config", "in traffic_groups[2].protocol_config"),
            ("traffic_groups.3.protocol_config", "in traffic_groups[3].protocol_config"),
            (
                "traffic_groups.3.protocol_config.scan",
                "in traffic_groups[3].protocol_config.scan",
            ),
            ("traffic_groups.4.protocol_config", "in traffic_groups[4].protocol_config"),
        ] {
            let profile = every_table_with(path, "bogus", toml::Value::Integer(1));
            let error = format!("{:#}", check_keys(&profile).unwrap_err());
            let expected = format!("unknown field `bogus` {location}");
            assert!(error.starts_with(&expected), "{path}: {error}");
            // Without the key check, serde or validation rejects it too
            let text = toml::to_string(&profile).unwrap();
            let parsed = toml::from_str::<ProfileConfig>(&text).map(|mut config| config.validate());
            assert!(!matches!(parsed, Ok(Ok(()))), "{path}: accepted without the key check");
        }
    }

    #[test]
    fn removed_keys_give_their_migration_message() {
        let table = |text: &str| -> toml::Value { toml::from_str(text).unwrap() };
        let string = |text: &str| toml::Value::String(text.into());
        for (path, key, value, expected) in [
            (
                "",
                "target",
                table("address = \"x\""),
                "The top-level [target] section was removed",
            ),
            ("", "workload", table(""), "The top-level [workload] section was removed"),
            ("stats", "streaming", table("path = \"x\""), "stats.streaming was removed"),
            (
                "stats.sampling_policy",
                "type",
                string("adaptive"),
                "The adaptive sampling policy was removed",
            ),
            (
                "traffic_groups.1",
                "sampling_rate",
                toml::Value::Float(0.5),
                "Traffic group 1 'memcached': sampling_rate was replaced by sampling_policy",
            ),
            (
                "traffic_groups.0.sampling_policy",
                "type",
                string("adaptive"),
                "Traffic group 0 'redis': the adaptive sampling policy was removed",
            ),
            (
                "traffic_groups.0.protocol_config",
                "data_import",
                table("file = \"x.csv\""),
                "Traffic group 0 'redis': data_import is not supported for redis protocol. \
                 Remove protocol_config.data_import.",
            ),
        ] {
            let profile = every_table_with(path, key, value);
            let error = format!("{:#}", check_keys(&profile).unwrap_err());
            assert!(error.starts_with(expected), "{path}.{key}: {error}");
        }
    }

    #[test]
    fn validation_reports_every_independent_error() {
        let mut profile = every_table_with("experiment", "name", toml::Value::String("".into()));
        profile["experiment"]["duration"] = toml::Value::String("0s".into());
        insert(&mut profile, "output.html", "theme", toml::Value::String("dark".into()));
        profile["traffic_groups"][0]["target"] = toml::Value::String("".into());
        profile["traffic_groups"][0]["traffic_policy"]["rate"] = toml::Value::Float(0.0);
        insert(
            &mut profile,
            "traffic_groups.2",
            "transport",
            toml::Value::String("pigeon".into()),
        );
        insert(
            &mut profile,
            "traffic_groups.3.protocol_config",
            "scna",
            toml::Value::Integer(1),
        );
        let mut config: ProfileConfig = profile.try_into().unwrap();
        let error = config.validate().unwrap_err().to_string();
        let expected = [
            "The profile has 7 errors:",
            "  - output.html.theme only supports 'light'",
            "  - Experiment name cannot be empty",
            "  - Experiment duration must be > 0",
            "  - Traffic group 0 'redis' must have a target address",
            "  - Traffic group 0 'redis' traffic_policy.rate must be > 0",
            "  - Invalid transport 'pigeon' for traffic group 2 'http'. Valid options: tcp, udp, unix",
            "  - unknown field `scna` in traffic_groups[3].protocol_config (did you mean `scan`?)",
        ];
        let lines: Vec<&str> = error.lines().collect();
        assert_eq!(lines.len(), expected.len(), "{error}");
        for (line, expected) in lines.iter().zip(expected) {
            assert!(line.starts_with(expected), "{line} does not start with {expected}");
        }

        // An unknown protocol skips the checks that need the protocol
        let mut profile: toml::Value = toml::from_str(EVERY_TABLE).unwrap();
        profile["traffic_groups"][0]["protocol"] = toml::Value::String("redis-cluster".into());
        insert(
            &mut profile,
            "traffic_groups.0.protocol_config",
            "bogus",
            toml::Value::Integer(1),
        );
        let mut config: ProfileConfig = profile.try_into().unwrap();
        let error = config.validate().unwrap_err().to_string();
        assert!(
            error.starts_with("Invalid protocol 'redis-cluster' for traffic group 0"),
            "{error}"
        );
        assert!(error.contains("Redis Cluster support was removed"), "{error}");
        assert_eq!(error.lines().count(), 1, "{error}");
    }

    #[test]
    fn loading_reports_every_unknown_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("profile.toml");
        let mut profile = every_table_with("experiment", "durationn", toml::Value::Integer(1));
        insert(&mut profile, "traffic_groups.1.traffic_policy", "rate", toml::Value::Float(5.0));
        let keys = "traffic_groups.0.protocol_config.keys";
        insert(&mut profile, keys, "valu_size", toml::Value::Integer(1));
        std::fs::write(&path, toml::to_string(&profile).unwrap()).unwrap();
        let error = format!("{:#}", ProfileConfig::from_file(&path).unwrap_err());
        assert!(error.contains("The profile has 3 errors:\n"), "{error}");
        for expected in [
            "  - unknown field `durationn` in experiment (did you mean `duration`?)",
            "  - unknown field `rate` in traffic_groups[1].traffic_policy; expected one of `type`",
            "  - unknown field `valu_size` in traffic_groups[0].protocol_config.keys (did you mean `value_size`?)",
        ] {
            assert!(error.contains(expected), "{expected}: {error}");
        }
    }

    #[test]
    fn the_timestamps_table_rejects_unknown_keys() {
        let timestamps = |text: &str| -> toml::Value {
            toml::from_str::<toml::Value>(&format!("t = {text}")).unwrap()["t"].clone()
        };
        let with_timestamps =
            |text: &str| every_table_with("traffic_groups.0", "timestamps", timestamps(text));
        check_keys(&with_timestamps(r#""kernel""#)).unwrap();
        check_keys(&with_timestamps(r#"{ hardware = "eth0" }"#)).unwrap();
        for (text, expected) in [
            (
                r#"{ hardware = "eth0", bogus = 1 }"#,
                "unknown field `bogus` in traffic_groups[0].timestamps; expected one of `hardware`",
            ),
            (
                r#"{ hardwre = "eth0" }"#,
                "unknown field `hardwre` in traffic_groups[0].timestamps (did you mean `hardware`?)",
            ),
        ] {
            let profile = with_timestamps(text);
            let error = format!("{:#}", check_keys(&profile).unwrap_err());
            assert!(error.starts_with(expected), "{text}: {error}");
            // serde rejects it too, without the key check
            let text = toml::to_string(&profile).unwrap();
            assert!(toml::from_str::<ProfileConfig>(&text).is_err(), "{text}");
        }
    }

    #[test]
    fn threshold_metrics_are_keys_the_key_check_accepts_and_thresholds_check() {
        // [thresholds] is a map: the key check takes any metric name
        let metric = "latency_breakdown.network_server_us{group=echo}";
        let profile = every_table_with("thresholds", metric, toml::Value::Array(vec![]));
        check_keys(&profile).unwrap();
        let profile =
            every_table_with("thresholds", "latncy", toml::Value::Array(vec!["p99 < 1ms".into()]));
        check_keys(&profile).unwrap();
        // and the thresholds parser rejects an unknown one, with the other errors
        let mut profile = profile;
        profile["experiment"]["duration"] = toml::Value::String("0s".into());
        let mut config: ProfileConfig = profile.try_into().unwrap();
        let error = config.validate().unwrap_err().to_string();
        let expected = [
            "The profile has 2 errors:",
            "  - Experiment duration must be > 0",
            "  - Invalid thresholds:",
            "      \"latncy\": unknown metric 'latncy'; metrics: throughput, ",
        ];
        let lines: Vec<&str> = error.lines().collect();
        assert_eq!(lines.len(), expected.len(), "{error}");
        for (line, expected) in lines.iter().zip(expected) {
            assert!(line.starts_with(expected), "{line} does not start with {expected}");
        }
        assert!(config.threshold_checks.is_empty());
        // A threshold's expressions are its own parser's to check: a list of
        // strings, which serde enforces
        let profile = every_table_with("thresholds", "latency", toml::Value::Integer(1));
        check_keys(&profile).unwrap();
        let text = toml::to_string(&profile).unwrap();
        assert!(toml::from_str::<ProfileConfig>(&text).is_err());
    }

    #[test]
    fn validity_rejects_unknown_keys_and_reports_every_bad_limit() {
        let profile = every_table_with("validity", "max_unsnet_ratio", toml::Value::Float(0.1));
        let error = format!("{:#}", check_keys(&profile).unwrap_err());
        assert!(
            error.starts_with(
                "unknown field `max_unsnet_ratio` in validity (did you mean `max_unsent_ratio`?)"
            ),
            "{error}"
        );
        let text = toml::to_string(&profile).unwrap();
        let error = toml::from_str::<ProfileConfig>(&text).unwrap_err().to_string();
        assert!(error.contains("unknown field `max_unsnet_ratio`"), "{error}");

        let mut profile = every_table_with("validity", "max_unsent_ratio", toml::Value::Float(2.0));
        insert(&mut profile, "validity", "max_late_send_ratio", toml::Value::Float(-1.0));
        let mut config: ProfileConfig = profile.try_into().unwrap();
        let error = config.validate().unwrap_err().to_string();
        assert!(error.starts_with("The profile has 2 errors:\n"), "{error}");
        assert!(error.contains("validity.max_unsent_ratio must be a fraction"), "{error}");
        assert!(error.contains("validity.max_late_send_ratio must be a fraction"), "{error}");
    }

    #[test]
    fn redis_command_params_are_checked_against_their_command() {
        let mut profile: toml::Value = toml::from_str(EVERY_TABLE).unwrap();
        let commands = toml::from_str::<toml::Value>(
            r#"commands = [
                { name = "scan", weight = 1.0, params = { cursor = 0, count = 5, patern = "k*" } },
                { name = "custom", weight = 1.0, params = { template = "GET __key__", count = 1 } },
                { name = "get", weight = 1.0, params = { count = 1 } },
            ]"#,
        )
        .unwrap();
        profile["traffic_groups"][0]["protocol_config"]["operations"]["commands"] =
            commands["commands"].clone();
        let error = format!("{:#}", check_keys(&profile).unwrap_err());
        let params = "in traffic_groups[0].protocol_config.operations.commands";
        for expected in [
            format!("unknown field `patern` {params}[0].params (did you mean `pattern`?); expected one of `cursor`, `count`, `pattern`"),
            format!("unknown field `count` {params}[1].params; expected one of `template`"),
            format!("unknown field `count` {params}[2].params, which takes no fields"),
        ] {
            assert!(error.contains(&expected), "{expected}: {error}");
        }
    }

    /// Shell completion offers only paths that `--set` accepts
    #[test]
    fn completion_paths_are_valid_set_paths() {
        let mut profile =
            serde_json::to_value(toml::from_str::<toml::Value>(EVERY_TABLE).unwrap()).unwrap();
        for path in crate::completions::get_config_paths() {
            let segments = parse_path(&path).unwrap();
            // A protocol_config path needs to fit one protocol
            let valid = PROTOCOLS.iter().any(|(protocol, _)| {
                profile["traffic_groups"][0]["protocol"] = (*protocol).into();
                keys::check_set_path(&profile, &segments).is_ok()
            });
            assert!(valid, "--set rejects completion path {path}");
        }
    }

    #[test]
    fn test_set_invalid_format() {
        let result = ProfileConfig::from_file_with_overrides(
            "../tests/redis/redis-get-zipfian.toml",
            &["invalid_no_equals".to_string()],
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid override format"));
    }

    #[test]
    fn test_set_array_out_of_bounds() {
        let result = ProfileConfig::from_file_with_overrides(
            "../tests/redis/redis-bench.toml",
            &["traffic_groups.999.sampling_rate=0.5".to_string()],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_set_duration_string() {
        let config = ProfileConfig::from_file_with_overrides(
            "../tests/redis/redis-get-zipfian.toml",
            &["experiment.duration=2h".to_string()],
        )
        .unwrap();
        assert_eq!(config.experiment.duration, Duration::from_secs(7200));
    }

    #[test]
    fn redis_cluster_is_an_unknown_protocol() {
        let err = ProfileConfig::from_file_with_overrides(
            "../tests/redis/redis-get-zipfian.toml",
            &["traffic_groups.0.protocol=redis-cluster".to_string()],
        )
        .unwrap_err();
        assert!(
            format!("{err:#}").contains(
                "Invalid protocol 'redis-cluster' for traffic group 0 'main'. Valid options: \
                 redis, memcached-binary, memcached-ascii, http, masstree, xylem-echo. \
                 Redis Cluster support was removed"
            ),
            "{err:#}"
        );
    }

    #[test]
    fn redis_data_import_fails_validation() {
        let toml_str = r#"
[experiment]
name = "test"
duration = "10s"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0]
connections_per_thread = 1

[traffic_groups.protocol_config.keys]
strategy = "sequential"
start = 0
value_size = 64

[traffic_groups.protocol_config.data_import]
file = "/tmp/test.csv"

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "/tmp/test.json"
"#;
        let mut config: ProfileConfig = toml::from_str(toml_str).expect("TOML should parse");
        let err = config.validate().expect_err("redis data_import must fail validation");
        assert_eq!(
            err.to_string(),
            "Traffic group 0 'main': data_import is not supported for redis protocol. \
             Remove protocol_config.data_import."
        );
    }

    #[test]
    fn invalid_masstree_protocol_config_fails_validation() {
        let profile = |protocol_config: &str| -> ProfileConfig {
            let toml_str = format!(
                r#"
[experiment]
name = "test"
duration = "10s"

[[traffic_groups]]
name = "main"
protocol = "masstree"
target = "127.0.0.1:2117"
threads = [0]
connections_per_thread = 1

[traffic_groups.protocol_config]
{protocol_config}

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "/tmp/test.json"
"#
            );
            toml::from_str(&toml_str).expect("TOML should parse")
        };

        let err = profile("operation = 5").validate().expect_err("operation must be a string");
        let err = err.to_string();
        assert!(
            err.starts_with("Traffic group 0 'main' has invalid Masstree protocol_config: "),
            "Error should name the invalid Masstree protocol_config: {}",
            err
        );

        // A valid config is kept, parsed, with the validated profile.
        let mut config = profile("operation = \"set\"");
        config.validate().expect("valid masstree protocol_config");
        match &config.protocol_configs[..] {
            [ProtocolConfig::Masstree(masstree)] => assert_eq!(masstree.operation, "set"),
            other => panic!("Expected one Masstree protocol config, got {:?}", other),
        }
    }

    fn redis_profile(protocol_config: &str) -> ProfileConfig {
        let toml_str = format!(
            r#"
[experiment]
name = "test"
duration = "10s"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0]
connections_per_thread = 1

[traffic_groups.protocol_config]
{protocol_config}

[traffic_groups.protocol_config.keys]
strategy = "sequential"
value_size = 8

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "/tmp/test.json"
"#
        );
        toml::from_str(&toml_str).expect("TOML should parse")
    }

    #[test]
    fn redis_connection_setup_is_parsed_and_validated() {
        let mut config = redis_profile(
            "auth = { username = \"bench\", password = \"s3cret\" }\ndb = 3\nresp = 3",
        );
        config.validate().expect("valid Redis connection setup");
        let [ProtocolConfig::Redis(redis)] = &config.protocol_configs[..] else {
            panic!("Expected one Redis protocol config");
        };
        assert_eq!((redis.db, redis.resp), (Some(3), Some(3)));
        assert_eq!(redis.connection_setup().unwrap().step_names(), ["AUTH", "SELECT", "HELLO"]);
        // Debug output of the validated profile never shows the password
        assert!(!format!("{:?}", config.protocol_configs).contains("s3cret"));

        // The variable password_env names must exist when the run starts
        let mut config = redis_profile(
            "auth = { password_env = \"XYLEM_TEST_REDIS_PASSWORD_THAT_IS_NEVER_SET\" }",
        );
        let err = format!("{:#}", config.validate().unwrap_err());
        assert!(
            err.starts_with("Traffic group 0 'main' has invalid Redis protocol_config"),
            "{err}"
        );
        assert!(
            err.contains(
                "environment variable XYLEM_TEST_REDIS_PASSWORD_THAT_IS_NEVER_SET is not set"
            ),
            "{err}"
        );

        for (protocol_config, expected) in [
            ("resp = 4", "Redis resp must be 2 or 3, got 4"),
            ("db = 2147483648", "Redis db must be at most 2147483647, got 2147483648"),
            ("db = -1", "invalid value: integer `-1`"),
            ("auth = { password = \"s3cret\", password_env = \"X\" }", "set exactly one"),
            ("auth = { username = \"bench\" }", "needs a password or password_env"),
            ("auth = { pasword = \"s3cret\" }", "unknown field `pasword`"),
        ] {
            let err = format!("{:#}", redis_profile(protocol_config).validate().unwrap_err());
            assert!(err.contains(expected), "{protocol_config}: {err}");
            assert!(!err.contains("s3cret"), "{protocol_config}: {err}");
        }
    }

    #[test]
    fn redis_auth_secrets_are_redacted_from_errors_and_logs() {
        // A parse error that would quote the value names no password
        let err = redis_profile("auth = \"s3cret\"").validate().unwrap_err().to_string();
        assert!(err.contains("invalid Redis protocol_config"), "{err}");
        assert!(!err.contains("s3cret"), "{err}");

        let pc = serde_json::json!({
            "auth": { "username": "bench", "password": "s3cret", "password_env": "PW" },
            "db": 3,
        });
        assert_eq!(
            redacted_protocol_config(&pc),
            serde_json::json!({
                "auth": { "username": "bench", "password": "<redacted>", "password_env": "PW" },
                "db": 3,
            })
        );
        let pc = serde_json::json!({ "auth": "s3cret" });
        assert_eq!(redacted_protocol_config(&pc), serde_json::json!({ "auth": "<redacted>" }));
        let pc = serde_json::json!({ "keys": { "strategy": "sequential" } });
        assert_eq!(redacted_protocol_config(&pc), pc);
    }

    #[test]
    fn test_request_dump_config_parsing() {
        let toml_str = r#"
[experiment]
name = "test-request-dump"
duration = "10s"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0]
connections_per_thread = 1

[traffic_groups.protocol_config.keys]
strategy = "sequential"
start = 0
value_size = 64

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "/tmp/test.json"

[request_dump]
directory = "/tmp/xylem_dumps"
prefix = "redis_requests"
encoding = "utf8"
rotation = "hourly"
max_files = 24
"#;
        let config: ProfileConfig = toml::from_str(toml_str).expect("TOML should parse");
        assert!(config.request_dump.is_some());

        let dump_config = config.request_dump.unwrap();
        assert_eq!(dump_config.directory.to_str().unwrap(), "/tmp/xylem_dumps");
        assert_eq!(dump_config.prefix, "redis_requests");
        assert_eq!(dump_config.encoding, xylem_core::request_dump::DataEncoding::Utf8Lossy);
        assert_eq!(dump_config.rotation, xylem_core::request_dump::RotationPolicy::Hourly);
        assert_eq!(dump_config.max_files, Some(24));
    }

    #[test]
    fn test_request_dump_size_rotation() {
        let toml_str = r#"
[experiment]
name = "test"
duration = "10s"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0]
connections_per_thread = 1

[traffic_groups.protocol_config.keys]
strategy = "sequential"
start = 0
value_size = 64

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "/tmp/test.json"

[request_dump]
directory = "/tmp/dumps"
rotation = { size = 10485760 }
"#;
        let config: ProfileConfig = toml::from_str(toml_str).expect("TOML should parse");
        assert!(config.request_dump.is_some());

        let dump_config = config.request_dump.unwrap();
        assert_eq!(dump_config.rotation, xylem_core::request_dump::RotationPolicy::Size(10485760));
    }

    #[test]
    fn test_request_dump_defaults() {
        let toml_str = r#"
[experiment]
name = "test"
duration = "10s"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0]
connections_per_thread = 1

[traffic_groups.protocol_config.keys]
strategy = "sequential"
start = 0
value_size = 64

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "/tmp/test.json"

[request_dump]
directory = "/tmp/dumps"
"#;
        let config: ProfileConfig = toml::from_str(toml_str).expect("TOML should parse");
        assert!(config.request_dump.is_some());

        let dump_config = config.request_dump.unwrap();
        // Check defaults
        assert_eq!(dump_config.prefix, "xylem_requests");
        assert_eq!(dump_config.encoding, xylem_core::request_dump::DataEncoding::Hex);
        assert_eq!(dump_config.rotation, xylem_core::request_dump::RotationPolicy::Never);
        assert_eq!(dump_config.max_files, None);
    }
}
