//! Configuration file support for Xylem
//!
//! This module provides TOML-based configuration for reproducible load testing experiments.
//! Configuration files are the primary interface for Xylem, with CLI arguments available
//! for quick overrides.

use anyhow::{bail, Context, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;
use xylem_core::traffic_group::PolicyConfig;

/// Top-level profile configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
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
    /// Typed protocol_config of each traffic group, in group order.
    /// Empty until `validate` succeeds; not part of the profile format.
    #[serde(skip)]
    pub protocol_configs: Vec<ProtocolConfig>,
}

/// Experiment metadata
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
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
}

/// Re-export KeysConfig from xylem-protocols for use in config files
pub use xylem_protocols::KeysConfig;

// Keep the legacy wire format available to downstream callers during migration.
#[allow(deprecated)]
mod legacy;
#[allow(deprecated)]
pub use legacy::{CommandParams, CommandWeightConfig, OperationsConfig};

/// Output configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
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
    /// Load profile from TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let config: ProfileConfig = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

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
                .with_context(|| format!("Failed to apply override: {}", override_str))?;
        }

        // Deserialize modified TOML to ProfileConfig
        let mut config: ProfileConfig = value
            .try_into()
            .with_context(|| "Failed to deserialize modified configuration")?;

        // Validate the final configuration
        config.validate()?;
        Ok(config)
    }

    /// Validate configuration and parse each group's protocol_config into `protocol_configs`
    pub fn validate(&mut self) -> Result<()> {
        if self.output.real_time {
            bail!("output.real_time is not supported by the CLI; use false");
        }
        if self.output.html.library != "chartjs" {
            bail!("output.html.library only supports 'chartjs'");
        }
        if self.output.html.theme != "light" {
            bail!("output.html.theme only supports 'light'");
        }

        // Experiment validation
        if self.experiment.name.is_empty() {
            bail!("Experiment name cannot be empty");
        }
        if self.experiment.duration.as_secs() == 0 {
            bail!("Experiment duration must be > 0");
        }

        let valid_transports = ["tcp", "udp", "unix"];

        // Traffic groups validation
        if self.traffic_groups.is_empty() {
            bail!("At least one traffic group must be defined");
        }

        // Track transport per thread for validation
        let mut thread_transports: std::collections::HashMap<usize, (&str, usize, &str)> =
            std::collections::HashMap::new();

        let mut protocol_configs = Vec::with_capacity(self.traffic_groups.len());

        for (i, group) in self.traffic_groups.iter().enumerate() {
            // Validate target address
            if group.target.is_empty() {
                bail!("Traffic group {} '{}' must have a target address", i, group.name);
            }

            // Validate transport
            if !valid_transports.contains(&group.transport.as_str()) {
                bail!(
                    "Invalid transport '{}' for traffic group {} '{}'. Valid options: {}",
                    group.transport,
                    i,
                    group.name,
                    valid_transports.join(", ")
                );
            }

            // Validate protocol
            let Some(&(_, protocol)) = PROTOCOLS.iter().find(|(name, _)| *name == group.protocol)
            else {
                let names: Vec<&str> = PROTOCOLS.iter().map(|(name, _)| *name).collect();
                bail!(
                    "Invalid protocol '{}' for traffic group {} '{}'. Valid options: {}",
                    group.protocol,
                    i,
                    group.name,
                    names.join(", ")
                );
            };

            if group.threads.is_empty() {
                bail!("Traffic group {} '{}' must have at least one thread", i, group.name);
            }
            if group.connections_per_thread == 0 {
                bail!("Traffic group {} '{}' connections_per_thread must be > 0", i, group.name);
            }
            if group.max_pending_per_connection == 0 {
                bail!(
                    "Traffic group {} '{}' max_pending_per_connection must be > 0",
                    i,
                    group.name
                );
            }

            // Rate-based policies need a positive rate. Pattern policies accept
            // zero, which pauses sending by design.
            match &group.traffic_policy {
                PolicyConfig::FixedRate { rate } | PolicyConfig::Poisson { rate } => {
                    validate_positive_rate(*rate, "rate", i, &group.name)?;
                }
                PolicyConfig::Adaptive { initial_rate, min_rate, max_rate, .. } => {
                    validate_positive_rate(*initial_rate, "initial_rate", i, &group.name)?;
                    validate_positive_rate(*min_rate, "min_rate", i, &group.name)?;
                    validate_positive_rate(*max_rate, "max_rate", i, &group.name)?;
                }
                PolicyConfig::ClosedLoop
                | PolicyConfig::Sinusoidal { .. }
                | PolicyConfig::Ramp { .. }
                | PolicyConfig::Spike { .. }
                | PolicyConfig::Sawtooth { .. } => {}
            }

            // Validate all groups on the same thread use the same transport
            for &thread_id in &group.threads {
                match thread_transports.get(&thread_id) {
                    Some((existing_transport, existing_group_idx, existing_group_name))
                        if *existing_transport != group.transport.as_str() =>
                    {
                        bail!(
                            "Thread {} has mixed transports: group {} '{}' uses '{}' but group {} '{}' uses '{}'. \
                             All traffic groups on the same thread must use the same transport.",
                            thread_id,
                            existing_group_idx,
                            existing_group_name,
                            existing_transport,
                            i,
                            group.name,
                            group.transport
                        );
                    }
                    None => {
                        thread_transports
                            .insert(thread_id, (group.transport.as_str(), i, group.name.as_str()));
                    }
                    _ => {} // Same transport, nothing to do
                }
            }

            // Key-value protocols (Redis, Memcached) require protocol_config with keys
            // HTTP and XylemEcho have sensible defaults and don't require protocol_config
            let requires_protocol_config = matches!(
                protocol,
                ProtocolKind::Redis | ProtocolKind::MemcachedBinary | ProtocolKind::MemcachedAscii
            );

            if requires_protocol_config && group.protocol_config.is_none() {
                bail!(
                    "Traffic group {} '{}' has no protocol_config. \
                     For {} protocol, set traffic_groups[{}].protocol_config with keys configuration.",
                    i,
                    group.name,
                    group.protocol,
                    i
                );
            }

            // Parse protocol_config once as the appropriate typed struct.
            // This validates the structure, provides better error messages,
            // and gives workers the config they build protocols from.
            protocol_configs.push(parse_protocol_config(
                protocol,
                group.protocol_config.as_ref(),
                i,
                &group.name,
            )?);
        }

        // Validate thread assignment
        let assignment =
            xylem_core::traffic_group::ThreadGroupAssignment::from_configs(&self.traffic_groups);
        assignment.validate()?;

        // Output validation - format is now an enum, so always valid

        self.protocol_configs = protocol_configs;
        Ok(())
    }
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
                parse_typed_config(pc, "Redis", group_idx, group_name)?;

            // data_import needs an imported-data driver, which the CLI does not provide
            if config.data_import.is_some() {
                bail!(
                    "Traffic group {} '{}': data_import is not supported for redis protocol. \
                     Remove protocol_config.data_import.",
                    group_idx,
                    group_name
                );
            }
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
            // Parquet streaming was removed; its section is an unknown field.
            ("stats.streaming.path=stats.parquet", "unknown field `streaming`"),
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
sampling_rate = 1.0

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
sampling_rate = 1.0

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
sampling_rate = 1.0

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
sampling_rate = 1.0

[traffic_groups.protocol_config.keys]
strategy = "sequential"
start = 0
value_size = 64

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
sampling_rate = 1.0

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
sampling_rate = 1.0

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
sampling_rate = 1.0

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
                "traffic_groups.0.protocol=memcached-binary".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(config.traffic_groups[0].target, "192.168.1.100:6379");
        assert_eq!(config.experiment.duration, Duration::from_secs(300));
        assert_eq!(config.experiment.seed, Some(999));
        assert_eq!(config.traffic_groups[0].protocol, "memcached-binary");
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
        let config = ProfileConfig::from_file_with_overrides(
            "../tests/redis/redis-get-zipfian.toml",
            &["traffic_groups.0.protocol=http".to_string()],
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
        let _ = result;
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
                 redis, memcached-binary, memcached-ascii, http, masstree, xylem-echo"
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
