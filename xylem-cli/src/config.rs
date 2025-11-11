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

/// Top-level profile configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct ProfileConfig {
    pub experiment: ExperimentConfig,
    pub target: TargetConfig,
    pub workload: WorkloadConfig,
    /// Traffic groups configuration
    pub traffic_groups: Vec<xylem_core::traffic_group::TrafficGroupConfig>,
    pub output: OutputConfig,
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

/// Target server configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct TargetConfig {
    /// Server address (e.g., "127.0.0.1:6379") - can be overridden via CLI
    #[serde(default)]
    pub address: Option<String>,
    /// Default protocol for traffic groups that don't specify their own
    /// Valid: redis, redis-cluster, memcached-binary, memcached-ascii, http
    #[serde(default)]
    pub protocol: Option<String>,
    /// Transport: tcp, udp, tls
    #[serde(default = "default_transport")]
    pub transport: String,
    /// Redis Cluster configuration (only used when protocol = "redis-cluster")
    #[serde(default)]
    pub redis_cluster: Option<RedisClusterConfig>,
}

/// Redis Cluster configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct RedisClusterConfig {
    /// Cluster nodes with their slot assignments
    pub nodes: Vec<RedisClusterNodeConfig>,
}

/// Redis Cluster node configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct RedisClusterNodeConfig {
    /// Node address (e.g., "127.0.0.1:7000")
    pub address: String,
    /// Start of slot range (0-16383)
    pub slot_start: u16,
    /// End of slot range (0-16383)
    pub slot_end: u16,
}

fn default_transport() -> String {
    "tcp".to_string()
}

/// Workload configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct WorkloadConfig {
    /// Key generation strategy
    pub keys: KeysConfig,
    /// Load pattern (MACRO level - time-varying traffic)
    pub pattern: LoadPatternConfig,
    /// Value size configuration (optional, defaults to fixed size from keys config)
    #[serde(default)]
    pub value_size: Option<ValueSizeConfig>,
    /// Operations/command configuration (optional, defaults to GET for redis)
    #[serde(default)]
    pub operations: Option<OperationsConfig>,
}

/// Key generation configuration (SPATIAL level)
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "strategy", rename_all = "lowercase")]
pub enum KeysConfig {
    Sequential {
        start: u64,
        value_size: usize,
    },
    Random {
        max: u64,
        value_size: usize,
    },
    #[serde(rename = "round-robin")]
    RoundRobin {
        max: u64,
        value_size: usize,
    },
    Zipfian {
        /// Number of keys in range [0, n-1]
        n: u64,
        /// Exponent (theta) controlling skewness
        theta: f64,
        value_size: usize,
    },
    Gaussian {
        /// Mean as percentage of keyspace (0.0 to 1.0)
        mean_pct: f64,
        /// Standard deviation as percentage of keyspace (0.0 to 1.0)
        std_dev_pct: f64,
        /// Maximum key value (keyspace size)
        max: u64,
        value_size: usize,
    },
}

/// Load pattern configuration (MACRO level - time-varying traffic)
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum LoadPatternConfig {
    Constant {
        /// Requests per second
        rate: f64,
    },
    Ramp {
        start_rate: f64,
        end_rate: f64,
        #[serde(with = "humantime_serde")]
        #[schemars(with = "String")]
        duration: Duration,
    },
    Spike {
        normal_rate: f64,
        spike_rate: f64,
        #[serde(with = "humantime_serde")]
        #[schemars(with = "String")]
        spike_start: Duration,
        #[serde(with = "humantime_serde")]
        #[schemars(with = "String")]
        spike_duration: Duration,
    },
    Sinusoidal {
        base_rate: f64,
        amplitude: f64,
        #[serde(with = "humantime_serde")]
        #[schemars(with = "String")]
        period: Duration,
        #[serde(with = "humantime_serde", default)]
        #[schemars(with = "Option<String>")]
        phase_shift: Option<Duration>,
    },
    Step {
        steps: Vec<StepConfig>,
    },
    Sawtooth {
        min_rate: f64,
        max_rate: f64,
        #[serde(with = "humantime_serde")]
        #[schemars(with = "String")]
        period: Duration,
    },
}

/// Step configuration for StepPattern
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct StepConfig {
    #[serde(with = "humantime_serde")]
    #[schemars(with = "String")]
    pub duration: Duration,
    pub rate: f64,
}

/// Value size configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "strategy", rename_all = "lowercase")]
pub enum ValueSizeConfig {
    /// Fixed size for all requests
    Fixed { size: usize },
    /// Uniform random size in range [min, max]
    Uniform { min: usize, max: usize },
    /// Normal (Gaussian) distribution
    Normal {
        mean: f64,
        std_dev: f64,
        min: usize,
        max: usize,
    },
    /// Per-command size configuration
    #[serde(rename = "per_command")]
    PerCommand {
        /// Size strategies for specific commands
        commands: std::collections::HashMap<String, CommandValueSizeConfig>,
        /// Default strategy for unspecified commands
        default: Box<ValueSizeConfig>,
    },
}

/// Value size configuration for a specific command
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "distribution", rename_all = "lowercase")]
pub enum CommandValueSizeConfig {
    Fixed {
        size: usize,
    },
    Uniform {
        min: usize,
        max: usize,
    },
    Normal {
        mean: f64,
        std_dev: f64,
        min: usize,
        max: usize,
    },
}

/// Operations configuration (command selection)
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "strategy", rename_all = "lowercase")]
pub enum OperationsConfig {
    /// Fixed operation (backward compatible)
    Fixed { operation: String },
    /// Weighted random selection
    Weighted { commands: Vec<CommandWeightConfig> },
}

/// Command weight configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct CommandWeightConfig {
    /// Command name: "get", "set", "incr", "mget", "wait", or "custom"
    pub name: String,
    /// Weight (probability) for this command
    pub weight: f64,
    /// Additional parameters for specific commands
    #[serde(flatten)]
    pub params: Option<CommandParams>,
}

/// Additional parameters for specific command types
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum CommandParams {
    /// MGET parameters
    MGet { count: usize },
    /// WAIT parameters
    Wait { num_replicas: usize, timeout_ms: u64 },
    /// Custom command template
    Custom { template: String },
}

/// Output configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct OutputConfig {
    /// Output format: json, csv
    #[serde(default = "default_format")]
    pub format: String,
    /// Output file path
    pub file: PathBuf,
    /// Print real-time updates
    #[serde(default)]
    pub real_time: bool,
}

fn default_format() -> String {
    "json".to_string()
}

impl ProfileConfig {
    /// Load profile from TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let config: ProfileConfig = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        // Note: validation happens in with_overrides(), not here
        // This allows config files without target to be loaded, then target set via CLI
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
        let config: ProfileConfig = value
            .try_into()
            .with_context(|| "Failed to deserialize modified configuration")?;

        // Validate the final configuration
        config.validate()?;
        Ok(config)
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        // Experiment validation
        if self.experiment.name.is_empty() {
            bail!("Experiment name cannot be empty");
        }
        if self.experiment.duration.as_secs() == 0 {
            bail!("Experiment duration must be > 0");
        }

        // Target validation
        if let Some(address) = &self.target.address {
            if address.is_empty() {
                bail!("Target address cannot be empty");
            }
        } else {
            bail!("Target address must be specified (either in config or via --target CLI flag)");
        }

        let valid_protocols = [
            "redis",
            "memcached-binary",
            "memcached-ascii",
            "http",
            "xylem-echo", // Test protocol
        ];

        // Validate target.protocol if specified
        if let Some(protocol) = &self.target.protocol {
            if !valid_protocols.contains(&protocol.as_str()) {
                bail!(
                    "Invalid target protocol '{}'. Valid options: {}",
                    protocol,
                    valid_protocols.join(", ")
                );
            }
        }

        // Validate that each traffic group has a protocol (either its own or from target)
        for (i, group) in self.traffic_groups.iter().enumerate() {
            let group_protocol = group.protocol.as_ref().or(self.target.protocol.as_ref());

            if group_protocol.is_none() {
                bail!(
                    "Traffic group {} '{}' has no protocol specified. Either set target.protocol or traffic_groups[{}].protocol",
                    i, group.name, i
                );
            }

            if let Some(protocol) = group_protocol {
                if !valid_protocols.contains(&protocol.as_str()) {
                    bail!(
                        "Invalid protocol '{}' for traffic group {} '{}'. Valid options: {}",
                        protocol,
                        i,
                        group.name,
                        valid_protocols.join(", ")
                    );
                }
            }
        }

        let valid_transports = ["tcp", "udp", "tls"];
        if !valid_transports.contains(&self.target.transport.as_str()) {
            bail!(
                "Invalid transport '{}'. Valid options: {}",
                self.target.transport,
                valid_transports.join(", ")
            );
        }

        // Workload validation
        self.validate_keys()?;
        self.validate_pattern()?;

        // Traffic groups validation
        if self.traffic_groups.is_empty() {
            bail!("At least one traffic group must be defined");
        }

        for (i, group) in self.traffic_groups.iter().enumerate() {
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
        }

        // Validate thread assignment
        let assignment =
            xylem_core::traffic_group::ThreadGroupAssignment::from_configs(&self.traffic_groups);
        assignment.validate()?;

        // Output validation
        let valid_formats = ["json", "csv"];
        if !valid_formats.contains(&self.output.format.as_str()) {
            bail!(
                "Invalid output format '{}'. Valid options: {}",
                self.output.format,
                valid_formats.join(", ")
            );
        }

        Ok(())
    }

    fn validate_keys(&self) -> Result<()> {
        match &self.workload.keys {
            KeysConfig::Sequential { value_size, .. } => {
                if *value_size == 0 {
                    bail!("value_size must be > 0");
                }
            }
            KeysConfig::Random { max, value_size } => {
                if *max == 0 {
                    bail!("Random max must be > 0");
                }
                if *value_size == 0 {
                    bail!("value_size must be > 0");
                }
            }
            KeysConfig::RoundRobin { max, value_size } => {
                if *max == 0 {
                    bail!("RoundRobin max must be > 0");
                }
                if *value_size == 0 {
                    bail!("value_size must be > 0");
                }
            }
            KeysConfig::Zipfian { n, theta, value_size } => {
                if *n == 0 {
                    bail!("Zipfian n must be > 0");
                }
                if *theta < 0.0 {
                    bail!("Zipfian theta must be >= 0.0");
                }
                if *value_size == 0 {
                    bail!("value_size must be > 0");
                }
            }
            KeysConfig::Gaussian { mean_pct, std_dev_pct, max, value_size } => {
                if *max == 0 {
                    bail!("Gaussian max must be > 0");
                }
                if !(0.0..=1.0).contains(mean_pct) {
                    bail!("Gaussian mean_pct must be in range [0.0, 1.0]");
                }
                if !(0.0..=1.0).contains(std_dev_pct) {
                    bail!("Gaussian std_dev_pct must be in range [0.0, 1.0]");
                }
                if *value_size == 0 {
                    bail!("value_size must be > 0");
                }
            }
        }
        Ok(())
    }

    fn validate_pattern(&self) -> Result<()> {
        match &self.workload.pattern {
            LoadPatternConfig::Constant { rate } => {
                if *rate <= 0.0 {
                    bail!("Constant rate must be > 0");
                }
            }
            LoadPatternConfig::Ramp { start_rate, end_rate, duration } => {
                if *start_rate <= 0.0 {
                    bail!("Ramp start_rate must be > 0");
                }
                if *end_rate <= 0.0 {
                    bail!("Ramp end_rate must be > 0");
                }
                if duration.as_secs() == 0 {
                    bail!("Ramp duration must be > 0");
                }
            }
            LoadPatternConfig::Spike {
                normal_rate,
                spike_rate,
                spike_start,
                spike_duration,
            } => {
                if *normal_rate <= 0.0 {
                    bail!("Spike normal_rate must be > 0");
                }
                if *spike_rate <= 0.0 {
                    bail!("Spike spike_rate must be > 0");
                }
                if spike_start.as_secs() == 0 {
                    bail!("Spike spike_start must be > 0");
                }
                if spike_duration.as_secs() == 0 {
                    bail!("Spike spike_duration must be > 0");
                }
            }
            LoadPatternConfig::Sinusoidal { base_rate, amplitude, period, .. } => {
                if *base_rate <= 0.0 {
                    bail!("Sinusoidal base_rate must be > 0");
                }
                if *amplitude < 0.0 {
                    bail!("Sinusoidal amplitude must be >= 0");
                }
                if period.as_secs() == 0 {
                    bail!("Sinusoidal period must be > 0");
                }
            }
            LoadPatternConfig::Step { steps } => {
                if steps.is_empty() {
                    bail!("Step pattern must have at least one step");
                }
                for (i, step) in steps.iter().enumerate() {
                    if step.rate <= 0.0 {
                        bail!("Step {} rate must be > 0", i);
                    }
                    if step.duration.as_secs() == 0 {
                        bail!("Step {} duration must be > 0", i);
                    }
                }
            }
            LoadPatternConfig::Sawtooth { min_rate, max_rate, period } => {
                if *min_rate <= 0.0 {
                    bail!("Sawtooth min_rate must be > 0");
                }
                if *max_rate <= 0.0 {
                    bail!("Sawtooth max_rate must be > 0");
                }
                if min_rate >= max_rate {
                    bail!("Sawtooth min_rate must be < max_rate");
                }
                if period.as_secs() == 0 {
                    bail!("Sawtooth period must be > 0");
                }
            }
        }
        Ok(())
    }
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

// Helper methods to convert config types to runtime types
impl KeysConfig {
    pub fn value_size(&self) -> usize {
        match self {
            Self::Sequential { value_size, .. } => *value_size,
            Self::Random { value_size, .. } => *value_size,
            Self::RoundRobin { value_size, .. } => *value_size,
            Self::Zipfian { value_size, .. } => *value_size,
            Self::Gaussian { value_size, .. } => *value_size,
        }
    }

    pub fn to_key_generation(
        &self,
        master_seed: Option<u64>,
    ) -> Result<xylem_core::workload::KeyGeneration> {
        use xylem_core::seed::{components, derive_seed};
        use xylem_core::workload::KeyGeneration;

        match self {
            Self::Sequential { start, .. } => Ok(KeyGeneration::sequential(*start)),
            Self::Random { max, .. } => {
                let seed = master_seed.map(|s| derive_seed(s, components::RANDOM_KEYS));
                Ok(KeyGeneration::random_with_seed(*max, seed))
            }
            Self::RoundRobin { max, .. } => Ok(KeyGeneration::round_robin(*max)),
            Self::Zipfian { n, theta, .. } => {
                let seed = master_seed.map(|s| derive_seed(s, components::ZIPFIAN_DIST));
                KeyGeneration::zipfian_with_seed(*n, *theta, seed)
            }
            Self::Gaussian { mean_pct, std_dev_pct, max, .. } => {
                let seed = master_seed.map(|s| derive_seed(s, components::GAUSSIAN_DIST));
                KeyGeneration::gaussian_with_seed(*mean_pct, *std_dev_pct, *max, seed)
            }
        }
    }
}

impl ValueSizeConfig {
    /// Convert to a ValueSizeGenerator
    pub fn to_generator(
        &self,
        master_seed: Option<u64>,
    ) -> anyhow::Result<Box<dyn xylem_core::workload::ValueSizeGenerator>> {
        use xylem_core::seed::derive_seed;
        use xylem_core::workload::{FixedSize, NormalSize, PerCommandSize, UniformSize};

        match self {
            Self::Fixed { size } => Ok(Box::new(FixedSize::new(*size))),
            Self::Uniform { min, max } => {
                let seed = master_seed.map(|s| derive_seed(s, "uniform_value_size"));
                Ok(Box::new(UniformSize::with_seed(*min, *max, seed)?))
            }
            Self::Normal { mean, std_dev, min, max } => {
                let seed = master_seed.map(|s| derive_seed(s, "normal_value_size"));
                Ok(Box::new(NormalSize::with_seed(*mean, *std_dev, *min, *max, seed)?))
            }
            Self::PerCommand { commands, default } => {
                let mut command_generators = std::collections::HashMap::new();
                for (cmd, cfg) in commands {
                    let gen = cfg.to_generator(master_seed)?;
                    command_generators.insert(cmd.clone(), gen);
                }
                let default_gen = default.to_generator(master_seed)?;
                Ok(Box::new(PerCommandSize::new(command_generators, default_gen)))
            }
        }
    }
}

impl CommandValueSizeConfig {
    /// Convert to a ValueSizeGenerator
    pub fn to_generator(
        &self,
        master_seed: Option<u64>,
    ) -> anyhow::Result<Box<dyn xylem_core::workload::ValueSizeGenerator>> {
        use xylem_core::seed::derive_seed;
        use xylem_core::workload::{FixedSize, NormalSize, UniformSize};

        match self {
            Self::Fixed { size } => Ok(Box::new(FixedSize::new(*size))),
            Self::Uniform { min, max } => {
                let seed = master_seed.map(|s| derive_seed(s, "uniform_cmd_size"));
                Ok(Box::new(UniformSize::with_seed(*min, *max, seed)?))
            }
            Self::Normal { mean, std_dev, min, max } => {
                let seed = master_seed.map(|s| derive_seed(s, "normal_cmd_size"));
                Ok(Box::new(NormalSize::with_seed(*mean, *std_dev, *min, *max, seed)?))
            }
        }
    }
}

impl OperationsConfig {
    /// Convert to a Redis CommandSelector
    pub fn to_redis_selector(
        &self,
        master_seed: Option<u64>,
    ) -> anyhow::Result<Box<dyn xylem_protocols::CommandSelector<xylem_protocols::RedisOp>>> {
        use xylem_core::seed::derive_seed;
        use xylem_protocols::{FixedCommandSelector, WeightedCommandSelector};

        match self {
            Self::Fixed { operation } => {
                let op = Self::parse_redis_op(operation, None)?;
                Ok(Box::new(FixedCommandSelector::new(op)))
            }
            Self::Weighted { commands } => {
                let mut weighted_ops = Vec::new();
                for cmd_cfg in commands {
                    let op = Self::parse_redis_op(&cmd_cfg.name, cmd_cfg.params.as_ref())?;
                    weighted_ops.push((op, cmd_cfg.weight));
                }
                let seed = master_seed.map(|s| derive_seed(s, "command_selector"));
                Ok(Box::new(WeightedCommandSelector::with_seed(weighted_ops, seed)?))
            }
        }
    }

    fn parse_redis_op(
        name: &str,
        params: Option<&CommandParams>,
    ) -> anyhow::Result<xylem_protocols::RedisOp> {
        use xylem_protocols::{CommandTemplate, RedisOp};

        match name.to_lowercase().as_str() {
            "get" => Ok(RedisOp::Get),
            "set" => Ok(RedisOp::Set),
            "incr" => Ok(RedisOp::Incr),
            "mget" => {
                if let Some(CommandParams::MGet { count }) = params {
                    Ok(RedisOp::MGet { count: *count })
                } else {
                    anyhow::bail!("MGET requires 'count' parameter")
                }
            }
            "wait" => {
                if let Some(CommandParams::Wait { num_replicas, timeout_ms }) = params {
                    Ok(RedisOp::Wait {
                        num_replicas: *num_replicas,
                        timeout_ms: *timeout_ms,
                    })
                } else {
                    anyhow::bail!("WAIT requires 'num_replicas' and 'timeout_ms' parameters")
                }
            }
            "custom" => {
                if let Some(CommandParams::Custom { template }) = params {
                    let cmd_template = CommandTemplate::parse(template)?;
                    Ok(RedisOp::Custom(cmd_template))
                } else {
                    anyhow::bail!("Custom command requires 'template' parameter")
                }
            }
            _ => anyhow::bail!("Unknown Redis operation: {}", name),
        }
    }
}
