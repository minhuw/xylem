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
    /// Protocol: echo, redis, memcached-binary, memcached-ascii, http, synthetic, masstree, xylem-echo
    pub protocol: String,
    /// Transport: tcp, udp, tls
    #[serde(default = "default_transport")]
    pub transport: String,
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

        let valid_protocols =
            ["redis", "memcached-binary", "memcached-ascii", "http", "masstree", "xylem-echo"];
        if !valid_protocols.contains(&self.target.protocol.as_str()) {
            bail!(
                "Invalid protocol '{}'. Valid options: {}",
                self.target.protocol,
                valid_protocols.join(", ")
            );
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
        }
    }
}
