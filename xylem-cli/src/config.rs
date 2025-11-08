//! Configuration file support for Xylem
//!
//! This module provides TOML-based configuration for reproducible load testing experiments.
//! Configuration files are the primary interface for Xylem, with CLI arguments available
//! for quick overrides.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Top-level profile configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProfileConfig {
    pub experiment: ExperimentConfig,
    pub target: TargetConfig,
    pub workload: WorkloadConfig,
    /// Traffic groups configuration
    pub traffic_groups: Vec<xylem_core::traffic_group::TrafficGroupConfig>,
    pub output: OutputConfig,
}

/// Experiment metadata
#[derive(Debug, Clone, Deserialize, Serialize)]
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
    pub duration: Duration,
}

/// Target server configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
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
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WorkloadConfig {
    /// Key generation strategy
    pub keys: KeysConfig,
    /// Load pattern (MACRO level - time-varying traffic)
    pub pattern: LoadPatternConfig,
}

/// Key generation configuration (SPATIAL level)
#[derive(Debug, Clone, Deserialize, Serialize)]
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
#[derive(Debug, Clone, Deserialize, Serialize)]
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
        duration: Duration,
    },
    Spike {
        normal_rate: f64,
        spike_rate: f64,
        #[serde(with = "humantime_serde")]
        spike_start: Duration,
        #[serde(with = "humantime_serde")]
        spike_duration: Duration,
    },
    Sinusoidal {
        base_rate: f64,
        amplitude: f64,
        #[serde(with = "humantime_serde")]
        period: Duration,
        #[serde(with = "humantime_serde", default)]
        phase_shift: Option<Duration>,
    },
    Step {
        steps: Vec<StepConfig>,
    },
    Sawtooth {
        min_rate: f64,
        max_rate: f64,
        #[serde(with = "humantime_serde")]
        period: Duration,
    },
}

/// Step configuration for StepPattern
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StepConfig {
    #[serde(with = "humantime_serde")]
    pub duration: Duration,
    pub rate: f64,
}

/// Output configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
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

    /// Apply CLI overrides to configuration
    pub fn with_overrides(
        mut self,
        target: Option<String>,
        duration: Option<String>,
        output: Option<PathBuf>,
        seed: Option<u64>,
    ) -> Result<Self> {
        if let Some(target) = target {
            self.target.address = Some(target);
        }

        if let Some(duration_str) = duration {
            let duration = humantime::parse_duration(&duration_str)
                .with_context(|| format!("Invalid duration format: {}", duration_str))?;
            self.experiment.duration = duration;
        }

        if let Some(output) = output {
            self.output.file = output;
        }

        if let Some(seed) = seed {
            self.experiment.seed = Some(seed);
        }

        self.validate()?;
        Ok(self)
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
            if group.sampling_rate < 0.0 || group.sampling_rate > 1.0 {
                bail!("Traffic group {} '{}' sampling_rate must be in [0.0, 1.0]", i, group.name);
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
