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
    pub connections: ConnectionConfig,
    pub threading: ThreadingConfig,
    pub statistics: StatisticsConfig,
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

/// Connection and policy configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConnectionConfig {
    /// Per-connection traffic policy (MICRO level)
    pub policy: PolicyConfig,
}

/// Policy scheduler configuration (MICRO level - per-connection timing)
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "scheduler", rename_all = "lowercase")]
pub enum PolicyConfig {
    /// All connections use the same policy
    Uniform {
        #[serde(flatten)]
        policy: PolicyType,
    },
    /// Explicit per-connection policy assignment
    #[serde(rename = "per-connection")]
    PerConnection { assignments: Vec<PolicyType> },
    /// Custom factory function (percentage-based)
    Factory { assignments: Vec<PolicyAssignment> },
}

/// Individual policy type
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum PolicyType {
    #[serde(rename = "closed-loop")]
    ClosedLoop,
    #[serde(rename = "fixed-rate")]
    FixedRate {
        rate: f64,
    },
    Poisson {
        rate: f64,
    },
    Adaptive {
        initial_rate: f64,
    },
}

/// Policy assignment with percentage (for factory scheduler)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PolicyAssignment {
    /// Percentage of connections (0-100)
    pub percentage: u8,
    #[serde(flatten)]
    pub policy: PolicyType,
}

/// Threading configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ThreadingConfig {
    /// Number of worker threads
    pub threads: usize,
    /// Connections per thread
    pub connections_per_thread: usize,
    /// Maximum pending requests per connection
    pub max_pending_per_connection: usize,
    /// Pin worker threads to CPU cores
    #[serde(default)]
    pub pin_cpus: bool,
    /// Starting CPU core offset for pinning
    #[serde(default)]
    pub cpu_start: usize,
}

/// Statistics configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StatisticsConfig {
    /// Sample all requests (false = probabilistic sampling)
    #[serde(default = "default_true")]
    pub sample_all: bool,
    /// Sampling rate (0.0-1.0)
    #[serde(default = "default_sampling_rate")]
    pub sampling_rate: f64,
    /// Percentiles to calculate
    #[serde(default = "default_percentiles")]
    pub percentiles: Vec<f64>,
    /// Confidence interval level (e.g., 0.95 for 95% CI)
    #[serde(default = "default_confidence")]
    pub confidence_level: f64,
}

fn default_true() -> bool {
    true
}
fn default_sampling_rate() -> f64 {
    1.0
}
fn default_percentiles() -> Vec<f64> {
    vec![50.0, 95.0, 99.0, 99.9]
}
fn default_confidence() -> f64 {
    0.95
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

        // Connection validation
        self.validate_policy()?;

        // Threading validation
        if self.threading.threads == 0 {
            bail!("threads must be > 0");
        }
        if self.threading.connections_per_thread == 0 {
            bail!("connections_per_thread must be > 0");
        }
        if self.threading.max_pending_per_connection == 0 {
            bail!("max_pending_per_connection must be > 0");
        }

        // Statistics validation
        if self.statistics.sampling_rate < 0.0 || self.statistics.sampling_rate > 1.0 {
            bail!("sampling_rate must be in range [0.0, 1.0]");
        }
        for p in &self.statistics.percentiles {
            if *p < 0.0 || *p > 100.0 {
                bail!("Percentile {} must be in range [0.0, 100.0]", p);
            }
        }
        if self.statistics.confidence_level < 0.0 || self.statistics.confidence_level > 1.0 {
            bail!("confidence_level must be in range [0.0, 1.0]");
        }

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

    fn validate_policy(&self) -> Result<()> {
        match &self.connections.policy {
            PolicyConfig::Uniform { policy } => {
                Self::validate_policy_type(policy)?;
            }
            PolicyConfig::PerConnection { assignments } => {
                if assignments.is_empty() {
                    bail!("PerConnection policy must have at least one assignment");
                }
                for (i, policy) in assignments.iter().enumerate() {
                    Self::validate_policy_type(policy)
                        .with_context(|| format!("Policy assignment {}", i))?;
                }
            }
            PolicyConfig::Factory { assignments } => {
                if assignments.is_empty() {
                    bail!("Factory policy must have at least one assignment");
                }

                let total_percentage: u32 = assignments.iter().map(|a| a.percentage as u32).sum();
                if total_percentage != 100 {
                    bail!("Factory policy percentages must sum to 100, got {}", total_percentage);
                }

                for (i, assignment) in assignments.iter().enumerate() {
                    if assignment.percentage == 0 {
                        bail!("Factory assignment {} percentage must be > 0", i);
                    }
                    Self::validate_policy_type(&assignment.policy)
                        .with_context(|| format!("Factory assignment {}", i))?;
                }
            }
        }
        Ok(())
    }

    fn validate_policy_type(policy: &PolicyType) -> Result<()> {
        match policy {
            PolicyType::ClosedLoop => Ok(()),
            PolicyType::FixedRate { rate } => {
                if *rate <= 0.0 {
                    bail!("FixedRate rate must be > 0");
                }
                Ok(())
            }
            PolicyType::Poisson { rate } => {
                if *rate <= 0.0 {
                    bail!("Poisson rate must be > 0");
                }
                Ok(())
            }
            PolicyType::Adaptive { initial_rate } => {
                if *initial_rate <= 0.0 {
                    bail!("Adaptive initial_rate must be > 0");
                }
                Ok(())
            }
        }
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

impl LoadPatternConfig {
    pub fn to_rate_control(&self) -> xylem_core::workload::RateControl {
        use xylem_core::workload::RateControl;

        // For now, we only support Constant rate
        // Full load pattern support requires scheduler integration
        match self {
            Self::Constant { rate } => RateControl::Fixed { rate: *rate },
            Self::Ramp { start_rate, .. } => {
                // TODO: Implement ramping in scheduler
                RateControl::Fixed { rate: *start_rate }
            }
            Self::Spike { normal_rate, .. } => {
                // TODO: Implement spike pattern in scheduler
                RateControl::Fixed { rate: *normal_rate }
            }
            Self::Sinusoidal { base_rate, .. } => {
                // TODO: Implement sinusoidal pattern in scheduler
                RateControl::Fixed { rate: *base_rate }
            }
            Self::Step { steps } => {
                // TODO: Implement step pattern in scheduler
                // For now, use first step's rate
                if let Some(first_step) = steps.first() {
                    RateControl::Fixed { rate: first_step.rate }
                } else {
                    RateControl::ClosedLoop
                }
            }
            Self::Sawtooth { min_rate, .. } => {
                // TODO: Implement sawtooth pattern in scheduler
                RateControl::Fixed { rate: *min_rate }
            }
        }
    }
}
