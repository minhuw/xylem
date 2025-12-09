//! Protocol-specific configuration types
//!
//! This module contains configuration structures for each built-in protocol.
//! Custom protocols should define their own config types implementing
//! `Serialize + DeserializeOwned`.

use serde::{Deserialize, Serialize};

/// Default key prefix for key-value protocols
fn default_key_prefix() -> String {
    "key:".to_string()
}

/// Key generation strategy configuration for key-value protocols (Redis, Memcached)
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "strategy", rename_all = "lowercase")]
pub enum KeysConfig {
    /// Sequential keys starting from a given value
    Sequential {
        /// Starting key value
        #[serde(default)]
        start: u64,
        /// Value size in bytes
        value_size: usize,
        /// Key prefix (default: "key:")
        #[serde(default = "default_key_prefix")]
        prefix: String,
    },
    /// Random keys within a range
    Random {
        /// Maximum key value (exclusive)
        max: u64,
        /// Value size in bytes
        value_size: usize,
        /// Key prefix (default: "key:")
        #[serde(default = "default_key_prefix")]
        prefix: String,
    },
    /// Round-robin through keys
    #[serde(rename = "round-robin")]
    RoundRobin {
        /// Maximum key value (exclusive)
        max: u64,
        /// Value size in bytes
        value_size: usize,
        /// Key prefix (default: "key:")
        #[serde(default = "default_key_prefix")]
        prefix: String,
    },
    /// Zipfian distribution (power-law)
    Zipfian {
        /// Number of keys in range [0, n-1]
        n: u64,
        /// Exponent (theta) controlling skewness
        theta: f64,
        /// Value size in bytes
        value_size: usize,
        /// Key prefix (default: "key:")
        #[serde(default = "default_key_prefix")]
        prefix: String,
    },
    /// Gaussian (normal) distribution
    Gaussian {
        /// Mean as percentage of keyspace (0.0 to 1.0)
        mean_pct: f64,
        /// Standard deviation as percentage of keyspace (0.0 to 1.0)
        std_dev_pct: f64,
        /// Maximum key value (keyspace size)
        max: u64,
        /// Value size in bytes
        value_size: usize,
        /// Key prefix (default: "key:")
        #[serde(default = "default_key_prefix")]
        prefix: String,
    },
}

impl KeysConfig {
    /// Get the value size from this key configuration
    pub fn value_size(&self) -> usize {
        match self {
            Self::Sequential { value_size, .. } => *value_size,
            Self::Random { value_size, .. } => *value_size,
            Self::RoundRobin { value_size, .. } => *value_size,
            Self::Zipfian { value_size, .. } => *value_size,
            Self::Gaussian { value_size, .. } => *value_size,
        }
    }

    /// Get the key prefix from this key configuration
    pub fn prefix(&self) -> &str {
        match self {
            Self::Sequential { prefix, .. } => prefix,
            Self::Random { prefix, .. } => prefix,
            Self::RoundRobin { prefix, .. } => prefix,
            Self::Zipfian { prefix, .. } => prefix,
            Self::Gaussian { prefix, .. } => prefix,
        }
    }

    /// Get the keyspace size (number of unique keys) from this configuration.
    /// For Sequential, returns None since keyspace is unbounded.
    /// For other strategies, returns the configured max/n value.
    pub fn keyspace_size(&self) -> Option<u64> {
        match self {
            Self::Sequential { .. } => None, // Unbounded
            Self::Random { max, .. } => Some(*max),
            Self::RoundRobin { max, .. } => Some(*max),
            Self::Zipfian { n, .. } => Some(*n),
            Self::Gaussian { max, .. } => Some(*max),
        }
    }

    /// Convert to a KeyGeneration instance for protocol-embedded workload
    pub fn to_key_gen(
        &self,
        master_seed: Option<u64>,
    ) -> anyhow::Result<crate::workload::KeyGeneration> {
        use crate::workload::KeyGeneration;
        use xylem_core::seed::{components, derive_seed};

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

    /// Validate the keys configuration
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::Sequential { value_size, .. } => {
                if *value_size == 0 {
                    anyhow::bail!("value_size must be > 0");
                }
            }
            Self::Random { max, value_size, .. } => {
                if *max == 0 {
                    anyhow::bail!("Random max must be > 0");
                }
                if *value_size == 0 {
                    anyhow::bail!("value_size must be > 0");
                }
            }
            Self::RoundRobin { max, value_size, .. } => {
                if *max == 0 {
                    anyhow::bail!("RoundRobin max must be > 0");
                }
                if *value_size == 0 {
                    anyhow::bail!("value_size must be > 0");
                }
            }
            Self::Zipfian { n, theta, value_size, .. } => {
                if *n == 0 {
                    anyhow::bail!("Zipfian n must be > 0");
                }
                if *theta <= 0.0 {
                    anyhow::bail!("Zipfian theta must be > 0");
                }
                if *value_size == 0 {
                    anyhow::bail!("value_size must be > 0");
                }
            }
            Self::Gaussian {
                mean_pct, std_dev_pct, max, value_size, ..
            } => {
                if *max == 0 {
                    anyhow::bail!("Gaussian max must be > 0");
                }
                if *value_size == 0 {
                    anyhow::bail!("value_size must be > 0");
                }
                if *mean_pct < 0.0 || *mean_pct > 1.0 {
                    anyhow::bail!("Gaussian mean_pct must be in [0.0, 1.0]");
                }
                if *std_dev_pct < 0.0 || *std_dev_pct > 1.0 {
                    anyhow::bail!("Gaussian std_dev_pct must be in [0.0, 1.0]");
                }
            }
        }
        Ok(())
    }
}

impl Default for KeysConfig {
    fn default() -> Self {
        Self::Sequential {
            start: 0,
            value_size: 64,
            prefix: default_key_prefix(),
        }
    }
}

/// Redis protocol configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RedisConfig {
    /// Key generation strategy
    #[serde(default)]
    pub keys: KeysConfig,
    /// Operations/command configuration
    #[serde(default)]
    pub operations: Option<RedisOperationsConfig>,
    /// Value size configuration (overrides keys.value_size for variable sizes)
    #[serde(default)]
    pub value_size: Option<ValueSizeConfig>,
    /// Use random data for values instead of repeated 'x' characters
    /// When true, generates pseudo-random bytes for each request
    #[serde(default)]
    pub random_data: bool,
    /// Data import configuration (use real data from CSV)
    #[serde(default)]
    pub data_import: Option<DataImportConfig>,
    /// Redis Cluster configuration (only for redis-cluster protocol)
    #[serde(default)]
    pub redis_cluster: Option<RedisClusterConfig>,
    /// Insert phase configuration (populate keys before measurement)
    ///
    /// When configured, the protocol will first insert all keys using SET
    /// before starting the normal workload. This is useful for benchmarks
    /// that need pre-populated data (e.g., GET workloads).
    #[serde(default)]
    pub insert_phase: Option<InsertPhaseConfig>,
}

/// Redis Cluster configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RedisClusterConfig {
    /// Cluster nodes with their slot assignments
    pub nodes: Vec<RedisClusterNodeConfig>,
}

/// Redis Cluster node configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RedisClusterNodeConfig {
    /// Node address (e.g., "127.0.0.1:7000")
    pub address: String,
    /// Start of slot range (0-16383)
    pub slot_start: u16,
    /// End of slot range (0-16383)
    pub slot_end: u16,
}

/// Value size configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
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
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
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

impl CommandValueSizeConfig {
    /// Convert to a ValueSizeGenerator
    pub fn to_generator(
        &self,
        master_seed: Option<u64>,
    ) -> anyhow::Result<Box<dyn crate::workload::ValueSizeGenerator>> {
        use crate::workload::{FixedSize, NormalSize, UniformSize};
        use xylem_core::seed::derive_seed;

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

impl ValueSizeConfig {
    /// Get a representative fixed size from this config.
    /// For Fixed, returns the size. For Uniform, returns mean. For Normal, returns mean (as usize).
    /// For PerCommand, returns the default's fixed size.
    pub fn fixed_size(&self) -> usize {
        match self {
            Self::Fixed { size } => *size,
            Self::Uniform { min, max } => (*min + *max) / 2,
            Self::Normal { mean, .. } => *mean as usize,
            Self::PerCommand { default, .. } => default.fixed_size(),
        }
    }

    /// Convert to a ValueSizeGenerator
    pub fn to_generator(
        &self,
        master_seed: Option<u64>,
    ) -> anyhow::Result<Box<dyn crate::workload::ValueSizeGenerator>> {
        use crate::workload::{FixedSize, NormalSize, PerCommandSize, UniformSize};
        use xylem_core::seed::derive_seed;

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

/// Operations configuration for Redis (command selection)
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "strategy", rename_all = "lowercase")]
pub enum RedisOperationsConfig {
    /// Fixed operation (single command)
    Fixed { operation: String },
    /// Weighted random selection
    Weighted { commands: Vec<RedisCommandWeight> },
}

/// Command weight configuration for Redis
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RedisCommandWeight {
    /// Command name: "get", "set", "incr", "mget", "wait", or "custom"
    pub name: String,
    /// Weight (probability) for this command
    pub weight: f64,
    /// Additional parameters for specific commands
    #[serde(default)]
    pub params: Option<RedisCommandParams>,
    /// Optional per-command key distribution
    #[serde(default)]
    pub keys: Option<KeysConfig>,
}

/// Additional parameters for specific Redis command types
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum RedisCommandParams {
    /// MGET parameters
    MGet { count: usize },
    /// WAIT parameters
    Wait { num_replicas: usize, timeout_ms: u64 },
    /// SCAN parameters
    Scan {
        #[serde(default)]
        cursor: u64,
        count: Option<usize>,
        pattern: Option<String>,
    },
    /// Custom command template
    Custom { template: String },
}

/// Data import configuration for loading test data from CSV
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DataImportConfig {
    /// Path to CSV file containing test data
    pub file: std::path::PathBuf,
    /// Verification mode
    #[serde(default)]
    pub verification: Option<VerificationConfig>,
}

/// Verification configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct VerificationConfig {
    /// When to verify: "during", "after", or "only"
    #[serde(default = "default_verification_mode")]
    pub mode: String,
    /// Sample rate for "during" mode (0.0-1.0)
    #[serde(default = "default_sample_rate")]
    pub sample_rate: f64,
}

fn default_verification_mode() -> String {
    "after".to_string()
}

fn default_sample_rate() -> f64 {
    0.1
}

/// Memcached protocol configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct MemcachedConfig {
    /// Key generation strategy
    #[serde(default)]
    pub keys: KeysConfig,
    /// Operation to execute (default: GET)
    #[serde(default = "default_memcached_operation")]
    pub operation: String,
    /// Insert phase configuration (populate keys before measurement)
    ///
    /// When configured, the protocol will first insert all keys using SET
    /// before starting the normal workload. This is useful for benchmarks
    /// that need pre-populated data (e.g., GET workloads).
    #[serde(default)]
    pub insert_phase: Option<InsertPhaseConfig>,
}

fn default_memcached_operation() -> String {
    "GET".to_string()
}

impl Default for MemcachedConfig {
    fn default() -> Self {
        Self {
            keys: KeysConfig::default(),
            operation: default_memcached_operation(),
            insert_phase: None,
        }
    }
}

/// HTTP protocol configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct HttpConfig {
    /// HTTP method (GET, POST, PUT)
    #[serde(default = "default_http_method")]
    pub method: String,
    /// Request path (e.g., "/api/endpoint")
    #[serde(default = "default_http_path")]
    pub path: String,
    /// Host header value
    #[serde(default)]
    pub host: Option<String>,
    /// Request body size for POST/PUT (bytes)
    #[serde(default = "default_body_size")]
    pub body_size: usize,
}

fn default_http_method() -> String {
    "GET".to_string()
}

fn default_http_path() -> String {
    "/".to_string()
}

fn default_body_size() -> usize {
    64
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            method: default_http_method(),
            path: default_http_path(),
            host: None,
            body_size: default_body_size(),
        }
    }
}

/// Xylem Echo protocol configuration (for testing)
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct XylemEchoConfig {
    /// Message size in bytes
    #[serde(default = "default_echo_size")]
    pub message_size: usize,
}

fn default_echo_size() -> usize {
    64
}

/// Masstree protocol configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct MasstreeConfig {
    /// Key generation configuration
    #[serde(default)]
    pub keys: KeysConfig,
    /// Operation to perform (get, set, put, remove, scan, checkpoint)
    #[serde(default = "default_masstree_operation")]
    pub operation: String,
    /// Number of columns for PUT operation
    #[serde(default)]
    pub put_columns: Option<usize>,
    /// Scan configuration
    #[serde(default)]
    pub scan: Option<MasstreeScanConfig>,
    /// Value size configuration (overrides keys.value_size for variable sizes)
    #[serde(default)]
    pub value_size: Option<ValueSizeConfig>,
    /// Use random data for values instead of repeated 'x' characters
    #[serde(default)]
    pub random_data: bool,
    /// Insert phase configuration (populate keys before measurement)
    ///
    /// When configured, the protocol will first insert all keys before
    /// starting the normal workload. This is useful for benchmarks that
    /// need pre-populated data (e.g., GET workloads).
    #[serde(default)]
    pub insert_phase: Option<InsertPhaseConfig>,
}

/// Insert phase configuration for populating data before measurement
///
/// During the insert phase:
/// - All requests are tagged as "warmup" (stats not collected)
/// - Keys [0, key_count) are inserted sequentially
/// - After all keys are inserted, normal measurement begins
///
/// Both fields are optional and default to values from the `keys` config:
/// - `key_count` defaults to the keyspace size from keys config
/// - `value_size` defaults to `keys.value_size`
///
/// Example configurations:
/// ```toml
/// # Minimal - use defaults from keys config
/// insert_phase = {}
///
/// # Override key count only (insert fewer keys than keyspace)
/// insert_phase = { key_count = 100000 }
///
/// # Override both
/// insert_phase = { key_count = 100000, value_size = 128 }
/// ```
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct InsertPhaseConfig {
    /// Number of keys to insert before measurement begins.
    /// Defaults to keyspace size from keys config (n for Zipfian, max for Random, etc.)
    #[serde(default)]
    pub key_count: Option<u64>,
    /// Value size for inserted keys.
    /// Defaults to keys.value_size if not specified.
    #[serde(default)]
    pub value_size: Option<usize>,
}

impl InsertPhaseConfig {
    /// Resolve key_count with fallback to provided default
    pub fn key_count_or(&self, default: u64) -> u64 {
        self.key_count.unwrap_or(default)
    }

    /// Resolve value_size with fallback to provided default
    pub fn value_size_or(&self, default: usize) -> usize {
        self.value_size.unwrap_or(default)
    }
}

/// Masstree scan configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct MasstreeScanConfig {
    /// Number of records to scan
    #[serde(default = "default_scan_count")]
    pub count: usize,
    /// Fields to return (empty = all fields)
    #[serde(default)]
    pub fields: Vec<String>,
}

fn default_masstree_operation() -> String {
    "get".to_string()
}

fn default_scan_count() -> usize {
    10
}

impl Default for MasstreeConfig {
    fn default() -> Self {
        Self {
            keys: KeysConfig::default(),
            operation: default_masstree_operation(),
            put_columns: None,
            scan: None,
            value_size: None,
            random_data: false,
            insert_phase: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_config_default() {
        let config = RedisConfig::default();
        assert!(config.operations.is_none()); // Default to GET at runtime
        assert_eq!(config.keys.value_size(), 64);
    }

    #[test]
    fn test_redis_config_deserialize() {
        let json = r#"{
            "keys": {
                "strategy": "zipfian",
                "n": 1000000,
                "theta": 0.99,
                "value_size": 128
            },
            "operations": {
                "strategy": "fixed",
                "operation": "SET"
            }
        }"#;

        let config: RedisConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(
            config.operations,
            Some(RedisOperationsConfig::Fixed { ref operation }) if operation == "SET"
        ));
        assert_eq!(config.keys.value_size(), 128);
    }

    #[test]
    fn test_http_config_default() {
        let config = HttpConfig::default();
        assert_eq!(config.method, "GET");
        assert_eq!(config.path, "/");
    }

    #[test]
    fn test_http_config_deserialize() {
        let json = r#"{
            "method": "POST",
            "path": "/api/data",
            "host": "example.com",
            "body_size": 1024
        }"#;

        let config: HttpConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.method, "POST");
        assert_eq!(config.path, "/api/data");
        assert_eq!(config.host, Some("example.com".to_string()));
        assert_eq!(config.body_size, 1024);
    }

    #[test]
    fn test_keys_config_variants() {
        let sequential: KeysConfig =
            serde_json::from_str(r#"{"strategy": "sequential", "start": 100, "value_size": 32}"#)
                .unwrap();
        assert_eq!(sequential.value_size(), 32);

        let random: KeysConfig =
            serde_json::from_str(r#"{"strategy": "random", "max": 1000, "value_size": 64}"#)
                .unwrap();
        assert_eq!(random.value_size(), 64);

        let zipfian: KeysConfig = serde_json::from_str(
            r#"{"strategy": "zipfian", "n": 100000, "theta": 0.99, "value_size": 128}"#,
        )
        .unwrap();
        assert_eq!(zipfian.value_size(), 128);
    }

    #[test]
    fn test_gaussian_keys_config() {
        let gaussian: KeysConfig = serde_json::from_str(
            r#"{"strategy": "gaussian", "mean_pct": 0.5, "std_dev_pct": 0.1, "max": 1000000, "value_size": 256}"#,
        )
        .unwrap();
        assert_eq!(gaussian.value_size(), 256);

        // Validate it
        gaussian.validate().expect("Gaussian config should be valid");

        // Convert to key generator
        let mut keygen = gaussian.to_key_gen(Some(42)).expect("Failed to create key generator");
        for _ in 0..100 {
            let key = keygen.next_key();
            assert!(key < 1000000, "Key {} out of range", key);
        }
    }

    #[test]
    fn test_gaussian_keys_config_validation() {
        // Invalid mean_pct (> 1.0)
        let invalid: KeysConfig = serde_json::from_str(
            r#"{"strategy": "gaussian", "mean_pct": 1.5, "std_dev_pct": 0.1, "max": 1000, "value_size": 64}"#,
        )
        .unwrap();
        assert!(invalid.validate().is_err());

        // Invalid std_dev_pct (< 0.0)
        let invalid: KeysConfig = serde_json::from_str(
            r#"{"strategy": "gaussian", "mean_pct": 0.5, "std_dev_pct": -0.1, "max": 1000, "value_size": 64}"#,
        )
        .unwrap();
        assert!(invalid.validate().is_err());

        // Invalid max (= 0)
        let invalid: KeysConfig = serde_json::from_str(
            r#"{"strategy": "gaussian", "mean_pct": 0.5, "std_dev_pct": 0.1, "max": 0, "value_size": 64}"#,
        )
        .unwrap();
        assert!(invalid.validate().is_err());

        // Invalid value_size (= 0)
        let invalid: KeysConfig = serde_json::from_str(
            r#"{"strategy": "gaussian", "mean_pct": 0.5, "std_dev_pct": 0.1, "max": 1000, "value_size": 0}"#,
        )
        .unwrap();
        assert!(invalid.validate().is_err());
    }
}
