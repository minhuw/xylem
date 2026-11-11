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
#[serde(tag = "strategy", rename_all = "lowercase", deny_unknown_fields)]
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
        self.validate()?;
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
                if !theta.is_finite() || *theta <= 0.0 {
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
                if !mean_pct.is_finite() || !(0.0..=1.0).contains(mean_pct) {
                    anyhow::bail!("Gaussian mean_pct must be in [0.0, 1.0]");
                }
                if !std_dev_pct.is_finite() || !(0.0..=1.0).contains(std_dev_pct) {
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
#[serde(deny_unknown_fields)]
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
    /// Insert phase configuration (populate keys before measurement)
    ///
    /// When configured, the protocol will first insert all keys using SET
    /// before starting the normal workload. This is useful for benchmarks
    /// that need pre-populated data (e.g., GET workloads).
    #[serde(default)]
    pub insert_phase: Option<InsertPhaseConfig>,
    /// Authenticate every connection with AUTH when it is established,
    /// before any other request
    #[serde(default)]
    pub auth: Option<RedisAuthConfig>,
    /// Database every connection selects with SELECT when it is established
    /// (default: none, so the server's default database 0)
    #[serde(default)]
    pub db: Option<u32>,
    /// RESP version: 2 or 3. 3 sends HELLO 3 when a connection is
    /// established. Default: no HELLO, so RESP2.
    #[serde(default)]
    pub resp: Option<u8>,
}

/// Credentials a Redis connection sends with AUTH when it is established
///
/// Set exactly one of `password` and `password_env`. Debug output never
/// shows the password.
#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RedisAuthConfig {
    /// ACL username (Redis 6+), sent as `AUTH <username> <password>`.
    /// Without it, `AUTH <password>` authenticates against requirepass.
    #[serde(default)]
    pub username: Option<String>,
    /// Password, written in the profile
    #[serde(default)]
    pub password: Option<String>,
    /// Name of the environment variable that holds the password, read
    /// when the run starts
    #[serde(default)]
    pub password_env: Option<String>,
}

impl std::fmt::Debug for RedisAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisAuthConfig")
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("password_env", &self.password_env)
            .finish()
    }
}

impl RedisAuthConfig {
    /// The password, from the profile or from the environment variable
    /// `password_env` names. Errors name the variable, never the password.
    pub fn resolve_password(&self) -> anyhow::Result<String> {
        match (&self.password, &self.password_env) {
            (Some(password), None) => Ok(password.clone()),
            (None, Some(name)) => {
                anyhow::ensure!(!name.is_empty(), "Redis auth.password_env must not be empty");
                match std::env::var(name) {
                    Ok(password) => Ok(password),
                    Err(std::env::VarError::NotPresent) => anyhow::bail!(
                        "Redis auth.password_env: environment variable {name} is not set"
                    ),
                    Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!(
                        "Redis auth.password_env: environment variable {name} is not valid UTF-8"
                    ),
                }
            }
            (Some(_), Some(_)) => {
                anyhow::bail!("Redis auth sets both password and password_env; set exactly one")
            }
            (None, None) => {
                anyhow::bail!("Redis auth needs a password or password_env; set exactly one")
            }
        }
    }
}

/// Value size configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "strategy", rename_all = "lowercase", deny_unknown_fields)]
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
    /// Sizes drawn from a weighted list, such as an empirical size
    /// histogram; weights need not sum to 1
    Weighted { sizes: Vec<SizeWeight> },
    /// Per-command size configuration
    #[serde(rename = "per_command")]
    PerCommand {
        /// Size strategies for specific commands
        commands: std::collections::HashMap<String, CommandValueSizeConfig>,
        /// Default strategy for unspecified commands
        default: Box<ValueSizeConfig>,
    },
}

/// One size of a weighted size list
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SizeWeight {
    /// Value size in bytes
    pub size: usize,
    /// Relative weight (probability) of this size
    pub weight: f64,
}

/// The `(size, weight)` pairs of a weighted size list
fn size_weights(sizes: &[SizeWeight]) -> Vec<(usize, f64)> {
    sizes.iter().map(|s| (s.size, s.weight)).collect()
}

/// Value size configuration for a specific command
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "distribution", rename_all = "lowercase", deny_unknown_fields)]
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
    /// Sizes drawn from a weighted list
    Weighted {
        sizes: Vec<SizeWeight>,
    },
}

impl CommandValueSizeConfig {
    /// Convert to a ValueSizeGenerator
    pub fn to_generator(
        &self,
        master_seed: Option<u64>,
    ) -> anyhow::Result<Box<dyn crate::workload::ValueSizeGenerator>> {
        use crate::workload::{FixedSize, NormalSize, UniformSize, WeightedSize};
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
            Self::Weighted { sizes } => {
                let seed = master_seed.map(|s| derive_seed(s, "weighted_cmd_size"));
                Ok(Box::new(WeightedSize::with_seed(size_weights(sizes), seed)?))
            }
        }
    }

    /// The largest size this config can produce
    pub fn max_size(&self) -> usize {
        match self {
            Self::Fixed { size } => *size,
            Self::Uniform { max, .. } | Self::Normal { max, .. } => *max,
            Self::Weighted { sizes } => sizes.iter().map(|s| s.size).max().unwrap_or(0),
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
            Self::Uniform { min, max } => min / 2 + max / 2 + (min % 2 + max % 2) / 2,
            Self::Normal { mean, .. } => *mean as usize,
            Self::Weighted { sizes } => {
                let total: f64 = sizes.iter().map(|s| s.weight).sum();
                let mean: f64 = sizes.iter().map(|s| s.size as f64 * s.weight).sum::<f64>() / total;
                mean.round() as usize
            }
            Self::PerCommand { default, .. } => default.fixed_size(),
        }
    }

    /// The largest size this config can produce, for any command
    pub fn max_size(&self) -> usize {
        match self {
            Self::Fixed { size } => *size,
            Self::Uniform { max, .. } | Self::Normal { max, .. } => *max,
            Self::Weighted { sizes } => sizes.iter().map(|s| s.size).max().unwrap_or(0),
            Self::PerCommand { commands, default } => commands
                .values()
                .map(CommandValueSizeConfig::max_size)
                .fold(default.max_size(), usize::max),
        }
    }

    /// Convert to a ValueSizeGenerator
    pub fn to_generator(
        &self,
        master_seed: Option<u64>,
    ) -> anyhow::Result<Box<dyn crate::workload::ValueSizeGenerator>> {
        use crate::workload::{FixedSize, NormalSize, PerCommandSize, UniformSize, WeightedSize};
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
            Self::Weighted { sizes } => {
                let seed = master_seed.map(|s| derive_seed(s, "weighted_value_size"));
                Ok(Box::new(WeightedSize::with_seed(size_weights(sizes), seed)?))
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
#[serde(tag = "strategy", rename_all = "lowercase", deny_unknown_fields)]
pub enum RedisOperationsConfig {
    /// Fixed operation (single command)
    Fixed { operation: String },
    /// Weighted random selection
    Weighted { commands: Vec<RedisCommandWeight> },
}

/// Command weight configuration for Redis
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RedisCommandWeight {
    /// Command name: "get", "set", "incr", "mget", "setex", "setrange",
    /// "getrange", "scan", "wait", "multi", "exec", "discard", or "custom"
    /// with `params.template` for any other command
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
#[serde(untagged, deny_unknown_fields)]
pub enum RedisCommandParams {
    /// MGET parameters
    MGet { count: usize },
    /// WAIT parameters
    Wait { num_replicas: usize, timeout_ms: u64 },
    /// Custom command template. It precedes SCAN, whose fields are all
    /// optional and would otherwise match any table, `template` included.
    Custom { template: String },
    /// SCAN parameters
    Scan {
        #[serde(default)]
        cursor: u64,
        count: Option<usize>,
        pattern: Option<String>,
    },
}

impl RedisCommandParams {
    /// The `params` keys the Redis command `name` (case-insensitive) takes,
    /// or None if `name` is no command. The params shapes are untagged, so
    /// only the command tells which keys belong to it.
    pub fn keys(name: &str) -> Option<&'static [&'static str]> {
        Some(match name.to_ascii_lowercase().as_str() {
            "mget" => &["count"],
            "wait" => &["num_replicas", "timeout_ms"],
            "custom" => &["template"],
            "scan" => &["cursor", "count", "pattern"],
            "get" | "set" | "incr" | "setex" | "setrange" | "getrange" | "multi" | "exec"
            | "discard" => &[],
            _ => return None,
        })
    }
}

/// Operation selection of a key-value protocol, in the shape of Redis's
/// `operations`: one operation, or a weighted random mix of commands. `P`
/// are the protocol's per-command parameters.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "strategy", rename_all = "lowercase", deny_unknown_fields)]
pub enum OperationsConfig<P> {
    /// Fixed operation (single command, default parameters)
    Fixed { operation: String },
    /// Weighted random selection
    Weighted { commands: Vec<OperationWeight<P>> },
}

/// One command of a weighted operation mix
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields, bound(deserialize = "P: Deserialize<'de>"))]
pub struct OperationWeight<P> {
    /// Command name
    pub name: String,
    /// Weight (probability) for this command; weights need not sum to 1
    pub weight: f64,
    /// Parameters of the command
    #[serde(default)]
    pub params: Option<P>,
    /// Optional per-command key distribution, with the protocol's prefix and
    /// value size
    #[serde(default)]
    pub keys: Option<KeysConfig>,
}

impl<P> OperationsConfig<P> {
    /// The operation mix of a config that takes either one `operation` or an
    /// `operations` mix: `default` when it sets neither, an error when both
    pub fn resolve<'a>(
        operation: Option<&str>,
        operations: Option<&'a Self>,
        default: &str,
    ) -> anyhow::Result<std::borrow::Cow<'a, Self>>
    where
        P: Clone,
    {
        use std::borrow::Cow;
        let fixed = |operation: &str| Cow::Owned(Self::Fixed { operation: operation.to_string() });
        match (operation, operations) {
            (Some(_), Some(_)) => anyhow::bail!("Set either operation or operations, not both"),
            (Some(operation), None) => Ok(fixed(operation)),
            (None, Some(operations)) => Ok(Cow::Borrowed(operations)),
            (None, None) => Ok(fixed(default)),
        }
    }

    /// Build the command selector of this mix: `parse` turns a command name
    /// and its params into an operation; per-command `keys` must keep the
    /// prefix and value size of `keys`
    pub fn selector<T: Clone + Send + 'static>(
        &self,
        keys: &KeysConfig,
        seed: Option<u64>,
        mut parse: impl FnMut(&str, Option<&P>) -> anyhow::Result<T>,
    ) -> anyhow::Result<Box<dyn crate::CommandSelector<T>>> {
        match self {
            Self::Fixed { operation } => {
                Ok(Box::new(crate::FixedCommandSelector::new(parse(operation, None)?)))
            }
            Self::Weighted { commands } => {
                let commands = commands
                    .iter()
                    .map(|cmd| {
                        let op = parse(&cmd.name, cmd.params.as_ref())?;
                        Ok((op, cmd.weight, cmd.keys.as_ref()))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                crate::workload::selector::weighted_selector(commands, keys, seed)
            }
        }
    }
}

/// Memcached operation mix
pub type MemcachedOperationsConfig = OperationsConfig<MemcachedCommandParams>;

/// Parameters of a Memcached command in an operation mix; each command
/// takes only its own
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct MemcachedCommandParams {
    /// Keys per multi-get (`mget`, default 10)
    #[serde(default)]
    pub count: Option<usize>,
    /// Amount to add or subtract (`incr` and `decr`, default 1)
    #[serde(default)]
    pub delta: Option<u64>,
    /// New expiration time in seconds, 0 for none (`touch`, default 0)
    #[serde(default)]
    pub exptime: Option<u32>,
}

/// Memcached protocol configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct MemcachedConfig {
    /// Key generation strategy
    #[serde(default)]
    pub keys: KeysConfig,
    /// Operation every request executes (default: GET): get, set, add,
    /// replace, delete, incr, decr, touch or mget. Set this or `operations`.
    #[serde(default)]
    pub operation: Option<String>,
    /// Operation mix, a fixed operation or weighted commands. Set this or
    /// `operation`.
    #[serde(default)]
    pub operations: Option<MemcachedOperationsConfig>,
    /// Value size of set, add and replace, also per command (overrides
    /// keys.value_size)
    #[serde(default)]
    pub value_size: Option<ValueSizeConfig>,
    /// Insert phase configuration (populate keys before measurement)
    ///
    /// When configured, the protocol will first insert all keys using SET
    /// before starting the normal workload. This is useful for benchmarks
    /// that need pre-populated data (e.g., GET workloads).
    #[serde(default)]
    pub insert_phase: Option<InsertPhaseConfig>,
}

/// HTTP protocol configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
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
///
/// The echo protocol has no options: its messages are always 16 bytes.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct XylemEchoConfig {}

/// Masstree protocol configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct MasstreeConfig {
    /// Key generation configuration
    #[serde(default)]
    pub keys: KeysConfig,
    /// Operation every request performs (default: get): get, set, put,
    /// remove, scan or checkpoint. Set this or `operations`.
    #[serde(default)]
    pub operation: Option<String>,
    /// Operation mix, a fixed operation or weighted commands. Set this or
    /// `operation`.
    #[serde(default)]
    pub operations: Option<MasstreeOperationsConfig>,
    /// Number of columns for PUT operation (default 1)
    #[serde(default)]
    pub put_columns: Option<usize>,
    /// Scan configuration
    #[serde(default)]
    pub scan: Option<MasstreeScanConfig>,
    /// Value size of set and of each put column, also per command
    /// (overrides keys.value_size)
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
#[serde(deny_unknown_fields)]
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
    /// Resolve insertion defaults; unbounded keyspaces require an explicit count.
    pub fn resolve(&self, keys: &KeysConfig) -> anyhow::Result<(u64, usize)> {
        let count = self.key_count.or_else(|| keys.keyspace_size()).ok_or_else(|| {
            anyhow::anyhow!("insert_phase.key_count is required for sequential keys")
        })?;
        Ok((count, self.value_size.unwrap_or_else(|| keys.value_size())))
    }
}

/// Masstree scan configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct MasstreeScanConfig {
    /// Number of records to scan
    #[serde(default = "default_scan_count")]
    pub count: usize,
    /// Column indices to return, as decimal strings (empty = all columns)
    #[serde(default)]
    pub fields: Vec<String>,
}

/// Masstree operation mix
pub type MasstreeOperationsConfig = OperationsConfig<MasstreeCommandParams>;

/// Parameters of a Masstree command in an operation mix; each command takes
/// only its own, and defaults to the top-level `scan` and `put_columns`
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct MasstreeCommandParams {
    /// Records per scan (`scan`)
    #[serde(default)]
    pub count: Option<usize>,
    /// Column indices a scan returns, empty for all (`scan`)
    #[serde(default)]
    pub fields: Option<Vec<u32>>,
    /// Columns a put writes (`put`)
    #[serde(default)]
    pub columns: Option<usize>,
}

fn default_scan_count() -> usize {
    10
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

    /// `config` parses as `T`, and fails with an unknown-field error once it
    /// has an extra key
    fn rejects_unknown_keys<T: serde::de::DeserializeOwned>(config: serde_json::Value) {
        serde_json::from_value::<T>(config.clone()).unwrap();
        let mut with_unknown = config.clone();
        with_unknown["bogus"] = 1.into();
        let Err(error) = serde_json::from_value::<T>(with_unknown) else {
            panic!("{config} accepts an unknown key");
        };
        assert!(error.to_string().contains("unknown field `bogus`"), "{config}: {error}");
    }

    #[test]
    fn every_protocol_config_rejects_unknown_keys() {
        use serde_json::json;
        rejects_unknown_keys::<RedisConfig>(json!({}));
        rejects_unknown_keys::<RedisAuthConfig>(json!({ "password": "pw" }));
        rejects_unknown_keys::<MemcachedConfig>(json!({}));
        rejects_unknown_keys::<HttpConfig>(json!({}));
        rejects_unknown_keys::<XylemEchoConfig>(json!({}));
        rejects_unknown_keys::<MasstreeConfig>(json!({}));
        rejects_unknown_keys::<MasstreeScanConfig>(json!({}));
        rejects_unknown_keys::<InsertPhaseConfig>(json!({}));
        rejects_unknown_keys::<RedisCommandWeight>(json!({ "name": "get", "weight": 1.0 }));

        // Every variant of the tagged enums
        for keys in [
            json!({ "strategy": "sequential", "value_size": 8 }),
            json!({ "strategy": "random", "max": 10, "value_size": 8 }),
            json!({ "strategy": "round-robin", "max": 10, "value_size": 8 }),
            json!({ "strategy": "zipfian", "n": 10, "theta": 0.99, "value_size": 8 }),
            json!({
                "strategy": "gaussian", "mean_pct": 0.5, "std_dev_pct": 0.1, "max": 10,
                "value_size": 8
            }),
        ] {
            rejects_unknown_keys::<KeysConfig>(keys);
        }
        for size in [
            json!({ "strategy": "fixed", "size": 8 }),
            json!({ "strategy": "uniform", "min": 1, "max": 8 }),
            json!({ "strategy": "normal", "mean": 4.0, "std_dev": 1.0, "min": 1, "max": 8 }),
            json!({ "strategy": "weighted", "sizes": [] }),
            json!({
                "strategy": "per_command", "commands": {},
                "default": { "strategy": "fixed", "size": 8 }
            }),
        ] {
            rejects_unknown_keys::<ValueSizeConfig>(size);
        }
        for size in [
            json!({ "distribution": "fixed", "size": 8 }),
            json!({ "distribution": "uniform", "min": 1, "max": 8 }),
            json!({ "distribution": "normal", "mean": 4.0, "std_dev": 1.0, "min": 1, "max": 8 }),
            json!({ "distribution": "weighted", "sizes": [] }),
        ] {
            rejects_unknown_keys::<CommandValueSizeConfig>(size);
        }
        rejects_unknown_keys::<SizeWeight>(json!({ "size": 8, "weight": 1.0 }));
        rejects_unknown_keys::<MemcachedCommandParams>(json!({}));
        rejects_unknown_keys::<MasstreeCommandParams>(json!({}));
        for operations in [
            json!({ "strategy": "fixed", "operation": "get" }),
            json!({ "strategy": "weighted", "commands": [] }),
        ] {
            rejects_unknown_keys::<MemcachedOperationsConfig>(operations.clone());
            rejects_unknown_keys::<MasstreeOperationsConfig>(operations);
        }
        rejects_unknown_keys::<OperationWeight<MemcachedCommandParams>>(
            json!({ "name": "get", "weight": 1.0 }),
        );
        for operations in [
            json!({ "strategy": "fixed", "operation": "get" }),
            json!({ "strategy": "weighted", "commands": [] }),
        ] {
            rejects_unknown_keys::<RedisOperationsConfig>(operations);
        }
    }

    #[test]
    fn redis_command_params_reject_keys_of_no_shape() {
        use serde_json::json;
        // The shapes are untagged, so an unknown key leaves no shape to match
        for params in [
            json!({ "count": 1 }),
            json!({ "num_replicas": 1, "timeout_ms": 10 }),
            json!({ "template": "GET __key__" }),
            json!({ "cursor": 0, "count": 1, "pattern": "key:*" }),
        ] {
            serde_json::from_value::<RedisCommandParams>(params.clone()).unwrap();
            let mut with_unknown = params.clone();
            with_unknown["bogus"] = 1.into();
            assert!(
                serde_json::from_value::<RedisCommandParams>(with_unknown).is_err(),
                "{params}"
            );
        }
        // Keys of two shapes match neither
        let mixed = json!({ "template": "GET __key__", "count": 1 });
        assert!(serde_json::from_value::<RedisCommandParams>(mixed).is_err());
    }

    #[test]
    fn weighted_value_sizes_parse_and_bound_their_sizes() {
        let sizes: ValueSizeConfig = serde_json::from_value(serde_json::json!({
            "strategy": "weighted",
            "sizes": [{ "size": 10, "weight": 3.0 }, { "size": 50, "weight": 1.0 }],
        }))
        .unwrap();
        assert_eq!(sizes.fixed_size(), 20);
        assert_eq!(sizes.max_size(), 50);
        let mut generator = sizes.to_generator(Some(1)).unwrap();
        let drawn: std::collections::HashSet<_> = (0..200).map(|_| generator.next_size()).collect();
        assert_eq!(drawn, [10, 50].into());

        let per_command: ValueSizeConfig = serde_json::from_value(serde_json::json!({
            "strategy": "per_command",
            "commands": { "set": { "distribution": "weighted", "sizes": [{ "size": 99, "weight": 1.0 }] } },
            "default": { "strategy": "uniform", "min": 1, "max": 70 },
        }))
        .unwrap();
        assert_eq!(per_command.max_size(), 99);
        let mut generator = per_command.to_generator(None).unwrap();
        assert_eq!(generator.next_size_for_command("SET"), 99);

        let empty: ValueSizeConfig =
            serde_json::from_value(serde_json::json!({ "strategy": "weighted", "sizes": [] }))
                .unwrap();
        assert!(empty.to_generator(None).is_err());
    }

    /// Redis SETs take their value sizes from the weighted list
    #[test]
    fn redis_sets_draw_weighted_value_sizes() {
        use crate::Protocol;
        let config: RedisConfig = serde_json::from_value(serde_json::json!({
            "operations": { "strategy": "fixed", "operation": "set" },
            "value_size": { "strategy": "weighted",
                "sizes": [{ "size": 2, "weight": 1.0 }, { "size": 12, "weight": 1.0 }] },
        }))
        .unwrap();
        let mut protocol = crate::RedisProtocol::from_config(&config, Some(4)).unwrap();
        let mut lengths = std::collections::HashSet::new();
        for _ in 0..100 {
            let data = String::from_utf8(protocol.next_request(0).data).unwrap();
            // *3 $3 SET $<key len> <key> $<value len> <value>
            lengths.insert(data.split("\r\n").nth(5).unwrap().to_string());
        }
        assert_eq!(lengths, ["$2", "$12"].map(String::from).into());
    }
}
