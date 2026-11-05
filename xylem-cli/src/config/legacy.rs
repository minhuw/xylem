//! Compatibility types for the retired CLI operations format.
//! These retain their original serialization and seed behavior. Profiles use
//! the protocol crate's config types, whose wire format is different.

use super::KeysConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Operations configuration (command selection)
#[deprecated(
    note = "Legacy CLI configuration; use xylem_protocols::RedisOperationsConfig. Its wire format differs; migrate explicitly."
)]
#[allow(deprecated)]
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "strategy", rename_all = "lowercase")]
pub enum OperationsConfig {
    /// Fixed operation (backward compatible)
    Fixed { operation: String },
    /// Weighted random selection
    Weighted { commands: Vec<CommandWeightConfig> },
}

/// Command weight configuration
#[deprecated(
    note = "Legacy CLI configuration; use xylem_protocols::RedisCommandWeight. Its wire format differs; migrate explicitly."
)]
#[allow(deprecated)]
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct CommandWeightConfig {
    /// Command name: "get", "set", "incr", "mget", "wait", or "custom"
    pub name: String,
    /// Weight (probability) for this command
    pub weight: f64,
    /// Additional parameters for specific commands
    #[serde(flatten)]
    pub params: Option<CommandParams>,
    /// Optional per-command key distribution (overrides protocol_config.keys for this command)
    #[serde(default)]
    pub keys: Option<KeysConfig>,
}

/// Additional parameters for specific command types
#[deprecated(
    note = "Legacy CLI configuration; use xylem_protocols::RedisCommandParams. Its wire format differs; migrate explicitly."
)]
#[allow(deprecated)]
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum CommandParams {
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

#[allow(deprecated)]
impl OperationsConfig {
    /// Helper: Process a command with per-command keys
    fn process_command_with_keys(
        cmd_cfg: &CommandWeightConfig,
        master_seed: Option<u64>,
    ) -> anyhow::Result<(xylem_protocols::RedisOp, Box<dyn xylem_protocols::KeyGenerator>)> {
        let op = Self::parse_redis_op(&cmd_cfg.name, cmd_cfg.params.as_ref())?;

        if let Some(ref keys_config) = cmd_cfg.keys {
            let key_gen = keys_config.to_key_gen(master_seed)?;
            Ok((op, Box::new(key_gen) as Box<dyn xylem_protocols::KeyGenerator>))
        } else {
            anyhow::bail!(
                "When using per-command keys, all commands must specify 'keys'. \
                 Command '{}' is missing keys configuration.",
                cmd_cfg.name
            )
        }
    }

    /// Convert to a Redis CommandSelector
    #[deprecated(
        note = "Legacy selector conversion; migrate to protocol configuration and factories in xylem_protocols."
    )]
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
                // Check if any command has per-command keys
                let has_per_command_keys = commands.iter().any(|cmd| cmd.keys.is_some());

                if has_per_command_keys {
                    // Create selector with per-command key generators
                    let mut weighted_ops = Vec::new();
                    let mut key_generators: Vec<Box<dyn xylem_protocols::KeyGenerator>> =
                        Vec::new();

                    for cmd_cfg in commands {
                        let (op, key_gen) = Self::process_command_with_keys(cmd_cfg, master_seed)?;
                        weighted_ops.push((op, cmd_cfg.weight));
                        key_generators.push(key_gen);
                    }

                    let seed = master_seed.map(|s| derive_seed(s, "command_selector"));
                    Ok(Box::new(WeightedCommandSelector::with_per_command_keys(
                        weighted_ops,
                        key_generators,
                        seed,
                    )?))
                } else {
                    // Traditional mode: no per-command keys
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
            "scan" => {
                if let Some(CommandParams::Scan { cursor, count, pattern }) = params {
                    Ok(RedisOp::Scan {
                        cursor: *cursor,
                        count: *count,
                        pattern: pattern.clone(),
                    })
                } else {
                    // Default SCAN with cursor=0, no count/pattern limits
                    Ok(RedisOp::Scan { cursor: 0, count: None, pattern: None })
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
