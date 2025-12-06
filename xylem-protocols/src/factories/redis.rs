//! Redis protocol factory

use crate::configs::{KeysConfig, RedisConfig, RedisOperationsConfig};
use crate::factory::ProtocolFactory;
use crate::redis::{RedisOp, RedisProtocol};
use crate::{CommandSelector, FixedCommandSelector, WeightedCommandSelector};
use anyhow::Result;

/// Factory for creating Redis protocol instances
pub struct RedisFactory;

impl ProtocolFactory for RedisFactory {
    type Config = RedisConfig;
    type Protocol = RedisProtocol;

    fn name(&self) -> &'static str {
        "redis"
    }

    fn create(&self, config: Self::Config, seed: Option<u64>) -> Result<Self::Protocol> {
        let command_selector: Box<dyn CommandSelector<RedisOp>> =
            if let Some(ref ops) = config.operations {
                match ops {
                    RedisOperationsConfig::Fixed { operation } => {
                        let op = parse_redis_op(operation)?;
                        Box::new(FixedCommandSelector::new(op))
                    }
                    RedisOperationsConfig::Weighted { commands } => {
                        let mut commands_weights = Vec::new();
                        for cmd in commands {
                            let op = parse_redis_op(&cmd.name)?;
                            commands_weights.push((op, cmd.weight));
                        }
                        Box::new(WeightedCommandSelector::with_seed(commands_weights, seed)?)
                    }
                }
            } else {
                // Default to GET
                Box::new(FixedCommandSelector::new(RedisOp::Get))
            };

        Ok(RedisProtocol::new(command_selector))
    }

    fn default_config(&self) -> Self::Config {
        RedisConfig::default()
    }

    fn validate(&self, config: &Self::Config) -> Result<()> {
        // Validate operations if present
        if let Some(ref ops) = config.operations {
            validate_operations(ops)?;
        }

        // Validate keys config
        validate_keys_config(&config.keys)?;

        Ok(())
    }
}

/// Parse a Redis operation from string
fn parse_redis_op(cmd: &str) -> Result<RedisOp> {
    match cmd.to_uppercase().as_str() {
        "GET" => Ok(RedisOp::Get),
        "SET" => Ok(RedisOp::Set),
        "INCR" => Ok(RedisOp::Incr),
        _ => anyhow::bail!("Unknown Redis command: {}", cmd),
    }
}

/// Validate operations configuration
fn validate_operations(ops: &RedisOperationsConfig) -> Result<()> {
    match ops {
        RedisOperationsConfig::Fixed { operation } => {
            parse_redis_op(operation)?;
        }
        RedisOperationsConfig::Weighted { commands } => {
            for cmd in commands {
                parse_redis_op(&cmd.name)?;
                anyhow::ensure!(
                    cmd.weight >= 0.0,
                    "Command weight must be non-negative: {} = {}",
                    cmd.name,
                    cmd.weight
                );
            }
        }
    }
    Ok(())
}

/// Validate keys configuration
fn validate_keys_config(keys: &KeysConfig) -> Result<()> {
    match keys {
        KeysConfig::Sequential { value_size, .. } => {
            if *value_size == 0 {
                anyhow::bail!("value_size must be > 0");
            }
        }
        KeysConfig::Random { max, value_size, .. } => {
            if *max == 0 {
                anyhow::bail!("Random max must be > 0");
            }
            if *value_size == 0 {
                anyhow::bail!("value_size must be > 0");
            }
        }
        KeysConfig::RoundRobin { max, value_size, .. } => {
            if *max == 0 {
                anyhow::bail!("RoundRobin max must be > 0");
            }
            if *value_size == 0 {
                anyhow::bail!("value_size must be > 0");
            }
        }
        KeysConfig::Zipfian { n, theta, value_size, .. } => {
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
        KeysConfig::Gaussian {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::RedisCommandWeight;
    use crate::Protocol;

    #[test]
    fn test_create_default() {
        let factory = RedisFactory;
        let protocol = factory.create(RedisConfig::default(), None).unwrap();
        assert_eq!(Protocol::name(&protocol), "redis");
    }

    #[test]
    fn test_create_with_set_command() {
        let factory = RedisFactory;
        let config = RedisConfig {
            operations: Some(RedisOperationsConfig::Fixed { operation: "SET".to_string() }),
            ..Default::default()
        };
        let protocol = factory.create(config, None).unwrap();
        assert_eq!(Protocol::name(&protocol), "redis");
    }

    #[test]
    fn test_create_with_weighted_commands() {
        let factory = RedisFactory;
        let config = RedisConfig {
            operations: Some(RedisOperationsConfig::Weighted {
                commands: vec![
                    RedisCommandWeight {
                        name: "GET".to_string(),
                        weight: 0.8,
                        params: None,
                        keys: None,
                    },
                    RedisCommandWeight {
                        name: "SET".to_string(),
                        weight: 0.2,
                        params: None,
                        keys: None,
                    },
                ],
            }),
            ..Default::default()
        };
        let protocol = factory.create(config, None).unwrap();
        assert_eq!(Protocol::name(&protocol), "redis");
    }

    #[test]
    fn test_invalid_command() {
        let factory = RedisFactory;
        let config = RedisConfig {
            operations: Some(RedisOperationsConfig::Fixed { operation: "INVALID".to_string() }),
            ..Default::default()
        };
        assert!(factory.create(config, None).is_err());
    }

    #[test]
    fn test_validate_keys_config() {
        // Valid random
        let keys = KeysConfig::Random {
            max: 1000,
            value_size: 64,
            prefix: "key:".to_string(),
        };
        assert!(validate_keys_config(&keys).is_ok());

        // Invalid: zero max
        let keys = KeysConfig::Random {
            max: 0,
            value_size: 64,
            prefix: "key:".to_string(),
        };
        assert!(validate_keys_config(&keys).is_err());

        // Invalid: zero value_size
        let keys = KeysConfig::Random {
            max: 1000,
            value_size: 0,
            prefix: "key:".to_string(),
        };
        assert!(validate_keys_config(&keys).is_err());
    }
}
