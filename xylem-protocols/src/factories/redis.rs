//! Config-aware Redis construction shared by factories and CLI callers.
use crate::configs::{RedisConfig, RedisOperationsConfig};
use crate::factory::ProtocolFactory;
use crate::redis::{RedisOp, RedisProtocol};
use crate::{CommandSelector, FixedCommandSelector, WeightedCommandSelector};
use anyhow::Result;

pub struct RedisFactory;

impl ProtocolFactory for RedisFactory {
    type Config = RedisConfig;
    type Protocol = RedisProtocol;
    fn name(&self) -> &'static str {
        "redis"
    }
    fn create(&self, config: RedisConfig, seed: Option<u64>) -> Result<RedisProtocol> {
        RedisProtocol::from_config(&config, seed)
    }
    fn default_config(&self) -> RedisConfig {
        RedisConfig::default()
    }
    fn validate(&self, config: &RedisConfig) -> Result<()> {
        RedisProtocol::from_config(config, Some(0)).map(|_| ())
    }
}

impl RedisConfig {
    /// Build a selector, honoring command parameters and per-command key distributions.
    /// A per-command key override uses the protocol's prefix and value size settings.
    pub fn command_selector(&self, seed: Option<u64>) -> Result<Box<dyn CommandSelector<RedisOp>>> {
        self.keys.validate()?;
        match &self.operations {
            None => Ok(Box::new(FixedCommandSelector::new(RedisOp::Get))),
            Some(RedisOperationsConfig::Fixed { operation }) => {
                Ok(Box::new(FixedCommandSelector::new(parse_redis_operation(operation, None)?)))
            }
            Some(RedisOperationsConfig::Weighted { commands }) => {
                let mut weighted = Vec::new();
                let mut keys = Vec::new();
                let per_command = commands.iter().any(|cmd| cmd.keys.is_some());
                for cmd in commands {
                    weighted
                        .push((parse_redis_operation(&cmd.name, cmd.params.as_ref())?, cmd.weight));
                    let config = cmd.keys.as_ref().unwrap_or(&self.keys);
                    config.validate()?;
                    anyhow::ensure!(
                        config.prefix() == self.keys.prefix(),
                        "Per-command key prefixes are unsupported"
                    );
                    anyhow::ensure!(
                        config.value_size() == self.keys.value_size(),
                        "Use value_size.per_command for per-command value sizes"
                    );
                    if per_command {
                        keys.push(
                            Box::new(config.to_key_gen(seed)?) as Box<dyn crate::KeyGenerator>
                        );
                    }
                }
                if per_command {
                    Ok(Box::new(WeightedCommandSelector::with_per_command_keys(
                        weighted, keys, seed,
                    )?))
                } else {
                    Ok(Box::new(WeightedCommandSelector::with_seed(weighted, seed)?))
                }
            }
        }
    }
}

impl RedisProtocol {
    /// Construct a standalone Redis workload from all supported configuration fields.
    /// Data import requires a separate imported-data driver; cluster configs use
    /// `RedisClusterProtocol::from_config`.
    pub fn from_config(config: &RedisConfig, seed: Option<u64>) -> Result<Self> {
        anyhow::ensure!(
            config.redis_cluster.is_none(),
            "Use RedisClusterProtocol::from_config for cluster configuration"
        );
        Self::from_workload_config(config, seed)
    }

    pub(crate) fn from_workload_config(config: &RedisConfig, seed: Option<u64>) -> Result<Self> {
        anyhow::ensure!(
            config.data_import.is_none(),
            "data_import requires an imported-data driver and is unsupported by this constructor"
        );
        let mut protocol = Self::with_workload_and_options(
            config.command_selector(seed)?,
            config.keys.to_key_gen(seed)?,
            config.keys.value_size(),
            config.keys.prefix().to_string(),
            config.random_data,
            seed,
        );
        if let Some(sizes) = &config.value_size {
            protocol = protocol.with_value_size_generator(sizes.to_generator(seed)?);
        }
        if let Some(insert) = &config.insert_phase {
            let (count, size) = insert.resolve(&config.keys)?;
            protocol = protocol.with_insert_phase(count, size);
        }
        Ok(protocol)
    }
}

impl crate::configs::RedisClusterConfig {
    /// Build a static topology, rejecting invalid or overlapping inclusive slot ranges.
    pub fn to_topology(&self) -> Result<crate::redis::cluster::ClusterTopology> {
        let mut ranges = Vec::<crate::redis::cluster::topology::SlotRange>::new();
        for node in &self.nodes {
            anyhow::ensure!(
                node.slot_start <= node.slot_end && node.slot_end < 16384,
                "Invalid Redis slot range {}..={} for {}",
                node.slot_start,
                node.slot_end,
                node.address
            );
            for previous in &ranges {
                anyhow::ensure!(
                    node.slot_end < previous.start || node.slot_start > previous.end,
                    "Redis slot range {}..={} for {} overlaps {}..={} for {}",
                    node.slot_start,
                    node.slot_end,
                    node.address,
                    previous.start,
                    previous.end,
                    previous.master
                );
            }
            ranges.push(crate::redis::cluster::topology::SlotRange {
                start: node.slot_start,
                end: node.slot_end,
                master: node.address.parse()?,
                replicas: vec![],
            });
        }
        Ok(crate::redis::cluster::ClusterTopology::from_slot_ranges(ranges))
    }
}

impl crate::redis::cluster::RedisClusterProtocol {
    /// Construct a cluster workload and optional static topology. Register actual
    /// connection IDs separately with `register_connections`.
    pub fn from_config(config: &RedisConfig, seed: Option<u64>) -> Result<Self> {
        let mut protocol =
            Self::from_base_protocol(RedisProtocol::from_workload_config(config, seed)?);
        if let Some(cluster) = &config.redis_cluster {
            protocol.update_topology(cluster.to_topology()?);
        }
        Ok(protocol)
    }
}

pub fn parse_redis_operation(
    cmd: &str,
    params: Option<&crate::RedisCommandParams>,
) -> anyhow::Result<crate::RedisOp> {
    if let Some(params) = params {
        let matches = matches!(
            (cmd.to_ascii_uppercase().as_str(), params),
            ("MGET", crate::RedisCommandParams::MGet { .. })
                | ("WAIT", crate::RedisCommandParams::Wait { .. })
                | ("SCAN", crate::RedisCommandParams::Scan { .. })
                | ("CUSTOM", crate::RedisCommandParams::Custom { .. })
        );
        anyhow::ensure!(matches, "Parameters do not match Redis command {cmd}");
    }
    match cmd.to_uppercase().as_str() {
        "GET" => Ok(crate::RedisOp::Get),
        "SET" => Ok(crate::RedisOp::Set),
        "INCR" => Ok(crate::RedisOp::Incr),
        "MGET" => {
            let count = match params {
                Some(crate::RedisCommandParams::MGet { count }) => *count,
                _ => 10, // Default to 10 keys if not specified
            };
            Ok(crate::RedisOp::MGet { count })
        }
        "WAIT" => {
            let (num_replicas, timeout_ms) = match params {
                Some(crate::RedisCommandParams::Wait { num_replicas, timeout_ms }) => {
                    (*num_replicas, *timeout_ms)
                }
                _ => (1, 1000), // Default: 1 replica, 1000ms timeout
            };
            Ok(crate::RedisOp::Wait { num_replicas, timeout_ms })
        }
        "SETEX" => Ok(crate::RedisOp::SetEx { ttl_seconds: 3600 }),
        "SETRANGE" => Ok(crate::RedisOp::SetRange { offset: 0 }),
        "GETRANGE" => Ok(crate::RedisOp::GetRange { offset: 0, end: -1 }),
        "SCAN" => {
            let (cursor, count, pattern) = match params {
                Some(crate::RedisCommandParams::Scan { cursor, count, pattern }) => {
                    (*cursor, *count, pattern.clone())
                }
                _ => (0, None, None),
            };
            Ok(crate::RedisOp::Scan { cursor, count, pattern })
        }
        "MULTI" => Ok(crate::RedisOp::Multi),
        "EXEC" => Ok(crate::RedisOp::Exec),
        "DISCARD" => Ok(crate::RedisOp::Discard),
        "CUSTOM" => {
            // Custom command requires a template in params
            let template_str = match params {
                Some(crate::RedisCommandParams::Custom { template }) => template.clone(),
                _ => {
                    anyhow::bail!(
                        "Custom command requires params.template. Example: \
                         {{ name = \"custom\", params = {{ template = \"HSET myhash __key__ __data__\" }} }}"
                    )
                }
            };
            let template = crate::CommandTemplate::parse(&template_str)?;
            Ok(crate::RedisOp::Custom(template))
        }
        _ => anyhow::bail!(
            "Unknown Redis command: {}. Supported: GET, SET, INCR, MGET, WAIT, \
             SETEX, SETRANGE, GETRANGE, SCAN, MULTI, EXEC, DISCARD, CUSTOM",
            cmd
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::{KeysConfig, RedisCommandWeight};
    use crate::Protocol;

    fn cluster_config(ranges: &[(u16, u16)]) -> RedisConfig {
        RedisConfig {
            redis_cluster: Some(crate::configs::RedisClusterConfig {
                nodes: ranges
                    .iter()
                    .enumerate()
                    .map(|(index, &(slot_start, slot_end))| {
                        crate::configs::RedisClusterNodeConfig {
                            address: format!("127.0.0.1:{}", 7000 + index),
                            slot_start,
                            slot_end,
                        }
                    })
                    .collect(),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn cluster_config_rejects_inclusive_overlaps_without_panicking() {
        for ranges in [vec![(0, 100), (100, 200)], vec![(50, 100), (0, 200)]] {
            let result = crate::RedisClusterProtocol::from_config(&cluster_config(&ranges), None);
            let error = result.err().expect("overlapping ranges must return an error").to_string();
            assert!(error.contains("overlaps"), "{error}");
            assert!(error.contains("127.0.0.1:7000"), "{error}");
            assert!(error.contains("127.0.0.1:7001"), "{error}");
        }
    }

    #[test]
    fn cluster_config_accepts_disjoint_ranges_in_any_order() {
        for ranges in [vec![(0, 100), (101, 16383)], vec![(101, 16383), (0, 100)]] {
            assert!(
                crate::RedisClusterProtocol::from_config(&cluster_config(&ranges), None).is_ok()
            );
        }
    }

    #[test]
    fn cluster_config_rejects_invalid_bounds() {
        for range in [(100, 99), (0, 16384), (16384, 16384)] {
            let result = crate::RedisClusterProtocol::from_config(&cluster_config(&[range]), None);
            let error = result.err().expect("invalid range must return an error").to_string();
            assert!(error.contains("Invalid Redis slot range"), "{error}");
        }
    }

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
        assert!(keys.validate().is_ok());

        // Invalid: zero max
        let keys = KeysConfig::Random {
            max: 0,
            value_size: 64,
            prefix: "key:".to_string(),
        };
        assert!(keys.validate().is_err());

        // Invalid: zero value_size
        let keys = KeysConfig::Random {
            max: 1000,
            value_size: 0,
            prefix: "key:".to_string(),
        };
        assert!(keys.validate().is_err());
    }
}
