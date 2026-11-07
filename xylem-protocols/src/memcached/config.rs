//! Config-aware Memcached construction.

use crate::configs::MemcachedConfig;
use crate::memcached::{MemcachedAsciiProtocol, MemcachedBinaryProtocol, MemcachedOp};
use anyhow::Result;

/// Parse Memcached operation from string
fn parse_memcached_op(op: &str) -> Result<MemcachedOp> {
    match op.to_uppercase().as_str() {
        "GET" => Ok(MemcachedOp::Get),
        "SET" => Ok(MemcachedOp::Set),
        _ => anyhow::bail!("Unknown Memcached operation: {}. Supported: GET, SET", op),
    }
}

fn validate_keys(config: &MemcachedConfig) -> Result<()> {
    config.keys.validate()?;
    let prefix = config.keys.prefix();
    anyhow::ensure!(
        !prefix.bytes().any(|b| b.is_ascii_whitespace() || b.is_ascii_control()) && prefix.len() <= 230,
        "Memcached key prefix must contain no whitespace/control bytes and leave room for a u64 key within 250 bytes"
    );
    Ok(())
}

impl MemcachedBinaryProtocol {
    /// Construct a workload with configured keys, prefix, value size and insertion phase.
    pub fn from_config(config: &MemcachedConfig, seed: Option<u64>) -> Result<Self> {
        validate_keys(config)?;
        let mut protocol = Self::with_workload(
            parse_memcached_op(&config.operation)?,
            config.keys.to_key_gen(seed)?,
            config.keys.value_size(),
        )
        .with_key_prefix(config.keys.prefix().to_string());
        if let Some(insert) = &config.insert_phase {
            let (count, size) = insert.resolve(&config.keys)?;
            protocol = protocol.with_insert_phase(count, size);
        }
        Ok(protocol)
    }
}

impl MemcachedAsciiProtocol {
    /// Construct a workload with configured keys, prefix, value size and insertion phase.
    pub fn from_config(config: &MemcachedConfig, seed: Option<u64>) -> Result<Self> {
        validate_keys(config)?;
        let mut protocol = Self::with_workload(
            parse_memcached_op(&config.operation)?,
            config.keys.to_key_gen(seed)?,
            config.keys.value_size(),
        )
        .with_key_prefix(config.keys.prefix().to_string());
        if let Some(insert) = &config.insert_phase {
            let (count, size) = insert.resolve(&config.keys)?;
            protocol = protocol.with_insert_phase(count, size);
        }
        Ok(protocol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Protocol;

    #[test]
    fn test_binary_from_config_default() {
        let protocol =
            MemcachedBinaryProtocol::from_config(&MemcachedConfig::default(), None).unwrap();
        assert_eq!(Protocol::name(&protocol), "memcached-binary");
    }

    #[test]
    fn test_ascii_from_config_default() {
        let protocol =
            MemcachedAsciiProtocol::from_config(&MemcachedConfig::default(), None).unwrap();
        assert_eq!(Protocol::name(&protocol), "memcached-ascii");
    }

    #[test]
    fn test_binary_from_config_with_set() {
        let config = MemcachedConfig {
            operation: "SET".to_string(),
            ..Default::default()
        };
        let protocol = MemcachedBinaryProtocol::from_config(&config, None).unwrap();
        assert_eq!(Protocol::name(&protocol), "memcached-binary");
    }

    #[test]
    fn test_invalid_operation() {
        let config = MemcachedConfig {
            operation: "INVALID".to_string(),
            ..Default::default()
        };
        assert!(MemcachedBinaryProtocol::from_config(&config, None).is_err());
    }
}
