//! Memcached protocol factories

use crate::configs::MemcachedConfig;
use crate::factory::ProtocolFactory;
use crate::memcached::{MemcachedAsciiProtocol, MemcachedBinaryProtocol, MemcachedOp};
use anyhow::Result;

/// Factory for creating Memcached Binary protocol instances
pub struct MemcachedBinaryFactory;

impl ProtocolFactory for MemcachedBinaryFactory {
    type Config = MemcachedConfig;
    type Protocol = MemcachedBinaryProtocol;

    fn name(&self) -> &'static str {
        "memcached-binary"
    }

    fn create(&self, config: Self::Config, seed: Option<u64>) -> Result<Self::Protocol> {
        MemcachedBinaryProtocol::from_config(&config, seed)
    }

    fn default_config(&self) -> Self::Config {
        MemcachedConfig::default()
    }

    fn validate(&self, config: &Self::Config) -> Result<()> {
        MemcachedBinaryProtocol::from_config(config, Some(0)).map(|_| ())
    }
}

/// Factory for creating Memcached ASCII protocol instances
pub struct MemcachedAsciiFactory;

impl ProtocolFactory for MemcachedAsciiFactory {
    type Config = MemcachedConfig;
    type Protocol = MemcachedAsciiProtocol;

    fn name(&self) -> &'static str {
        "memcached-ascii"
    }

    fn create(&self, config: Self::Config, seed: Option<u64>) -> Result<Self::Protocol> {
        MemcachedAsciiProtocol::from_config(&config, seed)
    }

    fn default_config(&self) -> Self::Config {
        MemcachedConfig::default()
    }

    fn validate(&self, config: &Self::Config) -> Result<()> {
        MemcachedAsciiProtocol::from_config(config, Some(0)).map(|_| ())
    }
}

/// Parse Memcached binary operation from string
fn parse_memcached_op(op: &str) -> Result<MemcachedOp> {
    match op.to_uppercase().as_str() {
        "GET" => Ok(MemcachedOp::Get),
        "SET" => Ok(MemcachedOp::Set),
        _ => anyhow::bail!("Unknown Memcached operation: {}. Supported: GET, SET", op),
    }
}

/// Parse Memcached ASCII operation from string
fn parse_memcached_ascii_op(op: &str) -> Result<crate::memcached::ascii::MemcachedOp> {
    Ok(match parse_memcached_op(op)? {
        MemcachedOp::Get => crate::memcached::ascii::MemcachedOp::Get,
        MemcachedOp::Set => crate::memcached::ascii::MemcachedOp::Set,
    })
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
            parse_memcached_ascii_op(&config.operation)?,
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
    fn test_binary_create_default() {
        let factory = MemcachedBinaryFactory;
        let protocol = factory.create(MemcachedConfig::default(), None).unwrap();
        assert_eq!(Protocol::name(&protocol), "memcached-binary");
    }

    #[test]
    fn test_ascii_create_default() {
        let factory = MemcachedAsciiFactory;
        let protocol = factory.create(MemcachedConfig::default(), None).unwrap();
        assert_eq!(Protocol::name(&protocol), "memcached-ascii");
    }

    #[test]
    fn test_binary_create_with_set() {
        let factory = MemcachedBinaryFactory;
        let config = MemcachedConfig {
            operation: "SET".to_string(),
            ..Default::default()
        };
        let protocol = factory.create(config, None).unwrap();
        assert_eq!(Protocol::name(&protocol), "memcached-binary");
    }

    #[test]
    fn test_invalid_operation() {
        let factory = MemcachedBinaryFactory;
        let config = MemcachedConfig {
            operation: "INVALID".to_string(),
            ..Default::default()
        };
        assert!(factory.create(config, None).is_err());
    }
}
