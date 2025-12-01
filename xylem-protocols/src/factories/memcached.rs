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

    fn create(&self, config: Self::Config, _seed: Option<u64>) -> Result<Self::Protocol> {
        let op = parse_memcached_op(&config.operation)?;
        Ok(MemcachedBinaryProtocol::new(op))
    }

    fn default_config(&self) -> Self::Config {
        MemcachedConfig::default()
    }

    fn validate(&self, config: &Self::Config) -> Result<()> {
        parse_memcached_op(&config.operation)?;
        Ok(())
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

    fn create(&self, config: Self::Config, _seed: Option<u64>) -> Result<Self::Protocol> {
        let op = parse_memcached_ascii_op(&config.operation)?;
        Ok(MemcachedAsciiProtocol::new(op))
    }

    fn default_config(&self) -> Self::Config {
        MemcachedConfig::default()
    }

    fn validate(&self, config: &Self::Config) -> Result<()> {
        parse_memcached_ascii_op(&config.operation)?;
        Ok(())
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
    match op.to_uppercase().as_str() {
        "GET" => Ok(crate::memcached::ascii::MemcachedOp::Get),
        "SET" => Ok(crate::memcached::ascii::MemcachedOp::Set),
        _ => anyhow::bail!("Unknown Memcached operation: {}. Supported: GET, SET", op),
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
