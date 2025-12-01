//! Protocol factory implementations for built-in protocols
//!
//! Each factory creates protocol instances from their respective config types.

mod http;
mod memcached;
mod redis;
mod xylem_echo;

pub use http::HttpFactory;
pub use memcached::{MemcachedAsciiFactory, MemcachedBinaryFactory};
pub use redis::RedisFactory;
pub use xylem_echo::XylemEchoFactory;

use crate::factory::DynProtocolFactory;
use std::collections::HashMap;
use std::sync::Arc;

/// Registry of protocol factories
///
/// This allows runtime lookup of protocol factories by name.
/// Users can register custom protocol factories here.
pub struct ProtocolRegistry {
    factories: HashMap<&'static str, Arc<dyn DynProtocolFactory>>,
}

impl ProtocolRegistry {
    /// Create a new registry with all built-in protocols
    pub fn new() -> Self {
        let mut registry = Self { factories: HashMap::new() };

        // Register built-in protocols
        registry.register(Arc::new(RedisFactory));
        registry.register(Arc::new(HttpFactory));
        registry.register(Arc::new(MemcachedBinaryFactory));
        registry.register(Arc::new(MemcachedAsciiFactory));
        registry.register(Arc::new(XylemEchoFactory));

        registry
    }

    /// Register a protocol factory
    pub fn register(&mut self, factory: Arc<dyn DynProtocolFactory>) {
        self.factories.insert(factory.name(), factory);
    }

    /// Get a factory by protocol name
    pub fn get(&self, name: &str) -> Option<&Arc<dyn DynProtocolFactory>> {
        self.factories.get(name)
    }

    /// List all registered protocol names
    pub fn protocols(&self) -> Vec<&'static str> {
        self.factories.keys().copied().collect()
    }

    /// Create a protocol instance
    pub fn create_protocol(
        &self,
        protocol_name: &str,
        config: serde_json::Value,
        seed: Option<u64>,
    ) -> anyhow::Result<Box<dyn crate::factory::DynProtocol>> {
        let factory = self
            .get(protocol_name)
            .ok_or_else(|| anyhow::anyhow!("Unknown protocol: {}", protocol_name))?;

        factory.create_from_value(config, seed)
    }
}

impl Default for ProtocolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_builtin_protocols() {
        let registry = ProtocolRegistry::new();

        assert!(registry.get("redis").is_some());
        assert!(registry.get("http").is_some());
        assert!(registry.get("memcached-binary").is_some());
        assert!(registry.get("memcached-ascii").is_some());
        assert!(registry.get("xylem-echo").is_some());
    }

    #[test]
    fn test_registry_unknown_protocol() {
        let registry = ProtocolRegistry::new();
        assert!(registry.get("unknown").is_none());
    }

    #[test]
    fn test_create_protocol_with_default_config() {
        let registry = ProtocolRegistry::new();

        let protocol = registry.create_protocol("redis", serde_json::Value::Null, None).unwrap();
        assert_eq!(protocol.name(), "redis");
    }

    #[test]
    fn test_create_protocol_with_config() {
        let registry = ProtocolRegistry::new();

        let config = serde_json::json!({
            "keys": {
                "strategy": "random",
                "max": 1000,
                "value_size": 128
            },
            "command": "SET"
        });

        let protocol = registry.create_protocol("redis", config, Some(42)).unwrap();
        assert_eq!(protocol.name(), "redis");
    }
}
