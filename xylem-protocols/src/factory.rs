//! Protocol factory trait for extensible protocol configuration
//!
//! This module provides the `ProtocolFactory` trait that allows protocols to define
//! their own configuration structure. Users writing custom protocols can implement
//! this trait to integrate their protocol with xylem's configuration system.
//!
//! # Example
//!
//! ```ignore
//! use xylem_protocols::{Protocol, ProtocolFactory};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Clone, Deserialize, Serialize)]
//! pub struct MyProtocolConfig {
//!     pub custom_field: String,
//!     pub timeout_ms: u64,
//! }
//!
//! pub struct MyProtocolFactory;
//!
//! impl ProtocolFactory for MyProtocolFactory {
//!     type Config = MyProtocolConfig;
//!     type Protocol = MyProtocol;
//!
//!     fn name(&self) -> &'static str {
//!         "my-protocol"
//!     }
//!
//!     fn create(&self, config: Self::Config) -> anyhow::Result<Self::Protocol> {
//!         Ok(MyProtocol::new(config))
//!     }
//!
//!     fn default_config(&self) -> Self::Config {
//!         MyProtocolConfig {
//!             custom_field: "default".to_string(),
//!             timeout_ms: 1000,
//!         }
//!     }
//! }
//! ```

use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};

use crate::Protocol;

/// Trait for protocol factories that create protocol instances from configuration.
///
/// Each protocol implementation should provide a factory that:
/// 1. Defines the protocol-specific configuration structure
/// 2. Creates protocol instances from that configuration
/// 3. Provides a default configuration
///
/// This allows xylem-core to remain decoupled from specific protocol implementations
/// while still supporting rich, protocol-specific configuration.
#[cfg(feature = "schema")]
pub trait ProtocolFactory: Send + Sync {
    /// The configuration type for this protocol.
    /// Must be deserializable from TOML/JSON and serializable for schema generation.
    type Config: DeserializeOwned + Serialize + Clone + Send + Sync + schemars::JsonSchema + 'static;

    /// The protocol type produced by this factory.
    /// Must implement the Protocol trait with a standard RequestId type.
    type Protocol: Protocol<RequestId = (usize, u64)> + Send + 'static;

    /// Protocol name (e.g., "redis", "http", "memcached-binary")
    fn name(&self) -> &'static str;

    /// Create a protocol instance from configuration.
    fn create(&self, config: Self::Config, seed: Option<u64>) -> Result<Self::Protocol>;

    /// Provide a default configuration for this protocol.
    fn default_config(&self) -> Self::Config;

    /// Validate configuration without creating a protocol.
    fn validate(&self, _config: &Self::Config) -> Result<()> {
        Ok(())
    }
}

#[cfg(not(feature = "schema"))]
pub trait ProtocolFactory: Send + Sync {
    /// The configuration type for this protocol.
    /// Must be deserializable from TOML/JSON and serializable for schema generation.
    type Config: DeserializeOwned + Serialize + Clone + Send + Sync + 'static;

    /// The protocol type produced by this factory.
    /// Must implement the Protocol trait with a standard RequestId type.
    type Protocol: Protocol<RequestId = (usize, u64)> + Send + 'static;

    /// Protocol name (e.g., "redis", "http", "memcached-binary")
    fn name(&self) -> &'static str;

    /// Create a protocol instance from configuration.
    ///
    /// # Arguments
    /// * `config` - Protocol-specific configuration
    /// * `seed` - Optional random seed for reproducibility
    ///
    /// # Returns
    /// A configured protocol instance
    fn create(&self, config: Self::Config, seed: Option<u64>) -> Result<Self::Protocol>;

    /// Provide a default configuration for this protocol.
    /// Used when no protocol_config is specified in the traffic group.
    fn default_config(&self) -> Self::Config;

    /// Validate configuration without creating a protocol.
    /// Default implementation always succeeds.
    fn validate(&self, _config: &Self::Config) -> Result<()> {
        Ok(())
    }
}

/// Type-erased protocol factory for use in registries.
///
/// This allows storing different protocol factories in a single collection.
pub trait DynProtocolFactory: Send + Sync {
    /// Protocol name
    fn name(&self) -> &'static str;

    /// Create a protocol from a JSON value configuration.
    ///
    /// # Arguments
    /// * `config` - Protocol configuration as JSON value (can be null for defaults)
    /// * `seed` - Optional random seed
    fn create_from_value(
        &self,
        config: serde_json::Value,
        seed: Option<u64>,
    ) -> Result<Box<dyn DynProtocol>>;

    /// Get the default configuration as a JSON value.
    fn default_config_value(&self) -> serde_json::Value;

    /// Validate configuration from a JSON value.
    fn validate_value(&self, config: &serde_json::Value) -> Result<()>;

    /// Get the JSON schema for this protocol's configuration (requires schema feature).
    #[cfg(feature = "schema")]
    fn config_schema(&self) -> schemars::Schema;

    /// Placeholder for config_schema when schema feature is disabled.
    #[cfg(not(feature = "schema"))]
    fn config_schema(&self) -> ();
}

/// Type-erased protocol trait for runtime dispatch.
pub trait DynProtocol: Send {
    /// Generate the next request for a connection
    ///
    /// This is the primary method that should be called by the worker.
    /// The protocol decides what to send based on its internal state.
    ///
    /// Returns (request_data, request_id, metadata) where metadata indicates
    /// whether this is a warmup request.
    fn next_request(&mut self, conn_id: usize) -> (Vec<u8>, (usize, u64), crate::RequestMeta);

    /// Regenerate a request for retry using the original request ID
    fn regenerate_request(
        &mut self,
        conn_id: usize,
        original_request_id: (usize, u64),
    ) -> (Vec<u8>, (usize, u64), crate::RequestMeta);

    /// Parse a response
    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<(usize, u64)>)>;

    /// Protocol name
    fn name(&self) -> &'static str;

    /// Reset state
    fn reset(&mut self);
}

/// Blanket implementation of DynProtocol for any Protocol implementation
impl<P> DynProtocol for P
where
    P: Protocol<RequestId = (usize, u64)> + Send,
{
    fn next_request(&mut self, conn_id: usize) -> (Vec<u8>, (usize, u64), crate::RequestMeta) {
        Protocol::next_request(self, conn_id)
    }

    fn regenerate_request(
        &mut self,
        conn_id: usize,
        original_request_id: (usize, u64),
    ) -> (Vec<u8>, (usize, u64), crate::RequestMeta) {
        Protocol::regenerate_request(self, conn_id, original_request_id)
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<(usize, u64)>)> {
        Protocol::parse_response(self, conn_id, data)
    }

    fn name(&self) -> &'static str {
        Protocol::name(self)
    }

    fn reset(&mut self) {
        Protocol::reset(self)
    }
}

/// Blanket implementation of DynProtocolFactory for any ProtocolFactory
impl<F> DynProtocolFactory for F
where
    F: ProtocolFactory,
    F::Config: 'static,
{
    fn name(&self) -> &'static str {
        ProtocolFactory::name(self)
    }

    fn create_from_value(
        &self,
        config: serde_json::Value,
        seed: Option<u64>,
    ) -> Result<Box<dyn DynProtocol>> {
        let typed_config: F::Config = if config.is_null() {
            self.default_config()
        } else {
            serde_json::from_value(config)?
        };
        let protocol = self.create(typed_config, seed)?;
        Ok(Box::new(protocol))
    }

    fn default_config_value(&self) -> serde_json::Value {
        serde_json::to_value(self.default_config()).unwrap_or(serde_json::Value::Null)
    }

    fn validate_value(&self, config: &serde_json::Value) -> Result<()> {
        let typed_config: F::Config = if config.is_null() {
            self.default_config()
        } else {
            serde_json::from_value(config.clone())?
        };
        self.validate(&typed_config)
    }

    #[cfg(feature = "schema")]
    fn config_schema(&self) -> schemars::Schema {
        schemars::generate::SchemaGenerator::default().into_root_schema_for::<F::Config>()
    }

    #[cfg(not(feature = "schema"))]
    fn config_schema(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
    #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
    struct TestConfig {
        value: u32,
    }

    struct TestProtocol {
        value: u32,
    }

    impl Protocol for TestProtocol {
        type RequestId = (usize, u64);

        fn next_request(
            &mut self,
            conn_id: usize,
        ) -> (Vec<u8>, Self::RequestId, crate::RequestMeta) {
            (vec![self.value as u8], (conn_id, 0), crate::RequestMeta::measurement())
        }

        fn parse_response(
            &mut self,
            conn_id: usize,
            data: &[u8],
        ) -> Result<(usize, Option<Self::RequestId>)> {
            if data.is_empty() {
                Ok((0, None))
            } else {
                Ok((data.len(), Some((conn_id, 0))))
            }
        }

        fn name(&self) -> &'static str {
            "test"
        }

        fn reset(&mut self) {}
    }

    struct TestFactory;

    impl ProtocolFactory for TestFactory {
        type Config = TestConfig;
        type Protocol = TestProtocol;

        fn name(&self) -> &'static str {
            "test"
        }

        fn create(&self, config: Self::Config, _seed: Option<u64>) -> Result<Self::Protocol> {
            Ok(TestProtocol { value: config.value })
        }

        fn default_config(&self) -> Self::Config {
            TestConfig { value: 42 }
        }
    }

    #[test]
    fn test_protocol_factory_basic() {
        let factory = TestFactory;
        let config = TestConfig { value: 100 };
        let mut protocol = factory.create(config, None).unwrap();

        let (data, _id, _meta) = Protocol::next_request(&mut protocol, 0);
        assert_eq!(data, vec![100]);
    }

    #[test]
    fn test_dyn_protocol_factory() {
        let factory: &dyn DynProtocolFactory = &TestFactory;

        assert_eq!(factory.name(), "test");

        let config = serde_json::json!({"value": 50});
        let mut protocol = factory.create_from_value(config, None).unwrap();

        let (data, _id, _meta) = DynProtocol::next_request(protocol.as_mut(), 0);
        assert_eq!(data, vec![50]);
    }

    #[test]
    fn test_default_config() {
        let factory: &dyn DynProtocolFactory = &TestFactory;

        let mut protocol = factory.create_from_value(serde_json::Value::Null, None).unwrap();
        let (data, _id, _meta) = DynProtocol::next_request(protocol.as_mut(), 0);
        assert_eq!(data, vec![42]); // default value
    }
}
