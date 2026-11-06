//! Protocol factory trait for extensible protocol configuration
//!
//! This module provides the `ProtocolFactory` trait that allows protocols to define
//! their own configuration structure and create protocol instances from it.
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

        fn next_request(&mut self, conn_id: usize) -> crate::Request<(usize, u64)> {
            crate::Request::new(
                vec![self.value as u8],
                (conn_id, 0),
                crate::RequestMeta::measurement(),
            )
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

        let request = Protocol::next_request(&mut protocol, 0);
        assert_eq!(request.data, vec![100]);
    }
}
