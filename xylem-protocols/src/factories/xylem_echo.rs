//! Xylem Echo protocol factory

use crate::configs::XylemEchoConfig;
use crate::factory::ProtocolFactory;
use crate::xylem_echo::XylemEchoProtocol;
use anyhow::Result;

/// Factory for creating Xylem Echo protocol instances
pub struct XylemEchoFactory;

impl ProtocolFactory for XylemEchoFactory {
    type Config = XylemEchoConfig;
    type Protocol = XylemEchoProtocol;

    fn name(&self) -> &'static str {
        "xylem-echo"
    }

    fn create(&self, _config: Self::Config, _seed: Option<u64>) -> Result<Self::Protocol> {
        Ok(XylemEchoProtocol::default())
    }

    fn default_config(&self) -> Self::Config {
        XylemEchoConfig::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Protocol;

    #[test]
    fn test_create_default() {
        let factory = XylemEchoFactory;
        let protocol = factory.create(XylemEchoConfig::default(), None).unwrap();
        assert_eq!(Protocol::name(&protocol), "xylem-echo");
    }
}
