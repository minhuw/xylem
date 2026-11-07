//! Config-aware Redis construction.
use crate::configs::{RedisConfig, RedisOperationsConfig};
use crate::redis::{ConnectionSetup, RedisOp, RedisProtocol};
use crate::{CommandSelector, FixedCommandSelector, WeightedCommandSelector};
use anyhow::Result;

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

impl RedisConfig {
    /// Validate the connection setup fields (auth, db, resp) and build the
    /// setup sequence. Reads the password from the environment when
    /// `auth.password_env` is set.
    pub fn connection_setup(&self) -> Result<ConnectionSetup> {
        let auth = match &self.auth {
            Some(auth) => Some((auth.username.clone(), auth.resolve_password()?)),
            None => None,
        };
        ConnectionSetup::new(auth, self.db, self.resp)
    }
}

impl RedisProtocol {
    /// Construct a standalone Redis workload from all supported configuration fields.
    pub fn from_config(config: &RedisConfig, seed: Option<u64>) -> Result<Self> {
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
        Ok(protocol.with_connection_setup(config.connection_setup()?))
    }
}

fn parse_redis_operation(
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

    #[test]
    fn test_from_config_default() {
        let protocol = RedisProtocol::from_config(&RedisConfig::default(), None).unwrap();
        assert_eq!(Protocol::name(&protocol), "redis");
    }

    #[test]
    fn test_from_config_with_set_command() {
        let config = RedisConfig {
            operations: Some(RedisOperationsConfig::Fixed { operation: "SET".to_string() }),
            ..Default::default()
        };
        let protocol = RedisProtocol::from_config(&config, None).unwrap();
        assert_eq!(Protocol::name(&protocol), "redis");
    }

    #[test]
    fn test_from_config_with_weighted_commands() {
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
        let protocol = RedisProtocol::from_config(&config, None).unwrap();
        assert_eq!(Protocol::name(&protocol), "redis");
    }

    fn auth(username: Option<&str>, password: Option<&str>, env: Option<&str>) -> RedisConfig {
        RedisConfig {
            auth: Some(crate::RedisAuthConfig {
                username: username.map(str::to_string),
                password: password.map(str::to_string),
                password_env: env.map(str::to_string),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn connection_setup_from_config() {
        let first_request = |config: &RedisConfig| {
            RedisProtocol::from_config(config, None).unwrap().next_request(0)
        };
        let request = first_request(&auth(None, Some("pw"), None));
        assert_eq!(
            request,
            crate::Request::setup(b"*2\r\n$4\r\nAUTH\r\n$2\r\npw\r\n".to_vec(), (0, 0))
        );

        // Cargo sets CARGO_PKG_NAME for the test process
        let request = first_request(&auth(Some("bench"), None, Some("CARGO_PKG_NAME")));
        let expected = b"*3\r\n$4\r\nAUTH\r\n$5\r\nbench\r\n$15\r\nxylem-protocols\r\n";
        assert_eq!(request, crate::Request::setup(expected.to_vec(), (0, 0)));

        let config = RedisConfig {
            db: Some(3),
            resp: Some(3),
            ..Default::default()
        };
        assert_eq!(config.connection_setup().unwrap().step_names(), ["SELECT", "HELLO"]);
        assert!(first_request(&config).metadata.is_setup);
        assert!(!first_request(&RedisConfig::default()).metadata.is_warmup);
    }

    #[test]
    fn connection_setup_validation_errors_never_show_the_password() {
        for (config, expected) in [
            (
                auth(None, None, Some("XYLEM_TEST_UNSET_REDIS_PASSWORD")),
                "Redis auth.password_env: environment variable XYLEM_TEST_UNSET_REDIS_PASSWORD \
                 is not set",
            ),
            (auth(None, None, Some("")), "Redis auth.password_env must not be empty"),
            (
                auth(None, Some("s3cret"), Some("X")),
                "Redis auth sets both password and password_env; set exactly one",
            ),
            (
                auth(Some("bench"), None, None),
                "Redis auth needs a password or password_env; set exactly one",
            ),
            (
                RedisConfig { resp: Some(0), ..Default::default() },
                "Redis resp must be 2 or 3, got 0",
            ),
            (
                RedisConfig { db: Some(u32::MAX), ..Default::default() },
                "Redis db must be at most 2147483647, got 4294967295",
            ),
        ] {
            let err = config.connection_setup().unwrap_err().to_string();
            assert_eq!(err, expected);
            assert!(RedisProtocol::from_config(&config, None).is_err());
        }
        let debug = format!("{:?}", auth(Some("bench"), Some("s3cret"), None));
        assert!(debug.contains("bench") && debug.contains("<redacted>"), "{debug}");
        assert!(!debug.contains("s3cret"), "{debug}");
    }

    #[test]
    fn auth_config_accepts_exactly_the_documented_fields() {
        let config: RedisConfig = serde_json::from_value(serde_json::json!({
            "auth": { "username": "bench", "password_env": "REDIS_PASSWORD" },
            "db": 3,
            "resp": 3,
        }))
        .unwrap();
        let auth = config.auth.unwrap();
        assert_eq!(auth.username.as_deref(), Some("bench"));
        assert_eq!(auth.password_env.as_deref(), Some("REDIS_PASSWORD"));
        assert_eq!((config.db, config.resp), (Some(3), Some(3)));
        let unknown = serde_json::json!({ "auth": { "user": "bench", "password": "pw" } });
        assert!(serde_json::from_value::<RedisConfig>(unknown).is_err());
        let negative = serde_json::json!({ "db": -1 });
        assert!(serde_json::from_value::<RedisConfig>(negative).is_err());
    }

    #[test]
    fn test_custom_template_params_from_profile_json() {
        let config: RedisConfig = serde_json::from_str(
            r#"{
                "keys": { "strategy": "sequential", "start": 7, "value_size": 3 },
                "operations": {
                    "strategy": "weighted",
                    "commands": [
                        { "name": "custom", "weight": 1.0,
                          "params": { "template": "HSET users __key__ __data__" } }
                    ]
                }
            }"#,
        )
        .unwrap();
        let mut protocol = RedisProtocol::from_config(&config, None).unwrap();
        assert_eq!(
            protocol.next_request(0).data,
            b"*4\r\n$4\r\nHSET\r\n$5\r\nusers\r\n$5\r\nkey:7\r\n$3\r\nxxx\r\n"
        );
    }

    #[test]
    fn test_invalid_command() {
        let config = RedisConfig {
            operations: Some(RedisOperationsConfig::Fixed { operation: "INVALID".to_string() }),
            ..Default::default()
        };
        assert!(RedisProtocol::from_config(&config, None).is_err());
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
