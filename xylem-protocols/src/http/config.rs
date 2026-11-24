//! Config-aware HTTP construction.

use crate::configs::HttpConfig;
use crate::http::{HttpMethod, HttpProtocol};
use anyhow::Result;

/// Parse HTTP method from string
fn parse_http_method(method: &str) -> Result<HttpMethod> {
    match method.to_uppercase().as_str() {
        "GET" => Ok(HttpMethod::Get),
        "POST" => Ok(HttpMethod::Post),
        "PUT" => Ok(HttpMethod::Put),
        _ => anyhow::bail!("Unknown HTTP method: {}. Supported: GET, POST, PUT", method),
    }
}

impl HttpConfig {
    /// Validate `keys`, `verify`, `verify_seed`, and `verify_body_size`,
    /// returning `Some((seed, body_size))` when opt-in response body
    /// verification is enabled.
    pub fn validate_verify(&self) -> Result<Option<(u64, usize)>> {
        if let Some(ref keys) = self.keys {
            keys.validate()?;
            anyhow::ensure!(
                !keys.prefix().bytes().any(|b| b.is_ascii_whitespace() || b.is_ascii_control()),
                "Invalid HTTP keys prefix"
            );
        }
        if !self.verify {
            if self.verify_seed.is_some() {
                anyhow::bail!("HTTP verify_seed requires verify = true");
            }
            if self.verify_body_size.is_some() {
                anyhow::bail!("HTTP verify_body_size requires verify = true");
            }
            return Ok(None);
        }
        let method = parse_http_method(&self.method)?;
        if method != HttpMethod::Get {
            anyhow::bail!("HTTP verify mode only supports GET requests, got {}", self.method);
        }
        let Some(body_size) = self.verify_body_size else {
            anyhow::bail!("HTTP verify mode requires verify_body_size");
        };
        if body_size == 0 {
            anyhow::bail!("HTTP verify_body_size must be > 0");
        }
        Ok(Some((self.verify_seed.unwrap_or(0), body_size)))
    }
}

impl HttpProtocol {
    /// Construct an HTTP workload, including the configured POST/PUT body size,
    /// optional per-request key paths, and optional GET response-body verification.
    pub fn from_config(config: &HttpConfig, seed: Option<u64>) -> Result<Self> {
        let method = parse_http_method(&config.method)?;
        anyhow::ensure!(!config.path.is_empty(), "HTTP path cannot be empty");
        let host = config.host.clone().unwrap_or_else(|| "localhost".to_string());
        anyhow::ensure!(
            !config.path.bytes().any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
                && !host.bytes().any(|b| b.is_ascii_whitespace() || b.is_ascii_control()),
            "Invalid HTTP path or host"
        );
        let verify = config.validate_verify()?;
        let mut protocol = Self::with_options(
            method,
            config.path.clone(),
            host,
            config.body_size,
            config.keep_alive,
        );
        if let Some(ref keys) = config.keys {
            protocol = protocol.with_keys(keys.to_key_gen(seed)?, keys.prefix().to_string());
        }
        if let Some((verify_seed, body_size)) = verify {
            protocol = protocol.with_verify(verify_seed, body_size);
        }
        Ok(protocol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::KeysConfig;
    use crate::Protocol;

    #[test]
    fn test_from_config_default() {
        let protocol = HttpProtocol::from_config(&HttpConfig::default(), None).unwrap();
        assert_eq!(Protocol::name(&protocol), "http");
    }

    #[test]
    fn test_from_config_with_post() {
        let config = HttpConfig {
            method: "POST".to_string(),
            path: "/api/data".to_string(),
            host: Some("example.com".to_string()),
            body_size: 1024,
            ..Default::default()
        };
        let protocol = HttpProtocol::from_config(&config, None).unwrap();
        assert_eq!(Protocol::name(&protocol), "http");
    }

    #[test]
    fn test_invalid_method() {
        let config = HttpConfig {
            method: "INVALID".to_string(),
            ..Default::default()
        };
        assert!(HttpProtocol::from_config(&config, None).is_err());
    }

    #[test]
    fn test_empty_path() {
        let config = HttpConfig {
            path: "".to_string(),
            ..Default::default()
        };
        assert!(HttpProtocol::from_config(&config, None).is_err());
    }

    #[test]
    fn test_verify_validation_rules() {
        // verify with POST is rejected
        let post_verify = HttpConfig {
            method: "POST".to_string(),
            verify: true,
            verify_body_size: Some(64),
            ..Default::default()
        };
        let err = post_verify.validate_verify().unwrap_err().to_string();
        assert!(err.contains("GET"), "{err}");

        // verify without verify_body_size is rejected
        let missing_size = HttpConfig { verify: true, ..Default::default() };
        let err = missing_size.validate_verify().unwrap_err().to_string();
        assert!(err.contains("verify_body_size"), "{err}");

        // verify_body_size = 0 is rejected
        let zero_size = HttpConfig {
            verify: true,
            verify_body_size: Some(0),
            ..Default::default()
        };
        let err = zero_size.validate_verify().unwrap_err().to_string();
        assert!(err.contains("> 0"), "{err}");

        // verify_seed without verify = true is rejected
        let seed_without_verify = HttpConfig {
            verify_seed: Some(7),
            ..Default::default()
        };
        let err = seed_without_verify.validate_verify().unwrap_err().to_string();
        assert!(err.contains("verify_seed"), "{err}");

        // verify_body_size without verify = true is rejected
        let size_without_verify = HttpConfig {
            verify_body_size: Some(64),
            ..Default::default()
        };
        let err = size_without_verify.validate_verify().unwrap_err().to_string();
        assert!(err.contains("verify_body_size"), "{err}");

        // valid verify defaults verify_seed to 0
        let valid = HttpConfig {
            verify: true,
            verify_body_size: Some(16384),
            ..Default::default()
        };
        assert_eq!(valid.validate_verify().unwrap(), Some((0, 16384)));

        // valid verify with explicit seed and keys
        let valid_keyed = HttpConfig {
            path: "/obj/".to_string(),
            verify: true,
            verify_seed: Some(42),
            verify_body_size: Some(98304),
            keys: Some(KeysConfig::RoundRobin {
                max: 8,
                value_size: 64,
                prefix: "k".to_string(),
            }),
            ..Default::default()
        };
        assert_eq!(valid_keyed.validate_verify().unwrap(), Some((42, 98304)));
        let mut proto = HttpProtocol::from_config(&valid_keyed, Some(1)).unwrap();
        let req = String::from_utf8(proto.next_request(0).data).unwrap();
        assert!(req.starts_with("GET /obj/k0 HTTP/1.1\r\n"), "{req}");
    }
}
