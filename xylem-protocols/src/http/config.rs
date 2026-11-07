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

impl HttpProtocol {
    /// Construct an HTTP workload, including the configured POST/PUT body size.
    pub fn from_config(config: &HttpConfig) -> Result<Self> {
        let method = parse_http_method(&config.method)?;
        anyhow::ensure!(!config.path.is_empty(), "HTTP path cannot be empty");
        let host = config.host.clone().unwrap_or_else(|| "localhost".to_string());
        anyhow::ensure!(
            !config.path.bytes().any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
                && !host.bytes().any(|b| b.is_ascii_whitespace() || b.is_ascii_control()),
            "Invalid HTTP path or host"
        );
        Ok(Self::with_body_size(method, config.path.clone(), host, config.body_size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Protocol;

    #[test]
    fn test_from_config_default() {
        let protocol = HttpProtocol::from_config(&HttpConfig::default()).unwrap();
        assert_eq!(Protocol::name(&protocol), "http");
    }

    #[test]
    fn test_from_config_with_post() {
        let config = HttpConfig {
            method: "POST".to_string(),
            path: "/api/data".to_string(),
            host: Some("example.com".to_string()),
            body_size: 1024,
        };
        let protocol = HttpProtocol::from_config(&config).unwrap();
        assert_eq!(Protocol::name(&protocol), "http");
    }

    #[test]
    fn test_invalid_method() {
        let config = HttpConfig {
            method: "INVALID".to_string(),
            ..Default::default()
        };
        assert!(HttpProtocol::from_config(&config).is_err());
    }

    #[test]
    fn test_empty_path() {
        let config = HttpConfig {
            path: "".to_string(),
            ..Default::default()
        };
        assert!(HttpProtocol::from_config(&config).is_err());
    }
}
