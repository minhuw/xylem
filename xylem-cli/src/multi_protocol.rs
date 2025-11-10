use anyhow::{bail, Result};
use xylem_protocols::Protocol;

/// Multi-protocol wrapper that can dispatch to any supported protocol
/// This allows different traffic groups to use different protocols
pub enum MultiProtocol {
    Redis(xylem_protocols::redis::RedisProtocol),
    MemcachedBinary(xylem_protocols::memcached::MemcachedBinaryProtocol),
    MemcachedAscii(xylem_protocols::memcached::MemcachedAsciiProtocol),
    Http(xylem_protocols::http::HttpProtocol),
    XylemEcho(xylem_protocols::xylem_echo::XylemEchoProtocol),
}

impl Protocol for MultiProtocol {
    type RequestId = (usize, u64);

    fn generate_request(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
    ) -> (Vec<u8>, Self::RequestId) {
        match self {
            MultiProtocol::Redis(p) => p.generate_request(conn_id, key, value_size),
            MultiProtocol::MemcachedBinary(p) => p.generate_request(conn_id, key, value_size),
            MultiProtocol::MemcachedAscii(p) => p.generate_request(conn_id, key, value_size),
            MultiProtocol::Http(p) => p.generate_request(conn_id, key, value_size),
            MultiProtocol::XylemEcho(p) => {
                let (data, req_id) = p.generate_request(conn_id, key, value_size);
                (data, (conn_id, req_id))
            }
        }
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)> {
        match self {
            MultiProtocol::Redis(p) => p.parse_response(conn_id, data),
            MultiProtocol::MemcachedBinary(p) => p.parse_response(conn_id, data),
            MultiProtocol::MemcachedAscii(p) => p.parse_response(conn_id, data),
            MultiProtocol::Http(p) => p.parse_response(conn_id, data),
            MultiProtocol::XylemEcho(p) => {
                let (consumed, req_id) = p.parse_response(conn_id, data)?;
                Ok((consumed, req_id.map(|id| (conn_id, id))))
            }
        }
    }

    fn name(&self) -> &'static str {
        match self {
            MultiProtocol::Redis(p) => p.name(),
            MultiProtocol::MemcachedBinary(p) => p.name(),
            MultiProtocol::MemcachedAscii(p) => p.name(),
            MultiProtocol::Http(p) => p.name(),
            MultiProtocol::XylemEcho(p) => p.name(),
        }
    }

    fn reset(&mut self) {
        match self {
            MultiProtocol::Redis(p) => p.reset(),
            MultiProtocol::MemcachedBinary(p) => p.reset(),
            MultiProtocol::MemcachedAscii(p) => p.reset(),
            MultiProtocol::Http(p) => p.reset(),
            MultiProtocol::XylemEcho(p) => p.reset(),
        }
    }
}

/// Create a protocol instance from a protocol name string
///
/// # Arguments
/// * `name` - Protocol name (redis, memcached-binary, memcached-ascii, http)
/// * `http_config` - Optional (path, host) for HTTP protocol
/// * `redis_selector` - Optional command selector for Redis protocol
///
/// # Returns
/// A MultiProtocol instance configured for the specified protocol
pub fn create_redis_protocol(
    redis_selector: Box<dyn xylem_protocols::CommandSelector<xylem_protocols::RedisOp>>,
) -> MultiProtocol {
    MultiProtocol::Redis(xylem_protocols::redis::RedisProtocol::new(redis_selector))
}

pub fn create_http_protocol(path: &str, host: &str) -> Result<MultiProtocol> {
    Ok(MultiProtocol::Http(xylem_protocols::http::HttpProtocol::new(
        xylem_protocols::HttpMethod::Get,
        path.to_string(),
        host.to_string(),
    )))
}

pub fn create_memcached_binary_protocol() -> MultiProtocol {
    MultiProtocol::MemcachedBinary(xylem_protocols::memcached::MemcachedBinaryProtocol::new(
        xylem_protocols::memcached::MemcachedOp::Get,
    ))
}

pub fn create_memcached_ascii_protocol() -> MultiProtocol {
    MultiProtocol::MemcachedAscii(xylem_protocols::memcached::MemcachedAsciiProtocol::new(
        xylem_protocols::memcached::ascii::MemcachedOp::Get,
    ))
}

pub fn create_xylem_echo_protocol() -> MultiProtocol {
    MultiProtocol::XylemEcho(xylem_protocols::xylem_echo::XylemEchoProtocol::default())
}

// Legacy function for backward compatibility with tests
pub fn create_protocol(
    name: &str,
    http_config: Option<(&str, &str)>,
    redis_selector: Option<Box<dyn xylem_protocols::CommandSelector<xylem_protocols::RedisOp>>>,
) -> Result<MultiProtocol> {
    match name {
        "redis" => {
            let selector = redis_selector.ok_or_else(|| {
                anyhow::anyhow!("Redis protocol requires a command selector")
            })?;
            Ok(create_redis_protocol(selector))
        }
        "memcached-binary" => Ok(MultiProtocol::MemcachedBinary(
            xylem_protocols::memcached::MemcachedBinaryProtocol::new(
                xylem_protocols::memcached::MemcachedOp::Get,
            ),
        )),
        "memcached-ascii" => Ok(MultiProtocol::MemcachedAscii(
            xylem_protocols::memcached::MemcachedAsciiProtocol::new(
                xylem_protocols::memcached::ascii::MemcachedOp::Get,
            ),
        )),
        "http" => {
            let (path, host) = http_config.ok_or_else(|| {
                anyhow::anyhow!("HTTP protocol requires path and host configuration")
            })?;
            Ok(MultiProtocol::Http(xylem_protocols::http::HttpProtocol::new(
                xylem_protocols::HttpMethod::Get,
                path.to_string(),
                host.to_string(),
            )))
        }
        "xylem-echo" => Ok(MultiProtocol::XylemEcho(
            xylem_protocols::xylem_echo::XylemEchoProtocol::default(),
        )),
        _ => bail!(
            "Unknown protocol '{}'. Supported: redis, memcached-binary, memcached-ascii, http, xylem-echo",
            name
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_redis_protocol() {
        let selector =
            Box::new(xylem_protocols::FixedCommandSelector::new(xylem_protocols::RedisOp::Get));
        let protocol = create_redis_protocol(selector);
        assert_eq!(protocol.name(), "redis");
    }

    #[test]
    fn test_create_memcached_binary_protocol() {
        let protocol = create_protocol("memcached-binary", None, None);
        assert!(protocol.is_ok());
        assert_eq!(protocol.unwrap().name(), "memcached-binary");
    }

    #[test]
    fn test_create_memcached_ascii_protocol() {
        let protocol = create_protocol("memcached-ascii", None, None);
        assert!(protocol.is_ok());
        assert_eq!(protocol.unwrap().name(), "memcached-ascii");
    }

    #[test]
    fn test_create_http_protocol() {
        let protocol = create_protocol("http", Some(("/", "localhost:8080")), None);
        assert!(protocol.is_ok());
        assert_eq!(protocol.unwrap().name(), "http");
    }

    #[test]
    fn test_create_http_protocol_without_config_fails() {
        let protocol = create_protocol("http", None, None);
        assert!(protocol.is_err());
    }

    #[test]
    fn test_create_xylem_echo_protocol() {
        let protocol = create_protocol("xylem-echo", None, None);
        assert!(protocol.is_ok());
        assert_eq!(protocol.unwrap().name(), "xylem-echo");
    }

    #[test]
    fn test_create_unknown_protocol_fails() {
        let protocol = create_protocol("unknown", None, None);
        assert!(protocol.is_err());
    }
}
