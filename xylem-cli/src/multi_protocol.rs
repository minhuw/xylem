use anyhow::{bail, Result};
use xylem_protocols::Protocol;

/// Multi-protocol wrapper that can dispatch to any supported protocol
/// This allows different traffic groups to use different protocols
pub enum MultiProtocol {
    Redis(xylem_protocols::redis::RedisProtocol),
    RedisCluster(Box<xylem_protocols::RedisClusterProtocol>),
    MemcachedBinary(xylem_protocols::memcached::MemcachedBinaryProtocol),
    MemcachedAscii(xylem_protocols::memcached::MemcachedAsciiProtocol),
    Http(xylem_protocols::http::HttpProtocol),
    XylemEcho(xylem_protocols::xylem_echo::XylemEchoProtocol),
}

impl MultiProtocol {
    /// Generate a SET request with imported data (string key + value bytes)
    /// Only supported for Redis protocols
    pub fn generate_set_with_imported_data(
        &mut self,
        conn_id: usize,
        key: &str,
        value: &[u8],
    ) -> (Vec<u8>, (usize, u64)) {
        match self {
            MultiProtocol::Redis(p) => p.generate_set_with_imported_data(conn_id, key, value),
            _ => panic!("generate_set_with_imported_data only supported for Redis protocol"),
        }
    }
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
            MultiProtocol::RedisCluster(p) => {
                let (data, (_orig_conn, _slot, (_target_conn, seq))) =
                    p.generate_request(conn_id, key, value_size);
                // Map cluster request ID to simple request ID
                (data, (conn_id, seq))
            }
            MultiProtocol::MemcachedBinary(p) => p.generate_request(conn_id, key, value_size),
            MultiProtocol::MemcachedAscii(p) => p.generate_request(conn_id, key, value_size),
            MultiProtocol::Http(p) => p.generate_request(conn_id, key, value_size),
            MultiProtocol::XylemEcho(p) => p.generate_request(conn_id, key, value_size),
        }
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)> {
        match self {
            MultiProtocol::Redis(p) => p.parse_response(conn_id, data),
            MultiProtocol::RedisCluster(p) => {
                let (consumed, cluster_req_id) = p.parse_response(conn_id, data)?;
                // Map cluster request ID to simple request ID
                let req_id =
                    cluster_req_id.map(|(orig_conn, _slot, (_target_conn, seq))| (orig_conn, seq));
                Ok((consumed, req_id))
            }
            MultiProtocol::MemcachedBinary(p) => p.parse_response(conn_id, data),
            MultiProtocol::MemcachedAscii(p) => p.parse_response(conn_id, data),
            MultiProtocol::Http(p) => p.parse_response(conn_id, data),
            MultiProtocol::XylemEcho(p) => p.parse_response(conn_id, data),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            MultiProtocol::Redis(p) => p.name(),
            MultiProtocol::RedisCluster(p) => p.name(),
            MultiProtocol::MemcachedBinary(p) => p.name(),
            MultiProtocol::MemcachedAscii(p) => p.name(),
            MultiProtocol::Http(p) => p.name(),
            MultiProtocol::XylemEcho(p) => p.name(),
        }
    }

    fn reset(&mut self) {
        match self {
            MultiProtocol::Redis(p) => p.reset(),
            MultiProtocol::RedisCluster(p) => p.reset(),
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

/// Configuration for Redis Cluster nodes
#[derive(Debug, Clone)]
pub struct RedisClusterConfig {
    /// Cluster node addresses with slot ranges
    pub nodes: Vec<RedisClusterNode>,
}

#[derive(Debug, Clone)]
pub struct RedisClusterNode {
    /// Node address (e.g., "127.0.0.1:7000")
    pub address: String,
    /// Start slot (0-16383)
    pub slot_start: u16,
    /// End slot (0-16383)
    pub slot_end: u16,
}

pub fn create_redis_cluster_protocol(
    redis_selector: Box<dyn xylem_protocols::CommandSelector<xylem_protocols::RedisOp>>,
    cluster_config: RedisClusterConfig,
) -> Result<MultiProtocol> {
    use xylem_protocols::{ClusterTopology, SlotRange};

    let mut protocol = xylem_protocols::RedisClusterProtocol::new(redis_selector);

    // Register connections and build topology
    let mut ranges = Vec::new();
    for (idx, node) in cluster_config.nodes.iter().enumerate() {
        let addr: std::net::SocketAddr = node
            .address
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid node address '{}': {}", node.address, e))?;

        protocol.register_connection(addr, idx);

        ranges.push(SlotRange {
            start: node.slot_start,
            end: node.slot_end,
            master: addr,
            replicas: vec![],
        });
    }

    let topology = ClusterTopology::from_slot_ranges(ranges);
    protocol.update_topology(topology);

    Ok(MultiProtocol::RedisCluster(Box::new(protocol)))
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
            "Unknown protocol '{}'. Supported: redis, redis-cluster, memcached-binary, memcached-ascii, http, xylem-echo",
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

    #[test]
    fn test_multi_protocol_generate_set_with_imported_data_redis() {
        let selector =
            Box::new(xylem_protocols::FixedCommandSelector::new(xylem_protocols::RedisOp::Set));
        let mut protocol = create_redis_protocol(selector);

        let key = "test:key";
        let value = b"test_value";
        let (request, id) = protocol.generate_set_with_imported_data(0, key, value);

        // Verify request ID
        assert_eq!(id, (0, 0));

        // Verify it's a valid RESP SET command
        let request_str = String::from_utf8_lossy(&request);
        assert!(request_str.contains("SET"));
        assert!(request_str.contains(key));
        assert!(request_str.contains("test_value"));
    }

    #[test]
    #[should_panic(expected = "only supported for Redis protocol")]
    fn test_multi_protocol_generate_set_with_imported_data_http_panics() {
        let protocol_result = create_protocol("http", Some(("/", "localhost")), None);
        let mut protocol = protocol_result.unwrap();

        // This should panic because HTTP doesn't support imported data
        protocol.generate_set_with_imported_data(0, "key", b"value");
    }

    #[test]
    #[should_panic(expected = "only supported for Redis protocol")]
    fn test_multi_protocol_generate_set_with_imported_data_memcached_panics() {
        let protocol_result = create_protocol("memcached-binary", None, None);
        let mut protocol = protocol_result.unwrap();

        // This should panic because Memcached doesn't support imported data
        protocol.generate_set_with_imported_data(0, "key", b"value");
    }
}
