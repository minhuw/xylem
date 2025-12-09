use anyhow::{bail, Result};
use std::collections::HashMap;
use xylem_protocols::Protocol;

pub struct RedisClusterState {
    protocol: xylem_protocols::RedisClusterProtocol,
    request_map: HashMap<(usize, u64), xylem_protocols::ClusterRequestId>,
}

impl RedisClusterState {
    fn simple_id(cluster_id: xylem_protocols::ClusterRequestId) -> (usize, u64) {
        (cluster_id.0, (cluster_id.2).1)
    }

    fn store_mapping(&mut self, cluster_id: xylem_protocols::ClusterRequestId) -> (usize, u64) {
        let simple_id = Self::simple_id(cluster_id);
        self.request_map.insert(simple_id, cluster_id);
        simple_id
    }

    fn remove_mapping(&mut self, simple_id: &(usize, u64)) {
        self.request_map.remove(simple_id);
    }

    fn resolve_cluster_id(
        &mut self,
        conn_id: usize,
        simple_id: (usize, u64),
    ) -> xylem_protocols::ClusterRequestId {
        self.request_map
            .remove(&simple_id)
            .unwrap_or((conn_id, 0, (conn_id, simple_id.1)))
    }
}

/// Multi-protocol wrapper that can dispatch to any supported protocol
/// This allows different traffic groups to use different protocols
pub enum MultiProtocol {
    Redis(xylem_protocols::redis::RedisProtocol),
    RedisCluster(Box<RedisClusterState>),
    MemcachedBinary(xylem_protocols::memcached::MemcachedBinaryProtocol),
    MemcachedAscii(xylem_protocols::memcached::MemcachedAsciiProtocol),
    Http(xylem_protocols::http::HttpProtocol),
    XylemEcho(xylem_protocols::xylem_echo::XylemEchoProtocol),
    Masstree(xylem_protocols::MasstreeProtocol),
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

    /// Register cluster connections from actual pool mappings
    ///
    /// Only applicable for RedisCluster protocol. For other protocols, this is a no-op.
    ///
    /// # Arguments
    ///
    /// * `connections` - Slice of (connection_id, target_address) pairs
    pub fn register_cluster_connections(&mut self, connections: &[(usize, std::net::SocketAddr)]) {
        if let MultiProtocol::RedisCluster(p) = self {
            p.protocol.register_connections(connections.iter().copied());
        }
    }
}

impl Protocol for MultiProtocol {
    type RequestId = (usize, u64);

    fn next_request(
        &mut self,
        conn_id: usize,
    ) -> (Vec<u8>, Self::RequestId, xylem_protocols::RequestMeta) {
        match self {
            MultiProtocol::Redis(p) => p.next_request(conn_id),
            MultiProtocol::RedisCluster(p) => {
                let (data, cluster_req_id, meta) = p.protocol.next_request(conn_id);
                let simple_id = p.store_mapping(cluster_req_id);
                (data, simple_id, meta)
            }
            MultiProtocol::MemcachedBinary(p) => p.next_request(conn_id),
            MultiProtocol::MemcachedAscii(p) => p.next_request(conn_id),
            MultiProtocol::Http(p) => p.next_request(conn_id),
            MultiProtocol::XylemEcho(p) => p.next_request(conn_id),
            MultiProtocol::Masstree(p) => {
                let (data, (conn, seq), meta) = p.next_request(conn_id);
                (data, (conn, seq as u64), meta)
            }
        }
    }

    fn regenerate_request(
        &mut self,
        conn_id: usize,
        original_request_id: Self::RequestId,
    ) -> (Vec<u8>, Self::RequestId, xylem_protocols::RequestMeta) {
        match self {
            MultiProtocol::Redis(p) => p.regenerate_request(conn_id, original_request_id),
            MultiProtocol::RedisCluster(p) => {
                let cluster_req_id = p.resolve_cluster_id(conn_id, original_request_id);
                let (data, new_cluster_id, meta) =
                    p.protocol.regenerate_request(conn_id, cluster_req_id);
                let simple_id = p.store_mapping(new_cluster_id);
                (data, simple_id, meta)
            }
            MultiProtocol::MemcachedBinary(p) => p.regenerate_request(conn_id, original_request_id),
            MultiProtocol::MemcachedAscii(p) => p.regenerate_request(conn_id, original_request_id),
            MultiProtocol::Http(p) => p.regenerate_request(conn_id, original_request_id),
            MultiProtocol::XylemEcho(p) => p.regenerate_request(conn_id, original_request_id),
            MultiProtocol::Masstree(p) => {
                let (data, (conn, seq), meta) = p.regenerate_request(
                    conn_id,
                    (original_request_id.0, original_request_id.1 as u16),
                );
                (data, (conn, seq as u64), meta)
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
            MultiProtocol::RedisCluster(p) => {
                let (consumed, cluster_req_id) = p.protocol.parse_response(conn_id, data)?;
                // Map cluster request ID to simple request ID and clean up mappings
                let req_id = cluster_req_id.map(|cluster_id| {
                    let simple_id = RedisClusterState::simple_id(cluster_id);
                    p.remove_mapping(&simple_id);
                    simple_id
                });
                Ok((consumed, req_id))
            }
            MultiProtocol::MemcachedBinary(p) => p.parse_response(conn_id, data),
            MultiProtocol::MemcachedAscii(p) => p.parse_response(conn_id, data),
            MultiProtocol::Http(p) => p.parse_response(conn_id, data),
            MultiProtocol::XylemEcho(p) => p.parse_response(conn_id, data),
            MultiProtocol::Masstree(p) => {
                let (consumed, req_id) = p.parse_response(conn_id, data)?;
                Ok((consumed, req_id.map(|(conn, seq)| (conn, seq as u64))))
            }
        }
    }

    fn parse_response_extended(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<xylem_protocols::ParseResult<Self::RequestId>> {
        match self {
            MultiProtocol::Redis(p) => p.parse_response_extended(conn_id, data),
            MultiProtocol::RedisCluster(p) => {
                let result = p.protocol.parse_response_extended(conn_id, data)?;
                Ok(match result {
                    xylem_protocols::ParseResult::Complete { bytes_consumed, request_id } => {
                        let simple_id = RedisClusterState::simple_id(request_id);
                        p.remove_mapping(&simple_id);
                        xylem_protocols::ParseResult::Complete {
                            bytes_consumed,
                            request_id: simple_id,
                        }
                    }
                    xylem_protocols::ParseResult::Incomplete => {
                        xylem_protocols::ParseResult::Incomplete
                    }
                    xylem_protocols::ParseResult::Retry(retry) => {
                        let simple_id = RedisClusterState::simple_id(retry.original_request_id);
                        p.request_map.entry(simple_id).or_insert(retry.original_request_id);
                        xylem_protocols::ParseResult::Retry(xylem_protocols::RetryRequest {
                            bytes_consumed: retry.bytes_consumed,
                            original_request_id: simple_id,
                            is_warmup: retry.is_warmup,
                            target_conn_id: retry.target_conn_id,
                            prepare_commands: retry.prepare_commands,
                            attempt: retry.attempt,
                        })
                    }
                })
            }
            MultiProtocol::MemcachedBinary(p) => p.parse_response_extended(conn_id, data),
            MultiProtocol::MemcachedAscii(p) => p.parse_response_extended(conn_id, data),
            MultiProtocol::Http(p) => p.parse_response_extended(conn_id, data),
            MultiProtocol::XylemEcho(p) => p.parse_response_extended(conn_id, data),
            MultiProtocol::Masstree(p) => {
                let result = p.parse_response_extended(conn_id, data)?;
                Ok(match result {
                    xylem_protocols::ParseResult::Complete { bytes_consumed, request_id } => {
                        xylem_protocols::ParseResult::Complete {
                            bytes_consumed,
                            request_id: (request_id.0, request_id.1 as u64),
                        }
                    }
                    xylem_protocols::ParseResult::Incomplete => {
                        xylem_protocols::ParseResult::Incomplete
                    }
                    xylem_protocols::ParseResult::Retry(retry) => {
                        xylem_protocols::ParseResult::Retry(xylem_protocols::RetryRequest {
                            bytes_consumed: retry.bytes_consumed,
                            original_request_id: (
                                retry.original_request_id.0,
                                retry.original_request_id.1 as u64,
                            ),
                            is_warmup: retry.is_warmup,
                            target_conn_id: retry.target_conn_id,
                            prepare_commands: retry.prepare_commands,
                            attempt: retry.attempt,
                        })
                    }
                })
            }
        }
    }

    fn name(&self) -> &'static str {
        match self {
            MultiProtocol::Redis(p) => p.name(),
            MultiProtocol::RedisCluster(p) => p.protocol.name(),
            MultiProtocol::MemcachedBinary(p) => p.name(),
            MultiProtocol::MemcachedAscii(p) => p.name(),
            MultiProtocol::Http(p) => p.name(),
            MultiProtocol::XylemEcho(p) => p.name(),
            MultiProtocol::Masstree(p) => p.name(),
        }
    }

    fn reset(&mut self) {
        match self {
            MultiProtocol::Redis(p) => p.reset(),
            MultiProtocol::RedisCluster(p) => {
                p.request_map.clear();
                p.protocol.reset()
            }
            MultiProtocol::MemcachedBinary(p) => p.reset(),
            MultiProtocol::MemcachedAscii(p) => p.reset(),
            MultiProtocol::Http(p) => p.reset(),
            MultiProtocol::XylemEcho(p) => p.reset(),
            MultiProtocol::Masstree(p) => p.reset(),
        }
    }
}

fn resolve_insert_key_count(
    keys_config: &xylem_protocols::KeysConfig,
    insert_config: &xylem_protocols::InsertPhaseConfig,
    protocol_name: &str,
) -> Result<u64> {
    if let Some(key_count) = insert_config.key_count {
        return Ok(key_count);
    }

    if let Some(keyspace) = keys_config.keyspace_size() {
        return Ok(keyspace);
    }

    bail!(
        "insert_phase.key_count is required for {} when using sequential keys; set insert_phase.key_count explicitly.",
        protocol_name
    );
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

/// Create a Redis protocol with embedded workload generators
pub fn create_redis_protocol_with_workload(
    redis_selector: Box<dyn xylem_protocols::CommandSelector<xylem_protocols::RedisOp>>,
    key_gen: xylem_protocols::workload::KeyGeneration,
    value_size: usize,
) -> MultiProtocol {
    MultiProtocol::Redis(xylem_protocols::redis::RedisProtocol::with_workload(
        redis_selector,
        key_gen,
        value_size,
    ))
}

/// Create a Redis protocol from full config (supports insert phase)
pub fn create_redis_protocol_from_config(
    config: &xylem_protocols::RedisConfig,
    redis_selector: Box<dyn xylem_protocols::CommandSelector<xylem_protocols::RedisOp>>,
    key_gen: xylem_protocols::workload::KeyGeneration,
    seed: Option<u64>,
) -> Result<MultiProtocol> {
    let value_size = config
        .value_size
        .as_ref()
        .map(|vs| vs.fixed_size())
        .unwrap_or_else(|| config.keys.value_size());

    let mut proto = xylem_protocols::redis::RedisProtocol::with_workload_and_options(
        redis_selector,
        key_gen,
        value_size,
        config.keys.prefix().to_string(),
        config.random_data,
        seed,
    );

    // Apply insert phase if configured
    if let Some(ref insert_config) = config.insert_phase {
        let key_count = resolve_insert_key_count(&config.keys, insert_config, "redis")?;
        let insert_value_size = insert_config.value_size.unwrap_or(value_size);
        proto = proto.with_insert_phase(key_count, insert_value_size);
    }

    Ok(MultiProtocol::Redis(proto))
}

pub fn create_http_protocol(path: &str, host: &str) -> Result<MultiProtocol> {
    Ok(MultiProtocol::Http(xylem_protocols::http::HttpProtocol::new(
        xylem_protocols::HttpMethod::Get,
        path.to_string(),
        host.to_string(),
    )))
}

/// Create HTTP protocol with full configuration (method, path, host)
pub fn create_http_protocol_full(
    method: xylem_protocols::HttpMethod,
    path: &str,
    host: &str,
) -> MultiProtocol {
    MultiProtocol::Http(xylem_protocols::http::HttpProtocol::new(
        method,
        path.to_string(),
        host.to_string(),
    ))
}

pub fn create_memcached_binary_protocol() -> MultiProtocol {
    MultiProtocol::MemcachedBinary(xylem_protocols::memcached::MemcachedBinaryProtocol::new(
        xylem_protocols::memcached::MemcachedOp::Get,
    ))
}

/// Create Memcached binary protocol with specified operation
pub fn create_memcached_binary_protocol_with_op(
    operation: xylem_protocols::memcached::MemcachedOp,
) -> MultiProtocol {
    MultiProtocol::MemcachedBinary(xylem_protocols::memcached::MemcachedBinaryProtocol::new(
        operation,
    ))
}

/// Create Memcached binary protocol from full config (supports insert phase)
pub fn create_memcached_binary_protocol_from_config(
    config: &xylem_protocols::MemcachedConfig,
    operation: xylem_protocols::memcached::MemcachedOp,
    key_gen: xylem_protocols::workload::KeyGeneration,
) -> Result<MultiProtocol> {
    let value_size = config.keys.value_size();
    let mut proto = xylem_protocols::memcached::MemcachedBinaryProtocol::with_workload(
        operation, key_gen, value_size,
    );

    // Apply insert phase if configured
    if let Some(ref insert_config) = config.insert_phase {
        let key_count = resolve_insert_key_count(&config.keys, insert_config, "memcached-binary")?;
        let insert_value_size = insert_config.value_size.unwrap_or(value_size);
        proto = proto.with_insert_phase(key_count, insert_value_size);
    }

    Ok(MultiProtocol::MemcachedBinary(proto))
}

pub fn create_memcached_ascii_protocol() -> MultiProtocol {
    MultiProtocol::MemcachedAscii(xylem_protocols::memcached::MemcachedAsciiProtocol::new(
        xylem_protocols::memcached::ascii::MemcachedOp::Get,
    ))
}

/// Create Memcached ASCII protocol with specified operation
pub fn create_memcached_ascii_protocol_with_op(
    operation: xylem_protocols::memcached::ascii::MemcachedOp,
) -> MultiProtocol {
    MultiProtocol::MemcachedAscii(xylem_protocols::memcached::MemcachedAsciiProtocol::new(
        operation,
    ))
}

/// Create Memcached ASCII protocol from full config (supports insert phase)
pub fn create_memcached_ascii_protocol_from_config(
    config: &xylem_protocols::MemcachedConfig,
    operation: xylem_protocols::memcached::ascii::MemcachedOp,
    key_gen: xylem_protocols::workload::KeyGeneration,
) -> Result<MultiProtocol> {
    let value_size = config.keys.value_size();
    let mut proto = xylem_protocols::memcached::MemcachedAsciiProtocol::with_workload(
        operation, key_gen, value_size,
    );

    // Apply insert phase if configured
    if let Some(ref insert_config) = config.insert_phase {
        let key_count = resolve_insert_key_count(&config.keys, insert_config, "memcached-ascii")?;
        let insert_value_size = insert_config.value_size.unwrap_or(value_size);
        proto = proto.with_insert_phase(key_count, insert_value_size);
    }

    Ok(MultiProtocol::MemcachedAscii(proto))
}

pub fn create_xylem_echo_protocol() -> MultiProtocol {
    MultiProtocol::XylemEcho(xylem_protocols::xylem_echo::XylemEchoProtocol::default())
}

/// Create Masstree protocol with specified operation
pub fn create_masstree_protocol(operation: xylem_protocols::MasstreeOp) -> MultiProtocol {
    MultiProtocol::Masstree(xylem_protocols::MasstreeProtocol::new(operation))
}

/// Create Masstree protocol from full config (supports insert phase)
pub fn create_masstree_protocol_from_config(
    config: &xylem_protocols::MasstreeConfig,
    operation: xylem_protocols::MasstreeOp,
    key_gen: xylem_protocols::workload::KeyGeneration,
    seed: Option<u64>,
) -> Result<MultiProtocol> {
    let value_size = config
        .value_size
        .as_ref()
        .map(|vs| vs.fixed_size())
        .unwrap_or_else(|| config.keys.value_size());

    let mut proto = xylem_protocols::MasstreeProtocol::with_workload_and_options(
        operation,
        key_gen,
        value_size,
        config.keys.prefix().to_string(),
        config.random_data,
        seed,
        None, // insert_phase handled below
    );

    // Apply insert phase if configured
    if let Some(ref insert_config) = config.insert_phase {
        let key_count = resolve_insert_key_count(&config.keys, insert_config, "masstree")?; // Sequential requires explicit key_count
        let insert_value_size = insert_config.value_size.unwrap_or(value_size);
        proto = proto.with_insert_phase(key_count, insert_value_size);
    }

    Ok(MultiProtocol::Masstree(proto))
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
    create_redis_cluster_protocol_with_workload(redis_selector, cluster_config, None, 64)
}

fn finalize_redis_cluster_protocol(
    mut protocol: xylem_protocols::RedisClusterProtocol,
    cluster_config: RedisClusterConfig,
) -> Result<MultiProtocol> {
    use xylem_protocols::{ClusterTopology, SlotRange};

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

    Ok(MultiProtocol::RedisCluster(Box::new(RedisClusterState {
        protocol,
        request_map: HashMap::new(),
    })))
}

/// Create Redis cluster protocol with embedded workload
pub fn create_redis_cluster_protocol_with_workload(
    redis_selector: Box<dyn xylem_protocols::CommandSelector<xylem_protocols::RedisOp>>,
    cluster_config: RedisClusterConfig,
    key_gen: Option<xylem_protocols::workload::KeyGeneration>,
    value_size: usize,
) -> Result<MultiProtocol> {
    let protocol = if let Some(kg) = key_gen {
        xylem_protocols::RedisClusterProtocol::with_workload(redis_selector, kg, value_size)
    } else {
        xylem_protocols::RedisClusterProtocol::new(redis_selector)
    };

    finalize_redis_cluster_protocol(protocol, cluster_config)
}

/// Create Redis cluster protocol from full config (supports insert phase)
pub fn create_redis_cluster_protocol_from_config(
    config: &xylem_protocols::RedisConfig,
    redis_selector: Box<dyn xylem_protocols::CommandSelector<xylem_protocols::RedisOp>>,
    cluster_config: RedisClusterConfig,
    key_gen: xylem_protocols::workload::KeyGeneration,
    seed: Option<u64>,
) -> Result<MultiProtocol> {
    let value_size = config
        .value_size
        .as_ref()
        .map(|vs| vs.fixed_size())
        .unwrap_or_else(|| config.keys.value_size());

    let mut protocol = xylem_protocols::RedisClusterProtocol::with_workload_and_options(
        redis_selector,
        key_gen,
        value_size,
        config.keys.prefix().to_string(),
        config.random_data,
        seed,
    );

    // Apply insert phase if configured
    if let Some(ref insert_config) = config.insert_phase {
        let key_count = resolve_insert_key_count(&config.keys, insert_config, "redis-cluster")?;
        let insert_value_size = insert_config.value_size.unwrap_or(value_size);
        protocol = protocol.with_insert_phase(key_count, insert_value_size);
    }

    finalize_redis_cluster_protocol(protocol, cluster_config)
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
