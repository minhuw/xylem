use anyhow::{bail, Result};
use std::collections::HashMap;
use xylem_common::Request;
use xylem_protocols::Protocol;

pub struct RedisClusterState {
    protocol: xylem_protocols::RedisClusterProtocol,
    request_map: HashMap<(usize, u64), xylem_protocols::ClusterRequestId>,
}

impl RedisClusterState {
    fn simple_id(cluster_id: xylem_protocols::ClusterRequestId) -> (usize, u64) {
        // The base ID includes the actual target connection. Its sequence is
        // unique only on that connection, not on the original connection.
        cluster_id.2
    }

    fn store_mapping(&mut self, cluster_id: xylem_protocols::ClusterRequestId) -> (usize, u64) {
        let simple_id = Self::simple_id(cluster_id);
        self.request_map.insert(simple_id, cluster_id);
        simple_id
    }

    fn remove_mapping(&mut self, simple_id: &(usize, u64)) {
        self.request_map.remove(simple_id);
    }

    fn resolve_cluster_id(&mut self, simple_id: (usize, u64)) -> xylem_protocols::ClusterRequestId {
        self.request_map
            .remove(&simple_id)
            .expect("Cannot retry a cluster request without its original ID mapping")
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
    /// Wrap a configured cluster protocol; register the pool's real connections
    /// before generating requests.
    pub fn redis_cluster(protocol: xylem_protocols::RedisClusterProtocol) -> Self {
        Self::RedisCluster(Box::new(RedisClusterState { protocol, request_map: HashMap::new() }))
    }

    /// Generate a SET request with imported data (string key + value bytes)
    /// Only supported for Redis protocols
    pub fn generate_set_with_imported_data(
        &mut self,
        conn_id: usize,
        key: &str,
        value: &[u8],
    ) -> Request<(usize, u64)> {
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

    fn next_request(&mut self, conn_id: usize) -> Request<Self::RequestId> {
        match self {
            MultiProtocol::Redis(p) => p.next_request(conn_id),
            MultiProtocol::RedisCluster(p) => {
                let request = p.protocol.next_request_on_connection(conn_id);
                let simple_id = p.store_mapping(request.request_id);
                Request::new(request.data, simple_id, request.metadata)
            }
            MultiProtocol::MemcachedBinary(p) => p.next_request(conn_id),
            MultiProtocol::MemcachedAscii(p) => p.next_request(conn_id),
            MultiProtocol::Http(p) => p.next_request(conn_id),
            MultiProtocol::XylemEcho(p) => p.next_request(conn_id),
            MultiProtocol::Masstree(p) => {
                let request = p.next_request(conn_id);
                Request::new(
                    request.data,
                    (request.request_id.0, request.request_id.1 as u64),
                    request.metadata,
                )
            }
        }
    }

    fn regenerate_request(
        &mut self,
        conn_id: usize,
        original_request_id: Self::RequestId,
    ) -> Request<Self::RequestId> {
        match self {
            MultiProtocol::Redis(p) => p.regenerate_request(conn_id, original_request_id),
            MultiProtocol::RedisCluster(p) => {
                let cluster_req_id = p.resolve_cluster_id(original_request_id);
                let request = p.protocol.regenerate_request(conn_id, cluster_req_id);
                let simple_id = p.store_mapping(request.request_id);
                Request::new(request.data, simple_id, request.metadata)
            }
            MultiProtocol::MemcachedBinary(p) => p.regenerate_request(conn_id, original_request_id),
            MultiProtocol::MemcachedAscii(p) => p.regenerate_request(conn_id, original_request_id),
            MultiProtocol::Http(p) => p.regenerate_request(conn_id, original_request_id),
            MultiProtocol::XylemEcho(p) => p.regenerate_request(conn_id, original_request_id),
            MultiProtocol::Masstree(p) => {
                let request = p.regenerate_request(
                    conn_id,
                    (original_request_id.0, original_request_id.1 as u16),
                );
                Request::new(
                    request.data,
                    (request.request_id.0, request.request_id.1 as u64),
                    request.metadata,
                )
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

    fn can_send(&self, conn_id: usize) -> bool {
        match self {
            MultiProtocol::Redis(p) => p.can_send(conn_id),
            MultiProtocol::RedisCluster(p) => p.protocol.can_send(conn_id),
            MultiProtocol::MemcachedBinary(p) => p.can_send(conn_id),
            MultiProtocol::MemcachedAscii(p) => p.can_send(conn_id),
            MultiProtocol::Http(p) => p.can_send(conn_id),
            MultiProtocol::XylemEcho(p) => p.can_send(conn_id),
            MultiProtocol::Masstree(p) => p.can_send(conn_id),
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
    config.keys.validate()?;
    let value_size = config.keys.value_size();

    let mut proto = xylem_protocols::redis::RedisProtocol::with_workload_and_options(
        redis_selector,
        key_gen,
        value_size,
        config.keys.prefix().to_string(),
        config.random_data,
        seed,
    );

    if let Some(sizes) = &config.value_size {
        proto = proto.with_value_size_generator(sizes.to_generator(seed)?);
    }
    if let Some(insert) = &config.insert_phase {
        let (count, size) = insert.resolve(&config.keys)?;
        proto = proto.with_insert_phase(count, size);
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

    proto = proto.with_key_prefix(config.keys.prefix().to_string());
    config.keys.validate()?;
    if let Some(insert) = &config.insert_phase {
        let (count, size) = insert.resolve(&config.keys)?;
        proto = proto.with_insert_phase(count, size);
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

    proto = proto.with_key_prefix(config.keys.prefix().to_string());
    config.keys.validate()?;
    if let Some(insert) = &config.insert_phase {
        let (count, size) = insert.resolve(&config.keys)?;
        proto = proto.with_insert_phase(count, size);
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
        let (key_count, insert_value_size) = insert_config.resolve(&config.keys)?;
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
    let config = xylem_protocols::configs::RedisClusterConfig {
        nodes: cluster_config
            .nodes
            .into_iter()
            .map(|node| xylem_protocols::configs::RedisClusterNodeConfig {
                address: node.address,
                slot_start: node.slot_start,
                slot_end: node.slot_end,
            })
            .collect(),
    };
    let topology = config.to_topology()?;
    for (idx, node) in config.nodes.iter().enumerate() {
        protocol.register_connection(node.address.parse()?, idx);
    }
    protocol.update_topology(topology);

    Ok(MultiProtocol::redis_cluster(protocol))
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
    let MultiProtocol::Redis(base) =
        create_redis_protocol_from_config(config, redis_selector, key_gen, seed)?
    else {
        unreachable!("Redis adapter always returns Redis")
    };
    let protocol = xylem_protocols::RedisClusterProtocol::from_base_protocol(base);

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
    fn cluster_retry_ids_are_unique_across_targets_and_asking_is_atomic() {
        let config = xylem_protocols::RedisConfig::default();
        let mut protocol = MultiProtocol::redis_cluster(
            xylem_protocols::RedisClusterProtocol::from_config(&config, Some(42)).unwrap(),
        );
        protocol.register_cluster_connections(&[
            (0, "127.0.0.1:7000".parse().unwrap()),
            (1, "127.0.0.1:7001".parse().unwrap()),
            (2, "127.0.0.1:7002".parse().unwrap()),
        ]);
        let originals = [protocol.next_request(0), protocol.next_request(0)];
        let mut retries = Vec::new();
        for (index, original) in originals.iter().enumerate() {
            let target = index + 1;
            let reply = format!("-ASK 123 127.0.0.1:{}\r\n", 7000 + target);
            let xylem_protocols::ParseResult::Retry(retry) =
                protocol.parse_response_extended(0, reply.as_bytes()).unwrap()
            else {
                panic!("expected redirect");
            };
            assert_eq!(retry.original_request_id, original.request_id);
            assert_eq!(retry.prepare_commands, [b"*1\r\n$6\r\nASKING\r\n".to_vec()]);
            let request = protocol.regenerate_request(target, retry.original_request_id);
            assert_eq!(request.data, original.data);
            retries.push(request);
        }
        assert_ne!(retries[0].request_id, retries[1].request_id);
        for (index, retry) in retries.iter().enumerate() {
            let response = b"+OK\r\n$-1\r\n";
            for end in 0..response.len() {
                assert!(matches!(
                    protocol.parse_response_extended(index + 1, &response[..end]).unwrap(),
                    xylem_protocols::ParseResult::Incomplete
                ));
            }
            assert!(matches!(
                protocol.parse_response_extended(index + 1, response).unwrap(),
                xylem_protocols::ParseResult::Complete { request_id, bytes_consumed: 10 }
                    if request_id == retry.request_id
            ));
        }
        let MultiProtocol::RedisCluster(state) = protocol else {
            unreachable!()
        };
        assert!(state.request_map.is_empty());
    }

    #[test]
    fn cluster_wrapper_tracks_the_socket_selected_by_the_worker() {
        let config: xylem_protocols::RedisConfig = serde_json::from_value(serde_json::json!({
            "redis_cluster": { "nodes": [{
                "address": "127.0.0.1:7001", "slot_start": 0, "slot_end": 16383
            }] }
        }))
        .unwrap();
        let mut protocol = MultiProtocol::redis_cluster(
            xylem_protocols::RedisClusterProtocol::from_config(&config, Some(42)).unwrap(),
        );
        protocol.register_cluster_connections(&[
            (7, "127.0.0.1:7000".parse().unwrap()),
            (19, "127.0.0.1:7001".parse().unwrap()),
        ]);
        let request = protocol.next_request(7);
        assert_eq!(request.request_id, (7, 0));
        let xylem_protocols::ParseResult::Retry(retry) =
            protocol.parse_response_extended(7, b"-MOVED 123 127.0.0.1:7001\r\n").unwrap()
        else {
            panic!("expected redirect");
        };
        assert_eq!(retry.original_request_id, request.request_id);
        assert_eq!(retry.target_conn_id, Some(19));
        let retry = protocol.regenerate_request(19, request.request_id);
        assert_eq!(retry.data, request.data);
        assert_eq!(protocol.parse_response(19, b"$-1\r\n").unwrap(), (5, Some(retry.request_id)));
    }

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
        let request = protocol.generate_set_with_imported_data(0, key, value);

        // Verify request ID
        assert_eq!(request.request_id, (0, 0));

        // Verify it's a valid RESP SET command
        let request_str = String::from_utf8_lossy(&request.data);
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
