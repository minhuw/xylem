use crate::config::ProtocolConfig;
use anyhow::Result;
use xylem_common::{ConnectionRole, Outbox, Parsed, Request};
use xylem_protocols::Protocol;

/// Multi-protocol wrapper that can dispatch to any supported protocol
/// This allows different traffic groups to use different protocols
pub enum MultiProtocol {
    Redis(xylem_protocols::redis::RedisProtocol),
    MemcachedBinary(xylem_protocols::memcached::MemcachedBinaryProtocol),
    MemcachedAscii(xylem_protocols::memcached::MemcachedAsciiProtocol),
    Http(xylem_protocols::http::HttpProtocol),
    Http2(xylem_protocols::Http2Protocol),
    Grpc(xylem_protocols::Http2Protocol),
    XylemEcho(xylem_protocols::xylem_echo::XylemEchoProtocol),
    Masstree(xylem_protocols::MasstreeProtocol),
    Dns(xylem_protocols::DnsProtocol),
    NvmeTcp(xylem_protocols::NvmeTcpProtocol),
    ClickHouse(xylem_protocols::ClickHouseProtocol),
}

impl MultiProtocol {
    /// Construct a traffic group's protocol from the protocol_config that
    /// `ProfileConfig::validate` parsed. HTTP sends `target` as the Host header
    /// unless the config sets `host`, and HTTP/2 and gRPC as `:authority`
    /// unless the config sets `authority`, or "localhost" over "unix". Memcached frames its datagrams, and
    /// DNS sends bare messages instead of length-prefixed ones, when the
    /// group's `transport` is "udp". NVMe/TCP sizes its I/O queues to
    /// `max_pending`, the group's `max_pending_per_connection`.
    pub fn from_config(
        config: &ProtocolConfig,
        target: &str,
        transport: &str,
        max_pending: usize,
        seed: Option<u64>,
    ) -> Result<Self> {
        let udp = transport == "udp";
        // A socket path is no authority
        let authority = if transport == "unix" {
            "localhost"
        } else {
            target
        };
        let protocol = match config {
            ProtocolConfig::Redis(redis_config) => {
                Self::Redis(xylem_protocols::RedisProtocol::from_config(redis_config, seed)?)
            }
            ProtocolConfig::MemcachedBinary(mc_config) => {
                let protocol = xylem_protocols::memcached::MemcachedBinaryProtocol::from_config(
                    mc_config, seed,
                )?;
                Self::MemcachedBinary(if udp {
                    protocol.with_udp_framing()
                } else {
                    protocol
                })
            }
            ProtocolConfig::MemcachedAscii(mc_config) => {
                let protocol = xylem_protocols::memcached::MemcachedAsciiProtocol::from_config(
                    mc_config, seed,
                )?;
                Self::MemcachedAscii(if udp {
                    protocol.with_udp_framing()
                } else {
                    protocol
                })
            }
            ProtocolConfig::Http(http_config) => {
                let mut http_config = http_config.clone();
                http_config.host.get_or_insert_with(|| target.to_string());
                Self::Http(xylem_protocols::HttpProtocol::from_config(&http_config)?)
            }
            ProtocolConfig::Http2(http2_config) => {
                Self::Http2(xylem_protocols::Http2Protocol::from_config(http2_config, authority)?)
            }
            ProtocolConfig::Grpc(grpc_config) => Self::Grpc(
                xylem_protocols::Http2Protocol::grpc_from_config(grpc_config, authority)?,
            ),
            ProtocolConfig::Masstree(mt_config) => {
                Self::Masstree(xylem_protocols::MasstreeProtocol::from_config(mt_config, seed)?)
            }
            ProtocolConfig::Dns(dns_config) => {
                let framing = if udp {
                    xylem_protocols::DnsFraming::Datagram
                } else {
                    xylem_protocols::DnsFraming::Stream
                };
                Self::Dns(
                    xylem_protocols::DnsProtocol::from_config(dns_config, seed)?
                        .with_framing(framing),
                )
            }
            ProtocolConfig::NvmeTcp(nvme_config) => Self::NvmeTcp(
                xylem_protocols::NvmeTcpProtocol::from_config(nvme_config, max_pending, seed)?,
            ),
            ProtocolConfig::ClickHouse(ch_config) => {
                Self::ClickHouse(xylem_protocols::ClickHouseProtocol::from_config(ch_config, seed)?)
            }
            ProtocolConfig::XylemEcho(_echo_config) => create_xylem_echo_protocol(),
        };
        Ok(protocol)
    }
}

impl Protocol for MultiProtocol {
    type RequestId = (usize, u64);

    fn next_request(&mut self, conn_id: usize) -> Request<Self::RequestId> {
        match self {
            MultiProtocol::Redis(p) => p.next_request(conn_id),
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
            MultiProtocol::Dns(p) => {
                let request = p.next_request(conn_id);
                let (conn, id) = request.request_id;
                Request::new(request.data, (conn, u64::from(id)), request.metadata)
            }
            MultiProtocol::NvmeTcp(p) => {
                let request = p.next_request(conn_id);
                let (conn, tag) = request.request_id;
                Request::new(request.data, (conn, u64::from(tag)), request.metadata)
            }
            MultiProtocol::Http2(p) | MultiProtocol::Grpc(p) => {
                let request = p.next_request(conn_id);
                let (conn, stream) = request.request_id;
                Request::new(request.data, (conn, u64::from(stream)), request.metadata)
            }
            MultiProtocol::ClickHouse(p) => p.next_request(conn_id),
        }
    }

    fn parse_response<'a>(
        &mut self,
        conn_id: usize,
        data: &'a [u8],
    ) -> Result<Parsed<'a, Self::RequestId>> {
        match self {
            MultiProtocol::Redis(p) => p.parse_response(conn_id, data),
            MultiProtocol::MemcachedBinary(p) => p.parse_response(conn_id, data),
            MultiProtocol::MemcachedAscii(p) => p.parse_response(conn_id, data),
            MultiProtocol::Http(p) => p.parse_response(conn_id, data),
            MultiProtocol::XylemEcho(p) => p.parse_response(conn_id, data),
            MultiProtocol::Masstree(p) => {
                Ok(p.parse_response(conn_id, data)?.map_id(|(conn, seq)| (conn, seq as u64)))
            }
            MultiProtocol::Dns(p) => {
                Ok(p.parse_response(conn_id, data)?.map_id(|(conn, id)| (conn, u64::from(id))))
            }
            MultiProtocol::NvmeTcp(p) => {
                Ok(p.parse_response(conn_id, data)?.map_id(|(conn, tag)| (conn, u64::from(tag))))
            }
            MultiProtocol::Http2(p) | MultiProtocol::Grpc(p) => Ok(p
                .parse_response(conn_id, data)?
                .map_id(|(conn, stream)| (conn, u64::from(stream)))),
            MultiProtocol::ClickHouse(p) => p.parse_response(conn_id, data),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            MultiProtocol::Redis(p) => p.name(),
            MultiProtocol::MemcachedBinary(p) => p.name(),
            MultiProtocol::MemcachedAscii(p) => p.name(),
            MultiProtocol::Http(p) => p.name(),
            MultiProtocol::XylemEcho(p) => p.name(),
            MultiProtocol::Masstree(p) => p.name(),
            MultiProtocol::Dns(p) => p.name(),
            MultiProtocol::NvmeTcp(p) => p.name(),
            MultiProtocol::Http2(p) | MultiProtocol::Grpc(p) => p.name(),
            MultiProtocol::ClickHouse(p) => p.name(),
        }
    }

    fn reset(&mut self) {
        match self {
            MultiProtocol::Redis(p) => p.reset(),
            MultiProtocol::MemcachedBinary(p) => p.reset(),
            MultiProtocol::MemcachedAscii(p) => p.reset(),
            MultiProtocol::Http(p) => p.reset(),
            MultiProtocol::XylemEcho(p) => p.reset(),
            MultiProtocol::Masstree(p) => p.reset(),
            MultiProtocol::Dns(p) => p.reset(),
            MultiProtocol::NvmeTcp(p) => p.reset(),
            MultiProtocol::Http2(p) | MultiProtocol::Grpc(p) => p.reset(),
            MultiProtocol::ClickHouse(p) => p.reset(),
        }
    }

    fn can_send(&self, conn_id: usize) -> bool {
        match self {
            MultiProtocol::Redis(p) => p.can_send(conn_id),
            MultiProtocol::MemcachedBinary(p) => p.can_send(conn_id),
            MultiProtocol::MemcachedAscii(p) => p.can_send(conn_id),
            MultiProtocol::Http(p) => p.can_send(conn_id),
            MultiProtocol::XylemEcho(p) => p.can_send(conn_id),
            MultiProtocol::Masstree(p) => p.can_send(conn_id),
            MultiProtocol::Dns(p) => p.can_send(conn_id),
            MultiProtocol::NvmeTcp(p) => p.can_send(conn_id),
            MultiProtocol::Http2(p) | MultiProtocol::Grpc(p) => p.can_send(conn_id),
            MultiProtocol::ClickHouse(p) => p.can_send(conn_id),
        }
    }

    fn setup_pending(&self, conn_id: usize) -> bool {
        match self {
            MultiProtocol::Redis(p) => p.setup_pending(conn_id),
            MultiProtocol::MemcachedBinary(p) => p.setup_pending(conn_id),
            MultiProtocol::MemcachedAscii(p) => p.setup_pending(conn_id),
            MultiProtocol::Http(p) => p.setup_pending(conn_id),
            MultiProtocol::XylemEcho(p) => p.setup_pending(conn_id),
            MultiProtocol::Masstree(p) => p.setup_pending(conn_id),
            MultiProtocol::Dns(p) => p.setup_pending(conn_id),
            MultiProtocol::NvmeTcp(p) => p.setup_pending(conn_id),
            MultiProtocol::Http2(p) | MultiProtocol::Grpc(p) => p.setup_pending(conn_id),
            MultiProtocol::ClickHouse(p) => p.setup_pending(conn_id),
        }
    }

    fn connection_roles(&self, load: usize) -> Vec<ConnectionRole> {
        match self {
            MultiProtocol::Redis(p) => p.connection_roles(load),
            MultiProtocol::MemcachedBinary(p) => p.connection_roles(load),
            MultiProtocol::MemcachedAscii(p) => p.connection_roles(load),
            MultiProtocol::Http(p) => p.connection_roles(load),
            MultiProtocol::XylemEcho(p) => p.connection_roles(load),
            MultiProtocol::Masstree(p) => p.connection_roles(load),
            MultiProtocol::Dns(p) => p.connection_roles(load),
            MultiProtocol::NvmeTcp(p) => p.connection_roles(load),
            MultiProtocol::Http2(p) | MultiProtocol::Grpc(p) => p.connection_roles(load),
            MultiProtocol::ClickHouse(p) => p.connection_roles(load),
        }
    }

    fn bind_connections(&mut self, conn_ids: &[usize]) {
        match self {
            MultiProtocol::Redis(p) => p.bind_connections(conn_ids),
            MultiProtocol::MemcachedBinary(p) => p.bind_connections(conn_ids),
            MultiProtocol::MemcachedAscii(p) => p.bind_connections(conn_ids),
            MultiProtocol::Http(p) => p.bind_connections(conn_ids),
            MultiProtocol::XylemEcho(p) => p.bind_connections(conn_ids),
            MultiProtocol::Masstree(p) => p.bind_connections(conn_ids),
            MultiProtocol::Dns(p) => p.bind_connections(conn_ids),
            MultiProtocol::NvmeTcp(p) => p.bind_connections(conn_ids),
            MultiProtocol::Http2(p) | MultiProtocol::Grpc(p) => p.bind_connections(conn_ids),
            MultiProtocol::ClickHouse(p) => p.bind_connections(conn_ids),
        }
    }

    fn drain_outbox(&mut self, conn_id: usize, outbox: &mut Outbox) {
        match self {
            MultiProtocol::Redis(p) => p.drain_outbox(conn_id, outbox),
            MultiProtocol::MemcachedBinary(p) => p.drain_outbox(conn_id, outbox),
            MultiProtocol::MemcachedAscii(p) => p.drain_outbox(conn_id, outbox),
            MultiProtocol::Http(p) => p.drain_outbox(conn_id, outbox),
            MultiProtocol::XylemEcho(p) => p.drain_outbox(conn_id, outbox),
            MultiProtocol::Masstree(p) => p.drain_outbox(conn_id, outbox),
            MultiProtocol::Dns(p) => p.drain_outbox(conn_id, outbox),
            MultiProtocol::NvmeTcp(p) => p.drain_outbox(conn_id, outbox),
            MultiProtocol::Http2(p) | MultiProtocol::Grpc(p) => p.drain_outbox(conn_id, outbox),
            MultiProtocol::ClickHouse(p) => p.drain_outbox(conn_id, outbox),
        }
    }
}

/// Create a Redis protocol that draws commands from `redis_selector`
pub fn create_redis_protocol(
    redis_selector: Box<dyn xylem_protocols::CommandSelector<xylem_protocols::RedisOp>>,
) -> MultiProtocol {
    MultiProtocol::Redis(xylem_protocols::redis::RedisProtocol::new(redis_selector))
}

pub fn create_xylem_echo_protocol() -> MultiProtocol {
    MultiProtocol::XylemEcho(xylem_protocols::xylem_echo::XylemEchoProtocol::default())
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
    fn dns_sends_bare_messages_over_udp_only() {
        let config: xylem_protocols::DnsConfig =
            serde_json::from_value(serde_json::json!({ "names": [{ "name": "a.test" }] })).unwrap();
        let config = ProtocolConfig::Dns(config);
        for (transport, framing) in [
            ("tcp", xylem_protocols::DnsFraming::Stream),
            ("unix", xylem_protocols::DnsFraming::Stream),
            ("udp", xylem_protocols::DnsFraming::Datagram),
        ] {
            let protocol = MultiProtocol::from_config(&config, "127.0.0.1:53", transport, 1, None);
            let Ok(MultiProtocol::Dns(dns)) = protocol else {
                panic!("not a DNS protocol");
            };
            assert_eq!(dns.framing(), framing, "{transport}");
            assert_eq!(dns.name(), "dns");
        }
    }
}
