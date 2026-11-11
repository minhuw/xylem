use crate::config::ProtocolConfig;
use anyhow::Result;
use xylem_common::{Parsed, Request};
use xylem_protocols::Protocol;

/// Multi-protocol wrapper that can dispatch to any supported protocol
/// This allows different traffic groups to use different protocols
pub enum MultiProtocol {
    Redis(xylem_protocols::redis::RedisProtocol),
    MemcachedBinary(xylem_protocols::memcached::MemcachedBinaryProtocol),
    MemcachedAscii(xylem_protocols::memcached::MemcachedAsciiProtocol),
    Http(xylem_protocols::http::HttpProtocol),
    XylemEcho(xylem_protocols::xylem_echo::XylemEchoProtocol),
    Masstree(xylem_protocols::MasstreeProtocol),
    Dns(xylem_protocols::DnsProtocol),
}

impl MultiProtocol {
    /// Construct a traffic group's protocol from the protocol_config that
    /// `ProfileConfig::validate` parsed. HTTP sends `target` as the Host header
    /// unless the config sets `host`. Memcached frames its datagrams, and
    /// DNS sends bare messages instead of length-prefixed ones, when the
    /// group's `transport` is "udp".
    pub fn from_config(
        config: &ProtocolConfig,
        target: &str,
        transport: &str,
        seed: Option<u64>,
    ) -> Result<Self> {
        let udp = transport == "udp";
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
            let protocol = MultiProtocol::from_config(&config, "127.0.0.1:53", transport, None);
            let Ok(MultiProtocol::Dns(dns)) = protocol else {
                panic!("not a DNS protocol");
            };
            assert_eq!(dns.framing(), framing, "{transport}");
            assert_eq!(dns.name(), "dns");
        }
    }
}
