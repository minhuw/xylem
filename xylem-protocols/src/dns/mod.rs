//! DNS queries (RFC 1035) over a byte stream or datagrams
//!
//! Every request is a standard query (opcode QUERY) with one question in
//! class IN, optionally with an EDNS(0) OPT record. Its name and type are
//! drawn independently, or together as a pair ([`DnsConfig::queries`]). Its
//! 16-bit message ID is its request ID, unique among the connection's
//! queries in flight.
//!
//! # Framing
//!
//! [`DnsFraming::Stream`] prefixes each message with its length as a
//! big-endian u16 (RFC 1035 section 4.2.2, RFC 7766 section 8), for byte
//! stream transports. [`DnsFraming::Datagram`] sends and receives bare
//! messages, one per datagram (RFC 1035 section 4.2.1). Responses on a
//! stream may arrive in any order (RFC 7766 section 7) and are matched to
//! their query by ID.
//!
//! # Responses
//!
//! A response must have the QR bit, opcode QUERY, and the ID of a query in
//! flight; it must echo its query's question, unless it has no question and
//! an RCODE other than NOERROR (as a FORMERR may), and its records must be
//! well formed. Anything else is an error that fails the run, except on
//! datagrams, where a response for no query in flight or for another
//! question is absorbed as a duplicate or a late response to a reused ID.
//!
//! A response with the TC bit is an error reply of kind "TC". Otherwise an
//! RCODE other than NOERROR (with the extended bits of an OPT record) is an
//! error reply whose kind is the RCODE's name, such as "SERVFAIL"; NXDOMAIN
//! may count as success instead ([`DnsNxdomain`]).
//!
//! # IDs over datagrams
//!
//! On a stream, a new query skips the IDs of queries still in flight. On
//! datagrams, a lost response never arrives to free its ID, so IDs are
//! handed out in turn and a query reuses the ID of the query sent 65536
//! queries before it, as memcached's UDP request IDs do.
//!
//! Zone transfers (AXFR, IXFR), NOTIFY, UPDATE and TSIG are out of scope.

pub mod config;
pub mod wire;

pub use config::{
    DnsConfig, DnsEdnsConfig, DnsKeysConfig, DnsNameConfig, DnsNxdomain, DnsQtypeConfig,
    DnsQueryConfig,
};

use crate::workload::{CommandSelector, KeyGeneration, WeightedCommandSelector};
use crate::{ErrorReply, Parsed, Protocol, Request};
use anyhow::{bail, ensure, Context, Result};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use rand_distr::{weighted::WeightedIndex, Distribution};
use std::collections::HashMap;
use wire::Edns;

/// Number of distinct message IDs
pub const ID_COUNT: usize = 1 << 16;

/// The `{key}` placeholder of a name pattern
const KEY_PLACEHOLDER: &str = "{key}";

/// The longest decimal key, which name patterns must have room for
const LONGEST_KEY: &str = "18446744073709551615";

/// How DNS messages are delimited
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DnsFraming {
    /// Each message follows its 2-byte length (TCP, RFC 7766)
    #[default]
    Stream,
    /// Each message is one datagram (UDP)
    Datagram,
}

/// Where a query's name comes from
#[derive(Debug)]
enum NameSource {
    /// A fixed name in wire form
    Fixed(Vec<u8>),
    /// The text around `{key}` and the key generator
    Pattern {
        before: String,
        after: String,
        keys: KeyGeneration,
    },
}

/// A weighted choice among `items`; a single item needs no random draw
struct Mix<T> {
    items: Vec<T>,
    weights: Option<WeightedIndex<f64>>,
}

impl<T> Mix<T> {
    fn new(entries: Vec<(T, f64)>, what: &str) -> Result<Self> {
        let (items, weights): (Vec<T>, Vec<f64>) = entries.into_iter().unzip();
        check_weights(&weights, what)?;
        let weights = if items.len() > 1 {
            Some(WeightedIndex::new(weights)?)
        } else {
            None
        };
        Ok(Self { items, weights })
    }

    fn pick(&mut self, rng: &mut SmallRng) -> &mut T {
        let index = self.weights.as_ref().map_or(0, |weights| weights.sample(rng));
        &mut self.items[index]
    }
}

/// Check the weights of a mix: at least one, none negative, not all zero
fn check_weights(weights: &[f64], what: &str) -> Result<()> {
    ensure!(!weights.is_empty(), "DNS {what} must list at least one entry");
    for weight in weights {
        ensure!(
            weight.is_finite() && *weight >= 0.0,
            "DNS {what} weight {weight} must be finite and >= 0"
        );
    }
    ensure!(weights.iter().any(|w| *w > 0.0), "DNS {what} weights must not all be zero");
    Ok(())
}

/// How each query's question is drawn
enum Questions {
    /// A name and a type, independently
    Independent { names: Mix<NameSource>, qtypes: Mix<u16> },
    /// One of `pairs`, by its index from `selector`
    Pairs {
        pairs: Vec<(NameSource, u16)>,
        selector: WeightedCommandSelector<usize>,
    },
}

/// A query in flight
#[derive(Debug)]
struct InFlight {
    /// Its question's name in wire form
    name: Box<[u8]>,
    qtype: u16,
}

/// The queries in flight on one connection
#[derive(Debug, Default)]
struct Connection {
    /// ID to try next
    next_id: u16,
    in_flight: HashMap<u16, InFlight>,
}

/// DNS query load
pub struct DnsProtocol {
    questions: Questions,
    /// Draws the names and types of `Questions::Independent`
    rng: SmallRng,
    /// Header flags of every query
    flags: u16,
    edns: Option<Edns>,
    nxdomain_is_success: bool,
    framing: DnsFraming,
    connections: HashMap<usize, Connection>,
}

impl DnsProtocol {
    /// Build the query load of `config`; `seed` makes its draws reproducible
    pub fn from_config(config: &DnsConfig, seed: Option<u64>) -> Result<Self> {
        use xylem_core::seed::{components, derive_seed};
        config.validate()?;
        let seed_for = |component: &str| seed.map(|seed| derive_seed(seed, component));

        let source = |parsed: ParsedName, component: String| -> Result<NameSource> {
            Ok(match parsed {
                ParsedName::Fixed(name) => NameSource::Fixed(name),
                ParsedName::Pattern { before, after, keys } => NameSource::Pattern {
                    before: before.to_string(),
                    after: after.to_string(),
                    keys: keys.to_keys_config().to_key_gen(seed_for(&component))?,
                },
            })
        };

        let questions = if config.queries.is_empty() {
            let mut names = Vec::with_capacity(config.names.len());
            for (index, entry) in config.names.iter().enumerate() {
                let parsed = parse_name(entry.name(), entry.pattern(), entry.keys.as_ref())?;
                names.push((source(parsed, format!("dns_name_{index}"))?, entry.weight));
            }
            let mut qtypes = Vec::new();
            for entry in config.qtypes.clone().unwrap_or_else(config::default_qtypes) {
                qtypes.push((wire::parse_qtype(&entry.qtype)?, entry.weight));
            }
            Questions::Independent {
                names: Mix::new(names, "names")?,
                qtypes: Mix::new(qtypes, "qtypes")?,
            }
        } else {
            let mut pairs = Vec::with_capacity(config.queries.len());
            let mut weights = Vec::with_capacity(config.queries.len());
            for (index, entry) in config.queries.iter().enumerate() {
                let parsed = parse_name(entry.name(), entry.pattern(), entry.keys.as_ref())?;
                let name = source(parsed, format!("dns_query_{index}"))?;
                pairs.push((name, wire::parse_qtype(&entry.qtype)?));
                weights.push((index, entry.weight));
            }
            let selector = WeightedCommandSelector::with_seed(weights, seed_for("dns_queries"))?;
            Questions::Pairs { pairs, selector }
        };
        let edns = config.edns.as_ref().map(|edns| Edns {
            udp_size: edns.udp_size,
            dnssec_ok: edns.dnssec_ok,
        });
        let rng = match seed_for(components::PROTOCOL) {
            Some(seed) => SmallRng::seed_from_u64(seed),
            None => SmallRng::from_os_rng(),
        };
        Ok(Self {
            questions,
            rng,
            flags: if config.recursion_desired {
                wire::FLAG_RD
            } else {
                0
            },
            edns,
            nxdomain_is_success: config.nxdomain == DnsNxdomain::Success,
            framing: DnsFraming::Stream,
            connections: HashMap::new(),
        })
    }

    /// Delimit messages with `framing` instead of the default 2-byte length
    pub fn with_framing(mut self, framing: DnsFraming) -> Self {
        self.framing = framing;
        self
    }

    /// The framing messages use
    pub fn framing(&self) -> DnsFraming {
        self.framing
    }
}

/// A free ID on `connection`, which then names its next query
fn allocate_id(connection: &mut Connection, framing: DnsFraming) -> u16 {
    let mut id = connection.next_id;
    if framing == DnsFraming::Stream {
        assert!(
            connection.in_flight.len() < ID_COUNT,
            "every DNS message ID is in flight; can_send must hold back"
        );
        while connection.in_flight.contains_key(&id) {
            id = id.wrapping_add(1);
        }
    }
    connection.next_id = id.wrapping_add(1);
    id
}

impl DnsConfig {
    /// Check every name, type, weight and the EDNS settings, and that
    /// `queries` comes without `names` and `qtypes`
    pub fn validate(&self) -> Result<()> {
        if self.queries.is_empty() {
            ensure!(
                !self.names.is_empty(),
                "DNS names must list at least one entry, or set queries"
            );
            for (index, entry) in self.names.iter().enumerate() {
                parse_name(entry.name(), entry.pattern(), entry.keys.as_ref())
                    .with_context(|| format!("Invalid DNS names[{index}]"))?;
            }
            let weights: Vec<f64> = self.names.iter().map(|entry| entry.weight).collect();
            check_weights(&weights, "names")?;
            let qtypes = self.qtypes.clone().unwrap_or_else(config::default_qtypes);
            for entry in &qtypes {
                wire::parse_qtype(&entry.qtype)?;
            }
            check_weights(&qtypes.iter().map(|entry| entry.weight).collect::<Vec<_>>(), "qtypes")?;
        } else {
            ensure!(
                self.names.is_empty() && self.qtypes.is_none(),
                "DNS queries replace names and qtypes: set either queries, or names and qtypes"
            );
            for (index, entry) in self.queries.iter().enumerate() {
                parse_name(entry.name(), entry.pattern(), entry.keys.as_ref())
                    .and_then(|_| wire::parse_qtype(&entry.qtype))
                    .with_context(|| format!("Invalid DNS queries[{index}]"))?;
            }
            let weights: Vec<f64> = self.queries.iter().map(|entry| entry.weight).collect();
            check_weights(&weights, "queries")?;
        }
        if let Some(edns) = &self.edns {
            ensure!(
                edns.udp_size >= 512,
                "DNS edns.udp_size {} must be >= 512 (RFC 6891)",
                edns.udp_size
            );
        }
        Ok(())
    }
}

/// A checked entry of the name mix
enum ParsedName<'a> {
    /// A fixed name in wire form
    Fixed(Vec<u8>),
    /// The text around `{key}` and the keys
    Pattern {
        before: &'a str,
        after: &'a str,
        keys: &'a DnsKeysConfig,
    },
}

impl DnsNameConfig {
    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    fn pattern(&self) -> Option<&str> {
        self.pattern.as_deref()
    }
}

impl DnsQueryConfig {
    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    fn pattern(&self) -> Option<&str> {
        self.pattern.as_deref()
    }
}

/// Check a name entry: a valid `name`, or a `pattern` that makes valid
/// names with valid `keys`
fn parse_name<'a>(
    name: Option<&str>,
    pattern: Option<&'a str>,
    keys: Option<&'a DnsKeysConfig>,
) -> Result<ParsedName<'a>> {
    match (name, pattern, keys) {
        (Some(name), None, None) => Ok(ParsedName::Fixed(wire::encode_name(name)?)),
        (None, Some(pattern), Some(keys)) => {
            let Some((before, after)) = pattern.split_once(KEY_PLACEHOLDER) else {
                bail!("pattern '{pattern}' has no {KEY_PLACEHOLDER}");
            };
            ensure!(
                !after.contains(KEY_PLACEHOLDER),
                "pattern '{pattern}' has more than one {KEY_PLACEHOLDER}"
            );
            // Every key must make a valid name, the longest included
            wire::encode_name(&format!("{before}{LONGEST_KEY}{after}"))
                .with_context(|| format!("pattern '{pattern}' with a 20-digit key"))?;
            keys.to_keys_config().validate()?;
            Ok(ParsedName::Pattern { before, after, keys })
        }
        (None, Some(_), None) => bail!("a pattern needs keys"),
        (Some(_), None, Some(_)) => bail!("keys are for a pattern, not a fixed name"),
        (None, None, _) => bail!("set either name or pattern"),
        (Some(_), Some(_), _) => bail!("set either name or pattern, not both"),
    }
}

impl NameSource {
    /// The next name in wire form
    fn next_name(&mut self) -> Vec<u8> {
        match self {
            Self::Fixed(name) => name.clone(),
            Self::Pattern { before, after, keys } => {
                let name = format!("{before}{}{after}", keys.next_key());
                wire::encode_name(&name).expect("a checked pattern makes valid names")
            }
        }
    }

    fn reset(&mut self) {
        if let Self::Pattern { keys, .. } = self {
            keys.reset();
        }
    }
}

impl Protocol for DnsProtocol {
    /// The connection and the message ID
    type RequestId = (usize, u16);

    fn next_request(&mut self, conn_id: usize) -> Request<Self::RequestId> {
        let (name, qtype) = match &mut self.questions {
            Questions::Independent { names, qtypes } => {
                (names.pick(&mut self.rng).next_name(), *qtypes.pick(&mut self.rng))
            }
            Questions::Pairs { pairs, selector } => {
                let (source, qtype) = &mut pairs[selector.next_command()];
                (source.next_name(), *qtype)
            }
        };
        let connection = self.connections.entry(conn_id).or_default();
        let id = allocate_id(connection, self.framing);

        let len = wire::query_len(name.len(), self.edns.is_some());
        let mut data = Vec::with_capacity(len + 2);
        if self.framing == DnsFraming::Stream {
            data.extend_from_slice(&(len as u16).to_be_bytes());
        }
        wire::write_query(&mut data, id, self.flags, &name, qtype, self.edns);

        connection
            .in_flight
            .insert(id, InFlight { name: name.into_boxed_slice(), qtype });
        Request::measurement(data, (conn_id, id))
    }

    fn parse_response<'a>(
        &mut self,
        conn_id: usize,
        data: &'a [u8],
    ) -> Result<Parsed<'a, Self::RequestId>> {
        let datagram = self.framing == DnsFraming::Datagram;
        let (message, consumed) = if datagram {
            (data, data.len())
        } else {
            let Some(len) = data.get(..2) else {
                return Ok(Parsed::Incomplete);
            };
            let end = 2 + u16::from_be_bytes([len[0], len[1]]) as usize;
            let Some(message) = data.get(2..end) else {
                return Ok(Parsed::Incomplete);
            };
            (message, end)
        };

        let response = wire::decode_response(message)?;
        ensure!(
            response.flags & wire::FLAG_QR != 0,
            "DNS message {} is not a response",
            response.id
        );
        ensure!(
            response.opcode() == 0,
            "DNS response {} has opcode {}, not QUERY",
            response.id,
            response.opcode()
        );
        let connection = self.connections.entry(conn_id).or_default();
        let Some(query) = connection.in_flight.get(&response.id) else {
            if datagram {
                return Ok(Parsed::Absorbed(consumed));
            }
            bail!("DNS response {} matches no query in flight", response.id);
        };
        let echoed = match &response.question {
            Some(question) => question.matches(&query.name, query.qtype),
            None => response.rcode != wire::RCODE_NOERROR,
        };
        if !echoed {
            if datagram {
                return Ok(Parsed::Absorbed(consumed));
            }
            bail!("DNS response {} does not echo its query's question", response.id);
        }
        connection.in_flight.remove(&response.id);

        let id = (conn_id, response.id);
        if response.flags & wire::FLAG_TC != 0 {
            return Ok(Parsed::error(
                consumed,
                id,
                ErrorReply::new("TC", "TC: truncated response"),
            ));
        }
        match response.rcode {
            wire::RCODE_NOERROR => Ok(Parsed::ok(consumed, id)),
            wire::RCODE_NXDOMAIN if self.nxdomain_is_success => Ok(Parsed::ok(consumed, id)),
            rcode => {
                let name = wire::rcode_name(rcode);
                Ok(Parsed::error(consumed, id, ErrorReply::new(name, name)))
            }
        }
    }

    fn can_send(&self, conn_id: usize) -> bool {
        self.framing == DnsFraming::Datagram
            || self.connections.get(&conn_id).is_none_or(|c| c.in_flight.len() < ID_COUNT)
    }

    fn name(&self) -> &'static str {
        "dns"
    }

    fn reset(&mut self) {
        self.connections.clear();
        match &mut self.questions {
            Questions::Independent { names, .. } => {
                names.items.iter_mut().for_each(NameSource::reset)
            }
            Questions::Pairs { pairs, selector } => {
                pairs.iter_mut().for_each(|(source, _)| source.reset());
                selector.reset();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire::{encode_name, CLASS_IN, FLAG_QR, FLAG_RD, FLAG_TC};

    fn config(names: &[&str], qtypes: &[&str]) -> DnsConfig {
        DnsConfig {
            names: names
                .iter()
                .map(|name| DnsNameConfig {
                    name: Some(name.to_string()),
                    pattern: None,
                    keys: None,
                    weight: 1.0,
                })
                .collect(),
            qtypes: Some(
                qtypes
                    .iter()
                    .map(|qtype| DnsQtypeConfig { qtype: qtype.to_string(), weight: 1.0 })
                    .collect(),
            ),
            ..DnsConfig::default()
        }
    }

    fn protocol(names: &[&str], qtypes: &[&str]) -> DnsProtocol {
        DnsProtocol::from_config(&config(names, qtypes), Some(7)).unwrap()
    }

    /// The query message of a stream request, without its length
    fn message(request: &Request<(usize, u16)>) -> &[u8] {
        let len = u16::from_be_bytes([request.data[0], request.data[1]]) as usize;
        assert_eq!(request.data.len(), len + 2);
        &request.data[2..]
    }

    /// A response to `query` (a query message) with `flags` OR'd into its
    /// header flags and `rcode`, answering with one A record
    fn reply(query: &[u8], flags: u16, rcode: u16) -> Vec<u8> {
        let question_end = query.len() - 11 * usize::from(query[11] == 1);
        let mut message = query[..question_end].to_vec();
        let old = u16::from_be_bytes([message[2], message[3]]);
        message[2..4].copy_from_slice(&(old | FLAG_QR | flags | rcode).to_be_bytes());
        message[6..8].copy_from_slice(&1u16.to_be_bytes());
        message[10..12].copy_from_slice(&0u16.to_be_bytes());
        message.extend_from_slice(&[0xc0, 12, 0, 1, 0, 1, 0, 0, 1, 44, 0, 4, 192, 0, 2, 80]);
        message
    }

    fn framed(message: &[u8]) -> Vec<u8> {
        let mut data = (message.len() as u16).to_be_bytes().to_vec();
        data.extend_from_slice(message);
        data
    }

    #[test]
    fn queries_carry_name_type_and_flags() {
        let mut dns = protocol(&["www.example.test."], &["AAAA"]);
        let request = dns.next_request(0);
        assert_eq!(request.request_id, (0, 0));
        assert!(!request.metadata.is_warmup);
        let name = encode_name("www.example.test").unwrap();
        let mut expected = vec![0, 0, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0];
        expected.extend_from_slice(&name);
        expected.extend_from_slice(&[0, 28, 0, CLASS_IN as u8]);
        assert_eq!(message(&request), expected);
        assert_eq!(dns.next_request(0).request_id, (0, 1));
        assert_eq!(dns.next_request(3).request_id, (3, 0), "IDs are per connection");
        assert_eq!(dns.name(), "dns");
    }

    #[test]
    fn rd_and_edns_follow_the_config() {
        let mut config = config(&["a.test"], &["A"]);
        config.recursion_desired = false;
        config.edns = Some(DnsEdnsConfig { udp_size: 4096, dnssec_ok: true });
        let mut dns = DnsProtocol::from_config(&config, None).unwrap();
        let request = dns.next_request(0);
        let query = message(&request);
        assert_eq!(u16::from_be_bytes([query[2], query[3]]) & FLAG_RD, 0);
        assert_eq!(&query[10..12], &[0, 1]);
        assert_eq!(&query[query.len() - 11..], &[0, 0, 41, 0x10, 0, 0, 0, 0x80, 0, 0, 0]);

        config.edns = Some(DnsEdnsConfig { udp_size: 511, dnssec_ok: false });
        assert!(DnsProtocol::from_config(&config, None).is_err());
    }

    #[test]
    fn datagram_queries_have_no_length_prefix() {
        let mut dns = protocol(&["a.test"], &["A"]).with_framing(DnsFraming::Datagram);
        assert_eq!(dns.framing(), DnsFraming::Datagram);
        let request = dns.next_request(0);
        assert_eq!(&request.data[..2], &[0, 0], "the ID comes first");
        assert_eq!(request.data.len(), wire::query_len(8, false));
        let response = reply(&request.data, 0, 0);
        assert_eq!(dns.parse_response(0, &response).unwrap(), Parsed::ok(response.len(), (0, 0)));
    }

    #[test]
    fn responses_complete_their_query_in_any_order() {
        let mut dns = protocol(&["a.test", "b.test"], &["A", "MX"]);
        let requests: Vec<_> = (0..4).map(|_| dns.next_request(0)).collect();
        let mut data = Vec::new();
        for request in requests.iter().rev() {
            data.extend_from_slice(&framed(&reply(message(request), 0x0400, 0)));
        }
        let mut at = 0;
        for request in requests.iter().rev() {
            let parsed = dns.parse_response(0, &data[at..]).unwrap();
            assert_eq!(parsed, Parsed::ok(parsed.consumed(), request.request_id));
            at += parsed.consumed();
        }
        assert_eq!(at, data.len());
        assert!(dns.connections[&0].in_flight.is_empty());
    }

    #[test]
    fn partial_stream_responses_are_incomplete() {
        let mut dns = protocol(&["a.test"], &["A"]);
        let request = dns.next_request(0);
        let data = framed(&reply(message(&request), 0, 0));
        for end in 0..data.len() {
            assert_eq!(dns.parse_response(0, &data[..end]).unwrap(), Parsed::Incomplete, "{end}");
        }
        assert_eq!(dns.parse_response(0, &data).unwrap(), Parsed::ok(data.len(), (0, 0)));
    }

    #[test]
    fn rcodes_map_to_error_replies() {
        let mut dns = protocol(&["a.test"], &["A"]);
        for (rcode, kind) in [(2, "SERVFAIL"), (3, "NXDOMAIN"), (5, "REFUSED"), (4, "NOTIMP")] {
            let request = dns.next_request(0);
            let data = framed(&reply(message(&request), 0, rcode));
            let parsed = dns.parse_response(0, &data).unwrap();
            let Parsed::Complete { id, error: Some(error), .. } = parsed else {
                panic!("{parsed:?}");
            };
            assert_eq!((id, error.kind), (request.request_id, kind));
        }
    }

    #[test]
    fn nxdomain_may_count_as_success() {
        let mut config = config(&["absent.test"], &["A"]);
        config.nxdomain = DnsNxdomain::Success;
        let mut dns = DnsProtocol::from_config(&config, None).unwrap();
        let request = dns.next_request(0);
        let data = framed(&reply(message(&request), 0, 3));
        assert_eq!(dns.parse_response(0, &data).unwrap(), Parsed::ok(data.len(), (0, 0)));
        // Other RCODEs stay errors
        let request = dns.next_request(0);
        let data = framed(&reply(message(&request), 0, 2));
        assert!(matches!(
            dns.parse_response(0, &data).unwrap(),
            Parsed::Complete { error: Some(_), .. }
        ));
    }

    #[test]
    fn truncated_responses_are_error_replies() {
        let mut dns = protocol(&["a.test"], &["TXT"]).with_framing(DnsFraming::Datagram);
        let request = dns.next_request(0);
        let response = reply(&request.data, FLAG_TC, 0);
        let parsed = dns.parse_response(0, &response).unwrap();
        let Parsed::Complete { error: Some(error), consumed, .. } = parsed else {
            panic!("{parsed:?}");
        };
        assert_eq!((error.kind, consumed), ("TC", response.len()));
    }

    #[test]
    fn formerr_without_question_completes_its_query() {
        let mut dns = protocol(&["a.test"], &["A"]);
        let request = dns.next_request(0);
        let response = [0, 0, 0x81, 0x01, 0, 0, 0, 0, 0, 0, 0, 0];
        let data = framed(&response);
        let parsed = dns.parse_response(0, &data).unwrap();
        assert_eq!(
            parsed,
            Parsed::error(14, request.request_id, ErrorReply::new("FORMERR", "FORMERR"))
        );
        // A NOERROR response must echo the question
        dns.next_request(0);
        let response = [0, 1, 0x81, 0x00, 0, 0, 0, 0, 0, 0, 0, 0];
        assert!(dns.parse_response(0, &framed(&response)).is_err());
    }

    #[test]
    fn mismatched_responses_fail_on_a_stream() {
        let mut dns = protocol(&["a.test"], &["A"]);
        let request = dns.next_request(0);
        let query = message(&request).to_vec();
        // Unknown ID
        let mut unknown = reply(&query, 0, 0);
        unknown[1] = 9;
        assert!(dns.parse_response(0, &framed(&unknown)).is_err());
        // A query, not a response
        assert!(dns.parse_response(0, &framed(&query)).is_err());
        // Another question
        let mut other = reply(&query, 0, 0);
        other[13] = b'b';
        assert!(dns.parse_response(0, &framed(&other)).is_err());
        // Another type
        let mut other_type = reply(&query, 0, 0);
        other_type[12 + 8 + 1] = 28;
        assert!(dns.parse_response(0, &framed(&other_type)).is_err());
        // Another opcode
        let notify = reply(&query, 4 << 11, 0);
        assert!(dns.parse_response(0, &framed(&notify)).is_err());
        // Malformed records
        let mut cut = reply(&query, 0, 0);
        cut.pop();
        assert!(dns.parse_response(0, &framed(&cut)).is_err());
        assert!(dns.parse_response(0, &framed(&[0; 5])).is_err());
        // Case-insensitive question echo still completes the query
        let mut upper = reply(&query, 0, 0);
        upper[13] = b'A';
        assert!(dns.parse_response(0, &framed(&upper)).is_ok());
    }

    #[test]
    fn stray_datagrams_are_absorbed() {
        let mut dns = protocol(&["a.test"], &["A"]).with_framing(DnsFraming::Datagram);
        let request = dns.next_request(0);
        let mut unknown = reply(&request.data, 0, 0);
        unknown[1] = 9;
        assert_eq!(dns.parse_response(0, &unknown).unwrap(), Parsed::Absorbed(unknown.len()));
        let mut other = reply(&request.data, 0, 0);
        other[13] = b'b';
        assert_eq!(dns.parse_response(0, &other).unwrap(), Parsed::Absorbed(other.len()));
        let response = reply(&request.data, 0, 0);
        assert_eq!(dns.parse_response(0, &response).unwrap(), Parsed::ok(response.len(), (0, 0)));
        // A duplicate of a completed response
        assert_eq!(dns.parse_response(0, &response).unwrap(), Parsed::Absorbed(response.len()));
        // Malformed datagrams still fail
        assert!(dns.parse_response(0, &response[..5]).is_err());
    }

    #[test]
    fn stream_ids_skip_queries_in_flight() {
        let mut dns = protocol(&["a.test"], &["A"]);
        let first = dns.next_request(0);
        assert_eq!(first.request_id, (0, 0));
        // Answer everything but ID 0 through a full wrap of the ID space
        for expected in 1..ID_COUNT as u32 {
            let request = dns.next_request(0);
            assert_eq!(request.request_id, (0, expected as u16));
            let data = framed(&reply(message(&request), 0, 0));
            dns.parse_response(0, &data).unwrap();
        }
        let request = dns.next_request(0);
        assert_eq!(request.request_id, (0, 1), "ID 0 is still in flight");
        let data = framed(&reply(message(&first), 0, 0));
        assert_eq!(dns.parse_response(0, &data).unwrap(), Parsed::ok(data.len(), (0, 0)));
        assert_eq!(dns.next_request(0).request_id, (0, 2));
        assert!(dns.can_send(0));
    }

    #[test]
    fn stream_holds_back_when_every_id_is_in_flight() {
        let mut dns = protocol(&["a.test"], &["A"]);
        for _ in 0..ID_COUNT {
            assert!(dns.can_send(0));
            dns.next_request(0);
        }
        assert!(!dns.can_send(0));
        assert!(dns.can_send(1));
        let data = framed(&reply(
            &[
                0, 7, 0x01, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, b'a', 4, b't', b'e', b's', b't', 0, 0, 1,
                0, 1,
            ],
            0,
            0,
        ));
        dns.parse_response(0, &data).unwrap();
        assert!(dns.can_send(0));
        assert_eq!(dns.next_request(0).request_id, (0, 7));
    }

    #[test]
    fn datagram_ids_wrap_around() {
        let mut dns = protocol(&["a.test"], &["A"]).with_framing(DnsFraming::Datagram);
        for expected in 0..ID_COUNT as u32 {
            assert_eq!(dns.next_request(0).request_id, (0, expected as u16));
        }
        assert!(dns.can_send(0));
        assert_eq!(dns.next_request(0).request_id, (0, 0));
        assert_eq!(dns.connections[&0].in_flight.len(), ID_COUNT);
    }

    #[test]
    fn reset_forgets_queries_in_flight() {
        let mut dns = protocol(&["a.test"], &["A"]);
        let request = dns.next_request(0);
        dns.reset();
        let data = framed(&reply(message(&request), 0, 0));
        assert!(dns.parse_response(0, &data).is_err());
        assert_eq!(dns.next_request(0).request_id, (0, 0));
    }

    fn pattern(pattern: &str, keys: DnsKeysConfig) -> DnsNameConfig {
        DnsNameConfig {
            name: None,
            pattern: Some(pattern.to_string()),
            keys: Some(keys),
            weight: 1.0,
        }
    }

    #[test]
    fn patterns_fill_in_generated_keys() {
        let mut config = config(&[], &["A"]);
        config.names = vec![pattern("h{key}.example.test.", DnsKeysConfig::RoundRobin { max: 3 })];
        let mut dns = DnsProtocol::from_config(&config, Some(1)).unwrap();
        for key in [0, 1, 2, 0] {
            let request = dns.next_request(0);
            let name = encode_name(&format!("h{key}.example.test")).unwrap();
            assert_eq!(&message(&request)[12..12 + name.len()], &name[..]);
        }
    }

    #[test]
    fn invalid_name_entries_are_rejected() {
        let keys = DnsKeysConfig::Sequential { start: 0 };
        let both = DnsNameConfig {
            name: Some("a.test".into()),
            ..pattern("{key}.test", keys.clone())
        };
        let fixed_with_keys = DnsNameConfig { pattern: None, ..both.clone() };
        let neither = DnsNameConfig {
            name: None,
            pattern: None,
            keys: None,
            weight: 1.0,
        };
        let without_keys = DnsNameConfig {
            keys: None,
            ..pattern("{key}.test", keys.clone())
        };
        let entries = [
            both,
            fixed_with_keys,
            neither,
            without_keys,
            pattern("no-placeholder.test", keys.clone()),
            pattern("{key}.{key}.test", keys.clone()),
            pattern(&format!("{}{{key}}.test", "x".repeat(50)), keys.clone()),
            pattern("{key}.test", DnsKeysConfig::Random { max: 0 }),
        ];
        for entry in entries {
            let mut config = config(&[], &["A"]);
            config.names = vec![entry.clone()];
            assert!(DnsProtocol::from_config(&config, None).is_err(), "{entry:?}");
        }
        assert!(DnsProtocol::from_config(&config(&[], &["A"]), None).is_err(), "no names");
        assert!(DnsProtocol::from_config(&config(&["a..b"], &["A"]), None).is_err());
        assert!(DnsProtocol::from_config(&config(&["a.test"], &[]), None).is_err());
        assert!(DnsProtocol::from_config(&config(&["a.test"], &["AXFR"]), None).is_err());
        let mut zero = config(&["a.test"], &["A"]);
        zero.names[0].weight = 0.0;
        assert!(DnsProtocol::from_config(&zero, None).is_err());
        zero.names[0].weight = f64::NAN;
        assert!(DnsProtocol::from_config(&zero, None).is_err());
    }

    #[test]
    fn weighted_mixes_follow_their_weights() {
        let mut config = config(&["a.test", "b.test"], &["A", "AAAA"]);
        config.names[0].weight = 3.0;
        config.qtypes.as_mut().unwrap()[1].weight = 0.0;
        let mut dns = DnsProtocol::from_config(&config, Some(3)).unwrap();
        let mut a_names = 0;
        for _ in 0..4000 {
            let request = dns.next_request(0);
            let query = message(&request);
            a_names += usize::from(query[13] == b'a');
            assert_eq!(&query[query.len() - 4..], &[0, 1, 0, 1], "AAAA has weight 0");
            let data = framed(&reply(query, 0, 0));
            dns.parse_response(0, &data).unwrap();
        }
        assert!((2800..3200).contains(&a_names), "{a_names}");
    }

    #[test]
    fn seeded_draws_are_reproducible() {
        let run = || {
            let mut config = config(&["a.test", "b.test", "c.test"], &["A", "MX", "TXT"]);
            config
                .names
                .push(pattern("{key}.test", DnsKeysConfig::Zipfian { n: 100, theta: 0.9 }));
            let mut dns = DnsProtocol::from_config(&config, Some(11)).unwrap();
            (0..50).map(|conn| dns.next_request(conn).data).collect::<Vec<_>>()
        };
        assert_eq!(run(), run());
    }

    fn query(name: &str, qtype: &str, weight: f64) -> DnsQueryConfig {
        DnsQueryConfig {
            name: Some(name.to_string()),
            pattern: None,
            keys: None,
            qtype: qtype.to_string(),
            weight,
        }
    }

    fn queries(queries: Vec<DnsQueryConfig>) -> DnsConfig {
        DnsConfig { queries, ..DnsConfig::default() }
    }

    /// The name's first label and the type of a query message
    fn question(query: &[u8]) -> (String, u16) {
        let label = &query[13..13 + query[12] as usize];
        let at = query.len() - 4;
        (
            String::from_utf8(label.to_vec()).unwrap(),
            u16::from_be_bytes([query[at], query[at + 1]]),
        )
    }

    #[test]
    fn queries_draw_name_and_type_together() {
        let config = queries(vec![query("a.test", "A", 3.0), query("b.test", "AAAA", 1.0)]);
        let mut dns = DnsProtocol::from_config(&config, Some(5)).unwrap();
        let mut a_names = 0;
        for _ in 0..4000 {
            let request = dns.next_request(0);
            let (label, qtype) = question(message(&request));
            match label.as_str() {
                "a" => {
                    assert_eq!(qtype, 1);
                    a_names += 1;
                }
                "b" => assert_eq!(qtype, 28),
                other => panic!("{other}"),
            }
            let data = framed(&reply(message(&request), 0, 0));
            dns.parse_response(0, &data).unwrap();
        }
        assert!((2800..3200).contains(&a_names), "{a_names}");
    }

    #[test]
    fn query_patterns_fill_in_generated_keys() {
        let mut entry = query("unused", "TXT", 1.0);
        entry.name = None;
        entry.pattern = Some("h{key}.test".to_string());
        entry.keys = Some(DnsKeysConfig::RoundRobin { max: 2 });
        let mut dns = DnsProtocol::from_config(&queries(vec![entry]), Some(1)).unwrap();
        for key in [0, 1, 0] {
            let request = dns.next_request(0);
            assert_eq!(question(message(&request)), (format!("h{key}"), 16));
        }
        dns.reset();
        assert_eq!(question(message(&dns.next_request(0))).0, "h0", "reset restarts the keys");
    }

    #[test]
    fn seeded_query_draws_are_reproducible() {
        let run = || {
            let config = queries(vec![
                query("a.test", "A", 1.0),
                query("b.test", "MX", 2.0),
                query("c.test", "TXT", 1.0),
            ]);
            let mut dns = DnsProtocol::from_config(&config, Some(11)).unwrap();
            (0..50).map(|conn| dns.next_request(conn).data).collect::<Vec<_>>()
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn invalid_queries_are_rejected() {
        let error = |config: DnsConfig| format!("{:#}", config.validate().unwrap_err());
        let mut with_names = queries(vec![query("a.test", "A", 1.0)]);
        with_names.names = config(&["b.test"], &[]).names;
        assert!(error(with_names).contains("set either queries, or names and qtypes"));
        let mut with_qtypes = queries(vec![query("a.test", "A", 1.0)]);
        with_qtypes.qtypes = Some(Vec::new());
        assert!(error(with_qtypes).contains("set either queries, or names and qtypes"));
        let mut without_keys = query("a.test", "A", 1.0);
        without_keys.name = None;
        without_keys.pattern = Some("{key}.test".to_string());
        for (entries, expected) in [
            (vec![query("a.test", "AXFR", 1.0)], "queries[0]: DNS qtype 'AXFR'"),
            (vec![query("a.test", "BOGUS", 1.0)], "Unknown DNS qtype 'BOGUS'"),
            (vec![query("a..test", "A", 1.0)], "empty label"),
            (
                vec![query("a.test", "A", 1.0), without_keys],
                "queries[1]: a pattern needs keys",
            ),
            (vec![query("a.test", "A", 0.0)], "queries weights must not all be zero"),
            (vec![query("a.test", "A", f64::INFINITY)], "must be finite"),
        ] {
            let message = error(queries(entries));
            assert!(message.contains(expected), "{message}");
        }
        assert!(error(DnsConfig::default()).contains("names must list at least one entry"));
    }

    #[test]
    fn config_parses_from_json() {
        let config: DnsConfig = serde_json::from_value(serde_json::json!({
            "names": [
                { "name": "www.example.test.", "weight": 3 },
                { "pattern": "h{key}.example.test.", "keys": { "strategy": "zipfian", "n": 10, "theta": 0.99 } }
            ],
            "qtypes": [{ "qtype": "A", "weight": 8 }, { "qtype": "AAAA" }],
            "recursion_desired": false,
            "edns": { "udp_size": 4096, "dnssec_ok": true },
            "nxdomain": "success"
        }))
        .unwrap();
        assert_eq!(config.names.len(), 2);
        assert_eq!(config.qtypes.as_ref().unwrap()[1].weight, 1.0);
        assert_eq!(config.nxdomain, DnsNxdomain::Success);
        assert!(DnsProtocol::from_config(&config, None).is_ok());

        let defaults: DnsConfig =
            serde_json::from_value(serde_json::json!({ "names": [{ "name": "a.test" }] })).unwrap();
        assert!(defaults.recursion_desired);
        let request = DnsProtocol::from_config(&defaults, None).unwrap().next_request(0);
        assert_eq!(
            &message(&request)[message(&request).len() - 4..],
            &[0, 1, 0, 1],
            "A by default"
        );
        assert!(defaults.edns.is_none());
        assert_eq!(defaults.nxdomain, DnsNxdomain::Error);
        let pairs: DnsConfig = serde_json::from_value(serde_json::json!({
            "queries": [
                { "name": "www.example.test.", "qtype": "A", "weight": 4 },
                { "pattern": "h{key}.test", "keys": { "strategy": "random", "max": 9 }, "qtype": "TXT" }
            ]
        }))
        .unwrap();
        assert_eq!((pairs.queries.len(), pairs.queries[1].weight), (2, 1.0));
        assert!(DnsProtocol::from_config(&pairs, None).is_ok());
        let edns: DnsConfig =
            serde_json::from_value(serde_json::json!({ "names": [], "edns": {} })).unwrap();
        assert_eq!(edns.edns.unwrap().udp_size, 1232);
        assert!(serde_json::from_value::<DnsConfig>(serde_json::json!({ "bogus": 1 })).is_err());
    }
}
