//! ClickHouse native protocol (TCP port 9000): SQL queries, one at a time
//! per connection
//!
//! # Connection setup
//!
//! Each connection sends a client Hello with the configured database, user
//! and password and protocol revision [`wire::REVISION`] (54453), and waits
//! for the server's Hello, a setup request. Both sides then speak the lower
//! of the two revisions; servers older than [`wire::MIN_REVISION`] (54429)
//! are refused. 54453 is the last revision before custom serializations,
//! so the server sends every column in its default serialization (never
//! sparse) and every String with its length in front. The revision needs
//! no addendum after Hello (54458) and no chunked packets (54470). An
//! Exception instead of the server's Hello (a failed login) fails the run.
//!
//! # Queries
//!
//! A query is a Query packet with the SQL text, the configured settings,
//! stage Complete and compression off, then the empty Data block that ends
//! the external tables. The client chooses compression in the Query
//! packet, so a server cannot force it on. The native protocol answers one
//! query at a time, so a connection has at most one in flight
//! ([`Protocol::can_send`]); the CLI requires `max_pending_per_connection
//! = 1`.
//!
//! # Responses
//!
//! The server answers with packets until EndOfStream or an Exception:
//! Data (the empty header block first, then the result blocks), Progress,
//! ProfileInfo, Totals, Extremes, Log and ProfileEvents. Blocks are parsed
//! column by column: each column's type name says how its data is laid out,
//! and [`column::Skipper`] walks the data without decoding values. The
//! parser streams blocks with [`Parsed::Absorbed`], so a result of any size
//! goes through a 64 KiB receive buffer. An Exception completes the query
//! as an error reply whose kind is the ClickHouse error code, such as `60`
//! for UNKNOWN_TABLE. A TableColumns packet means the query waits for data
//! from the client (an INSERT without its rows), which xylem never sends,
//! so it fails the run.

pub mod column;
pub mod config;
pub mod wire;

#[cfg(test)]
mod tests;

pub use config::{ClickHouseConfig, ClickHouseQueryConfig, ClickHouseSettingValue};

use crate::workload::{CommandSelector, WeightedCommandSelector};
use anyhow::{anyhow, bail, ensure, Result};
use column::{Skipper, Types};
use wire::{server, take, Exception, Reader, ServerHello, MAX_STEP};
use xylem_common::{ErrorReply, FastHashMap, Parsed, Protocol, Request};
use xylem_core::seed::derive_seed;

/// Request ID: the connection and its request number; the Hello is 0
pub type ClickHouseRequestId = (usize, u64);

/// Seed component of the query mix
const MIX_COMPONENT: &str = "clickhouse_query_mix";

/// Most columns a block may have, as ClickHouse's Native reader allows
const MAX_COLUMNS: u64 = 1 << 20;

/// Most rows a block may have
const MAX_ROWS: u64 = 1 << 40;

/// Where the parser is in a connection's received bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rx {
    /// At the start of a packet
    Packet,
    /// Before a column's name and type; `columns` are left in the block
    Column { columns: u64, rows: u64 },
    /// Inside a column's data, with `columns` more after it
    Data { columns: u64, rows: u64 },
}

/// One connection
#[derive(Debug)]
struct Conn {
    /// The revision both sides speak, once the server's Hello arrived
    revision: Option<u64>,
    /// Number of the request in flight
    in_flight: Option<u64>,
    next_request: u64,
    rx: Rx,
    skipper: Skipper,
}

impl Conn {
    fn new() -> Self {
        Self {
            revision: None,
            in_flight: None,
            next_request: 1,
            rx: Rx::Packet,
            skipper: Skipper::default(),
        }
    }
}

/// What a packet at the start of the data does
enum Packet<'a> {
    /// It continues the response; the first `n` bytes were parsed
    Continue(usize),
    /// EndOfStream completes the response
    End(usize),
    /// An Exception completes the response
    Exception(usize, Exception<'a>),
}

/// ClickHouse native protocol; see the module documentation
pub struct ClickHouseProtocol {
    hello: Vec<u8>,
    queries: Vec<String>,
    settings: Vec<(String, String)>,
    /// The Query packets of `queries` at a revision
    encoded: Option<(u64, Vec<Vec<u8>>)>,
    mix: Option<WeightedCommandSelector<usize>>,
    conns: FastHashMap<usize, Conn>,
    types: Types,
    /// The decimal text of each error code seen, which error replies name
    codes: FastHashMap<i32, &'static str>,
}

impl ClickHouseProtocol {
    pub fn from_config(config: &ClickHouseConfig, seed: Option<u64>) -> Result<Self> {
        config.validate()?;
        let password = config.resolve_password()?;
        let weighted = config.weighted_queries()?;
        let mix = if weighted.len() > 1 {
            let indices = weighted.iter().enumerate().map(|(i, (_, weight))| (i, *weight));
            Some(WeightedCommandSelector::with_seed(
                indices.collect(),
                seed.map(|seed| derive_seed(seed, MIX_COMPONENT)),
            )?)
        } else {
            None
        };
        Ok(Self {
            hello: wire::hello(&config.database, &config.user, &password),
            queries: weighted.into_iter().map(|(sql, _)| sql).collect(),
            settings: config
                .settings
                .iter()
                .map(|(name, value)| (name.clone(), value.to_text()))
                .collect(),
            encoded: None,
            mix,
            conns: FastHashMap::default(),
            types: Types::default(),
            codes: FastHashMap::default(),
        })
    }

    /// The Query packet of query `index` at `revision`
    fn query_packet(&mut self, revision: u64, index: usize) -> Vec<u8> {
        if self.encoded.as_ref().is_none_or(|(at, _)| *at != revision) {
            let packets = self
                .queries
                .iter()
                .map(|sql| wire::query(revision, sql, &self.settings))
                .collect();
            self.encoded = Some((revision, packets));
        }
        self.encoded.as_ref().expect("encoded above").1[index].clone()
    }
}

/// The error reply kind of error `code`: its decimal text, made once per
/// code seen
fn code_kind(codes: &mut FastHashMap<i32, &'static str>, code: i32) -> &'static str {
    codes
        .entry(code)
        .or_insert_with(|| Box::leak(code.to_string().into_boxed_str()))
}

/// Parse the server's Hello, or the Exception that refuses the connection
fn parse_hello(data: &[u8]) -> Result<Option<(usize, ServerHello)>> {
    let mut r = Reader::new(data);
    match take!(r.varuint()?) {
        server::HELLO => {
            let hello = take!(ServerHello::read(&mut r)?);
            Ok(Some((r.pos(), hello)))
        }
        server::EXCEPTION => {
            let exception = take!(Exception::read(&mut r)?);
            bail!(
                "ClickHouse refused the connection: {} (code {})",
                String::from_utf8_lossy(exception.message),
                exception.code
            )
        }
        packet => bail!(
            "ClickHouse answered Hello with a {} packet (type {packet})",
            server::name(packet)
        ),
    }
}

/// Parse the packet at the start of `data`, or the start of its block
fn parse_packet<'a>(rx: &mut Rx, data: &'a [u8]) -> Result<Option<Packet<'a>>> {
    let mut r = Reader::new(data);
    let packet = take!(r.varuint()?);
    match packet {
        server::DATA | server::TOTALS | server::EXTREMES | server::LOG | server::PROFILE_EVENTS => {
            take!(r.string()?); // table name
            let (columns, rows) = take!(wire::block_header(&mut r)?);
            let name = server::name(packet);
            ensure!(
                columns <= MAX_COLUMNS && rows <= MAX_ROWS,
                "ClickHouse {name} block has {columns} columns and {rows} rows"
            );
            if columns > 0 {
                *rx = Rx::Column { columns, rows };
            }
        }
        server::PROGRESS => take!(wire::skip_progress(&mut r)?),
        server::PROFILE_INFO => take!(wire::skip_profile_info(&mut r)?),
        server::END_OF_STREAM => return Ok(Some(Packet::End(r.pos()))),
        server::EXCEPTION => {
            let exception = take!(Exception::read(&mut r)?);
            return Ok(Some(Packet::Exception(r.pos(), exception)));
        }
        server::TABLE_COLUMNS => {
            take!(r.string()?); // table name
            let columns = take!(r.string()?);
            bail!(
                "ClickHouse waits for the rows of an INSERT into columns {}, but xylem sends \
                 no data blocks; put the rows in the query (INSERT ... VALUES or INSERT ... \
                 SELECT)",
                String::from_utf8_lossy(columns).trim()
            )
        }
        packet => bail!(
            "Unexpected ClickHouse {} packet (type {packet}) in a query response",
            server::name(packet)
        ),
    }
    Ok(Some(Packet::Continue(r.pos())))
}

/// Parse a column's name and type: its type and the bytes they took
fn parse_column_header(
    types: &mut Types,
    data: &[u8],
) -> Result<Option<(usize, column::ColumnType)>> {
    let mut r = Reader::new(data);
    take!(r.string()?); // name
    let name = take!(r.string()?);
    Ok(Some((r.pos(), types.get(name)?)))
}

/// What a parse step found once `pos` bytes were taken without a
/// completion
fn partial<'a>(pos: usize) -> Parsed<'a, ClickHouseRequestId> {
    if pos == 0 {
        Parsed::Incomplete
    } else {
        Parsed::Absorbed(pos)
    }
}

impl Protocol for ClickHouseProtocol {
    type RequestId = ClickHouseRequestId;

    fn next_request(&mut self, conn_id: usize) -> Request<Self::RequestId> {
        let conn = self.conns.entry(conn_id).or_insert_with(Conn::new);
        assert!(conn.in_flight.is_none(), "ClickHouse sends one request at a time (can_send)");
        let Some(revision) = conn.revision else {
            conn.in_flight = Some(0);
            return Request::setup(self.hello.clone(), (conn_id, 0));
        };
        let number = conn.next_request;
        conn.next_request += 1;
        conn.in_flight = Some(number);
        let index = match &mut self.mix {
            Some(mix) => mix.next_command(),
            None => 0,
        };
        Request::measurement(self.query_packet(revision, index), (conn_id, number))
    }

    fn parse_response<'a>(
        &mut self,
        conn_id: usize,
        data: &'a [u8],
    ) -> Result<Parsed<'a, Self::RequestId>> {
        let conn = self.conns.get_mut(&conn_id).ok_or_else(|| {
            anyhow!("ClickHouse data on connection {conn_id}, which sent nothing")
        })?;
        let Some(number) = conn.in_flight else {
            bail!("ClickHouse sent data on connection {conn_id} with no query in flight");
        };
        let id = (conn_id, number);
        if conn.revision.is_none() {
            let Some((consumed, hello)) = parse_hello(data)? else {
                return Ok(Parsed::Incomplete);
            };
            conn.revision = Some(hello.negotiated());
            conn.in_flight = None;
            return Ok(Parsed::ok(consumed, id));
        }
        let mut pos = 0;
        while pos < MAX_STEP {
            let rest = &data[pos..];
            let taken = match conn.rx {
                Rx::Data { columns, rows } => {
                    let (n, done) = conn.skipper.skip(&self.types, rest, MAX_STEP - pos)?;
                    pos += n;
                    if !done {
                        return Ok(partial(pos));
                    }
                    conn.rx = if columns > 0 {
                        Rx::Column { columns, rows }
                    } else {
                        Rx::Packet
                    };
                    continue;
                }
                Rx::Column { columns, rows } => {
                    let Some((n, column)) = parse_column_header(&mut self.types, rest)? else {
                        return Ok(partial(pos));
                    };
                    if pos > 0 && pos + n > MAX_STEP {
                        return Ok(partial(pos));
                    }
                    conn.skipper.start(column, rows);
                    conn.rx = Rx::Data { columns: columns - 1, rows };
                    n
                }
                Rx::Packet => {
                    let mut rx = Rx::Packet;
                    let Some(packet) = parse_packet(&mut rx, rest)? else {
                        return Ok(partial(pos));
                    };
                    let n = match packet {
                        Packet::Continue(n) | Packet::End(n) | Packet::Exception(n, _) => n,
                    };
                    // Leave a packet that does not fit this step to the next
                    if pos > 0 && pos + n > MAX_STEP {
                        return Ok(partial(pos));
                    }
                    conn.rx = rx;
                    match packet {
                        Packet::Continue(n) => n,
                        Packet::End(n) => {
                            conn.in_flight = None;
                            return Ok(Parsed::ok(pos + n, id));
                        }
                        Packet::Exception(n, exception) => {
                            conn.in_flight = None;
                            let kind = code_kind(&mut self.codes, exception.code);
                            let reply = ErrorReply::from_bytes(kind.as_bytes(), exception.message);
                            return Ok(Parsed::error(pos + n, id, reply));
                        }
                    }
                }
            };
            ensure!(
                taken <= MAX_STEP,
                "A ClickHouse packet header of {taken} bytes exceeds the {MAX_STEP} bytes xylem \
                 parses at once"
            );
            pos += taken;
        }
        Ok(partial(pos))
    }

    fn can_send(&self, conn_id: usize) -> bool {
        self.conns.get(&conn_id).is_none_or(|conn| conn.in_flight.is_none())
    }

    fn setup_pending(&self, conn_id: usize) -> bool {
        self.conns.get(&conn_id).is_none_or(|conn| conn.revision.is_none())
    }

    fn name(&self) -> &'static str {
        "clickhouse"
    }

    fn reset(&mut self) {
        if let Some(mix) = &mut self.mix {
            mix.reset();
        }
    }
}
