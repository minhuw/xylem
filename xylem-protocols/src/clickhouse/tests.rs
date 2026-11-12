//! ClickHouse protocol tests against recorded and hand-built server packets
//!
//! `testdata` holds responses of a ClickHouse 26.8.29.10 server
//! (clickhouse/clickhouse-server) to the queries below, at revision 54453:
//! - hello: the server's Hello
//! - select_one: `SELECT 1`
//! - unknown_table: `SELECT * FROM nope`
//! - types: one column of each of many types, five rows
//! - totals: a GROUP BY ... WITH TOTALS with `extremes = 1`
//! - create: a CREATE TABLE
//! - insert_values: an INSERT ... VALUES
//! - insert_nodata: `INSERT INTO t FORMAT Native`, which waits for rows

use super::wire::{put_block_info, put_string, put_varuint, server, MAX_STEP, REVISION};
use super::*;
use serde_json::json;

const HELLO: &[u8] = include_bytes!("testdata/hello.bin");
const SELECT_ONE: &[u8] = include_bytes!("testdata/select_one.bin");
const UNKNOWN_TABLE: &[u8] = include_bytes!("testdata/unknown_table.bin");
const TYPES: &[u8] = include_bytes!("testdata/types.bin");
const TOTALS: &[u8] = include_bytes!("testdata/totals.bin");
const CREATE: &[u8] = include_bytes!("testdata/create.bin");
const INSERT_VALUES: &[u8] = include_bytes!("testdata/insert_values.bin");
const INSERT_NODATA: &[u8] = include_bytes!("testdata/insert_nodata.bin");

fn protocol(config: serde_json::Value) -> ClickHouseProtocol {
    let config: ClickHouseConfig = serde_json::from_value(config).unwrap();
    ClickHouseProtocol::from_config(&config, Some(1)).unwrap()
}

/// A protocol whose connection 0 completed its Hello
fn connected(config: serde_json::Value) -> ClickHouseProtocol {
    let mut protocol = protocol(config);
    let hello = protocol.next_request(0);
    assert!(hello.metadata.is_setup);
    assert_eq!(protocol.parse_response(0, HELLO).unwrap(), Parsed::ok(HELLO.len(), (0, 0)));
    protocol
}

/// What feeding a byte stream produced
#[derive(Debug, Default)]
struct Fed {
    /// Each completion: its request ID, its bytes and its error kind
    completions: Vec<((usize, u64), usize, Option<String>)>,
    /// Most bytes buffered while the parser needed more
    max_buffered: usize,
}

/// Feed `stream` to connection 0 in reads of `read` bytes the way the
/// worker does: parse the unconsumed bytes until the parser needs more
fn feed(protocol: &mut ClickHouseProtocol, stream: &[u8], read: usize) -> Result<Fed> {
    let mut fed = Fed::default();
    let (mut start, mut end, mut since_completion) = (0, 0, 0);
    while end < stream.len() {
        end = (end + read).min(stream.len());
        loop {
            let parsed = protocol.parse_response(0, &stream[start..end])?;
            let consumed = parsed.consumed();
            assert!(consumed <= MAX_STEP, "a step took {consumed} bytes");
            if consumed == 0 {
                fed.max_buffered = fed.max_buffered.max(end - start);
                break;
            }
            start += consumed;
            since_completion += consumed;
            if let Parsed::Complete { id, error, .. } = parsed {
                let kind = error.map(|error| error.kind.to_string());
                fed.completions.push((id, std::mem::take(&mut since_completion), kind));
            }
            if start == end {
                break;
            }
        }
    }
    assert_eq!(start, stream.len(), "bytes left over");
    Ok(fed)
}

/// Send a query on connection 0 and feed its whole response in reads of
/// `read` bytes; returns the response's error kind
fn query(protocol: &mut ClickHouseProtocol, response: &[u8], read: usize) -> Option<String> {
    let request = protocol.next_request(0);
    assert!(!protocol.can_send(0));
    let fed = feed(protocol, response, read).unwrap();
    assert_eq!(fed.completions.len(), 1, "{:?}", fed.completions);
    let (id, bytes, kind) = fed.completions.into_iter().next().unwrap();
    assert_eq!((id, bytes), (request.request_id, response.len()));
    assert!(protocol.can_send(0));
    kind
}

fn one_query() -> serde_json::Value {
    json!({ "query": "SELECT 1" })
}

#[test]
fn hello_is_a_setup_request_with_database_and_credentials() {
    let mut protocol = protocol(
        json!({ "query": "SELECT 1", "database": "bench", "user": "u", "password": "pw" }),
    );
    assert!(protocol.setup_pending(0) && protocol.can_send(0));
    let hello = protocol.next_request(0);
    assert!(hello.metadata.is_setup);
    assert_eq!(hello.request_id, (0, 0));
    let mut expected = vec![0, 5];
    expected.extend_from_slice(b"xylem");
    expected.extend_from_slice(&[0, 1]); // version 0.1
    put_varuint(&mut expected, REVISION);
    expected.extend_from_slice(b"\x05bench\x01u\x02pw");
    assert_eq!(hello.data, expected);
    assert!(protocol.setup_pending(0) && !protocol.can_send(0));

    // The recorded Hello comes in pieces
    assert_eq!(protocol.parse_response(0, &HELLO[..20]).unwrap(), Parsed::Incomplete);
    assert_eq!(protocol.parse_response(0, HELLO).unwrap(), Parsed::ok(HELLO.len(), (0, 0)));
    assert!(!protocol.setup_pending(0) && protocol.can_send(0));
    let query = protocol.next_request(0);
    assert!(!query.metadata.is_setup && !query.metadata.is_warmup);
    assert_eq!(query.request_id, (0, 1));
}

#[test]
fn server_hello_parses_and_negotiates_the_lower_revision() {
    let mut r = Reader::new(HELLO);
    assert_eq!(r.varuint().unwrap(), Some(server::HELLO));
    let hello = ServerHello::read(&mut r).unwrap().unwrap();
    assert_eq!(r.pos(), HELLO.len());
    assert_eq!(hello.name, "ClickHouse");
    assert_eq!((hello.version, hello.revision), ([26, 8, 29], 54492));
    assert_eq!(hello.timezone, "UTC");
    assert_eq!(hello.negotiated(), REVISION);

    // An older server's revision is the one both speak
    let mut protocol = protocol(one_query());
    protocol.next_request(0);
    let old = server_hello(54440);
    assert_eq!(protocol.parse_response(0, &old).unwrap(), Parsed::ok(old.len(), (0, 0)));
    let query = protocol.next_request(0);
    assert_eq!(query.data, wire::query(54440, "SELECT 1", &[]));
}

/// A server Hello at `revision`
fn server_hello(revision: u64) -> Vec<u8> {
    let mut hello = vec![0];
    put_string(&mut hello, b"ClickHouse");
    hello.extend_from_slice(&[21, 3]);
    put_varuint(&mut hello, revision);
    put_string(&mut hello, b"UTC");
    put_string(&mut hello, b"old");
    hello.push(1);
    hello
}

#[test]
fn refused_logins_and_old_servers_fail_setup() {
    let mut refused = vec![server::EXCEPTION as u8];
    exception_body(&mut refused, 516, "DB::Exception: xylem: Authentication failed", false);
    let too_old = server_hello(54428);
    for (reply, error) in [
        (
            &refused[..],
            "refused the connection: DB::Exception: xylem: Authentication failed (code 516)",
        ),
        (&too_old[..], "revision 54428; xylem needs 54429 or later"),
        (&[server::PONG as u8][..], "answered Hello with a Pong packet"),
    ] {
        let mut protocol = protocol(one_query());
        protocol.next_request(0);
        let message = protocol.parse_response(0, reply).unwrap_err().to_string();
        assert!(message.contains(error), "{message}");
    }
}

#[test]
fn query_packet_layout() {
    let settings = [("max_threads".to_string(), "1".to_string())];
    let packet = wire::query(REVISION, "SELECT 1", &settings);
    let mut expected = vec![1, 0, 1, 0, 0, 9];
    expected.extend_from_slice(b"0.0.0.0:0");
    expected.extend_from_slice(&[0; 8]); // initial query start time
    expected.extend_from_slice(&[1, 0, 0, 5]);
    expected.extend_from_slice(b"xylem");
    expected.extend_from_slice(&[0, 1]);
    put_varuint(&mut expected, REVISION);
    // quota key, distributed depth, patch, no trace, parallel replicas
    expected.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0]);
    expected.push(11);
    expected.extend_from_slice(b"max_threads");
    expected.extend_from_slice(&[1, 1, b'1', 0]);
    // interserver secret, stage Complete, compression off, the SQL
    expected.extend_from_slice(&[0, 2, 0, 8]);
    expected.extend_from_slice(b"SELECT 1");
    // An empty Data block
    expected.extend_from_slice(&[2, 0, 1, 0, 2, 0xff, 0xff, 0xff, 0xff, 0, 0, 0]);
    assert_eq!(packet, expected);

    // At the oldest revision: no start time, depth, trace context, parallel
    // replicas or interserver secret
    let old = wire::query(wire::MIN_REVISION, "SELECT 1", &[]);
    let fields = 8 + 1 + 1 + 3 + 1;
    assert_eq!(old.len() + fields, wire::query(REVISION, "SELECT 1", &[]).len());
}

#[test]
fn settings_are_sent_with_every_query() {
    let mut protocol = connected(json!({
        "query": "SELECT 1",
        "settings": { "max_threads": 2, "use_uncompressed_cache": true, "max_memory_usage": "1G" }
    }));
    let request = protocol.next_request(0);
    let settings = [
        ("max_memory_usage".to_string(), "1G".to_string()),
        ("max_threads".to_string(), "2".to_string()),
        ("use_uncompressed_cache".to_string(), "1".to_string()),
    ];
    assert_eq!(request.data, wire::query(REVISION, "SELECT 1", &settings));
}

#[test]
fn recorded_responses_complete_at_end_of_stream() {
    for read in [1, 7, 64, 100_000] {
        let mut protocol = connected(one_query());
        for response in [SELECT_ONE, TYPES, TOTALS, CREATE, INSERT_VALUES] {
            assert_eq!(query(&mut protocol, response, read), None, "read size {read}");
        }
    }
}

#[test]
fn exceptions_are_error_replies_named_by_code() {
    let mut protocol = connected(one_query());
    assert_eq!(query(&mut protocol, UNKNOWN_TABLE, 100).as_deref(), Some("60"));
    let request = protocol.next_request(0);
    let Parsed::Complete { consumed, id, error: Some(error) } =
        protocol.parse_response(0, UNKNOWN_TABLE).unwrap()
    else {
        panic!("not an error reply");
    };
    assert_eq!((consumed, id), (UNKNOWN_TABLE.len(), request.request_id));
    assert_eq!(error.kind, "60");
    assert!(
        error
            .message
            .starts_with("DB::Exception: Unknown table expression identifier 'nope'"),
        "{}",
        error.message
    );
    // The connection goes on
    assert_eq!(query(&mut protocol, SELECT_ONE, 100), None);

    // Nested exceptions report the first
    let mut nested = vec![server::EXCEPTION as u8];
    exception_body(&mut nested, 1001, "outer", true);
    exception_body(&mut nested, 2, "inner", false);
    assert_eq!(query(&mut protocol, &nested, 3).as_deref(), Some("1001"));
}

/// An exception with `code` and `message`, followed by another if `nested`
fn exception_body(out: &mut Vec<u8>, code: i32, message: &str, nested: bool) {
    out.extend_from_slice(&code.to_le_bytes());
    put_string(out, b"DB::Exception");
    put_string(out, message.as_bytes());
    put_string(out, b"0. DB::Exception::Exception() @ 0x1");
    out.push(u8::from(nested));
}

#[test]
fn insert_waiting_for_rows_fails() {
    let mut protocol = connected(one_query());
    protocol.next_request(0);
    let message = feed(&mut protocol, INSERT_NODATA, 100).unwrap_err().to_string();
    assert!(message.contains("waits for the rows of an INSERT into columns"), "{message}");
    assert!(message.contains("`k` UInt64"), "{message}");
}

#[test]
fn one_query_in_flight_per_connection() {
    let mut protocol = connected(one_query());
    assert!(protocol.can_send(0) && protocol.can_send(1));
    protocol.next_request(0);
    assert!(!protocol.can_send(0) && protocol.can_send(1));
    feed(&mut protocol, SELECT_ONE, 1000).unwrap();
    assert!(protocol.can_send(0));
    let message = protocol.parse_response(0, SELECT_ONE).unwrap_err().to_string();
    assert!(message.contains("with no query in flight"), "{message}");
    let message = protocol.parse_response(5, SELECT_ONE).unwrap_err().to_string();
    assert!(message.contains("connection 5, which sent nothing"), "{message}");
}

/// A block packet of `rows` rows with columns of (name, type, data)
fn block(packet: u64, rows: u64, columns: &[(&str, &str, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    put_varuint(&mut out, packet);
    put_string(&mut out, b"");
    put_block_info(&mut out);
    put_varuint(&mut out, columns.len() as u64);
    put_varuint(&mut out, rows);
    for (name, ty, data) in columns {
        put_string(&mut out, name.as_bytes());
        put_string(&mut out, ty.as_bytes());
        if rows > 0 {
            out.extend_from_slice(data);
        }
    }
    out
}

fn strings(values: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for value in values {
        put_string(&mut out, value);
    }
    out
}

fn u64s(values: &[u64]) -> Vec<u8> {
    values.iter().flat_map(|value| value.to_le_bytes()).collect()
}

/// A LowCardinality granule of String keys with a UInt8 index
fn granule(keys: &[&[u8]], index: &[u8]) -> Vec<u8> {
    let mut out = u64s(&[0x600, keys.len() as u64]);
    out.extend(strings(keys));
    out.extend(u64s(&[index.len() as u64]));
    out.extend_from_slice(index);
    out
}

fn progress() -> Vec<u8> {
    vec![server::PROGRESS as u8, 3, 24, 0, 0, 0]
}

fn profile_info() -> Vec<u8> {
    vec![server::PROFILE_INFO as u8, 3, 1, 24, 0, 0, 1]
}

fn end_of_stream() -> Vec<u8> {
    vec![server::END_OF_STREAM as u8]
}

/// The columns of a hand-built result of three rows, with data
fn many_types() -> Vec<(&'static str, &'static str, Vec<u8>)> {
    let concat = |parts: &[Vec<u8>]| parts.concat();
    vec![
        ("n", "UInt64", u64s(&[1, 2, 3])),
        ("s", "String", strings(&[b"", b"two", &[b'x'; 300]])),
        ("ns", "Nullable(String)", concat(&[vec![0, 1, 0], strings(&[b"a", b"", b"c"])])),
        // [[1], []], [], [[2, 3]]
        (
            "aa",
            "Array(Array(UInt32))",
            concat(&[u64s(&[2, 2, 3]), u64s(&[1, 1, 3]), vec![1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0]]),
        ),
        // Two granules: keys, then the index
        (
            "lc",
            "LowCardinality(Nullable(String))",
            concat(&[u64s(&[1]), granule(&[b"", b"x"], &[1, 0]), granule(&[b"", b"y"], &[1])]),
        ),
        // Empty arrays of LowCardinality: the key version, the offsets, no keys
        ("alc", "Array(LowCardinality(String))", concat(&[u64s(&[1]), u64s(&[0, 0, 0])])),
        (
            "m",
            "Map(String, UInt64)",
            concat(&[u64s(&[1, 2, 3]), strings(&[b"k", b"k", b"l"]), u64s(&[7, 8, 9])]),
        ),
        (
            "t",
            "Tuple(a Int8, `b c` FixedString(3))",
            concat(&[vec![1, 2, 3], b"abcdefghi".to_vec()]),
        ),
        ("e", "Enum8('a, b' = 1, 'c)' = 2)", vec![1, 2, 1]),
        ("d", "Decimal(38, 4)", vec![0; 48]),
        ("dt", "DateTime64(3, 'UTC')", u64s(&[1, 2, 3])),
        ("empty", "Tuple()", vec![b'0'; 3]),
        ("never", "Nullable(Nothing)", vec![1, 1, 1, b'0', b'0', b'0']),
    ]
}

#[test]
fn blocks_of_many_types_are_skipped() {
    let columns = many_types();
    let response = [
        block(server::DATA, 0, &columns),
        progress(),
        block(server::DATA, 3, &columns),
        block(server::TOTALS, 3, &columns),
        block(server::EXTREMES, 3, &columns),
        block(server::LOG, 3, &[("text", "String", strings(&[b"a", b"b", b"c"]))]),
        block(server::PROFILE_EVENTS, 3, &[("value", "Int64", u64s(&[1, 2, 3]))]),
        profile_info(),
        progress(),
        end_of_stream(),
    ]
    .concat();
    for read in [1, 2, 5, 13, 4096] {
        let mut protocol = connected(one_query());
        assert_eq!(query(&mut protocol, &response, read), None, "read size {read}");
    }
}

#[test]
fn large_results_stream_through_bounded_steps() {
    let rows = 300_000u64;
    let numbers: Vec<u64> = (0..rows).collect();
    let text = [b's'; 100];
    let values: Vec<&[u8]> = (0..rows).map(|_| &text[..]).collect();
    // A string longer than a read, cut across several
    let long = vec![b'l'; 200_000];
    let columns = [
        ("n", "UInt64", u64s(&numbers)),
        ("s", "String", strings(&values)),
        ("l", "Array(String)", [u64s(&[1; 300_000]), strings(&[&long])].concat()),
    ];
    let response = [
        block(server::DATA, 0, &columns),
        block(server::DATA, rows, &columns),
        end_of_stream(),
    ]
    .concat();
    assert!(response.len() > 30_000_000);
    let mut protocol = connected(one_query());
    protocol.next_request(0);
    let fed = feed(&mut protocol, &response, 65536).unwrap();
    assert_eq!(fed.completions, [((0, 1), response.len(), None)]);
    // Only a part that must be read whole waits for more bytes
    assert!(fed.max_buffered < 64, "{} bytes buffered", fed.max_buffered);
}

#[test]
fn malformed_responses_fail() {
    let header = |ty: &str| block(server::DATA, 1, &[("c", ty, vec![0; 64])]);
    let mut bad_granule = u64s(&[1]);
    bad_granule.extend(u64s(&[0x100, 1]));
    let mut bad_block_info = vec![server::DATA as u8, 0, 4, 0];
    bad_block_info.extend_from_slice(&[0, 0]);
    for (response, error) in [
        (vec![99], "Unexpected ClickHouse unknown packet (type 99)"),
        (server_hello(54453), "Unexpected ClickHouse Hello packet"),
        (bad_block_info, "unknown BlockInfo field 4"),
        (header("Dynamic"), "cannot skip Dynamic columns"),
        (
            block(server::DATA, 1, &[("c", "LowCardinality(String)", bad_granule)]),
            "global dictionary",
        ),
        (
            vec![server::DATA as u8, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80],
            "longer than 10 bytes",
        ),
    ] {
        let mut protocol = connected(one_query());
        protocol.next_request(0);
        let message = format!("{:#}", feed(&mut protocol, &response, 1000).unwrap_err());
        assert!(message.contains(error), "{error}: {message}");
    }
}

#[test]
fn query_mix_follows_weights() {
    let mut protocol = connected(json!({
        "queries": [{ "sql": "SELECT 1", "weight": 3 }, { "sql": "SELECT 2" }]
    }));
    let one = wire::query(REVISION, "SELECT 1", &[]);
    let mut ones = 0;
    for _ in 0..4000 {
        let request = protocol.next_request(0);
        ones += usize::from(request.data == one);
        feed(&mut protocol, CREATE, 1).unwrap();
    }
    assert!((2800..3200).contains(&ones), "{ones} of 4000 were SELECT 1");
}

#[test]
fn configs_are_validated() {
    for (config, error) in [
        (json!({}), "needs a query or queries"),
        (
            json!({ "query": "SELECT 1", "queries": [{ "sql": "SELECT 2" }] }),
            "both query and queries",
        ),
        (json!({ "queries": [] }), "queries must not be empty"),
        (json!({ "query": " " }), "queries must not be empty"),
        (
            json!({ "queries": [{ "sql": "SELECT 1", "weight": 0 }] }),
            "weights are all zero",
        ),
        (
            json!({ "queries": [{ "sql": "SELECT 1", "weight": -1 }] }),
            "not a non-negative number",
        ),
        (json!({ "query": "SELECT 1", "user": "" }), "user must not be empty"),
        (
            json!({ "query": "SELECT 1", "password": "a", "password_env": "B" }),
            "set at most one",
        ),
        (
            json!({ "query": "SELECT 1", "password_env": "XYLEM_TEST_CLICKHOUSE_UNSET" }),
            "environment variable XYLEM_TEST_CLICKHOUSE_UNSET is not set",
        ),
    ] {
        let config: ClickHouseConfig = serde_json::from_value(config.clone()).unwrap();
        let message = ClickHouseProtocol::from_config(&config, None).err().unwrap().to_string();
        assert!(message.contains(error), "{config:?}: {message}");
    }
    let config: ClickHouseConfig =
        serde_json::from_value(json!({ "query": "SELECT 1", "password": "secret" })).unwrap();
    assert!(!format!("{config:?}").contains("secret"));
    assert!(serde_json::from_value::<ClickHouseConfig>(
        json!({ "query": "x", "compression": true })
    )
    .is_err());
}
