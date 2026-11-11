//! Error replies: every protocol reports a complete response that says its
//! request failed as an error reply with a kind, and keeps nil replies and
//! misses successful. Failed connection setup and malformed responses stay
//! errors.

use xylem_protocols::memcached::{MemcachedAsciiProtocol, MemcachedBinaryProtocol, MemcachedOp};
use xylem_protocols::redis::ConnectionSetup;
use xylem_protocols::workload::KeyGeneration;
use xylem_protocols::{
    ErrorReply, FixedCommandSelector, HttpProtocol, MasstreeProtocol, Parsed, Protocol, RedisOp,
    RedisProtocol, XylemEchoProtocol,
};

/// A reply and the kind and message of its error reply, `None` for a success
type Reply<'a> = (&'a [u8], Option<(&'a str, &'a str)>);

/// Parse `replies` sent back to back on connection 0, checking that each one
/// completes the next request with its error reply.
fn assert_pipelined<P>(protocol: &mut P, replies: &[Reply<'_>])
where
    P: Protocol<RequestId = (usize, u64)>,
{
    let wire: Vec<u8> = replies.iter().flat_map(|(reply, _)| reply.iter().copied()).collect();
    let mut offset = 0;
    for (seq, (reply, error)) in replies.iter().enumerate() {
        let expected = Parsed::Complete {
            consumed: reply.len(),
            id: (0, seq as u64),
            error: error.map(|(kind, message)| ErrorReply::new(kind, message)),
        };
        let parsed = protocol.parse_response(0, &wire[offset..]).unwrap();
        assert_eq!(parsed, expected, "{}", reply.escape_ascii());
        offset += reply.len();
    }
    assert_eq!(offset, wire.len());
}

fn redis() -> RedisProtocol {
    RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Get)))
}

#[test]
fn redis_top_level_errors_are_named_by_their_code() {
    let wrongtype = "WRONGTYPE Operation against a key holding the wrong kind of value";
    let oom = "OOM command not allowed when used memory > 'maxmemory'.";
    assert_pipelined(
        &mut redis(),
        &[
            (b"+OK\r\n", None),
            (format!("-{wrongtype}\r\n").as_bytes(), Some(("WRONGTYPE", wrongtype))),
            // Nil bulk string, nil array and RESP3 null are successful
            (b"$-1\r\n", None),
            (b"*-1\r\n", None),
            (b"_\r\n", None),
            (format!("-{oom}\r\n").as_bytes(), Some(("OOM", oom))),
            (
                b"-NOAUTH Authentication required.\r\n",
                Some(("NOAUTH", "NOAUTH Authentication required.")),
            ),
            (b"-MOVED 3999 127.0.0.1:6381\r\n", Some(("MOVED", "MOVED 3999 127.0.0.1:6381"))),
            (b"!21\r\nSYNTAX invalid syntax\r\n", Some(("SYNTAX", "SYNTAX invalid syntax"))),
            // An attribute before an error is still an error
            (b"|1\r\n+a\r\n+b\r\n-ERR boom\r\n", Some(("ERR", "ERR boom"))),
            // Errors inside an aggregate, such as EXEC's, are not top-level
            (b"*2\r\n+OK\r\n-ERR value is not an integer\r\n", None),
            (
                b"-EXECABORT Transaction discarded\r\n",
                Some(("EXECABORT", "EXECABORT Transaction discarded")),
            ),
            (b":1\r\n", None),
        ],
    );
}

#[test]
fn redis_setup_and_malformed_replies_stay_fatal() {
    let setup = ConnectionSetup::new(Some((None, "secret".into())), None, None).unwrap();
    let mut protocol = redis().with_connection_setup(setup);
    assert!(protocol.next_request(0).metadata.is_setup);
    let error = protocol
        .parse_response(0, b"-WRONGPASS invalid username-password pair\r\n")
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("Redis connection setup failed: AUTH"), "{error}");

    for malformed in [&b"$x\r\n"[..], b"@what\r\n", b"%-1\r\n"] {
        assert!(redis().parse_response(0, malformed).is_err(), "{}", malformed.escape_ascii());
    }
    // An error reply without its CRLF is incomplete, not an error yet
    assert_eq!(redis().parse_response(0, b"-ERR no end").unwrap(), Parsed::Incomplete);
}

fn http(status: &str, body: &str) -> Vec<u8> {
    format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\n\r\n{body}", body.len()).into_bytes()
}

#[test]
fn http_statuses_from_400_are_error_replies() {
    let replies = [
        http("200 OK", "ok"),
        http("503 Service Unavailable", "busy"),
        http("301 Moved Permanently", ""),
        http("404 Not Found", "missing"),
        [&b"HTTP/1.1 100 Continue\r\n\r\n"[..], &http("500 Internal Server Error", "")].concat(),
        b"HTTP/1.1 204 No Content\r\n\r\n".to_vec(),
        http("429 Too Many Requests", ""),
        http("599 Custom", ""),
        http("400 ", ""),
        b"HTTP/1.1 304 Not Modified\r\n\r\n".to_vec(),
    ];
    let errors = [
        None,
        Some(("503", "HTTP/1.1 503 Service Unavailable")),
        None,
        Some(("404", "HTTP/1.1 404 Not Found")),
        // The status line of the final response, after the informational one
        Some(("500", "HTTP/1.1 500 Internal Server Error")),
        None,
        Some(("429", "HTTP/1.1 429 Too Many Requests")),
        Some(("599", "HTTP/1.1 599 Custom")),
        Some(("400", "HTTP/1.1 400 ")),
        None,
    ];
    let pipelined: Vec<_> = replies.iter().map(Vec::as_slice).zip(errors).collect();
    assert_pipelined(&mut HttpProtocol::default(), &pipelined);
}

#[test]
fn memcached_ascii_error_lines_are_error_replies() {
    let protocol = || MemcachedAsciiProtocol::new(MemcachedOp::Get);
    assert_pipelined(
        &mut protocol(),
        &[
            (b"VALUE key:0 0 2\r\nab\r\nEND\r\n", None),
            // A miss and a stored value are successful
            (b"END\r\n", None),
            (
                b"SERVER_ERROR object too large for cache\r\n",
                Some(("SERVER_ERROR", "SERVER_ERROR object too large for cache")),
            ),
            (b"STORED\r\n", None),
            (
                b"CLIENT_ERROR bad data chunk\r\n",
                Some(("CLIENT_ERROR", "CLIENT_ERROR bad data chunk")),
            ),
            (b"ERROR\r\n", Some(("ERROR", "ERROR"))),
            // An error line ends a response after the values before it
            (
                b"VALUE key:1 0 1\r\nx\r\nSERVER_ERROR out of memory writing get response\r\n",
                Some(("SERVER_ERROR", "SERVER_ERROR out of memory writing get response")),
            ),
        ],
    );
    // A line that is neither is still malformed
    assert!(protocol().parse_response(0, b"ERRORS\r\n").is_err());
    assert!(protocol().parse_response(0, b"SERVER_ERROR\r\n").is_err());
}

/// One datagram of memcached's UDP framing
fn datagram(request_id: u16, number: u16, total: u16, payload: &[u8]) -> Vec<u8> {
    let header = [request_id, number, total, 0].map(u16::to_be_bytes).concat();
    [&header[..], payload].concat()
}

#[test]
fn memcached_udp_error_replies_keep_their_kind_across_datagrams() {
    let mut protocol =
        MemcachedAsciiProtocol::with_workload(MemcachedOp::Get, KeyGeneration::sequential(0), 4)
            .with_udp_framing();
    for _ in 0..2 {
        protocol.next_request(0);
    }
    let line = b"SERVER_ERROR out of memory\r\n";
    let single = datagram(0, 0, 1, line);
    let error = ErrorReply::new("SERVER_ERROR", "SERVER_ERROR out of memory");
    assert_eq!(
        protocol.parse_response(0, &single).unwrap(),
        Parsed::error(single.len(), (0, 0), error)
    );

    // A reassembled error reply keeps its kind; its text is gone with the
    // reassembly buffer, so the kind stands in for it
    let (first, second) = (datagram(1, 0, 2, &line[..10]), datagram(1, 1, 2, &line[10..]));
    assert_eq!(protocol.parse_response(0, &second).unwrap(), Parsed::Absorbed(second.len()));
    let error = ErrorReply::new("SERVER_ERROR", "SERVER_ERROR");
    assert_eq!(
        protocol.parse_response(0, &first).unwrap(),
        Parsed::error(first.len(), (0, 1), error)
    );
}

/// A binary protocol response with `opcode`, `status`, `key` and `value`
fn binary_response(opcode: u8, status: u16, key: &[u8], value: &[u8]) -> Vec<u8> {
    let extras: &[u8] = if status == 0 && opcode == 0x0c {
        &[0; 4]
    } else {
        &[]
    };
    let body_len = (extras.len() + key.len() + value.len()) as u32;
    let mut response = vec![0x81, opcode];
    response.extend_from_slice(&(key.len() as u16).to_be_bytes());
    response.extend_from_slice(&[extras.len() as u8, 0]);
    response.extend_from_slice(&status.to_be_bytes());
    response.extend_from_slice(&body_len.to_be_bytes());
    response.extend_from_slice(&[0; 12]); // opaque and CAS
    response.extend_from_slice(extras);
    response.extend_from_slice(key);
    response.extend_from_slice(value);
    response
}

#[test]
fn memcached_binary_statuses_are_error_replies_except_misses_and_conditions() {
    const GETK: u8 = 0x0c;
    const SET: u8 = 0x01;
    let replies = [
        binary_response(GETK, 0x0000, b"key:0", b"value"),
        // A miss or a failed condition is a result, for any command
        binary_response(GETK, 0x0001, b"", b"Not found"),
        binary_response(SET, 0x0001, b"", b"Not found"),
        binary_response(SET, 0x0002, b"", b"Data exists for key."),
        binary_response(SET, 0x0005, b"", b"Not stored."),
        binary_response(SET, 0x0004, b"", b"Invalid arguments"),
        binary_response(SET, 0x0003, b"", b"Too large."),
        binary_response(SET, 0x0082, b"", b"Out of memory"),
        binary_response(SET, 0x0000, b"", b""),
        // Without a message, the status name is the text
        binary_response(SET, 0x0086, b"", b""),
        binary_response(SET, 0x0042, b"", b"?"),
    ];
    let errors = [
        None,
        None,
        None,
        None,
        None,
        Some(("EINVAL", "Invalid arguments")),
        Some(("E2BIG", "Too large.")),
        Some(("ENOMEM", "Out of memory")),
        None,
        Some(("ETMPFAIL", "ETMPFAIL")),
        Some(("UNKNOWN_STATUS", "?")),
    ];
    let pipelined: Vec<_> = replies.iter().map(Vec::as_slice).zip(errors).collect();
    assert_pipelined(&mut MemcachedBinaryProtocol::new(MemcachedOp::Get), &pipelined);

    // Over UDP, too
    let mut protocol =
        MemcachedBinaryProtocol::with_workload(MemcachedOp::Set, KeyGeneration::sequential(0), 4)
            .with_udp_framing();
    protocol.next_request(0);
    let response = datagram(0, 0, 1, &replies[6]);
    let error = ErrorReply::new("E2BIG", "Too large.");
    assert_eq!(
        protocol.parse_response(0, &response).unwrap(),
        Parsed::error(response.len(), (0, 0), error)
    );
}

/// A Masstree reply `[seq, command, rest...]` with a fixint seq and command
fn masstree_reply(elements: u8, seq: u8, command: u8, rest: &[u8]) -> Vec<u8> {
    [&[0x90 | elements, seq, command][..], rest].concat()
}

#[test]
fn masstree_result_codes_name_error_replies() {
    let mut protocol = MasstreeProtocol::default();
    // MessagePack negative fixints: -1 is 0xff, -2 is 0xfe
    for (reply, error) in [
        (masstree_reply(3, 1, 9, &[0x01]), None), // REPLACE: Inserted
        (masstree_reply(3, 2, 9, &[0x02]), None), // REPLACE: Updated
        (masstree_reply(3, 3, 7, &[0xfe]), None), // PUT: NotFound is no error
        (masstree_reply(3, 4, 9, &[0xff]), Some("Retry")),
        (masstree_reply(3, 5, 7, &[0x00]), Some("OutOfDate")),
        (masstree_reply(3, 6, 9, &[0x10]), Some("UnknownResult")),
        // A GET miss echoes the key: a successful reply
        (masstree_reply(3, 7, 3, &[0xa3, b'k', b':', b'1']), None),
        (masstree_reply(3, 8, 11, &[0xc2]), None), // REMOVE of a missing key
    ] {
        let seq = u16::from(reply[1]);
        let expected = Parsed::Complete {
            consumed: reply.len(),
            id: (0, seq),
            error: error.map(|kind| ErrorReply::new(kind, kind)),
        };
        assert_eq!(protocol.parse_response(0, &reply).unwrap(), expected);
    }
}

#[test]
fn masstree_failed_handshakes_stay_fatal() {
    // [0, 15, false, "bad maxkeylen"], as the server rejects a handshake
    let rejected = masstree_reply(
        4,
        0,
        15,
        &[
            0xc2, 0xad, b'b', b'a', b'd', b' ', b'm', b'a', b'x', b'k', b'e', b'y', b'l', b'e',
            b'n',
        ],
    );
    assert!(MasstreeProtocol::default().parse_response(0, &rejected).is_err());
    // [0, 15, false, 0, "1.0"]
    let failed = masstree_reply(5, 0, 15, &[0xc2, 0x00, 0xa3, b'1', b'.', b'0']);
    let error = MasstreeProtocol::default().parse_response(0, &failed).unwrap_err().to_string();
    assert!(error.contains("handshake failed"), "{error}");
}

#[test]
fn xylem_echo_replies_are_never_errors() {
    let mut protocol = XylemEchoProtocol::default();
    let request = protocol.next_request(0);
    assert_eq!(protocol.parse_response(0, &request.data).unwrap(), Parsed::ok(16, (0, 0)));
}
