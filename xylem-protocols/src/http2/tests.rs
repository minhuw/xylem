//! The HTTP/2 session against a scripted server

use super::frame::*;
use super::*;

/// What one parse call returned, owned
#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Absorbed(usize),
    Ok(u32),
    Error(u32, String),
}

/// A frame from the server
fn server_frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    write_header(&mut out, payload.len(), kind, flags, stream);
    out.extend_from_slice(payload);
    out
}

fn settings(values: &[(u16, u32)]) -> Vec<u8> {
    let mut out = Vec::new();
    write_settings(&mut out, values);
    out
}

/// Header fields as (name, value)
type Fields = &'static [(&'static str, &'static str)];

/// A header block of `fields`, without indexing
fn block(fields: &[(&str, &str)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, value) in fields {
        hpack::encode_field(&mut out, name, value);
    }
    out
}

fn headers(stream: u32, flags: u8, fields: &[(&str, &str)]) -> Vec<u8> {
    server_frame(TYPE_HEADERS, flags | FLAG_END_HEADERS, stream, &block(fields))
}

fn data(stream: u32, flags: u8, payload: &[u8]) -> Vec<u8> {
    server_frame(TYPE_DATA, flags, stream, payload)
}

fn window_update(stream: u32, increment: u32) -> Vec<u8> {
    let mut out = Vec::new();
    write_window_update(&mut out, stream, increment);
    out
}

/// Parse `wire` as the core does, frame after frame, until it is used up
/// or incomplete
fn feed(protocol: &mut Http2Protocol, conn: usize, wire: &[u8]) -> Result<Vec<Event>> {
    let mut events = Vec::new();
    let mut offset = 0;
    while offset < wire.len() {
        let parsed = protocol.parse_response(conn, &wire[offset..])?;
        offset += parsed.consumed();
        events.push(match parsed {
            Parsed::Incomplete => break,
            Parsed::Absorbed(n) => Event::Absorbed(n),
            Parsed::Complete { id, error: None, .. } => {
                assert_eq!(id.0, conn);
                Event::Ok(id.1)
            }
            Parsed::Complete { id, error: Some(error), .. } => {
                Event::Error(id.1, format!("{} {}", error.kind, error.message))
            }
        });
    }
    Ok(events)
}

/// Frames as (header, payload)
type Frames = Vec<(FrameHeader, Vec<u8>)>;

fn frames(wire: &[u8]) -> Frames {
    let mut frames = Vec::new();
    let mut rest = wire;
    while !rest.is_empty() {
        let header = FrameHeader::parse(rest);
        frames.push((header, rest[HEADER_LEN..HEADER_LEN + header.len].to_vec()));
        rest = &rest[HEADER_LEN + header.len..];
    }
    frames
}

fn outbox(protocol: &mut Http2Protocol, conn: usize) -> Frames {
    let mut outbox = Outbox::default();
    protocol.drain_outbox(conn, &mut outbox);
    assert!(outbox.wake.is_empty());
    frames(&outbox.data)
}

fn http2(method: &str, body_size: usize) -> Http2Protocol {
    let config = Http2Config {
        method: method.to_string(),
        body_size,
        ..Default::default()
    };
    Http2Protocol::from_config(&config, "example.com").unwrap()
}

fn grpc() -> Http2Protocol {
    let config: GrpcConfig = serde_json::from_value(
        serde_json::json!({ "call": "pkg.Echo/Say", "request_hex": "0a0141" }),
    )
    .unwrap();
    Http2Protocol::grpc_from_config(&config, "localhost").unwrap()
}

/// Set up `conn` with a server sending `server_settings`
fn open(protocol: &mut Http2Protocol, conn: usize, server_settings: &[(u16, u32)]) {
    let setup = protocol.next_request(conn);
    assert!(setup.metadata.is_setup);
    assert_eq!(setup.request_id, (conn, 0));
    let events = feed(protocol, conn, &settings(server_settings)).unwrap();
    assert_eq!(events, [Event::Ok(0)]);
    let acks = outbox(protocol, conn);
    assert_eq!(acks.len(), 1);
    assert_eq!((acks[0].0.kind, acks[0].0.flags), (TYPE_SETTINGS, FLAG_ACK));
}

/// The request's header fields and the frames after its HEADERS
fn request_fields(data: &[u8]) -> (FrameHeader, Vec<(String, String)>, Frames) {
    let mut frames = frames(data);
    let (header, block) = frames.remove(0);
    assert_eq!(header.kind, TYPE_HEADERS);
    let mut fields = Vec::new();
    hpack::Decoder::default()
        .decode(&block, |name, value| {
            let text = |bytes: &[u8]| String::from_utf8(bytes.to_vec()).unwrap();
            fields.push((text(name), text(value)));
            Ok(())
        })
        .unwrap();
    (header, fields, frames)
}

#[test]
fn setup_sends_the_preface_and_waits_for_server_settings() {
    let mut protocol = http2("GET", 0);
    assert!(protocol.can_send(4) && protocol.setup_pending(4));
    let setup = protocol.next_request(4);
    assert!(setup.data.starts_with(PREFACE));
    let sent = frames(&setup.data[PREFACE.len()..]);
    assert_eq!(sent.len(), 2);
    assert_eq!((sent[0].0.kind, sent[0].0.flags, sent[0].0.stream), (TYPE_SETTINGS, 0, 0));
    assert_eq!(
        sent[0].1,
        settings(&[(SETTINGS_ENABLE_PUSH, 0), (SETTINGS_INITIAL_WINDOW_SIZE, STREAM_WINDOW)])
            [HEADER_LEN..]
    );
    assert_eq!(sent[1].0.kind, TYPE_WINDOW_UPDATE);
    assert_eq!(u32_at(&sent[1].1, 0), CONNECTION_WINDOW - DEFAULT_WINDOW);
    assert!(!protocol.can_send(4) && protocol.setup_pending(4));

    // The server's SETTINGS completes setup, and the acknowledgement goes out
    let wire = [settings(&[(SETTINGS_MAX_CONCURRENT_STREAMS, 100)]), settings(&[])].concat();
    assert_eq!(feed(&mut protocol, 4, &wire[..12]).unwrap(), []);
    assert_eq!(protocol.parse_response(4, &wire[..12]).unwrap(), Parsed::Incomplete);
    assert_eq!(feed(&mut protocol, 4, &wire).unwrap(), [Event::Ok(0), Event::Absorbed(9)]);
    assert!(protocol.can_send(4) && !protocol.setup_pending(4));
    let acks = outbox(&mut protocol, 4);
    assert_eq!(acks.len(), 2);
    assert!(acks
        .iter()
        .all(|(h, p)| h.kind == TYPE_SETTINGS && h.has(FLAG_ACK) && p.is_empty()));
    // A SETTINGS acknowledgement needs no answer
    let ack = server_frame(TYPE_SETTINGS, FLAG_ACK, 0, &[]);
    assert_eq!(feed(&mut protocol, 4, &ack).unwrap(), [Event::Absorbed(9)]);
    assert!(outbox(&mut protocol, 4).is_empty());
}

#[test]
fn the_server_must_speak_http2_first() {
    let mut protocol = http2("GET", 0);
    protocol.next_request(0);
    let error = feed(&mut protocol, 0, b"HTTP/1.1 400 Bad Request\r\n\r\n").unwrap_err();
    assert!(error.to_string().contains("HTTP/1.x"), "{error}");
    let mut protocol = http2("GET", 0);
    protocol.next_request(0);
    let ping = server_frame(TYPE_PING, 0, 0, &[0; 8]);
    assert!(feed(&mut protocol, 0, &ping).unwrap_err().to_string().contains("SETTINGS"));
}

#[test]
fn requests_are_streams_with_odd_ids_and_static_headers() {
    let mut protocol = http2("GET", 0);
    open(&mut protocol, 0, &[]);
    for expected in [1, 3, 5] {
        let request = protocol.next_request(0);
        assert_eq!(request.request_id, (0, expected));
        assert!(!request.metadata.is_setup && !request.metadata.is_warmup);
        let (header, fields, rest) = request_fields(&request.data);
        assert_eq!(header.stream, expected);
        assert_eq!(header.flags, FLAG_END_STREAM | FLAG_END_HEADERS);
        assert!(rest.is_empty());
        let fields: Vec<_> = fields.iter().map(|(n, v)| (n.as_str(), v.as_str())).collect();
        assert_eq!(
            fields,
            [
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                (":authority", "example.com"),
                ("user-agent", "xylem")
            ]
        );
    }
    assert_eq!(protocol.open_streams(0), 3);

    let mut protocol = http2("POST", 3);
    open(&mut protocol, 0, &[]);
    let (header, fields, rest) = request_fields(&protocol.next_request(0).data);
    assert_eq!(header.flags, FLAG_END_HEADERS);
    assert!(fields.contains(&("content-length".to_string(), "3".to_string())));
    assert_eq!(rest.len(), 1);
    assert_eq!(
        (rest[0].0.kind, rest[0].0.flags, rest[0].0.stream),
        (TYPE_DATA, FLAG_END_STREAM, 1)
    );
    assert_eq!(rest[0].1, b"xxx");
}

#[test]
fn grpc_calls_are_framed_posts_with_trailers() {
    let mut protocol = grpc();
    open(&mut protocol, 0, &[]);
    let (_, fields, rest) = request_fields(&protocol.next_request(0).data);
    let fields: Vec<_> = fields.iter().map(|(n, v)| (n.as_str(), v.as_str())).collect();
    assert_eq!(
        fields,
        [
            (":method", "POST"),
            (":scheme", "http"),
            (":path", "/pkg.Echo/Say"),
            (":authority", "localhost"),
            ("content-type", "application/grpc"),
            ("te", "trailers"),
            ("user-agent", "grpc-xylem")
        ]
    );
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0].0.flags, FLAG_END_STREAM);
    assert_eq!(rest[0].1, [0, 0, 0, 0, 3, 0x0a, 1, b'A']);
}

#[test]
fn interleaved_streams_complete_at_their_end_stream() {
    let mut protocol = http2("GET", 0);
    open(&mut protocol, 0, &[]);
    for _ in 0..3 {
        protocol.next_request(0);
    }
    let status = [(":status", "200")];
    let wire = [
        headers(3, 0, &status),
        headers(1, 0, &status),
        data(3, 0, b"three"),
        headers(5, FLAG_END_STREAM, &status),
        data(1, FLAG_END_STREAM, b"one"),
        data(3, FLAG_END_STREAM, b""),
    ]
    .concat();
    let events = feed(&mut protocol, 0, &wire).unwrap();
    let completed: Vec<_> = events.iter().filter(|e| !matches!(e, Event::Absorbed(_))).collect();
    assert_eq!(completed, [&Event::Ok(5), &Event::Ok(1), &Event::Ok(3)]);
    assert_eq!(events.len(), 6);
    assert_eq!(protocol.open_streams(0), 0);
    // Frames for streams that ended are errors, except those that may race
    assert!(feed(&mut protocol, 0, &data(1, 0, b"late")).is_err());
    let late = [window_update(1, 5), server_frame(TYPE_RST_STREAM, 0, 1, &[0, 0, 0, 0])].concat();
    assert_eq!(
        feed(&mut protocol, 0, &late).unwrap(),
        [Event::Absorbed(13), Event::Absorbed(13)]
    );
    let never = server_frame(TYPE_RST_STREAM, 0, 7, &[0, 0, 0, 8]);
    assert!(feed(&mut protocol, 0, &never).unwrap_err().to_string().contains("never opened"));
}

#[test]
fn every_split_point_of_a_frame_is_incomplete() {
    let mut protocol = http2("GET", 0);
    open(&mut protocol, 0, &[]);
    protocol.next_request(0);
    let wire = [
        headers(1, 0, &[(":status", "200"), ("server", "test")]),
        data(1, FLAG_END_STREAM, b"ok"),
    ]
    .concat();
    let first = FrameHeader::parse(&wire).len + HEADER_LEN;
    for end in 0..first {
        assert_eq!(protocol.parse_response(0, &wire[..end]).unwrap(), Parsed::Incomplete, "{end}");
    }
    assert_eq!(protocol.parse_response(0, &wire).unwrap(), Parsed::Absorbed(first));
    for end in first..wire.len() {
        let parsed = protocol.parse_response(0, &wire[first..end]).unwrap();
        assert_eq!(parsed, Parsed::Incomplete, "{end}");
    }
    assert_eq!(protocol.parse_response(0, &wire[first..]).unwrap(), Parsed::ok(11, (0, 1)));
}

#[test]
fn continuation_padding_and_priority_are_handled() {
    let mut protocol = grpc();
    open(&mut protocol, 0, &[]);
    protocol.next_request(0);
    protocol.next_request(0);
    // Response headers in a padded HEADERS with priority and a CONTINUATION
    let response = block(&[(":status", "200"), ("content-type", "application/grpc")]);
    let mut first = vec![3];
    first.extend_from_slice(&[0x80, 0, 0, 0, 16]);
    first.extend_from_slice(&response[..2]);
    first.extend_from_slice(&[0; 3]);
    let wire = [
        server_frame(TYPE_HEADERS, FLAG_PADDED | FLAG_PRIORITY, 1, &first),
        server_frame(TYPE_CONTINUATION, 0, 1, &response[2..4]),
        server_frame(TYPE_CONTINUATION, FLAG_END_HEADERS, 1, &response[4..]),
        // Padded DATA, and PRIORITY and unknown frames, which are ignored
        server_frame(TYPE_DATA, FLAG_PADDED, 1, &[2, 0, 0, 0, 0, 0, 9, 9]),
        server_frame(0x2, 0, 3, &[0, 0, 0, 1, 16]),
        server_frame(0xfa, 0xff, 0, b"extension"),
        // Trailers in a HEADERS and a CONTINUATION
        server_frame(TYPE_HEADERS, FLAG_END_STREAM, 1, &[0x00]),
        server_frame(TYPE_CONTINUATION, FLAG_END_HEADERS, 1, &block(&[("grpc-status", "0")])[1..]),
    ];
    let events = feed(&mut protocol, 0, &wire.concat()).unwrap();
    assert_eq!(events.last(), Some(&Event::Ok(1)));
    assert_eq!(events.len(), wire.len());

    // Nothing may come between a header block's frames
    let wire = [
        server_frame(TYPE_HEADERS, 0, 3, &response[..2]),
        server_frame(TYPE_PING, 0, 0, &[0; 8]),
    ]
    .concat();
    assert!(feed(&mut protocol, 0, &wire).unwrap_err().to_string().contains("interrupts"));

    // Padding longer than the frame
    let mut protocol = http2("GET", 0);
    open(&mut protocol, 0, &[]);
    protocol.next_request(0);
    let bad = server_frame(TYPE_HEADERS, FLAG_PADDED | FLAG_END_HEADERS, 1, &[9, 0x88]);
    assert!(feed(&mut protocol, 0, &bad).unwrap_err().to_string().contains("padding"));
}

#[test]
fn responses_share_the_servers_dynamic_table() {
    let mut protocol = grpc();
    open(&mut protocol, 0, &[]);
    protocol.next_request(0);
    protocol.next_request(0);
    // Literals with incremental indexing, then their dynamic table indices
    let mut indexed = vec![0x48, 3];
    indexed.extend_from_slice(b"200");
    indexed.extend_from_slice(&[0x40, 11]);
    indexed.extend_from_slice(b"grpc-status");
    indexed.extend_from_slice(&[1, b'0']);
    let wire = [
        server_frame(TYPE_HEADERS, FLAG_END_HEADERS | FLAG_END_STREAM, 1, &indexed),
        // grpc-status: 0 is index 62, :status: 200 index 63
        server_frame(TYPE_HEADERS, FLAG_END_HEADERS | FLAG_END_STREAM, 3, &[0xbf, 0xbe]),
    ]
    .concat();
    assert_eq!(feed(&mut protocol, 0, &wire).unwrap(), [Event::Ok(1), Event::Ok(3)]);
    // A block whose HPACK does not decode is a connection error
    protocol.next_request(0);
    let broken = server_frame(TYPE_HEADERS, FLAG_END_HEADERS, 5, &[0xff, 0x00]);
    assert!(feed(&mut protocol, 0, &broken).is_err());
}

#[test]
fn statuses_map_to_error_replies() {
    let cases: [(&[Fields], Event); 6] = [
        (&[&[(":status", "503")]], Event::Error(1, "503 503".to_string())),
        (&[&[(":status", "204")]], Event::Error(1, "204 204".to_string())),
        (
            &[&[(":status", "200")], &[("grpc-status", "14"), ("grpc-message", "down")]],
            Event::Error(1, "14 UNAVAILABLE".to_string()),
        ),
        // Trailers-only
        (
            &[&[(":status", "200"), ("grpc-status", "12")]],
            Event::Error(1, "12 UNIMPLEMENTED".to_string()),
        ),
        (
            &[&[(":status", "200"), ("grpc-status", "99")]],
            Event::Error(1, "unknown unknown".to_string()),
        ),
        // Informational responses come before the final one
        (
            &[&[(":status", "103")], &[(":status", "200")], &[("grpc-status", "0")]],
            Event::Ok(1),
        ),
    ];
    for (blocks, expected) in cases {
        let mut protocol = grpc();
        open(&mut protocol, 0, &[]);
        protocol.next_request(0);
        let last = blocks.len() - 1;
        let wire: Vec<u8> = blocks
            .iter()
            .enumerate()
            .flat_map(|(i, fields)| headers(1, if i == last { FLAG_END_STREAM } else { 0 }, fields))
            .collect();
        let events = feed(&mut protocol, 0, &wire).unwrap();
        assert_eq!(events.last(), Some(&expected), "{blocks:?}");
    }
    // Plain HTTP/2 does not look for grpc-status
    let mut protocol = http2("GET", 0);
    open(&mut protocol, 0, &[]);
    protocol.next_request(0);
    let wire = headers(1, FLAG_END_STREAM, &[(":status", "200"), ("grpc-status", "3")]);
    assert_eq!(feed(&mut protocol, 0, &wire).unwrap(), [Event::Ok(1)]);
}

#[test]
fn malformed_responses_fail() {
    let cases: [(&[Vec<u8>], &str); 9] = [
        (&[headers(1, FLAG_END_STREAM, &[(":status", "200")])], "without grpc-status"),
        (&[headers(1, FLAG_END_STREAM, &[("grpc-status", "0")])], "no :status"),
        (&[headers(1, FLAG_END_STREAM, &[(":status", "2x0")])], ":status"),
        (
            &[headers(1, 0, &[(":status", "200")]), headers(1, 0, &[("a", "b")])],
            "do not end",
        ),
        (&[headers(1, FLAG_END_STREAM, &[(":status", "100")])], "informational"),
        (&[data(1, 0, b"early")], "before its headers"),
        (&[headers(2, 0, &[(":status", "200")])], "stream 2"),
        (
            &[server_frame(TYPE_PUSH_PROMISE, FLAG_END_HEADERS, 1, &[0, 0, 0, 2, 0x82])],
            "PUSH_PROMISE",
        ),
        (&[server_frame(TYPE_DATA, 0, 1, &vec![0; 16_385])], "FRAME_SIZE_ERROR"),
    ];
    for (frames, expected) in cases {
        let mut protocol = grpc();
        open(&mut protocol, 0, &[]);
        protocol.next_request(0);
        let error = feed(&mut protocol, 0, &frames.concat()).unwrap_err().to_string();
        assert!(error.contains(expected), "{expected}: {error}");
    }
}

#[test]
fn rst_stream_ends_its_stream_as_an_error_reply() {
    let mut protocol = http2("POST", 100_000);
    open(&mut protocol, 0, &[]);
    protocol.next_request(0);
    protocol.next_request(0);
    let wire = [
        server_frame(TYPE_RST_STREAM, 0, 3, &[0, 0, 0, 7]),
        headers(1, FLAG_END_STREAM, &[(":status", "200")]),
    ]
    .concat();
    let events = feed(&mut protocol, 0, &wire).unwrap();
    assert_eq!(
        events,
        [Event::Error(3, "REFUSED_STREAM REFUSED_STREAM".to_string()), Event::Ok(1)]
    );
    // Stream 1 ended with its body unsent: it is reset, and nothing waits
    let sent = outbox(&mut protocol, 0);
    assert_eq!(sent.len(), 1);
    assert_eq!((sent[0].0.kind, sent[0].0.stream), (TYPE_RST_STREAM, 1));
    assert_eq!(protocol.open_streams(0), 0);
    feed(&mut protocol, 0, &window_update(0, 1_000_000)).unwrap();
    assert!(outbox(&mut protocol, 0).is_empty());
}

#[test]
fn graceful_goaway_stops_new_streams() {
    let goaway = |last: u32, code: u32, debug: &[u8]| {
        let payload = [&last.to_be_bytes()[..], &code.to_be_bytes(), debug].concat();
        server_frame(TYPE_GOAWAY, 0, 0, &payload)
    };
    let mut protocol = http2("GET", 0);
    open(&mut protocol, 0, &[]);
    protocol.next_request(0);
    protocol.next_request(0);
    // A first GOAWAY with the largest ID refuses nothing
    feed(&mut protocol, 0, &goaway(MAX_STREAM_ID, NO_ERROR, b"")).unwrap();
    assert!(!protocol.can_send(0));
    feed(&mut protocol, 0, &goaway(3, NO_ERROR, b"bye")).unwrap();
    let done = [
        headers(1, FLAG_END_STREAM, &[(":status", "200")]),
        headers(3, FLAG_END_STREAM, &[(":status", "200")]),
    ];
    assert_eq!(feed(&mut protocol, 0, &done.concat()).unwrap(), [Event::Ok(1), Event::Ok(3)]);
    assert!(!protocol.can_send(0));
    // Another connection is unaffected
    open(&mut protocol, 1, &[]);
    assert!(protocol.can_send(1));

    let mut protocol = http2("GET", 0);
    open(&mut protocol, 0, &[]);
    protocol.next_request(0);
    protocol.next_request(0);
    let error = feed(&mut protocol, 0, &goaway(1, NO_ERROR, b"")).unwrap_err().to_string();
    assert!(error.contains("refusing 1 streams"), "{error}");
    let mut protocol = http2("GET", 0);
    open(&mut protocol, 0, &[]);
    let error = feed(&mut protocol, 0, &goaway(0, 0xb, b"calm")).unwrap_err().to_string();
    assert!(error.contains("ENHANCE_YOUR_CALM") && error.contains("calm"), "{error}");
}

#[test]
fn pings_are_answered() {
    let mut protocol = http2("GET", 0);
    open(&mut protocol, 0, &[]);
    let wire = [
        server_frame(TYPE_PING, 0, 0, b"pingpong"),
        server_frame(TYPE_PING, FLAG_ACK, 0, b"ignored!"),
    ]
    .concat();
    feed(&mut protocol, 0, &wire).unwrap();
    let sent = outbox(&mut protocol, 0);
    assert_eq!(sent.len(), 1);
    assert_eq!((sent[0].0.kind, sent[0].0.flags), (TYPE_PING, FLAG_ACK));
    assert_eq!(sent[0].1, b"pingpong");
    assert!(feed(&mut protocol, 0, &server_frame(TYPE_PING, 0, 0, b"short")).is_err());
    assert!(feed(&mut protocol, 0, &server_frame(TYPE_PING, 0, 1, b"pingpong")).is_err());
}

#[test]
fn max_concurrent_streams_bounds_can_send() {
    let mut protocol = http2("GET", 0);
    open(&mut protocol, 0, &[(SETTINGS_MAX_CONCURRENT_STREAMS, 2)]);
    protocol.next_request(0);
    assert!(protocol.can_send(0));
    protocol.next_request(0);
    assert!(!protocol.can_send(0));
    feed(&mut protocol, 0, &headers(1, FLAG_END_STREAM, &[(":status", "200")])).unwrap();
    assert!(protocol.can_send(0));
    // A later SETTINGS can lower the limit
    feed(&mut protocol, 0, &settings(&[(SETTINGS_MAX_CONCURRENT_STREAMS, 1)])).unwrap();
    assert!(!protocol.can_send(0));
    let mut protocol = http2("GET", 0);
    open(&mut protocol, 0, &[(SETTINGS_MAX_CONCURRENT_STREAMS, 0)]);
    assert!(!protocol.can_send(0));
}

#[test]
fn received_data_gives_its_window_back() {
    let mut protocol = http2("GET", 0).with_windows(20_000, 70_000);
    let setup = protocol.next_request(0);
    let sent = frames(&setup.data[PREFACE.len()..]);
    assert_eq!(u32_at(&sent[1].1, 0), 70_000 - DEFAULT_WINDOW);
    feed(&mut protocol, 0, &settings(&[])).unwrap();
    outbox(&mut protocol, 0);
    protocol.next_request(0);
    protocol.next_request(0);
    let status = [(":status", "200")];
    let big = vec![b'd'; 16_000];
    // 16 000 bytes use more than half of stream 1's window
    let wire = [headers(1, 0, &status), data(1, 0, &big)].concat();
    feed(&mut protocol, 0, &wire).unwrap();
    let updates: Vec<_> = outbox(&mut protocol, 0)
        .iter()
        .map(|(h, p)| (h.kind, h.stream, u32_at(p, 0)))
        .collect();
    assert_eq!(updates, [(TYPE_WINDOW_UPDATE, 1, 16_000)]);
    // Then the connection's half: 16 000 + 9 999 + 9 001, padding included
    let mut padded = vec![8];
    padded.extend_from_slice(&[b'p'; 9_000]);
    let wire = [
        headers(3, 0, &status),
        data(3, 0, &big[..9_999]),
        server_frame(TYPE_DATA, FLAG_PADDED | FLAG_END_STREAM, 3, &padded),
    ]
    .concat();
    let events = feed(&mut protocol, 0, &wire).unwrap();
    assert_eq!(events.last(), Some(&Event::Ok(3)));
    let updates: Vec<_> = outbox(&mut protocol, 0)
        .iter()
        .map(|(h, p)| (h.kind, h.stream, u32_at(p, 0)))
        .collect();
    // Stream 3 ended, so only the connection gets its window back
    assert_eq!(updates, [(TYPE_WINDOW_UPDATE, 0, 35_000)]);

    // Overrunning a stream window is a flow-control error
    let mut protocol = http2("GET", 0).with_windows(100, DEFAULT_WINDOW);
    open(&mut protocol, 0, &[]);
    protocol.next_request(0);
    let wire = [headers(1, 0, &status), data(1, 0, &[0; 101])].concat();
    let error = feed(&mut protocol, 0, &wire).unwrap_err().to_string();
    assert!(error.contains("overran stream 1"), "{error}");
}

#[test]
fn request_bodies_wait_for_the_servers_windows() {
    // A 10-byte stream window: the rest waits for WINDOW_UPDATE and SETTINGS
    let mut protocol = http2("POST", 64);
    open(&mut protocol, 0, &[(SETTINGS_INITIAL_WINDOW_SIZE, 10)]);
    let (_, _, rest) = request_fields(&protocol.next_request(0).data);
    assert_eq!(rest.len(), 1);
    assert_eq!((rest[0].0.flags, rest[0].1.len()), (0, 10));
    feed(&mut protocol, 0, &window_update(1, 30)).unwrap();
    let sent = outbox(&mut protocol, 0);
    assert_eq!((sent[0].0.kind, sent[0].0.stream, sent[0].0.flags), (TYPE_DATA, 1, 0));
    assert_eq!(sent[0].1.len(), 30);
    // Raising the initial window raises the open streams' windows
    feed(&mut protocol, 0, &settings(&[(SETTINGS_INITIAL_WINDOW_SIZE, 100)])).unwrap();
    let sent = outbox(&mut protocol, 0);
    assert_eq!(sent.len(), 2);
    assert_eq!(
        (sent[0].0.kind, sent[0].0.flags, sent[0].1.len()),
        (TYPE_DATA, FLAG_END_STREAM, 24)
    );
    assert_eq!((sent[1].0.kind, sent[1].0.flags), (TYPE_SETTINGS, FLAG_ACK));
    // A new stream starts with the new initial window
    let (_, _, rest) = request_fields(&protocol.next_request(0).data);
    assert_eq!((rest[0].0.flags, rest[0].1.len()), (FLAG_END_STREAM, 64));

    // The connection window, 65 535 bytes, in frames of at most 16 384
    let mut protocol = http2("POST", 70_000);
    open(&mut protocol, 0, &[(SETTINGS_INITIAL_WINDOW_SIZE, 1 << 20)]);
    let (_, _, rest) = request_fields(&protocol.next_request(0).data);
    let lens: Vec<_> = rest.iter().map(|(h, p)| (h.flags, p.len())).collect();
    assert_eq!(lens, [(0, 16_384), (0, 16_384), (0, 16_384), (0, 16_383)]);
    // A second request's body waits behind it
    let (_, _, rest) = request_fields(&protocol.next_request(0).data);
    assert!(rest.is_empty());
    feed(&mut protocol, 0, &window_update(0, 5_000)).unwrap();
    let sent = outbox(&mut protocol, 0);
    let lens: Vec<_> = sent.iter().map(|(h, p)| (h.stream, h.flags, p.len())).collect();
    assert_eq!(lens, [(1, FLAG_END_STREAM, 4_465), (3, 0, 535)]);
    // A larger SETTINGS_MAX_FRAME_SIZE makes larger frames
    feed(&mut protocol, 0, &settings(&[(SETTINGS_MAX_FRAME_SIZE, 1 << 17)])).unwrap();
    feed(&mut protocol, 0, &window_update(0, 100_000)).unwrap();
    let sent = outbox(&mut protocol, 0);
    let lens: Vec<_> = sent.iter().map(|(h, p)| (h.kind, h.flags, p.len())).collect();
    assert_eq!(lens, [(TYPE_SETTINGS, FLAG_ACK, 0), (TYPE_DATA, FLAG_END_STREAM, 69_465)]);

    // Windows may not overflow
    let mut protocol = http2("GET", 0);
    open(&mut protocol, 0, &[]);
    assert!(feed(&mut protocol, 0, &window_update(0, MAX_WINDOW)).is_err());
    let mut protocol = http2("GET", 0);
    open(&mut protocol, 0, &[]);
    assert!(feed(&mut protocol, 0, &server_frame(TYPE_WINDOW_UPDATE, 0, 0, &[0; 4])).is_err());
    let mut protocol = http2("GET", 0);
    open(&mut protocol, 0, &[]);
    assert!(feed(&mut protocol, 0, &settings(&[(SETTINGS_INITIAL_WINDOW_SIZE, 1 << 31)])).is_err());
    assert!(feed(&mut protocol, 0, &settings(&[(SETTINGS_MAX_FRAME_SIZE, 100)])).is_err());
}

#[test]
fn messages_are_sent_in_turn() {
    let dir = std::env::temp_dir().join(format!("xylem-http2-turns-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("messages.bin");
    std::fs::write(&path, b"\0\0\0\0\x01a\0\0\0\0\x02bb").unwrap();
    let config: GrpcConfig = serde_json::from_value(
        serde_json::json!({ "call": "pkg.Echo/Say", "messages_file": path }),
    )
    .unwrap();
    let mut protocol = Http2Protocol::grpc_from_config(&config, "localhost").unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    open(&mut protocol, 0, &[]);
    let bodies: Vec<_> = (0..3)
        .map(|_| request_fields(&protocol.next_request(0).data).2[0].1.clone())
        .collect();
    assert_eq!(bodies, [&b"\0\0\0\0\x01a"[..], b"\0\0\0\0\x02bb", b"\0\0\0\0\x01a"]);
    assert_eq!(protocol.name(), "grpc");
    assert_eq!(http2("GET", 0).name(), "http2");
}
