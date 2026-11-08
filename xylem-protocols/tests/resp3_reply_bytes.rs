//! Exact RESP3 reply framing: every RESP3 type, nested aggregates,
//! attributes before a reply, and out-of-band push messages, which are
//! skipped and never matched to a request. Each complete reply is also fed
//! one byte short, and every shorter prefix must be incomplete rather than
//! misframed.

use xylem_protocols::{ErrorReply, FixedCommandSelector, Parsed, Protocol, RedisOp, RedisProtocol};

fn protocol() -> RedisProtocol {
    RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Get)))
}

/// Asserts that `reply` frames as exactly one reply of `reply.len()` bytes,
/// even with another reply after it, and that every proper prefix of it is
/// incomplete.
fn assert_frames(reply: &[u8]) {
    for len in 0..reply.len() {
        assert_eq!(
            protocol().parse_response(0, &reply[..len]).unwrap(),
            Parsed::Incomplete,
            "prefix {} of {} is incomplete",
            reply[..len].escape_ascii(),
            reply.escape_ascii()
        );
    }
    let mut protocol = protocol();
    assert_eq!(protocol.parse_response(0, reply).unwrap(), Parsed::ok(reply.len(), (0, 0)));
    let followed = [reply, b"+OK\r\n"].concat();
    assert_eq!(protocol.parse_response(0, &followed).unwrap(), Parsed::ok(reply.len(), (0, 1)));
    assert_eq!(protocol.push_messages(), 0);
}

#[test]
fn null() {
    assert_frames(b"_\r\n");
}

#[test]
fn booleans() {
    assert_frames(b"#t\r\n");
    assert_frames(b"#f\r\n");
    assert!(protocol().parse_response(0, b"#x\r\n").is_err());
}

#[test]
fn doubles() {
    for reply in [
        &b",1.5\r\n"[..],
        b",-0.25\r\n",
        b",10\r\n",
        b",1.23e-42\r\n",
        b",inf\r\n",
        b",-inf\r\n",
        b",nan\r\n",
    ] {
        assert_frames(reply);
    }
}

#[test]
fn big_numbers() {
    assert_frames(b"(3492890328409238509324850943850943825024385\r\n");
    assert_frames(b"(-3492890328409238509324850943850943825024385\r\n");
}

#[test]
fn verbatim_strings() {
    assert_frames(b"=15\r\ntxt:Some string\r\n");
    // The payload may contain CRLF
    assert_frames(b"=10\r\nmkd:a\r\nb\r\n\r\n");
    assert_frames(b"=4\r\ntxt:\r\n");
}

#[test]
fn maps() {
    assert_frames(b"%0\r\n");
    assert_frames(b"%1\r\n$1\r\nf\r\n$1\r\nv\r\n");
    assert_frames(b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n");
}

#[test]
fn sets() {
    assert_frames(b"~0\r\n");
    assert_frames(b"~1\r\n$1\r\na\r\n");
    assert_frames(b"~3\r\n+orange\r\n+apple\r\n#t\r\n");
}

#[test]
fn nested_aggregates() {
    // Redis 8.6 reply to HELLO 3: a map holding an array of maps
    assert_frames(
        b"%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n8.6.3\r\n\
          $5\r\nproto\r\n:3\r\n$2\r\nid\r\n:4\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n\
          $4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*1\r\n%4\r\n$4\r\nname\r\n\
          $9\r\nvectorset\r\n$3\r\nver\r\n:1\r\n$4\r\npath\r\n$0\r\n\r\n$4\r\nargs\r\n*0\r\n",
    );
    // Every RESP3 scalar inside a set inside an array inside a map
    assert_frames(
        b"%2\r\n~1\r\n*3\r\n_\r\n,1.5\r\n=7\r\ntxt:abc\r\n*2\r\n#t\r\n(12\r\n\
          $3\r\nkey\r\n%1\r\n:1\r\n*-1\r\n",
    );
}

#[test]
fn attributes_before_a_reply() {
    assert_frames(b"|1\r\n+key-popularity\r\n%2\r\n$1\r\na\r\n,0.1923\r\n$1\r\nb\r\n,0.0012\r\n*2\r\n:2039123\r\n:9543892\r\n");
    // Several attributes in a row, and an attribute on an aggregate element
    assert_frames(b"|1\r\n+a\r\n+b\r\n|1\r\n+c\r\n+d\r\n+OK\r\n");
    assert_frames(b"*2\r\n|1\r\n+ttl\r\n:3600\r\n$1\r\nv\r\n:7\r\n");
    // An attribute before an error is still an error reply
    let reply = b"|1\r\n+a\r\n+b\r\n-ERR boom\r\n";
    let error = ErrorReply::new("ERR", "ERR boom");
    assert_eq!(
        protocol().parse_response(0, reply).unwrap(),
        Parsed::error(reply.len(), (0, 0), error)
    );
}

#[test]
fn blob_errors_are_errors() {
    // The kind is the first word of the content; the content may hold CRLF
    for (reply, kind, message) in [
        (
            b"!21\r\nSYNTAX invalid syntax\r\n".as_slice(),
            "SYNTAX",
            "SYNTAX invalid syntax",
        ),
        (b"!9\r\nERR\r\nmore\r\n", "ERR", "ERR\r\nmore"),
        (b"!0\r\n\r\n", "unknown", ""),
    ] {
        let error = ErrorReply::new(kind, message);
        assert_eq!(
            protocol().parse_response(0, reply).unwrap(),
            Parsed::error(reply.len(), (0, 0), error)
        );
    }
    // Incomplete, so not an error yet
    assert_eq!(protocol().parse_response(0, b"!21\r\nSYNTAX").unwrap(), Parsed::Incomplete);
}

#[test]
fn push_messages_are_skipped_and_counted() {
    let invalidate = b">2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\nk\r\n";
    let mut protocol = protocol();
    // Redis sends a tracking invalidation right after the reply that caused it
    let read = [&b"+OK\r\n"[..], invalidate].concat();
    assert_eq!(protocol.parse_response(0, &read).unwrap(), Parsed::ok(5, (0, 0)));
    // The push alone answers no request: nothing is consumed
    assert_eq!(protocol.parse_response(0, invalidate).unwrap(), Parsed::Incomplete);
    assert_eq!(protocol.push_messages(), 0);

    // Pushes before the next reply are consumed with it, and counted once
    let pubsub = b">3\r\n$7\r\nmessage\r\n$7\r\nchannel\r\n$5\r\nhello\r\n";
    let attributed = b"|1\r\n+a\r\n+b\r\n>2\r\n$10\r\ninvalidate\r\n_\r\n";
    let read = [&invalidate[..], pubsub, attributed, b"$1\r\nv\r\n"].concat();
    for len in 0..read.len() {
        assert_eq!(protocol.parse_response(0, &read[..len]).unwrap(), Parsed::Incomplete);
    }
    assert_eq!(protocol.push_messages(), 0);
    assert_eq!(protocol.parse_response(0, &read).unwrap(), Parsed::ok(read.len(), (0, 1)));
    assert_eq!(protocol.push_messages(), 3);
}

#[test]
fn malformed_resp3_is_rejected() {
    for reply in [
        &b"%-1\r\n"[..],
        b"~-1\r\n",
        b">-1\r\n",
        b"%x\r\n",
        b"=-2\r\n",
        b"=3\r\nabcde\r\n",
        // Streamed aggregates and strings: Redis does not send them
        b"*?\r\n",
        b"$?\r\n",
    ] {
        assert!(protocol().parse_response(0, reply).is_err(), "{}", reply.escape_ascii());
    }
}
