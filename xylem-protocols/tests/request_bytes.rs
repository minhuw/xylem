//! Exact request bytes, request ids, and warmup tags for the memcached, HTTP,
//! xylem-echo, and masstree codecs.

use xylem_protocols::configs::{HttpConfig, InsertPhaseConfig, KeysConfig, MemcachedConfig};
use xylem_protocols::http::{HttpMethod, HttpProtocol};
use xylem_protocols::masstree::{InsertPhaseState, MasstreeOp, MasstreeProtocol};
use xylem_protocols::memcached::ascii::MemcachedOp as AsciiOp;
use xylem_protocols::memcached::MemcachedOp as BinaryOp;
use xylem_protocols::memcached::{MemcachedAsciiProtocol, MemcachedBinaryProtocol};
use xylem_protocols::workload::KeyGeneration;
use xylem_protocols::xylem_echo::XylemEchoProtocol;
use xylem_protocols::{Parsed, Protocol, Request};

/// Sends one request per entry on the entry's connection and compares the whole
/// request with the expected one: bytes, request id, and warmup tag.
fn assert_requests<P: Protocol>(protocol: &mut P, expected: Vec<(usize, Request<P::RequestId>)>) {
    for (index, (conn_id, want)) in expected.into_iter().enumerate() {
        let got = protocol.next_request(conn_id);
        assert_eq!(got, want, "request {index} on connection {conn_id}");
    }
}

/// 24-byte memcached binary request header with zero data type, vbucket, opaque and cas.
fn binary_header(opcode: u8, key_len: u16, extras_len: u8, body_len: u32) -> Vec<u8> {
    let mut header = vec![0x80, opcode];
    header.extend_from_slice(&key_len.to_be_bytes());
    header.push(extras_len);
    header.push(0x00); // data type
    header.extend_from_slice(&[0x00, 0x00]); // vbucket
    header.extend_from_slice(&body_len.to_be_bytes());
    header.extend_from_slice(&[0x00; 4]); // opaque
    header.extend_from_slice(&[0x00; 8]); // cas
    header
}

/// Memcached binary GETK request: header followed by the key.
fn binary_get(key: &str) -> Vec<u8> {
    let key_len = key.len() as u16;
    [binary_header(0x0c, key_len, 0, u32::from(key_len)), key.as_bytes().to_vec()].concat()
}

/// Memcached binary SET request: header, zero flags and expiration, key, and
/// `value_len` bytes of b'x'.
fn binary_set(key: &str, value_len: usize) -> Vec<u8> {
    let key_len = key.len() as u16;
    let body_len = 8 + u32::from(key_len) + value_len as u32;
    [
        binary_header(0x01, key_len, 8, body_len),
        vec![0x00; 8],
        key.as_bytes().to_vec(),
        vec![b'x'; value_len],
    ]
    .concat()
}

fn seeded_random_get_config() -> MemcachedConfig {
    MemcachedConfig {
        keys: KeysConfig::Random {
            max: 1000,
            value_size: 8,
            prefix: "r:".into(),
        },
        operation: Some("GET".into()),
        operations: None,
        value_size: None,
        insert_phase: Some(InsertPhaseConfig { key_count: Some(3), value_size: Some(5) }),
    }
}

fn seeded_zipfian_set_config() -> MemcachedConfig {
    MemcachedConfig {
        keys: KeysConfig::Zipfian {
            n: 50,
            theta: 0.99,
            value_size: 4,
            prefix: "z:".into(),
        },
        operation: Some("SET".into()),
        operations: None,
        value_size: None,
        insert_phase: Some(InsertPhaseConfig { key_count: Some(2), value_size: Some(2) }),
    }
}

/// Lowercase operation and an empty insert phase that resolves to the keyspace
/// size and the keys value size.
fn round_robin_empty_insert_phase_config() -> MemcachedConfig {
    MemcachedConfig {
        keys: KeysConfig::RoundRobin {
            max: 3,
            value_size: 2,
            prefix: "rr:".into(),
        },
        operation: Some("get".into()),
        operations: None,
        value_size: None,
        insert_phase: Some(InsertPhaseConfig::default()),
    }
}

#[test]
fn memcached_ascii_new_and_default_get_repeat_key_zero() {
    for mut protocol in
        [MemcachedAsciiProtocol::new(AsciiOp::Get), MemcachedAsciiProtocol::default()]
    {
        assert!(!protocol.is_in_insert_phase());
        assert_requests(
            &mut protocol,
            vec![
                (0, Request::measurement(b"get key:0\r\n".to_vec(), (0, 0))),
                (1, Request::measurement(b"get key:0\r\n".to_vec(), (1, 0))),
                (0, Request::measurement(b"get key:0\r\n".to_vec(), (0, 1))),
                (1, Request::measurement(b"get key:0\r\n".to_vec(), (1, 1))),
                (1, Request::measurement(b"get key:0\r\n".to_vec(), (1, 2))),
            ],
        );
    }
}

#[test]
fn memcached_ascii_new_set_uses_key_zero_and_64_byte_value() {
    let set = [b"set key:0 0 0 64\r\n".as_slice(), &[b'x'; 64], b"\r\n"].concat();
    let mut protocol = MemcachedAsciiProtocol::new(AsciiOp::Set);
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(set.clone(), (1, 0))),
            (0, Request::measurement(set.clone(), (0, 0))),
            (1, Request::measurement(set.clone(), (1, 1))),
        ],
    );
}

#[test]
fn memcached_ascii_with_workload_get_draws_keys_across_connections() {
    let mut protocol =
        MemcachedAsciiProtocol::with_workload(AsciiOp::Get, KeyGeneration::sequential(7), 3)
            .with_key_prefix("item:".to_string());
    assert!(!protocol.is_in_insert_phase());
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(b"get item:7\r\n".to_vec(), (0, 0))),
            (1, Request::measurement(b"get item:8\r\n".to_vec(), (1, 0))),
            (1, Request::measurement(b"get item:9\r\n".to_vec(), (1, 1))),
            (0, Request::measurement(b"get item:10\r\n".to_vec(), (0, 1))),
        ],
    );
}

#[test]
fn memcached_ascii_with_workload_set_uses_configured_value_size() {
    let mut protocol =
        MemcachedAsciiProtocol::with_workload(AsciiOp::Set, KeyGeneration::round_robin(2), 3);
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(b"set key:0 0 0 3\r\nxxx\r\n".to_vec(), (1, 0))),
            (0, Request::measurement(b"set key:1 0 0 3\r\nxxx\r\n".to_vec(), (0, 0))),
            (1, Request::measurement(b"set key:0 0 0 3\r\nxxx\r\n".to_vec(), (1, 1))),
        ],
    );
}

#[test]
fn memcached_ascii_insert_phase_without_workload_counts_up_from_key_zero() {
    let mut protocol = MemcachedAsciiProtocol::new(AsciiOp::Get).with_insert_phase(2, 4);
    assert!(protocol.is_in_insert_phase());
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::warmup(b"set key:0 0 0 4\r\nxxxx\r\n".to_vec(), (0, 0))),
            (1, Request::warmup(b"set key:1 0 0 4\r\nxxxx\r\n".to_vec(), (1, 0))),
        ],
    );
    assert!(!protocol.is_in_insert_phase());
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(b"get key:0\r\n".to_vec(), (1, 1))),
            (0, Request::measurement(b"get key:0\r\n".to_vec(), (0, 1))),
        ],
    );
}

#[test]
fn memcached_ascii_insert_phase_replays_workload_keys_with_later_prefix() {
    let mut protocol =
        MemcachedAsciiProtocol::with_workload(AsciiOp::Set, KeyGeneration::sequential(25), 3)
            .with_insert_phase(2, 5)
            .with_key_prefix("item:".to_string());
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::warmup(b"set item:25 0 0 5\r\nxxxxx\r\n".to_vec(), (0, 0))),
            (1, Request::warmup(b"set item:26 0 0 5\r\nxxxxx\r\n".to_vec(), (1, 0))),
            (1, Request::measurement(b"set item:25 0 0 3\r\nxxx\r\n".to_vec(), (1, 1))),
            (0, Request::measurement(b"set item:26 0 0 3\r\nxxx\r\n".to_vec(), (0, 1))),
            (0, Request::measurement(b"set item:27 0 0 3\r\nxxx\r\n".to_vec(), (0, 2))),
        ],
    );
}

#[test]
fn memcached_ascii_from_default_config_counts_keys_from_zero() {
    let mut protocol =
        MemcachedAsciiProtocol::from_config(&MemcachedConfig::default(), None).unwrap();
    assert!(!protocol.is_in_insert_phase());
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(b"get key:0\r\n".to_vec(), (0, 0))),
            (1, Request::measurement(b"get key:1\r\n".to_vec(), (1, 0))),
            (0, Request::measurement(b"get key:2\r\n".to_vec(), (0, 1))),
        ],
    );
}

#[test]
fn memcached_ascii_from_config_seeded_random_get_replays_insert_keys() {
    let mut protocol =
        MemcachedAsciiProtocol::from_config(&seeded_random_get_config(), Some(42)).unwrap();
    assert!(protocol.is_in_insert_phase());
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::warmup(b"set r:728 0 0 5\r\nxxxxx\r\n".to_vec(), (0, 0))),
            (1, Request::warmup(b"set r:394 0 0 5\r\nxxxxx\r\n".to_vec(), (1, 0))),
            (0, Request::warmup(b"set r:704 0 0 5\r\nxxxxx\r\n".to_vec(), (0, 1))),
            (1, Request::measurement(b"get r:728\r\n".to_vec(), (1, 1))),
            (0, Request::measurement(b"get r:394\r\n".to_vec(), (0, 2))),
            (1, Request::measurement(b"get r:704\r\n".to_vec(), (1, 2))),
            (0, Request::measurement(b"get r:393\r\n".to_vec(), (0, 3))),
            (1, Request::measurement(b"get r:777\r\n".to_vec(), (1, 3))),
        ],
    );
    assert!(!protocol.is_in_insert_phase());
}

#[test]
fn memcached_ascii_from_config_seeded_zipfian_set_replays_insert_keys() {
    let mut protocol =
        MemcachedAsciiProtocol::from_config(&seeded_zipfian_set_config(), Some(42)).unwrap();
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::warmup(b"set z:2 0 0 2\r\nxx\r\n".to_vec(), (0, 0))),
            (1, Request::warmup(b"set z:3 0 0 2\r\nxx\r\n".to_vec(), (1, 0))),
            (0, Request::measurement(b"set z:2 0 0 4\r\nxxxx\r\n".to_vec(), (0, 1))),
            (1, Request::measurement(b"set z:3 0 0 4\r\nxxxx\r\n".to_vec(), (1, 1))),
            (0, Request::measurement(b"set z:33 0 0 4\r\nxxxx\r\n".to_vec(), (0, 2))),
            (1, Request::measurement(b"set z:0 0 0 4\r\nxxxx\r\n".to_vec(), (1, 2))),
            (0, Request::measurement(b"set z:3 0 0 4\r\nxxxx\r\n".to_vec(), (0, 3))),
            (1, Request::measurement(b"set z:4 0 0 4\r\nxxxx\r\n".to_vec(), (1, 3))),
        ],
    );
}

#[test]
fn memcached_ascii_from_config_empty_insert_phase_covers_keyspace() {
    let mut protocol =
        MemcachedAsciiProtocol::from_config(&round_robin_empty_insert_phase_config(), Some(42))
            .unwrap();
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::warmup(b"set rr:0 0 0 2\r\nxx\r\n".to_vec(), (0, 0))),
            (1, Request::warmup(b"set rr:1 0 0 2\r\nxx\r\n".to_vec(), (1, 0))),
            (0, Request::warmup(b"set rr:2 0 0 2\r\nxx\r\n".to_vec(), (0, 1))),
            (1, Request::measurement(b"get rr:0\r\n".to_vec(), (1, 1))),
            (1, Request::measurement(b"get rr:1\r\n".to_vec(), (1, 2))),
            (0, Request::measurement(b"get rr:2\r\n".to_vec(), (0, 2))),
            (0, Request::measurement(b"get rr:0\r\n".to_vec(), (0, 3))),
        ],
    );
}

#[test]
fn memcached_ascii_reset_restarts_ids_and_keys_but_not_insert_phase() {
    let mut protocol =
        MemcachedAsciiProtocol::with_workload(AsciiOp::Get, KeyGeneration::sequential(10), 1)
            .with_insert_phase(2, 1);
    assert_requests(
        &mut protocol,
        vec![(0, Request::warmup(b"set key:10 0 0 1\r\nx\r\n".to_vec(), (0, 0)))],
    );
    protocol.reset();
    assert!(protocol.is_in_insert_phase());
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::warmup(b"set key:11 0 0 1\r\nx\r\n".to_vec(), (0, 0))),
            (1, Request::measurement(b"get key:10\r\n".to_vec(), (1, 0))),
            (0, Request::measurement(b"get key:11\r\n".to_vec(), (0, 1))),
        ],
    );
    protocol.reset();
    assert!(!protocol.is_in_insert_phase());
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(b"get key:10\r\n".to_vec(), (1, 0))),
            (0, Request::measurement(b"get key:11\r\n".to_vec(), (0, 0))),
        ],
    );
}

#[test]
fn memcached_ascii_response_ids_count_per_connection() {
    let mut protocol = MemcachedAsciiProtocol::new(AsciiOp::Get);
    assert_eq!(protocol.parse_response(0, b"END\r\n").unwrap(), Parsed::ok(5, (0, 0)));
    assert_eq!(protocol.parse_response(1, b"STORED\r\n").unwrap(), Parsed::ok(8, (1, 0)));
    assert_eq!(protocol.parse_response(0, b"STORED\r\n").unwrap(), Parsed::ok(8, (0, 1)));
    assert_eq!(protocol.parse_response(1, b"END\r\n").unwrap(), Parsed::ok(5, (1, 1)));
    protocol.reset();
    assert_eq!(protocol.parse_response(1, b"END\r\n").unwrap(), Parsed::ok(5, (1, 0)));
}

#[test]
fn memcached_binary_new_and_default_get_repeat_key_zero() {
    #[rustfmt::skip]
    let get = vec![
        0x80, 0x0c, 0x00, 0x05, // magic, GETK, key length 5
        0x00, 0x00, 0x00, 0x00, // extras length, data type, vbucket
        0x00, 0x00, 0x00, 0x05, // body length 5
        0x00, 0x00, 0x00, 0x00, // opaque
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
        b'k', b'e', b'y', b':', b'0',
    ];
    assert_eq!(binary_get("key:0"), get);
    for mut protocol in
        [MemcachedBinaryProtocol::new(BinaryOp::Get), MemcachedBinaryProtocol::default()]
    {
        assert!(!protocol.is_in_insert_phase());
        assert_requests(
            &mut protocol,
            vec![
                (0, Request::measurement(get.clone(), (0, 0))),
                (1, Request::measurement(get.clone(), (1, 0))),
                (0, Request::measurement(get.clone(), (0, 1))),
                (1, Request::measurement(get.clone(), (1, 1))),
                (1, Request::measurement(get.clone(), (1, 2))),
            ],
        );
    }
}

#[test]
fn memcached_binary_new_set_uses_key_zero_and_64_byte_value() {
    #[rustfmt::skip]
    let header = [
        0x80, 0x01, 0x00, 0x05, // magic, SET, key length 5
        0x08, 0x00, 0x00, 0x00, // extras length 8, data type, vbucket
        0x00, 0x00, 0x00, 0x4d, // body length 77
        0x00, 0x00, 0x00, 0x00, // opaque
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
    ];
    let set = [header.as_slice(), &[0x00; 8], b"key:0", &[b'x'; 64]].concat();
    assert_eq!(binary_set("key:0", 64), set);
    let mut protocol = MemcachedBinaryProtocol::new(BinaryOp::Set);
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(set.clone(), (1, 0))),
            (0, Request::measurement(set.clone(), (0, 0))),
            (1, Request::measurement(set.clone(), (1, 1))),
        ],
    );
}

#[test]
fn memcached_binary_with_workload_get_draws_keys_across_connections() {
    let mut protocol =
        MemcachedBinaryProtocol::with_workload(BinaryOp::Get, KeyGeneration::sequential(7), 3)
            .with_key_prefix("item:".to_string());
    assert!(!protocol.is_in_insert_phase());
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(binary_get("item:7"), (0, 0))),
            (1, Request::measurement(binary_get("item:8"), (1, 0))),
            (1, Request::measurement(binary_get("item:9"), (1, 1))),
            (0, Request::measurement(binary_get("item:10"), (0, 1))),
        ],
    );
}

#[test]
fn memcached_binary_with_workload_set_uses_configured_value_size() {
    let mut protocol =
        MemcachedBinaryProtocol::with_workload(BinaryOp::Set, KeyGeneration::round_robin(2), 3);
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(binary_set("key:0", 3), (1, 0))),
            (0, Request::measurement(binary_set("key:1", 3), (0, 0))),
            (1, Request::measurement(binary_set("key:0", 3), (1, 1))),
        ],
    );
}

#[test]
fn memcached_binary_insert_phase_without_workload_counts_up_from_key_zero() {
    let mut protocol = MemcachedBinaryProtocol::new(BinaryOp::Get).with_insert_phase(2, 4);
    assert!(protocol.is_in_insert_phase());
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::warmup(binary_set("key:0", 4), (0, 0))),
            (1, Request::warmup(binary_set("key:1", 4), (1, 0))),
        ],
    );
    assert!(!protocol.is_in_insert_phase());
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(binary_get("key:0"), (1, 1))),
            (0, Request::measurement(binary_get("key:0"), (0, 1))),
        ],
    );
}

#[test]
fn memcached_binary_insert_phase_replays_workload_keys_with_later_prefix() {
    let mut protocol =
        MemcachedBinaryProtocol::with_workload(BinaryOp::Set, KeyGeneration::sequential(25), 3)
            .with_insert_phase(2, 5)
            .with_key_prefix("item:".to_string());
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::warmup(binary_set("item:25", 5), (0, 0))),
            (1, Request::warmup(binary_set("item:26", 5), (1, 0))),
            (1, Request::measurement(binary_set("item:25", 3), (1, 1))),
            (0, Request::measurement(binary_set("item:26", 3), (0, 1))),
            (0, Request::measurement(binary_set("item:27", 3), (0, 2))),
        ],
    );
}

#[test]
fn memcached_binary_from_default_config_counts_keys_from_zero() {
    let mut protocol =
        MemcachedBinaryProtocol::from_config(&MemcachedConfig::default(), None).unwrap();
    assert!(!protocol.is_in_insert_phase());
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(binary_get("key:0"), (0, 0))),
            (1, Request::measurement(binary_get("key:1"), (1, 0))),
            (0, Request::measurement(binary_get("key:2"), (0, 1))),
        ],
    );
}

#[test]
fn memcached_binary_from_config_seeded_random_get_replays_insert_keys() {
    let mut protocol =
        MemcachedBinaryProtocol::from_config(&seeded_random_get_config(), Some(42)).unwrap();
    assert!(protocol.is_in_insert_phase());
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::warmup(binary_set("r:728", 5), (0, 0))),
            (1, Request::warmup(binary_set("r:394", 5), (1, 0))),
            (0, Request::warmup(binary_set("r:704", 5), (0, 1))),
            (1, Request::measurement(binary_get("r:728"), (1, 1))),
            (0, Request::measurement(binary_get("r:394"), (0, 2))),
            (1, Request::measurement(binary_get("r:704"), (1, 2))),
            (0, Request::measurement(binary_get("r:393"), (0, 3))),
            (1, Request::measurement(binary_get("r:777"), (1, 3))),
        ],
    );
    assert!(!protocol.is_in_insert_phase());
}

#[test]
fn memcached_binary_from_config_seeded_zipfian_set_replays_insert_keys() {
    let mut protocol =
        MemcachedBinaryProtocol::from_config(&seeded_zipfian_set_config(), Some(42)).unwrap();
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::warmup(binary_set("z:2", 2), (0, 0))),
            (1, Request::warmup(binary_set("z:3", 2), (1, 0))),
            (0, Request::measurement(binary_set("z:2", 4), (0, 1))),
            (1, Request::measurement(binary_set("z:3", 4), (1, 1))),
            (0, Request::measurement(binary_set("z:33", 4), (0, 2))),
            (1, Request::measurement(binary_set("z:0", 4), (1, 2))),
            (0, Request::measurement(binary_set("z:3", 4), (0, 3))),
            (1, Request::measurement(binary_set("z:4", 4), (1, 3))),
        ],
    );
}

#[test]
fn memcached_binary_from_config_empty_insert_phase_covers_keyspace() {
    let mut protocol =
        MemcachedBinaryProtocol::from_config(&round_robin_empty_insert_phase_config(), Some(42))
            .unwrap();
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::warmup(binary_set("rr:0", 2), (0, 0))),
            (1, Request::warmup(binary_set("rr:1", 2), (1, 0))),
            (0, Request::warmup(binary_set("rr:2", 2), (0, 1))),
            (1, Request::measurement(binary_get("rr:0"), (1, 1))),
            (1, Request::measurement(binary_get("rr:1"), (1, 2))),
            (0, Request::measurement(binary_get("rr:2"), (0, 2))),
            (0, Request::measurement(binary_get("rr:0"), (0, 3))),
        ],
    );
}

#[test]
fn memcached_binary_reset_restarts_ids_and_keys_but_not_insert_phase() {
    let mut protocol =
        MemcachedBinaryProtocol::with_workload(BinaryOp::Get, KeyGeneration::sequential(10), 1)
            .with_insert_phase(2, 1);
    assert_requests(&mut protocol, vec![(0, Request::warmup(binary_set("key:10", 1), (0, 0)))]);
    protocol.reset();
    assert!(protocol.is_in_insert_phase());
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::warmup(binary_set("key:11", 1), (0, 0))),
            (1, Request::measurement(binary_get("key:10"), (1, 0))),
            (0, Request::measurement(binary_get("key:11"), (0, 1))),
        ],
    );
    protocol.reset();
    assert!(!protocol.is_in_insert_phase());
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(binary_get("key:10"), (1, 0))),
            (0, Request::measurement(binary_get("key:11"), (0, 0))),
        ],
    );
}

#[test]
fn memcached_binary_response_ids_count_per_connection() {
    // A SET reply has an empty body; a GETK hit carries 4 flag bytes, the key, and the value.
    let stored = [&[0x81, 0x01][..], &[0x00; 22]].concat();
    #[rustfmt::skip]
    let hit = [
        &[0x81, 0x0c, 0x00, 0x05, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c][..],
        &[0x00; 12],
        &[0x00; 4],
        b"key:0xyz",
    ]
    .concat();
    let mut protocol = MemcachedBinaryProtocol::new(BinaryOp::Get);
    assert_eq!(protocol.parse_response(0, &hit).unwrap(), Parsed::ok(36, (0, 0)));
    assert_eq!(protocol.parse_response(1, &stored).unwrap(), Parsed::ok(24, (1, 0)));
    assert_eq!(protocol.parse_response(0, &stored).unwrap(), Parsed::ok(24, (0, 1)));
    assert_eq!(protocol.parse_response(1, &hit).unwrap(), Parsed::ok(36, (1, 1)));
    protocol.reset();
    assert_eq!(protocol.parse_response(1, &stored).unwrap(), Parsed::ok(24, (1, 0)));
}

/// HTTP/1.1 request with a `body_len`-byte body of b'x', as POST and PUT send it.
fn http_with_body(method: &str, path: &str, host: &str, body_len: usize) -> Vec<u8> {
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: keep-alive\r\n\
         Content-Length: {body_len}\r\nContent-Type: application/octet-stream\r\n\r\n"
    );
    [head.into_bytes(), vec![b'x'; body_len]].concat()
}

#[test]
fn http_new_and_with_body_size_get_send_no_body() {
    let get = b"GET /api/test HTTP/1.1\r\nHost: example.com\r\nConnection: keep-alive\r\n\r\n";
    for mut protocol in [
        HttpProtocol::new(HttpMethod::Get, "/api/test".to_string(), "example.com".to_string()),
        HttpProtocol::with_body_size(
            HttpMethod::Get,
            "/api/test".to_string(),
            "example.com".to_string(),
            32,
        ),
    ] {
        assert_requests(
            &mut protocol,
            vec![
                (0, Request::measurement(get.to_vec(), (0, 0))),
                (1, Request::measurement(get.to_vec(), (1, 0))),
                (0, Request::measurement(get.to_vec(), (0, 1))),
                (1, Request::measurement(get.to_vec(), (1, 1))),
                (1, Request::measurement(get.to_vec(), (1, 2))),
            ],
        );
    }
}

#[test]
fn http_default_and_default_config_get_root_from_localhost() {
    let get = b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n";
    for mut protocol in [
        HttpProtocol::default(),
        HttpProtocol::from_config(&HttpConfig::default()).unwrap(),
    ] {
        assert_requests(
            &mut protocol,
            vec![
                (1, Request::measurement(get.to_vec(), (1, 0))),
                (0, Request::measurement(get.to_vec(), (0, 0))),
                (1, Request::measurement(get.to_vec(), (1, 1))),
            ],
        );
    }
}

#[test]
fn http_post_with_body_size_sends_x_body() {
    let post = [
        b"POST /data HTTP/1.1\r\n".as_slice(),
        b"Host: api.example.com\r\n",
        b"Connection: keep-alive\r\n",
        b"Content-Length: 5\r\n",
        b"Content-Type: application/octet-stream\r\n",
        b"\r\n",
        b"xxxxx",
    ]
    .concat();
    assert_eq!(http_with_body("POST", "/data", "api.example.com", 5), post);
    let mut protocol = HttpProtocol::with_body_size(
        HttpMethod::Post,
        "/data".to_string(),
        "api.example.com".to_string(),
        5,
    );
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(post.clone(), (1, 0))),
            (0, Request::measurement(post.clone(), (0, 0))),
            (1, Request::measurement(post.clone(), (1, 1))),
            (0, Request::measurement(post.clone(), (0, 1))),
            (0, Request::measurement(post.clone(), (0, 2))),
        ],
    );
}

#[test]
fn http_post_new_uses_64_byte_body() {
    let post = http_with_body("POST", "/", "localhost", 64);
    let mut protocol =
        HttpProtocol::new(HttpMethod::Post, "/".to_string(), "localhost".to_string());
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(post.clone(), (0, 0))),
            (1, Request::measurement(post.clone(), (1, 0))),
            (0, Request::measurement(post.clone(), (0, 1))),
        ],
    );
}

#[test]
fn http_post_body_sizes_from_empty_to_large() {
    let empty = b"POST /upload HTTP/1.1\r\nHost: example.com\r\nConnection: keep-alive\r\n\
        Content-Length: 0\r\nContent-Type: application/octet-stream\r\n\r\n";
    assert_eq!(http_with_body("POST", "/upload", "example.com", 0), empty.to_vec());
    for body_size in [0, 1, 4096, 100_000] {
        let post = http_with_body("POST", "/upload", "example.com", body_size);
        let mut protocol = HttpProtocol::with_body_size(
            HttpMethod::Post,
            "/upload".to_string(),
            "example.com".to_string(),
            body_size,
        );
        assert_requests(
            &mut protocol,
            vec![
                (0, Request::measurement(post.clone(), (0, 0))),
                (1, Request::measurement(post.clone(), (1, 0))),
                (0, Request::measurement(post.clone(), (0, 1))),
            ],
        );
    }
}

#[test]
fn http_put_with_body_size_sends_x_body() {
    let put = b"PUT /k HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\
        Content-Length: 3\r\nContent-Type: application/octet-stream\r\n\r\nxxx";
    let mut protocol =
        HttpProtocol::with_body_size(HttpMethod::Put, "/k".to_string(), "localhost".to_string(), 3);
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(put.to_vec(), (0, 0))),
            (1, Request::measurement(put.to_vec(), (1, 0))),
            (1, Request::measurement(put.to_vec(), (1, 1))),
        ],
    );
}

#[test]
fn http_from_config_post_uses_body_size_and_localhost() {
    let config = HttpConfig {
        method: "post".to_string(),
        path: "/submit".to_string(),
        host: None,
        body_size: 2,
    };
    let post = b"POST /submit HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\
        Content-Length: 2\r\nContent-Type: application/octet-stream\r\n\r\nxx";
    let mut protocol = HttpProtocol::from_config(&config).unwrap();
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(post.to_vec(), (1, 0))),
            (0, Request::measurement(post.to_vec(), (0, 0))),
            (1, Request::measurement(post.to_vec(), (1, 1))),
        ],
    );
}

#[test]
fn http_reset_restarts_request_ids() {
    let post = http_with_body("POST", "/", "localhost", 4);
    let mut protocol =
        HttpProtocol::with_body_size(HttpMethod::Post, "/".to_string(), "localhost".to_string(), 4);
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(post.clone(), (0, 0))),
            (1, Request::measurement(post.clone(), (1, 0))),
            (0, Request::measurement(post.clone(), (0, 1))),
        ],
    );
    protocol.reset();
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(post.clone(), (0, 0))),
            (1, Request::measurement(post.clone(), (1, 0))),
        ],
    );
}

/// 16-byte xylem-echo request: `conn_id << 32 | seq`, then the delay, both little-endian.
fn echo_message(conn_id: u64, seq: u64, delay_us: u64) -> Vec<u8> {
    let encoded_id = (conn_id << 32) | seq;
    [encoded_id.to_le_bytes(), delay_us.to_le_bytes()].concat()
}

#[test]
fn xylem_echo_new_encodes_connection_sequence_and_delay() {
    #[rustfmt::skip]
    let second_on_conn_1 = vec![
        0x01, 0x00, 0x00, 0x00, // sequence 1
        0x01, 0x00, 0x00, 0x00, // connection 1
        0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // delay 100 us
    ];
    assert_eq!(echo_message(1, 1, 100), second_on_conn_1);
    let mut protocol = XylemEchoProtocol::new(100);
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(echo_message(0, 0, 100), (0, 0))),
            (1, Request::measurement(echo_message(1, 0, 100), (1, 0))),
            (1, Request::measurement(second_on_conn_1, (1, 1))),
            (0, Request::measurement(echo_message(0, 1, 100), (0, 1))),
            (0, Request::measurement(echo_message(0, 2, 100), (0, 2))),
        ],
    );
}

#[test]
fn xylem_echo_default_has_zero_delay() {
    let mut protocol = XylemEchoProtocol::default();
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(vec![0x00; 16], (0, 0))),
            (1, Request::measurement(echo_message(1, 0, 0), (1, 0))),
            (0, Request::measurement(echo_message(0, 1, 0), (0, 1))),
        ],
    );
}

#[test]
fn xylem_echo_largest_connection_id_and_delay() {
    #[rustfmt::skip]
    let first = vec![
        0x00, 0x00, 0x00, 0x00, // sequence 0
        0xff, 0xff, 0xff, 0xff, // connection 0xffff_ffff
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // delay u64::MAX
    ];
    let mut protocol = XylemEchoProtocol::new(u64::MAX);
    assert_requests(
        &mut protocol,
        vec![
            (0xffff_ffff, Request::measurement(first, (0xffff_ffff, 0))),
            (2, Request::measurement(echo_message(2, 0, u64::MAX), (2, 0))),
            (
                0xffff_ffff,
                Request::measurement(echo_message(0xffff_ffff, 1, u64::MAX), (0xffff_ffff, 1)),
            ),
        ],
    );
}

#[test]
fn xylem_echo_reset_restarts_sequences() {
    let mut protocol = XylemEchoProtocol::new(7);
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(echo_message(0, 0, 7), (0, 0))),
            (1, Request::measurement(echo_message(1, 0, 7), (1, 0))),
            (1, Request::measurement(echo_message(1, 1, 7), (1, 1))),
        ],
    );
    protocol.reset();
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(echo_message(1, 0, 7), (1, 0))),
            (0, Request::measurement(echo_message(0, 0, 7), (0, 0))),
        ],
    );
}

/// Masstree handshake request [0, 14, {"core": -1, "maxkeylen": 255}].
#[rustfmt::skip]
const MASSTREE_HANDSHAKE: &[u8] = &[
    0x93, // array of 3
    0xce, 0x00, 0x00, 0x00, 0x00, // u32 seq 0
    0xcc, 0x0e, // u8 command 14 (handshake)
    0x82, // map of 2
    0xa4, b'c', b'o', b'r', b'e', 0xff, // "core": -1
    0xa9, b'm', b'a', b'x', b'k', b'e', b'y', b'l', b'e', b'n', 0xcc, 0xff, // "maxkeylen": 255
];

/// Masstree handshake reply [0, 15, true, 0, "1.0"].
const MASSTREE_HANDSHAKE_REPLY: &[u8] = &[0x95, 0x00, 0x0f, 0xc3, 0x00, 0xa3, b'1', b'.', b'0'];

/// MessagePack u32 in its fixed five-byte form.
fn msgpack_u32(value: u32) -> Vec<u8> {
    [&[0xce][..], &value.to_be_bytes()].concat()
}

/// MessagePack string: the shortest str header for its length, then the bytes.
fn msgpack_str(bytes: &[u8]) -> Vec<u8> {
    let len = bytes.len();
    let header = if len < 32 {
        vec![0xa0 | len as u8]
    } else if len < 256 {
        vec![0xd9, len as u8]
    } else if len < 65536 {
        [&[0xda][..], &(len as u16).to_be_bytes()].concat()
    } else {
        [&[0xdb][..], &(len as u32).to_be_bytes()].concat()
    };
    [header, bytes.to_vec()].concat()
}

/// MessagePack array header for fewer than 16 elements.
fn msgpack_fixarray(len: usize) -> Vec<u8> {
    assert!(len < 16);
    vec![0x90 | len as u8]
}

/// Masstree GET request [seq, 2, key].
fn masstree_get(seq: u32, key: &str) -> Vec<u8> {
    [
        msgpack_fixarray(3),
        msgpack_u32(seq),
        vec![0xcc, 0x02],
        msgpack_str(key.as_bytes()),
    ]
    .concat()
}

/// Masstree SET (replace) request [seq, 8, key, value].
fn masstree_set(seq: u32, key: &str, value: &[u8]) -> Vec<u8> {
    [
        msgpack_fixarray(4),
        msgpack_u32(seq),
        vec![0xcc, 0x08],
        msgpack_str(key.as_bytes()),
        msgpack_str(value),
    ]
    .concat()
}

/// Masstree PUT request [seq, 6, key, col_idx, value, col_idx, value, ...].
fn masstree_put(seq: u32, key: &str, columns: &[(u32, Vec<u8>)]) -> Vec<u8> {
    let mut request = [
        msgpack_fixarray(3 + 2 * columns.len()),
        msgpack_u32(seq),
        vec![0xcc, 0x06],
        msgpack_str(key.as_bytes()),
    ]
    .concat();
    for (col_idx, value) in columns {
        request.extend_from_slice(&msgpack_u32(*col_idx));
        request.extend_from_slice(&msgpack_str(value));
    }
    request
}

/// Masstree REMOVE request [seq, 10, key].
fn masstree_remove(seq: u32, key: &str) -> Vec<u8> {
    [
        msgpack_fixarray(3),
        msgpack_u32(seq),
        vec![0xcc, 0x0a],
        msgpack_str(key.as_bytes()),
    ]
    .concat()
}

/// Masstree SCAN request [seq, 4, firstkey, count, field, field, ...].
fn masstree_scan(seq: u32, firstkey: &str, count: u32, fields: &[u32]) -> Vec<u8> {
    let mut request = [
        msgpack_fixarray(4 + fields.len()),
        msgpack_u32(seq),
        vec![0xcc, 0x04],
        msgpack_str(firstkey.as_bytes()),
        msgpack_u32(count),
    ]
    .concat();
    for field in fields {
        request.extend_from_slice(&msgpack_u32(*field));
    }
    request
}

/// Masstree CHECKPOINT request [seq, 12].
fn masstree_checkpoint(seq: u32) -> Vec<u8> {
    [msgpack_fixarray(2), msgpack_u32(seq), vec![0xcc, 0x0c]].concat()
}

/// Sends the masstree handshake on `conn_id` and feeds back the server's reply.
fn complete_masstree_handshake(protocol: &mut MasstreeProtocol, conn_id: usize) {
    assert!(protocol.can_send(conn_id));
    assert_requests(
        protocol,
        vec![(conn_id, Request::setup(MASSTREE_HANDSHAKE.to_vec(), (conn_id, 0)))],
    );
    assert!(!protocol.can_send(conn_id));
    assert_eq!(
        protocol.parse_response(conn_id, MASSTREE_HANDSHAKE_REPLY).unwrap(),
        Parsed::ok(MASSTREE_HANDSHAKE_REPLY.len(), (conn_id, 0))
    );
    assert!(protocol.can_send(conn_id));
}

#[test]
fn masstree_new_and_default_handshake_each_connection_before_key_zero_gets() {
    #[rustfmt::skip]
    let second_get = vec![
        0x93, // array of 3
        0xce, 0x00, 0x00, 0x00, 0x01, // u32 seq 1
        0xcc, 0x02, // u8 command 2 (get)
        0xa5, b'k', b'e', b'y', b':', b'0',
    ];
    assert_eq!(masstree_get(1, "key:0"), second_get);
    for mut protocol in [MasstreeProtocol::new(MasstreeOp::Get), MasstreeProtocol::default()] {
        assert!(protocol.can_send(0));
        assert!(protocol.can_send(1));
        assert_requests(
            &mut protocol,
            vec![
                (0, Request::setup(MASSTREE_HANDSHAKE.to_vec(), (0, 0))),
                (1, Request::setup(MASSTREE_HANDSHAKE.to_vec(), (1, 0))),
            ],
        );
        assert!(!protocol.can_send(0));
        assert!(!protocol.can_send(1));
        assert_eq!(
            protocol.parse_response(1, MASSTREE_HANDSHAKE_REPLY).unwrap(),
            Parsed::ok(9, (1, 0))
        );
        assert!(!protocol.can_send(0));
        assert!(protocol.can_send(1));
        assert_requests(
            &mut protocol,
            vec![
                (1, Request::measurement(masstree_get(0, "key:0"), (1, 0))),
                (1, Request::measurement(second_get.clone(), (1, 1))),
            ],
        );
        assert_eq!(
            protocol.parse_response(0, MASSTREE_HANDSHAKE_REPLY).unwrap(),
            Parsed::ok(9, (0, 0))
        );
        assert!(protocol.can_send(0));
        assert_requests(
            &mut protocol,
            vec![
                (0, Request::measurement(masstree_get(0, "key:0"), (0, 0))),
                (1, Request::measurement(masstree_get(2, "key:0"), (1, 2))),
                (0, Request::measurement(second_get.clone(), (0, 1))),
            ],
        );
    }
}

#[test]
fn masstree_new_set_uses_key_zero_and_64_byte_value() {
    let mut protocol = MasstreeProtocol::new(MasstreeOp::Set);
    complete_masstree_handshake(&mut protocol, 1);
    complete_masstree_handshake(&mut protocol, 0);
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(masstree_set(0, "key:0", &[b'x'; 64]), (1, 0))),
            (0, Request::measurement(masstree_set(0, "key:0", &[b'x'; 64]), (0, 0))),
            (1, Request::measurement(masstree_set(1, "key:0", &[b'x'; 64]), (1, 1))),
        ],
    );
}

#[test]
fn masstree_new_put_writes_each_column() {
    // Columns 0 and 1, each a value of the default 64 bytes
    let columns = vec![(0, vec![b'x'; 64]), (1, vec![b'x'; 64])];
    #[rustfmt::skip]
    let first_put = [
        vec![
            0x97, // array of 7
            0xce, 0x00, 0x00, 0x00, 0x00, // u32 seq 0
            0xcc, 0x06, // u8 command 6 (put)
            0xa5, b'k', b'e', b'y', b':', b'0',
            0xce, 0x00, 0x00, 0x00, 0x00, 0xd9, 64, // column 0, str8 of 64
        ],
        vec![b'x'; 64],
        vec![0xce, 0x00, 0x00, 0x00, 0x01, 0xd9, 64], // column 1
        vec![b'x'; 64],
    ]
    .concat();
    assert_eq!(masstree_put(0, "key:0", &columns), first_put);
    let mut protocol = MasstreeProtocol::new(MasstreeOp::Put { columns: 2 });
    complete_masstree_handshake(&mut protocol, 0);
    complete_masstree_handshake(&mut protocol, 1);
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(first_put.clone(), (0, 0))),
            (1, Request::measurement(first_put.clone(), (1, 0))),
            (0, Request::measurement(masstree_put(1, "key:0", &columns), (0, 1))),
        ],
    );
}

#[test]
fn masstree_new_remove_and_checkpoint() {
    #[rustfmt::skip]
    let remove = vec![
        0x93, 0xce, 0x00, 0x00, 0x00, 0x00, 0xcc, 0x0a, 0xa5, b'k', b'e', b'y', b':', b'0',
    ];
    assert_eq!(masstree_remove(0, "key:0"), remove);
    let mut protocol = MasstreeProtocol::new(MasstreeOp::Remove);
    complete_masstree_handshake(&mut protocol, 0);
    complete_masstree_handshake(&mut protocol, 1);
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(remove.clone(), (0, 0))),
            (1, Request::measurement(remove.clone(), (1, 0))),
            (1, Request::measurement(masstree_remove(1, "key:0"), (1, 1))),
        ],
    );

    let checkpoint = vec![0x92, 0xce, 0x00, 0x00, 0x00, 0x00, 0xcc, 0x0c];
    assert_eq!(masstree_checkpoint(0), checkpoint);
    let mut protocol = MasstreeProtocol::new(MasstreeOp::Checkpoint);
    complete_masstree_handshake(&mut protocol, 1);
    complete_masstree_handshake(&mut protocol, 0);
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(checkpoint.clone(), (1, 0))),
            (0, Request::measurement(checkpoint.clone(), (0, 0))),
            (0, Request::measurement(masstree_checkpoint(1), (0, 1))),
        ],
    );
}

#[test]
fn masstree_new_scan_sends_first_key_count_and_fields() {
    // Without a key generator, every scan starts at key 0
    #[rustfmt::skip]
    let first_scan = vec![
        0x96, // array of 6
        0xce, 0x00, 0x00, 0x00, 0x00, // u32 seq 0
        0xcc, 0x04, // u8 command 4 (scan)
        0xa5, b'k', b'e', b'y', b':', b'0', // first key
        0xce, 0x00, 0x00, 0x00, 0x0a, // count 10
        0xce, 0x00, 0x00, 0x00, 0x01, // field 1
        0xce, 0x00, 0x00, 0x00, 0x02, // field 2
    ];
    assert_eq!(masstree_scan(0, "key:0", 10, &[1, 2]), first_scan);
    let mut protocol = MasstreeProtocol::new(MasstreeOp::Scan { count: 10, fields: vec![1, 2] });
    complete_masstree_handshake(&mut protocol, 0);
    complete_masstree_handshake(&mut protocol, 1);
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(first_scan.clone(), (0, 0))),
            (1, Request::measurement(first_scan.clone(), (1, 0))),
            (0, Request::measurement(masstree_scan(1, "key:0", 10, &[1, 2]), (0, 1))),
        ],
    );

    // With one, each scan starts at the next generated key
    let mut protocol = MasstreeProtocol::with_workload(
        MasstreeOp::Scan { count: 1, fields: vec![] },
        KeyGeneration::sequential(8),
        3,
    );
    complete_masstree_handshake(&mut protocol, 0);
    complete_masstree_handshake(&mut protocol, 1);
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(masstree_scan(0, "key:8", 1, &[]), (1, 0))),
            (0, Request::measurement(masstree_scan(0, "key:9", 1, &[]), (0, 0))),
        ],
    );
}

#[test]
fn masstree_with_workload_get_draws_keys_across_connections() {
    let mut protocol =
        MasstreeProtocol::with_workload(MasstreeOp::Get, KeyGeneration::sequential(7), 3);
    complete_masstree_handshake(&mut protocol, 0);
    complete_masstree_handshake(&mut protocol, 1);
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(masstree_get(0, "key:7"), (0, 0))),
            (1, Request::measurement(masstree_get(0, "key:8"), (1, 0))),
            (1, Request::measurement(masstree_get(1, "key:9"), (1, 1))),
            (0, Request::measurement(masstree_get(1, "key:10"), (0, 1))),
        ],
    );
}

#[test]
fn masstree_with_workload_set_put_and_remove_use_generated_keys() {
    let mut protocol =
        MasstreeProtocol::with_workload(MasstreeOp::Set, KeyGeneration::round_robin(2), 3);
    complete_masstree_handshake(&mut protocol, 1);
    complete_masstree_handshake(&mut protocol, 0);
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(masstree_set(0, "key:0", b"xxx"), (1, 0))),
            (0, Request::measurement(masstree_set(0, "key:1", b"xxx"), (0, 0))),
            (1, Request::measurement(masstree_set(1, "key:0", b"xxx"), (1, 1))),
        ],
    );

    let columns = vec![(0, b"xxx".to_vec())];
    let mut protocol = MasstreeProtocol::with_workload(
        MasstreeOp::Put { columns: 1 },
        KeyGeneration::sequential(4),
        3,
    );
    complete_masstree_handshake(&mut protocol, 0);
    complete_masstree_handshake(&mut protocol, 1);
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(masstree_put(0, "key:4", &columns), (0, 0))),
            (1, Request::measurement(masstree_put(0, "key:5", &columns), (1, 0))),
            (0, Request::measurement(masstree_put(1, "key:6", &columns), (0, 1))),
        ],
    );

    let mut protocol =
        MasstreeProtocol::with_workload(MasstreeOp::Remove, KeyGeneration::sequential(1), 3);
    complete_masstree_handshake(&mut protocol, 0);
    complete_masstree_handshake(&mut protocol, 1);
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(masstree_remove(0, "key:1"), (1, 0))),
            (1, Request::measurement(masstree_remove(1, "key:2"), (1, 1))),
            (0, Request::measurement(masstree_remove(0, "key:3"), (0, 0))),
        ],
    );
}

#[test]
fn masstree_set_value_sizes_cross_string_header_boundaries() {
    #[rustfmt::skip]
    let cases: [(usize, &[u8]); 8] = [
        (0, &[0xa0]),
        (31, &[0xbf]),
        (32, &[0xd9, 0x20]),
        (255, &[0xd9, 0xff]),
        (256, &[0xda, 0x01, 0x00]),
        (65_535, &[0xda, 0xff, 0xff]),
        (65_536, &[0xdb, 0x00, 0x01, 0x00, 0x00]),
        (100_000, &[0xdb, 0x00, 0x01, 0x86, 0xa0]),
    ];
    for (value_size, value_header) in cases {
        let set = |seq: u8, key: u8| {
            let head = [0x94, 0xce, 0x00, 0x00, 0x00, seq, 0xcc, 0x08, 0xa5];
            [&head[..], b"key:", &[key], value_header, &vec![b'x'; value_size]].concat()
        };
        assert_eq!(masstree_set(1, "key:2", &vec![b'x'; value_size]), set(1, b'2'));
        let mut protocol = MasstreeProtocol::with_workload(
            MasstreeOp::Set,
            KeyGeneration::sequential(0),
            value_size,
        );
        complete_masstree_handshake(&mut protocol, 0);
        complete_masstree_handshake(&mut protocol, 1);
        assert_requests(
            &mut protocol,
            vec![
                (0, Request::measurement(set(0, b'0'), (0, 0))),
                (1, Request::measurement(set(0, b'1'), (1, 0))),
                (0, Request::measurement(set(1, b'2'), (0, 1))),
            ],
        );
    }
}

#[test]
fn masstree_long_key_prefix_uses_str8_key_header() {
    let prefix = "a-key-prefix-long-enough-for-str8:";
    let mut protocol = MasstreeProtocol::with_workload_and_options(
        MasstreeOp::Get,
        KeyGeneration::sequential(0),
        8,
        prefix.to_string(),
        false,
        Some(42),
        None,
    );
    #[rustfmt::skip]
    let first_get = [
        &[0x93, 0xce, 0x00, 0x00, 0x00, 0x00, 0xcc, 0x02, 0xd9, 0x23][..],
        prefix.as_bytes(),
        b"0",
    ]
    .concat();
    complete_masstree_handshake(&mut protocol, 0);
    complete_masstree_handshake(&mut protocol, 1);
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(first_get, (0, 0))),
            (1, Request::measurement(masstree_get(0, &format!("{prefix}1")), (1, 0))),
        ],
    );
}

#[test]
fn masstree_with_workload_and_options_inserts_before_measuring() {
    let mut protocol = MasstreeProtocol::with_workload_and_options(
        MasstreeOp::Get,
        KeyGeneration::round_robin(3),
        8,
        "mt:".to_string(),
        false,
        Some(42),
        Some(InsertPhaseState::new(3, 2)),
    );
    complete_masstree_handshake(&mut protocol, 0);
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::warmup(masstree_set(0, "mt:0", b"xx"), (0, 0))),
            (1, Request::setup(MASSTREE_HANDSHAKE.to_vec(), (1, 0))),
        ],
    );
    assert_eq!(
        protocol.parse_response(1, MASSTREE_HANDSHAKE_REPLY).unwrap(),
        Parsed::ok(9, (1, 0))
    );
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::warmup(masstree_set(0, "mt:1", b"xx"), (1, 0))),
            (0, Request::warmup(masstree_set(1, "mt:2", b"xx"), (0, 1))),
            (1, Request::measurement(masstree_get(1, "mt:0"), (1, 1))),
            (1, Request::measurement(masstree_get(2, "mt:1"), (1, 2))),
            (0, Request::measurement(masstree_get(2, "mt:2"), (0, 2))),
            (0, Request::measurement(masstree_get(3, "mt:0"), (0, 3))),
        ],
    );
}

#[test]
fn masstree_with_insert_phase_without_workload_counts_up_from_key_zero() {
    let mut protocol = MasstreeProtocol::new(MasstreeOp::Get).with_insert_phase(2, 3);
    complete_masstree_handshake(&mut protocol, 0);
    complete_masstree_handshake(&mut protocol, 1);
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::warmup(masstree_set(0, "key:0", b"xxx"), (0, 0))),
            (1, Request::warmup(masstree_set(0, "key:1", b"xxx"), (1, 0))),
            (1, Request::measurement(masstree_get(1, "key:0"), (1, 1))),
            (0, Request::measurement(masstree_get(1, "key:0"), (0, 1))),
        ],
    );
}

#[test]
fn masstree_seeded_random_data_fills_insert_and_set_values() {
    let mut protocol = MasstreeProtocol::with_workload_and_options(
        MasstreeOp::Set,
        KeyGeneration::sequential(5),
        6,
        "rd:".to_string(),
        true,
        Some(42),
        Some(InsertPhaseState::new(1, 4)),
    );
    complete_masstree_handshake(&mut protocol, 0);
    complete_masstree_handshake(&mut protocol, 1);
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::warmup(masstree_set(0, "rd:0", b"m>}b"), (0, 0))),
            (1, Request::measurement(masstree_set(0, "rd:5", b"kX,Y4x"), (1, 0))),
            (0, Request::measurement(masstree_set(1, "rd:6", b"Up`'FT"), (0, 1))),
            (1, Request::measurement(masstree_set(1, "rd:7", b"5%WL0r"), (1, 1))),
        ],
    );
}

#[test]
fn masstree_request_ids_wrap_after_65535() {
    let mut protocol = MasstreeProtocol::new(MasstreeOp::Remove);
    complete_masstree_handshake(&mut protocol, 0);
    complete_masstree_handshake(&mut protocol, 1);
    for _ in 0..65_535 {
        protocol.next_request(0);
    }
    #[rustfmt::skip]
    let last_before_wrap = vec![
        0x93, 0xce, 0x00, 0x00, 0xff, 0xff, 0xcc, 0x0a, 0xa5, b'k', b'e', b'y', b':', b'0',
    ];
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::measurement(last_before_wrap, (0, 65_535))),
            (1, Request::measurement(masstree_remove(0, "key:0"), (1, 0))),
            (0, Request::measurement(masstree_remove(0, "key:0"), (0, 0))),
            (0, Request::measurement(masstree_remove(1, "key:0"), (0, 1))),
        ],
    );
}

#[test]
fn masstree_reset_restarts_handshakes_ids_and_keys_but_not_insert_phase() {
    let mut protocol =
        MasstreeProtocol::with_workload(MasstreeOp::Get, KeyGeneration::sequential(10), 1)
            .with_insert_phase(2, 1);
    complete_masstree_handshake(&mut protocol, 0);
    assert_requests(
        &mut protocol,
        vec![(0, Request::warmup(masstree_set(0, "key:0", b"x"), (0, 0)))],
    );
    protocol.reset();
    complete_masstree_handshake(&mut protocol, 0);
    complete_masstree_handshake(&mut protocol, 1);
    assert_requests(
        &mut protocol,
        vec![
            (0, Request::warmup(masstree_set(0, "key:1", b"x"), (0, 0))),
            (1, Request::measurement(masstree_get(0, "key:10"), (1, 0))),
            (0, Request::measurement(masstree_get(1, "key:11"), (0, 1))),
        ],
    );
    protocol.reset();
    complete_masstree_handshake(&mut protocol, 1);
    complete_masstree_handshake(&mut protocol, 0);
    assert_requests(
        &mut protocol,
        vec![
            (1, Request::measurement(masstree_get(0, "key:10"), (1, 0))),
            (0, Request::measurement(masstree_get(0, "key:11"), (0, 0))),
        ],
    );
}
