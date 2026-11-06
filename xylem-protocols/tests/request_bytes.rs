//! Exact request bytes, request ids, and warmup tags for the memcached codecs.

use xylem_protocols::configs::{InsertPhaseConfig, KeysConfig, MemcachedConfig};
use xylem_protocols::memcached::ascii::MemcachedOp as AsciiOp;
use xylem_protocols::memcached::MemcachedOp as BinaryOp;
use xylem_protocols::memcached::{MemcachedAsciiProtocol, MemcachedBinaryProtocol};
use xylem_protocols::workload::KeyGeneration;
use xylem_protocols::{Protocol, Request};

type RequestId = (usize, u64);

/// Sends one request per entry on the entry's connection and compares the whole
/// request with the expected one: bytes, request id, and warmup tag.
fn assert_requests(
    protocol: &mut impl Protocol<RequestId = RequestId>,
    expected: Vec<(usize, Request<RequestId>)>,
) {
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
        operation: "GET".into(),
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
        operation: "SET".into(),
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
        operation: "get".into(),
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
    assert_eq!(protocol.parse_response(0, b"END\r\n").unwrap(), (5, Some((0, 0))));
    assert_eq!(protocol.parse_response(1, b"STORED\r\n").unwrap(), (8, Some((1, 0))));
    assert_eq!(protocol.parse_response(0, b"STORED\r\n").unwrap(), (8, Some((0, 1))));
    assert_eq!(protocol.parse_response(1, b"END\r\n").unwrap(), (5, Some((1, 1))));
    protocol.reset();
    assert_eq!(protocol.parse_response(1, b"END\r\n").unwrap(), (5, Some((1, 0))));
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
    assert_eq!(protocol.parse_response(0, &hit).unwrap(), (36, Some((0, 0))));
    assert_eq!(protocol.parse_response(1, &stored).unwrap(), (24, Some((1, 0))));
    assert_eq!(protocol.parse_response(0, &stored).unwrap(), (24, Some((0, 1))));
    assert_eq!(protocol.parse_response(1, &hit).unwrap(), (36, Some((1, 1))));
    protocol.reset();
    assert_eq!(protocol.parse_response(1, &stored).unwrap(), (24, Some((1, 0))));
}
