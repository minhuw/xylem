//! Exact RESP request bytes and request ids for the Redis encoder: every
//! `RedisOp` variant, command templates (including templates for commands
//! without a variant), random data, key prefixes and the insert phase, and
//! framing of the replies those commands receive.

use xylem_protocols::workload::KeyGeneration;
use xylem_protocols::{
    CommandTemplate, FixedCommandSelector, KeyGenerator, Parsed, Protocol, RedisOp, RedisProtocol,
    Request, WeightedCommandSelector,
};

/// Seed for the random-data tests.
const SEED: u64 = 42;

/// Compares whole requests and prints both payloads as escaped ASCII on failure.
fn assert_request(got: Request<(usize, u64)>, want: Request<(usize, u64)>) {
    assert_eq!(
        got,
        want,
        "\n got bytes: {}\nwant bytes: {}",
        got.data.escape_ascii(),
        want.data.escape_ascii()
    );
}

/// Compares whole byte strings and prints both as escaped ASCII on failure.
fn assert_bytes(got: &[u8], want: &[u8]) {
    assert_eq!(got, want, "\n got: {}\nwant: {}", got.escape_ascii(), want.escape_ascii());
}

/// A protocol that always sends `op`, draws keys 7, 8, 9, ... and uses 3-byte
/// values under `prefix`, with random data off.
fn protocol_with_prefix(op: RedisOp, prefix: &str) -> RedisProtocol {
    RedisProtocol::with_workload_and_options(
        Box::new(FixedCommandSelector::new(op)),
        KeyGeneration::sequential(7),
        3,
        prefix.to_string(),
        false,
        None,
    )
}

/// A protocol that always sends `op`, draws keys 7, 8, 9, ... and uses 8-byte
/// random values from `SEED` under the "key:" prefix.
fn random_data_protocol(op: RedisOp) -> RedisProtocol {
    RedisProtocol::with_workload_and_options(
        Box::new(FixedCommandSelector::new(op)),
        KeyGeneration::sequential(7),
        8,
        "key:".to_string(),
        true,
        Some(SEED),
    )
}

/// Sends `op` on connections 0, 1 and 0, which draw keys 7, 8 and 9 under the
/// "key:" prefix with 3-byte values, and compares each whole request: bytes,
/// request id, and measurement tag.
fn assert_op_requests(op: RedisOp, expected: [&[u8]; 3]) {
    let mut protocol = protocol_with_prefix(op, "key:");
    let ids = [(0, 0), (1, 0), (0, 1)];
    for (want, id) in expected.into_iter().zip(ids) {
        let got = protocol.next_request(id.0);
        assert_request(got, Request::measurement(want.to_vec(), id));
    }
}

/// Bytes of the first request `protocol` sends on connection 0.
fn first_request_bytes(mut protocol: RedisProtocol) -> Vec<u8> {
    let request = protocol.next_request(0);
    assert_eq!(request.request_id, (0, 0));
    request.data
}

/// Bytes of the first request for `op` with key 7, the "key:" prefix and
/// 3-byte values.
fn first_op_bytes(op: RedisOp) -> Vec<u8> {
    first_request_bytes(protocol_with_prefix(op, "key:"))
}

/// Bytes of the first request for `op` with key 7 and `prefix`.
fn first_prefixed_bytes(op: RedisOp, prefix: &str) -> Vec<u8> {
    first_request_bytes(protocol_with_prefix(op, prefix))
}

/// Bytes of the first request for a custom command `template` with key 7, the
/// "key:" prefix and 3-byte values.
fn first_template_bytes(template: &str) -> Vec<u8> {
    first_op_bytes(RedisOp::Custom(CommandTemplate::parse(template).unwrap()))
}

/// A custom command from `source`.
fn template(source: &str) -> RedisOp {
    RedisOp::Custom(CommandTemplate::parse(source).unwrap())
}

/// Maps every `RedisOp` variant to the test that pins its request bytes. The
/// match has no wildcard arm, so adding a variant fails to compile until it is
/// mapped to a test here. It is only compiled, never called.
#[allow(dead_code)]
fn bytes_test_for(op: &RedisOp) -> fn() {
    match op {
        RedisOp::Get => get_request_bytes,
        RedisOp::Set => set_request_bytes,
        RedisOp::Incr => incr_request_bytes,
        RedisOp::MGet { .. } => mget_request_bytes,
        RedisOp::Wait { .. } => wait_request_bytes,
        RedisOp::Auth { .. } => auth_request_bytes,
        RedisOp::SelectDb { .. } => select_db_request_bytes,
        RedisOp::Hello { .. } => hello_request_bytes,
        RedisOp::SetEx { .. } => setex_request_bytes,
        RedisOp::SetRange { .. } => setrange_request_bytes,
        RedisOp::GetRange { .. } => getrange_request_bytes,
        RedisOp::Scan { .. } => scan_request_bytes,
        RedisOp::Multi => multi_request_bytes,
        RedisOp::Exec => exec_request_bytes,
        RedisOp::Discard => discard_request_bytes,
        RedisOp::Custom(_) => custom_request_bytes,
    }
}

// One test per RedisOp variant

#[test]
fn get_request_bytes() {
    assert_op_requests(
        RedisOp::Get,
        [
            b"*2\r\n$3\r\nGET\r\n$5\r\nkey:7\r\n",
            b"*2\r\n$3\r\nGET\r\n$5\r\nkey:8\r\n",
            b"*2\r\n$3\r\nGET\r\n$5\r\nkey:9\r\n",
        ],
    );
}

#[test]
fn set_request_bytes() {
    assert_op_requests(
        RedisOp::Set,
        [
            b"*3\r\n$3\r\nSET\r\n$5\r\nkey:7\r\n$3\r\nxxx\r\n",
            b"*3\r\n$3\r\nSET\r\n$5\r\nkey:8\r\n$3\r\nxxx\r\n",
            b"*3\r\n$3\r\nSET\r\n$5\r\nkey:9\r\n$3\r\nxxx\r\n",
        ],
    );
}

#[test]
fn incr_request_bytes() {
    assert_op_requests(
        RedisOp::Incr,
        [
            b"*2\r\n$4\r\nINCR\r\n$5\r\nkey:7\r\n",
            b"*2\r\n$4\r\nINCR\r\n$5\r\nkey:8\r\n",
            b"*2\r\n$4\r\nINCR\r\n$5\r\nkey:9\r\n",
        ],
    );
}

#[test]
fn mget_request_bytes() {
    // Each request reads `count` consecutive keys starting at the drawn key.
    assert_op_requests(
        RedisOp::MGet { count: 3 },
        [
            b"*4\r\n$4\r\nMGET\r\n$5\r\nkey:7\r\n$5\r\nkey:8\r\n$5\r\nkey:9\r\n",
            b"*4\r\n$4\r\nMGET\r\n$5\r\nkey:8\r\n$5\r\nkey:9\r\n$6\r\nkey:10\r\n",
            b"*4\r\n$4\r\nMGET\r\n$5\r\nkey:9\r\n$6\r\nkey:10\r\n$6\r\nkey:11\r\n",
        ],
    );
    assert_bytes(
        &first_op_bytes(RedisOp::MGet { count: 1 }),
        b"*2\r\n$4\r\nMGET\r\n$5\r\nkey:7\r\n",
    );
    assert_bytes(&first_op_bytes(RedisOp::MGet { count: 0 }), b"*1\r\n$4\r\nMGET\r\n");
}

#[test]
fn mget_keys_saturate_at_u64_max() {
    let mut protocol = protocol_with_prefix(RedisOp::MGet { count: 3 }, "key:");
    let request = protocol.generate_request_with_key(0, u64::MAX - 1, 3);
    assert_request(
        request,
        Request::measurement(
            b"*4\r\n$4\r\nMGET\r\n\
              $24\r\nkey:18446744073709551614\r\n\
              $24\r\nkey:18446744073709551615\r\n\
              $24\r\nkey:18446744073709551615\r\n"
                .to_vec(),
            (0, 0),
        ),
    );
}

#[test]
fn wait_request_bytes() {
    assert_op_requests(
        RedisOp::Wait { num_replicas: 1, timeout_ms: 500 },
        [
            b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$3\r\n500\r\n",
            b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$3\r\n500\r\n",
            b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$3\r\n500\r\n",
        ],
    );
    assert_bytes(
        &first_op_bytes(RedisOp::Wait { num_replicas: 0, timeout_ms: 0 }),
        b"*3\r\n$4\r\nWAIT\r\n$1\r\n0\r\n$1\r\n0\r\n",
    );
    assert_bytes(
        &first_op_bytes(RedisOp::Wait { num_replicas: 12, timeout_ms: u64::MAX }),
        b"*3\r\n$4\r\nWAIT\r\n$2\r\n12\r\n$20\r\n18446744073709551615\r\n",
    );
}

#[test]
fn auth_request_bytes() {
    assert_op_requests(
        RedisOp::Auth {
            username: None,
            password: "pw-123".to_string(),
        },
        [
            b"*2\r\n$4\r\nAUTH\r\n$6\r\npw-123\r\n",
            b"*2\r\n$4\r\nAUTH\r\n$6\r\npw-123\r\n",
            b"*2\r\n$4\r\nAUTH\r\n$6\r\npw-123\r\n",
        ],
    );
    assert_bytes(
        &first_op_bytes(RedisOp::Auth {
            username: Some("alice".to_string()),
            password: "pw-123".to_string(),
        }),
        b"*3\r\n$4\r\nAUTH\r\n$5\r\nalice\r\n$6\r\npw-123\r\n",
    );
    assert_bytes(
        &first_op_bytes(RedisOp::Auth { username: None, password: String::new() }),
        b"*2\r\n$4\r\nAUTH\r\n$0\r\n\r\n",
    );
}

#[test]
fn select_db_request_bytes() {
    assert_op_requests(
        RedisOp::SelectDb { db: 0 },
        [
            b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n",
            b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n",
            b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n",
        ],
    );
    assert_bytes(
        &first_op_bytes(RedisOp::SelectDb { db: 15 }),
        b"*2\r\n$6\r\nSELECT\r\n$2\r\n15\r\n",
    );
}

#[test]
fn hello_request_bytes() {
    assert_op_requests(
        RedisOp::Hello { version: 3 },
        [
            b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n",
            b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n",
            b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n",
        ],
    );
    assert_bytes(
        &first_op_bytes(RedisOp::Hello { version: 2 }),
        b"*2\r\n$5\r\nHELLO\r\n$1\r\n2\r\n",
    );
}

#[test]
fn setex_request_bytes() {
    assert_op_requests(
        RedisOp::SetEx { ttl_seconds: 60 },
        [
            b"*4\r\n$5\r\nSETEX\r\n$5\r\nkey:7\r\n$2\r\n60\r\n$3\r\nxxx\r\n",
            b"*4\r\n$5\r\nSETEX\r\n$5\r\nkey:8\r\n$2\r\n60\r\n$3\r\nxxx\r\n",
            b"*4\r\n$5\r\nSETEX\r\n$5\r\nkey:9\r\n$2\r\n60\r\n$3\r\nxxx\r\n",
        ],
    );
}

#[test]
fn setrange_request_bytes() {
    assert_op_requests(
        RedisOp::SetRange { offset: 12 },
        [
            b"*4\r\n$8\r\nSETRANGE\r\n$5\r\nkey:7\r\n$2\r\n12\r\n$3\r\nxxx\r\n",
            b"*4\r\n$8\r\nSETRANGE\r\n$5\r\nkey:8\r\n$2\r\n12\r\n$3\r\nxxx\r\n",
            b"*4\r\n$8\r\nSETRANGE\r\n$5\r\nkey:9\r\n$2\r\n12\r\n$3\r\nxxx\r\n",
        ],
    );
    assert_bytes(
        &first_op_bytes(RedisOp::SetRange { offset: 0 }),
        b"*4\r\n$8\r\nSETRANGE\r\n$5\r\nkey:7\r\n$1\r\n0\r\n$3\r\nxxx\r\n",
    );
}

#[test]
fn getrange_request_bytes() {
    assert_op_requests(
        RedisOp::GetRange { offset: 0, end: -1 },
        [
            b"*4\r\n$8\r\nGETRANGE\r\n$5\r\nkey:7\r\n$1\r\n0\r\n$2\r\n-1\r\n",
            b"*4\r\n$8\r\nGETRANGE\r\n$5\r\nkey:8\r\n$1\r\n0\r\n$2\r\n-1\r\n",
            b"*4\r\n$8\r\nGETRANGE\r\n$5\r\nkey:9\r\n$1\r\n0\r\n$2\r\n-1\r\n",
        ],
    );
    assert_bytes(
        &first_op_bytes(RedisOp::GetRange { offset: 100, end: 199 }),
        b"*4\r\n$8\r\nGETRANGE\r\n$5\r\nkey:7\r\n$3\r\n100\r\n$3\r\n199\r\n",
    );
}

#[test]
fn scan_request_bytes() {
    assert_op_requests(
        RedisOp::Scan { cursor: 0, count: None, pattern: None },
        [
            b"*2\r\n$4\r\nSCAN\r\n$1\r\n0\r\n",
            b"*2\r\n$4\r\nSCAN\r\n$1\r\n0\r\n",
            b"*2\r\n$4\r\nSCAN\r\n$1\r\n0\r\n",
        ],
    );
    assert_bytes(
        &first_op_bytes(RedisOp::Scan {
            cursor: 0,
            count: Some(100),
            pattern: None,
        }),
        b"*4\r\n$4\r\nSCAN\r\n$1\r\n0\r\n$5\r\nCOUNT\r\n$3\r\n100\r\n",
    );
    assert_bytes(
        &first_op_bytes(RedisOp::Scan {
            cursor: 17,
            count: None,
            pattern: Some("key:*".to_string()),
        }),
        b"*4\r\n$4\r\nSCAN\r\n$2\r\n17\r\n$5\r\nMATCH\r\n$5\r\nkey:*\r\n",
    );
    // COUNT comes before MATCH.
    assert_bytes(
        &first_op_bytes(RedisOp::Scan {
            cursor: 42,
            count: Some(10),
            pattern: Some("user:*".to_string()),
        }),
        b"*6\r\n$4\r\nSCAN\r\n$2\r\n42\r\n$5\r\nCOUNT\r\n$2\r\n10\r\n$5\r\nMATCH\r\n$6\r\nuser:*\r\n",
    );
}

#[test]
fn multi_request_bytes() {
    assert_op_requests(
        RedisOp::Multi,
        [b"*1\r\n$5\r\nMULTI\r\n", b"*1\r\n$5\r\nMULTI\r\n", b"*1\r\n$5\r\nMULTI\r\n"],
    );
}

#[test]
fn exec_request_bytes() {
    assert_op_requests(
        RedisOp::Exec,
        [b"*1\r\n$4\r\nEXEC\r\n", b"*1\r\n$4\r\nEXEC\r\n", b"*1\r\n$4\r\nEXEC\r\n"],
    );
}

#[test]
fn discard_request_bytes() {
    assert_op_requests(
        RedisOp::Discard,
        [
            b"*1\r\n$7\r\nDISCARD\r\n",
            b"*1\r\n$7\r\nDISCARD\r\n",
            b"*1\r\n$7\r\nDISCARD\r\n",
        ],
    );
}

#[test]
fn custom_request_bytes() {
    assert_op_requests(
        RedisOp::Custom(CommandTemplate::parse("HSET myhash __key__ __data__").unwrap()),
        [
            b"*4\r\n$4\r\nHSET\r\n$6\r\nmyhash\r\n$5\r\nkey:7\r\n$3\r\nxxx\r\n",
            b"*4\r\n$4\r\nHSET\r\n$6\r\nmyhash\r\n$5\r\nkey:8\r\n$3\r\nxxx\r\n",
            b"*4\r\n$4\r\nHSET\r\n$6\r\nmyhash\r\n$5\r\nkey:9\r\n$3\r\nxxx\r\n",
        ],
    );
}

// Templates for commands without a RedisOp variant

#[test]
fn del_template_bytes() {
    assert_op_requests(
        template("DEL __key__"),
        [
            b"*2\r\n$3\r\nDEL\r\n$5\r\nkey:7\r\n",
            b"*2\r\n$3\r\nDEL\r\n$5\r\nkey:8\r\n",
            b"*2\r\n$3\r\nDEL\r\n$5\r\nkey:9\r\n",
        ],
    );
}

#[test]
fn mset_template_bytes() {
    assert_op_requests(
        template("MSET __key__ __data__ other __data__"),
        [
            b"*5\r\n$4\r\nMSET\r\n$5\r\nkey:7\r\n$3\r\nxxx\r\n$5\r\nother\r\n$3\r\nxxx\r\n",
            b"*5\r\n$4\r\nMSET\r\n$5\r\nkey:8\r\n$3\r\nxxx\r\n$5\r\nother\r\n$3\r\nxxx\r\n",
            b"*5\r\n$4\r\nMSET\r\n$5\r\nkey:9\r\n$3\r\nxxx\r\n$5\r\nother\r\n$3\r\nxxx\r\n",
        ],
    );
}

#[test]
fn cluster_slots_template_bytes() {
    assert_op_requests(
        template("CLUSTER SLOTS"),
        [
            b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n",
            b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n",
            b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n",
        ],
    );
}

#[test]
fn list_template_bytes() {
    assert_op_requests(
        template("LPUSH queue __data__"),
        [
            b"*3\r\n$5\r\nLPUSH\r\n$5\r\nqueue\r\n$3\r\nxxx\r\n",
            b"*3\r\n$5\r\nLPUSH\r\n$5\r\nqueue\r\n$3\r\nxxx\r\n",
            b"*3\r\n$5\r\nLPUSH\r\n$5\r\nqueue\r\n$3\r\nxxx\r\n",
        ],
    );
    let cases: [(&str, &[u8]); 6] = [
        ("LPUSH __key__ __data__", b"*3\r\n$5\r\nLPUSH\r\n$5\r\nkey:7\r\n$3\r\nxxx\r\n"),
        ("RPUSH queue __data__", b"*3\r\n$5\r\nRPUSH\r\n$5\r\nqueue\r\n$3\r\nxxx\r\n"),
        ("LPOP queue", b"*2\r\n$4\r\nLPOP\r\n$5\r\nqueue\r\n"),
        ("RPOP queue", b"*2\r\n$4\r\nRPOP\r\n$5\r\nqueue\r\n"),
        (
            "LRANGE queue -10 25",
            b"*4\r\n$6\r\nLRANGE\r\n$5\r\nqueue\r\n$3\r\n-10\r\n$2\r\n25\r\n",
        ),
        ("LLEN queue", b"*2\r\n$4\r\nLLEN\r\n$5\r\nqueue\r\n"),
    ];
    for (source, want) in cases {
        assert_bytes(&first_template_bytes(source), want);
    }
}

#[test]
fn set_template_bytes() {
    assert_op_requests(
        template("SADD tags __key__"),
        [
            b"*3\r\n$4\r\nSADD\r\n$4\r\ntags\r\n$5\r\nkey:7\r\n",
            b"*3\r\n$4\r\nSADD\r\n$4\r\ntags\r\n$5\r\nkey:8\r\n",
            b"*3\r\n$4\r\nSADD\r\n$4\r\ntags\r\n$5\r\nkey:9\r\n",
        ],
    );
    let cases: [(&str, &[u8]); 5] = [
        ("SREM tags __key__", b"*3\r\n$4\r\nSREM\r\n$4\r\ntags\r\n$5\r\nkey:7\r\n"),
        ("SMEMBERS tags", b"*2\r\n$8\r\nSMEMBERS\r\n$4\r\ntags\r\n"),
        (
            "SISMEMBER tags __key__",
            b"*3\r\n$9\r\nSISMEMBER\r\n$4\r\ntags\r\n$5\r\nkey:7\r\n",
        ),
        ("SPOP tags 2", b"*3\r\n$4\r\nSPOP\r\n$4\r\ntags\r\n$1\r\n2\r\n"),
        ("SCARD tags", b"*2\r\n$5\r\nSCARD\r\n$4\r\ntags\r\n"),
    ];
    for (source, want) in cases {
        assert_bytes(&first_template_bytes(source), want);
    }
}

#[test]
fn sorted_set_template_bytes() {
    assert_op_requests(
        template("ZADD leaderboard 1.5 __key__"),
        [
            b"*4\r\n$4\r\nZADD\r\n$11\r\nleaderboard\r\n$3\r\n1.5\r\n$5\r\nkey:7\r\n",
            b"*4\r\n$4\r\nZADD\r\n$11\r\nleaderboard\r\n$3\r\n1.5\r\n$5\r\nkey:8\r\n",
            b"*4\r\n$4\r\nZADD\r\n$11\r\nleaderboard\r\n$3\r\n1.5\r\n$5\r\nkey:9\r\n",
        ],
    );
    let cases: [(&str, &[u8]); 6] = [
        // The value size can serve as the score.
        (
            "ZADD leaderboard __value_size__ __key__",
            b"*4\r\n$4\r\nZADD\r\n$11\r\nleaderboard\r\n$1\r\n3\r\n$5\r\nkey:7\r\n",
        ),
        (
            "ZREM leaderboard __key__",
            b"*3\r\n$4\r\nZREM\r\n$11\r\nleaderboard\r\n$5\r\nkey:7\r\n",
        ),
        (
            "ZRANGE leaderboard 0 9 WITHSCORES",
            b"*5\r\n$6\r\nZRANGE\r\n$11\r\nleaderboard\r\n$1\r\n0\r\n$1\r\n9\r\n$10\r\nWITHSCORES\r\n",
        ),
        (
            "ZRANGEBYSCORE leaderboard -inf +inf",
            b"*4\r\n$13\r\nZRANGEBYSCORE\r\n$11\r\nleaderboard\r\n$4\r\n-inf\r\n$4\r\n+inf\r\n",
        ),
        (
            "ZSCORE leaderboard __key__",
            b"*3\r\n$6\r\nZSCORE\r\n$11\r\nleaderboard\r\n$5\r\nkey:7\r\n",
        ),
        ("ZCARD leaderboard", b"*2\r\n$5\r\nZCARD\r\n$11\r\nleaderboard\r\n"),
    ];
    for (source, want) in cases {
        assert_bytes(&first_template_bytes(source), want);
    }
}

#[test]
fn hash_template_bytes() {
    assert_op_requests(
        template("HSET __key__ f __data__"),
        [
            b"*4\r\n$4\r\nHSET\r\n$5\r\nkey:7\r\n$1\r\nf\r\n$3\r\nxxx\r\n",
            b"*4\r\n$4\r\nHSET\r\n$5\r\nkey:8\r\n$1\r\nf\r\n$3\r\nxxx\r\n",
            b"*4\r\n$4\r\nHSET\r\n$5\r\nkey:9\r\n$1\r\nf\r\n$3\r\nxxx\r\n",
        ],
    );
    let cases: [(&str, &[u8]); 6] = [
        // The key can also be a field of one shared hash.
        (
            "HSET users __key__ __data__",
            b"*4\r\n$4\r\nHSET\r\n$5\r\nusers\r\n$5\r\nkey:7\r\n$3\r\nxxx\r\n",
        ),
        (
            "HSET __key__ a __data__ bb __data__",
            b"*6\r\n$4\r\nHSET\r\n$5\r\nkey:7\r\n$1\r\na\r\n$3\r\nxxx\r\n$2\r\nbb\r\n$3\r\nxxx\r\n",
        ),
        ("HGET __key__ f", b"*3\r\n$4\r\nHGET\r\n$5\r\nkey:7\r\n$1\r\nf\r\n"),
        (
            "HMGET __key__ a bb",
            b"*4\r\n$5\r\nHMGET\r\n$5\r\nkey:7\r\n$1\r\na\r\n$2\r\nbb\r\n",
        ),
        ("HGETALL __key__", b"*2\r\n$7\r\nHGETALL\r\n$5\r\nkey:7\r\n"),
        ("HDEL __key__ f", b"*3\r\n$4\r\nHDEL\r\n$5\r\nkey:7\r\n$1\r\nf\r\n"),
    ];
    for (source, want) in cases {
        assert_bytes(&first_template_bytes(source), want);
    }
}

// Replies to template commands

#[test]
fn template_replies_frame_through_the_generic_parser() {
    let replies: [(&str, &[u8]); 10] = [
        ("integer (DEL, LPUSH, SADD, ZADD, HSET)", b":1\r\n"),
        ("bulk string (HGET, LPOP)", b"$3\r\nxxx\r\n"),
        ("array (LRANGE, SMEMBERS, HGETALL)", b"*2\r\n$1\r\na\r\n$2\r\nbb\r\n"),
        (
            "nested array (CLUSTER SLOTS)",
            b"*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n$9\r\n127.0.0.1\r\n:6379\r\n$2\r\nid\r\n",
        ),
        (
            "RESP3 nested pairs (ZRANGE WITHSCORES)",
            b"*2\r\n*2\r\n$1\r\na\r\n,1.5\r\n*2\r\n$2\r\nbb\r\n,2\r\n",
        ),
        ("empty array (SMEMBERS of a missing key)", b"*0\r\n"),
        ("nil bulk string (HGET of a missing field)", b"$-1\r\n"),
        ("nil array (LPOP with count on a missing key)", b"*-1\r\n"),
        ("RESP3 null (ZSCORE of a missing member)", b"_\r\n"),
        ("RESP3 map (HGETALL)", b"%1\r\n$1\r\nf\r\n$3\r\nxxx\r\n"),
    ];
    let mut protocol = protocol_with_prefix(template("DEL __key__"), "key:");
    // Pipelined replies are consumed one frame at a time, in order.
    let stream: Vec<u8> = replies.iter().flat_map(|(_, frame)| frame.iter().copied()).collect();
    let mut offset = 0;
    for (seq, (what, frame)) in replies.iter().enumerate() {
        // A truncated frame is not a reply yet.
        let partial = &stream[offset..offset + frame.len() - 1];
        assert_eq!(
            protocol.parse_response(0, partial).unwrap(),
            Parsed::Incomplete,
            "partial {what}"
        );
        let got = protocol.parse_response(0, &stream[offset..]).unwrap();
        assert_eq!(got, Parsed::ok(frame.len(), (0, seq as u64)), "{what}");
        offset += frame.len();
    }
    assert_eq!(offset, stream.len());

    // An error reply, such as WRONGTYPE from mixing types on one key, is an error.
    let err = protocol
        .parse_response(
            0,
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
        )
        .unwrap_err();
    assert!(err.to_string().contains("WRONGTYPE"), "{err}");
}

// Value sizes and protocol defaults

#[test]
fn set_value_sizes_from_empty_to_large() {
    let mut protocol = protocol_with_prefix(RedisOp::Set, "key:");
    assert_request(
        protocol.generate_request_with_key(0, 7, 0),
        Request::measurement(b"*3\r\n$3\r\nSET\r\n$5\r\nkey:7\r\n$0\r\n\r\n".to_vec(), (0, 0)),
    );
    let large = [
        b"*3\r\n$3\r\nSET\r\n$5\r\nkey:7\r\n$1000\r\n".as_slice(),
        &[b'x'; 1000],
        b"\r\n",
    ]
    .concat();
    assert_request(
        protocol.generate_request_with_key(1, 7, 1000),
        Request::measurement(large, (1, 0)),
    );
}

#[test]
fn new_without_workload_uses_key_zero_and_64_byte_values() {
    let set =
        [b"*3\r\n$3\r\nSET\r\n$5\r\nkey:0\r\n$64\r\n".as_slice(), &[b'x'; 64], b"\r\n"].concat();
    let mut protocol = RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Set)));
    assert_request(protocol.next_request(0), Request::measurement(set.clone(), (0, 0)));
    assert_request(protocol.next_request(1), Request::measurement(set.clone(), (1, 0)));
    assert_request(protocol.next_request(0), Request::measurement(set, (0, 1)));
}

// Command templates

#[test]
fn template_literal_only() {
    assert_bytes(&first_template_bytes("PING"), b"*1\r\n$4\r\nPING\r\n");
    assert_bytes(&first_template_bytes("CLIENT LIST"), b"*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n");
}

#[test]
fn template_key_variable() {
    assert_bytes(&first_template_bytes("GET __key__"), b"*2\r\n$3\r\nGET\r\n$5\r\nkey:7\r\n");
    assert_bytes(&first_template_bytes("__key__"), b"*1\r\n$5\r\nkey:7\r\n");
}

#[test]
fn template_data_variable() {
    assert_bytes(
        &first_template_bytes("APPEND log __data__"),
        b"*3\r\n$6\r\nAPPEND\r\n$3\r\nlog\r\n$3\r\nxxx\r\n",
    );
}

#[test]
fn template_value_size_variable() {
    assert_bytes(
        &first_template_bytes("INCRBY counter __value_size__"),
        b"*3\r\n$6\r\nINCRBY\r\n$7\r\ncounter\r\n$1\r\n3\r\n",
    );
}

#[test]
fn template_mixed_variables() {
    assert_bytes(
        &first_template_bytes("SETEX __key__ __value_size__ __data__"),
        b"*4\r\n$5\r\nSETEX\r\n$5\r\nkey:7\r\n$1\r\n3\r\n$3\r\nxxx\r\n",
    );
    // A variable may appear more than once.
    assert_bytes(
        &first_template_bytes("HSET h __key__ __data__ __key__"),
        b"*5\r\n$4\r\nHSET\r\n$1\r\nh\r\n$5\r\nkey:7\r\n$3\r\nxxx\r\n$5\r\nkey:7\r\n",
    );
}

#[test]
fn template_extra_whitespace() {
    assert_bytes(
        &first_template_bytes("  SET\t __key__  \n __data__   "),
        b"*3\r\n$3\r\nSET\r\n$5\r\nkey:7\r\n$3\r\nxxx\r\n",
    );
    // A template of only whitespace parses and sends an empty array.
    assert_bytes(&first_template_bytes("   "), b"*0\r\n");
}

#[test]
fn template_variables_split_adjacent_text_into_arguments() {
    assert_bytes(
        &first_template_bytes("id:__key__:end"),
        b"*3\r\n$3\r\nid:\r\n$5\r\nkey:7\r\n$4\r\n:end\r\n",
    );
    assert_bytes(&first_template_bytes("__key____data__"), b"*2\r\n$5\r\nkey:7\r\n$3\r\nxxx\r\n");
    assert_bytes(&first_template_bytes("___key__"), b"*2\r\n$1\r\n_\r\n$5\r\nkey:7\r\n");
}

#[test]
fn template_unknown_underscore_words_stay_literal() {
    assert_bytes(
        &first_template_bytes("SET _x __unknown__ __key"),
        b"*4\r\n$3\r\nSET\r\n$2\r\n_x\r\n$11\r\n__unknown__\r\n$5\r\n__key\r\n",
    );
}

#[test]
fn template_lengths_count_bytes() {
    assert_bytes(
        &first_template_bytes("SET cl\u{e9} __key__"),
        b"*3\r\n$3\r\nSET\r\n$4\r\ncl\xc3\xa9\r\n$5\r\nkey:7\r\n",
    );
}

#[test]
fn template_generate_request_with_options_substitutes_given_values() {
    let template = CommandTemplate::parse("SET __key__ __data__ __value_size__").unwrap();
    assert_bytes(
        &template.generate_request_with_options(42, 5, "p:", "hello"),
        b"*4\r\n$3\r\nSET\r\n$4\r\np:42\r\n$5\r\nhello\r\n$1\r\n5\r\n",
    );
    // __value_size__ is the given size, not the length of the data.
    assert_bytes(
        &template.generate_request_with_options(1, 10, "", "ab"),
        b"*4\r\n$3\r\nSET\r\n$1\r\n1\r\n$2\r\nab\r\n$2\r\n10\r\n",
    );
}

// Random data

#[test]
fn random_data_off_ignores_the_seed() {
    let mut protocol = RedisProtocol::with_workload_and_options(
        Box::new(FixedCommandSelector::new(RedisOp::Set)),
        KeyGeneration::sequential(7),
        3,
        "key:".to_string(),
        false,
        Some(SEED),
    );
    assert_request(
        protocol.next_request(0),
        Request::measurement(b"*3\r\n$3\r\nSET\r\n$5\r\nkey:7\r\n$3\r\nxxx\r\n".to_vec(), (0, 0)),
    );
    assert_request(
        protocol.next_request(1),
        Request::measurement(b"*3\r\n$3\r\nSET\r\n$5\r\nkey:8\r\n$3\r\nxxx\r\n".to_vec(), (1, 0)),
    );
}

#[test]
fn random_data_on_with_fixed_seed_draws_one_value_per_request() {
    let mut protocol = random_data_protocol(RedisOp::Set);
    assert_request(
        protocol.next_request(0),
        Request::measurement(
            b"*3\r\n$3\r\nSET\r\n$5\r\nkey:7\r\n$8\r\nm>}bkX,Y\r\n".to_vec(),
            (0, 0),
        ),
    );
    assert_request(
        protocol.next_request(1),
        Request::measurement(
            b"*3\r\n$3\r\nSET\r\n$5\r\nkey:8\r\n$8\r\n4xUp`'FT\r\n".to_vec(),
            (1, 0),
        ),
    );
    assert_request(
        protocol.next_request(0),
        Request::measurement(
            b"*3\r\n$3\r\nSET\r\n$5\r\nkey:9\r\n$8\r\n5%WL0r^R\r\n".to_vec(),
            (0, 1),
        ),
    );
}

#[test]
fn random_data_on_fills_every_value_argument_from_the_first_draw() {
    let cases: Vec<(RedisOp, &[u8])> = vec![
        (RedisOp::Set, b"*3\r\n$3\r\nSET\r\n$5\r\nkey:7\r\n$8\r\nm>}bkX,Y\r\n"),
        (
            RedisOp::SetEx { ttl_seconds: 60 },
            b"*4\r\n$5\r\nSETEX\r\n$5\r\nkey:7\r\n$2\r\n60\r\n$8\r\nm>}bkX,Y\r\n",
        ),
        (
            RedisOp::SetRange { offset: 12 },
            b"*4\r\n$8\r\nSETRANGE\r\n$5\r\nkey:7\r\n$2\r\n12\r\n$8\r\nm>}bkX,Y\r\n",
        ),
        (
            RedisOp::Custom(CommandTemplate::parse("APPEND __key__ __data__").unwrap()),
            b"*3\r\n$6\r\nAPPEND\r\n$5\r\nkey:7\r\n$8\r\nm>}bkX,Y\r\n",
        ),
        (
            template("LPUSH queue __data__"),
            b"*3\r\n$5\r\nLPUSH\r\n$5\r\nqueue\r\n$8\r\nm>}bkX,Y\r\n",
        ),
        (
            template("HSET __key__ f1 __data__"),
            b"*4\r\n$4\r\nHSET\r\n$5\r\nkey:7\r\n$2\r\nf1\r\n$8\r\nm>}bkX,Y\r\n",
        ),
        // One value is drawn and repeated for every __data__.
        (
            template("MSET __key__ __data__ other __data__"),
            b"*5\r\n$4\r\nMSET\r\n$5\r\nkey:7\r\n$8\r\nm>}bkX,Y\r\n$5\r\nother\r\n$8\r\nm>}bkX,Y\r\n",
        ),
        (
            template("HSET __key__ a __data__ bb __data__"),
            b"*6\r\n$4\r\nHSET\r\n$5\r\nkey:7\r\n\
              $1\r\na\r\n$8\r\nm>}bkX,Y\r\n$2\r\nbb\r\n$8\r\nm>}bkX,Y\r\n",
        ),
    ];
    for (op, want) in cases {
        let name = op.name();
        let got = first_request_bytes(random_data_protocol(op));
        assert_eq!(got, want, "{name}: got {}", got.escape_ascii());
    }
}

#[test]
fn random_data_on_value_free_ops_do_not_draw() {
    let value_free_ops = vec![
        RedisOp::Get,
        RedisOp::Incr,
        RedisOp::MGet { count: 2 },
        RedisOp::Wait { num_replicas: 1, timeout_ms: 500 },
        RedisOp::Auth {
            username: None,
            password: "pw-123".to_string(),
        },
        RedisOp::SelectDb { db: 0 },
        RedisOp::Hello { version: 3 },
        RedisOp::GetRange { offset: 0, end: -1 },
        RedisOp::Scan { cursor: 0, count: None, pattern: None },
        RedisOp::Multi,
        RedisOp::Exec,
        RedisOp::Discard,
    ];
    // A SET after a value-free operation still gets the first value of the stream.
    let set: &[u8] = b"*3\r\n$3\r\nSET\r\n$5\r\nkey:7\r\n$8\r\nm>}bkX,Y\r\n";
    for op in value_free_ops {
        let mut protocol = random_data_protocol(RedisOp::Get);
        protocol.generate_request_for_op(0, 7, 8, &op);
        let got = protocol.generate_request_for_op(0, 7, 8, &RedisOp::Set).data;
        assert_eq!(got, set, "SET after {}: got {}", op.name(), got.escape_ascii());
    }
}

#[test]
fn random_data_on_custom_templates_draw_even_without_data_variable() {
    let mut protocol = random_data_protocol(RedisOp::Get);
    let ping = RedisOp::Custom(CommandTemplate::parse("PING").unwrap());
    assert_request(
        protocol.generate_request_for_op(0, 7, 8, &ping),
        Request::measurement(b"*1\r\n$4\r\nPING\r\n".to_vec(), (0, 0)),
    );
    assert_request(
        protocol.generate_request_for_op(0, 7, 8, &RedisOp::Set),
        Request::measurement(
            b"*3\r\n$3\r\nSET\r\n$5\r\nkey:7\r\n$8\r\n4xUp`'FT\r\n".to_vec(),
            (0, 1),
        ),
    );
}

// Key prefixes

#[test]
fn key_prefix_applies_to_every_key_argument() {
    let cases: Vec<(RedisOp, &[u8])> = vec![
        (RedisOp::Get, b"*2\r\n$3\r\nGET\r\n$6\r\nuser:7\r\n"),
        (
            RedisOp::MGet { count: 2 },
            b"*3\r\n$4\r\nMGET\r\n$6\r\nuser:7\r\n$6\r\nuser:8\r\n",
        ),
        (
            RedisOp::SetEx { ttl_seconds: 60 },
            b"*4\r\n$5\r\nSETEX\r\n$6\r\nuser:7\r\n$2\r\n60\r\n$3\r\nxxx\r\n",
        ),
        (
            RedisOp::Custom(CommandTemplate::parse("GET __key__").unwrap()),
            b"*2\r\n$3\r\nGET\r\n$6\r\nuser:7\r\n",
        ),
        // Every __key__ is prefixed; literal collection names and fields are not.
        (
            template("MSET __key__ __data__ other __data__"),
            b"*5\r\n$4\r\nMSET\r\n$6\r\nuser:7\r\n$3\r\nxxx\r\n$5\r\nother\r\n$3\r\nxxx\r\n",
        ),
        (
            template("SADD tags __key__"),
            b"*3\r\n$4\r\nSADD\r\n$4\r\ntags\r\n$6\r\nuser:7\r\n",
        ),
        (
            template("HSET __key__ f1 __data__"),
            b"*4\r\n$4\r\nHSET\r\n$6\r\nuser:7\r\n$2\r\nf1\r\n$3\r\nxxx\r\n",
        ),
    ];
    for (op, want) in cases {
        let name = op.name();
        let got = first_prefixed_bytes(op, "user:");
        assert_eq!(got, want, "{name}: got {}", got.escape_ascii());
    }
}

#[test]
fn empty_key_prefix_leaves_bare_keys() {
    let cases: Vec<(RedisOp, &[u8])> = vec![
        (RedisOp::Get, b"*2\r\n$3\r\nGET\r\n$1\r\n7\r\n"),
        (
            RedisOp::Custom(CommandTemplate::parse("GET __key__").unwrap()),
            b"*2\r\n$3\r\nGET\r\n$1\r\n7\r\n",
        ),
        (
            template("LPUSH __key__ __data__"),
            b"*3\r\n$5\r\nLPUSH\r\n$1\r\n7\r\n$3\r\nxxx\r\n",
        ),
        (template("HGETALL __key__"), b"*2\r\n$7\r\nHGETALL\r\n$1\r\n7\r\n"),
    ];
    for (op, want) in cases {
        let name = op.name();
        let got = first_prefixed_bytes(op, "");
        assert_eq!(got, want, "{name}: got {}", got.escape_ascii());
    }
}

#[test]
fn multibyte_key_prefix_lengths_count_bytes() {
    let get = [b"*2\r\n$3\r\nGET\r\n$10\r\n".as_slice(), "ключ:7".as_bytes(), b"\r\n"].concat();
    assert_bytes(&first_prefixed_bytes(RedisOp::Get, "ключ:"), &get);
    let lpush = [
        b"*3\r\n$5\r\nLPUSH\r\n$10\r\n".as_slice(),
        "ключ:7".as_bytes(),
        b"\r\n$3\r\nxxx\r\n",
    ]
    .concat();
    assert_bytes(&first_prefixed_bytes(template("LPUSH __key__ __data__"), "ключ:"), &lpush);
}

// Other request paths

#[test]
fn insert_phase_sends_set_warmups_then_replays_workload_keys() {
    let mut protocol = protocol_with_prefix(RedisOp::Get, "key:").with_insert_phase(2, 4);
    assert_request(
        protocol.next_request(0),
        Request::warmup(b"*3\r\n$3\r\nSET\r\n$5\r\nkey:7\r\n$4\r\nxxxx\r\n".to_vec(), (0, 0)),
    );
    assert_request(
        protocol.next_request(1),
        Request::warmup(b"*3\r\n$3\r\nSET\r\n$5\r\nkey:8\r\n$4\r\nxxxx\r\n".to_vec(), (1, 0)),
    );
    assert_request(
        protocol.next_request(0),
        Request::measurement(b"*2\r\n$3\r\nGET\r\n$5\r\nkey:7\r\n".to_vec(), (0, 1)),
    );
    assert_request(
        protocol.next_request(1),
        Request::measurement(b"*2\r\n$3\r\nGET\r\n$5\r\nkey:8\r\n".to_vec(), (1, 1)),
    );
}

#[test]
fn per_command_key_generators_supply_the_key() {
    let key_generators: Vec<Box<dyn KeyGenerator>> = vec![Box::new(KeyGeneration::sequential(100))];
    let selector = WeightedCommandSelector::with_per_command_keys(
        vec![(RedisOp::Set, 1.0)],
        key_generators,
        Some(SEED),
    )
    .unwrap();
    let mut protocol = RedisProtocol::with_workload_and_options(
        Box::new(selector),
        KeyGeneration::sequential(7),
        3,
        "key:".to_string(),
        false,
        None,
    );
    assert_request(
        protocol.next_request(0),
        Request::measurement(b"*3\r\n$3\r\nSET\r\n$7\r\nkey:100\r\n$3\r\nxxx\r\n".to_vec(), (0, 0)),
    );
    assert_request(
        protocol.next_request(1),
        Request::measurement(b"*3\r\n$3\r\nSET\r\n$7\r\nkey:101\r\n$3\r\nxxx\r\n".to_vec(), (1, 0)),
    );
}
