//! Exact requests from protocols constructed from config with fixed seeds.
//!
//! Each test drives two connections in an uneven interleaving and asserts the
//! whole request bytes, request IDs, and warmup tags, so per-connection
//! sequences, the shared key and value generators, and the insert phase are
//! all pinned.
use std::collections::{HashMap, HashSet};
use xylem_cli::multi_protocol::MultiProtocol;
use xylem_protocols::configs::*;
use xylem_protocols::{
    MasstreeProtocol, MemcachedAsciiProtocol, MemcachedBinaryProtocol, Parsed, Protocol,
    RedisProtocol, Request,
};

const SEED: Option<u64> = Some(42);

/// Connection order used by the multi-request tests.
const CONNECTIONS: [usize; 10] = [0, 1, 0, 1, 1, 0, 0, 1, 0, 1];

/// Masstree handshake reply `[0, 15, true, 0, ""]` in MessagePack.
const HANDSHAKE_REPLY: &[u8] = b"\x95\x00\x0f\xc3\x00\xa0";

/// Masstree handshake request `[0, 14, {"core": -1, "maxkeylen": 255}]`.
const HANDSHAKE: &[u8] = b"\x93\xce\x00\x00\x00\x00\xcc\x0e\x82\xa4core\xff\xa9maxkeylen\xcc\xff";

fn requests<P: Protocol>(protocol: &mut P, connections: &[usize]) -> Vec<Request<P::RequestId>> {
    let mut requests = Vec::new();
    for &conn_id in connections {
        requests.push(protocol.next_request(conn_id));
    }
    requests
}

/// Weighted GET/SET/MGET over seeded random keys with a custom prefix, uniform
/// value sizes, random value bytes, and an insert phase with its own value size.
fn redis_weighted_config() -> RedisConfig {
    RedisConfig {
        keys: KeysConfig::Random {
            max: 1000,
            value_size: 6,
            prefix: "app:".into(),
        },
        operations: Some(RedisOperationsConfig::Weighted {
            commands: vec![
                RedisCommandWeight {
                    name: "GET".into(),
                    weight: 0.4,
                    params: None,
                    keys: None,
                },
                RedisCommandWeight {
                    name: "set".into(),
                    weight: 0.4,
                    params: None,
                    keys: None,
                },
                RedisCommandWeight {
                    name: "MGET".into(),
                    weight: 0.2,
                    params: Some(RedisCommandParams::MGet { count: 2 }),
                    keys: None,
                },
            ],
        }),
        value_size: Some(ValueSizeConfig::Uniform { min: 2, max: 9 }),
        random_data: true,
        insert_phase: Some(InsertPhaseConfig { key_count: Some(2), value_size: Some(4) }),
        ..Default::default()
    }
}

/// Per-command key distributions (INCR falls back to the top-level keys),
/// per-command value sizes, and an insert phase that takes its count and size
/// from the round-robin keys.
fn redis_per_command_keys_config() -> RedisConfig {
    RedisConfig {
        keys: KeysConfig::RoundRobin {
            max: 3,
            value_size: 5,
            prefix: "pc:".into(),
        },
        operations: Some(RedisOperationsConfig::Weighted {
            commands: vec![
                RedisCommandWeight {
                    name: "GET".into(),
                    weight: 0.5,
                    params: None,
                    keys: Some(KeysConfig::Zipfian {
                        n: 100,
                        theta: 0.99,
                        value_size: 5,
                        prefix: "pc:".into(),
                    }),
                },
                RedisCommandWeight {
                    name: "SET".into(),
                    weight: 0.3,
                    params: None,
                    keys: Some(KeysConfig::Random {
                        max: 50,
                        value_size: 5,
                        prefix: "pc:".into(),
                    }),
                },
                RedisCommandWeight {
                    name: "INCR".into(),
                    weight: 0.2,
                    params: None,
                    keys: None,
                },
            ],
        }),
        value_size: Some(ValueSizeConfig::PerCommand {
            commands: HashMap::from([(
                "set".to_string(),
                CommandValueSizeConfig::Uniform { min: 3, max: 8 },
            )]),
            default: Box::new(ValueSizeConfig::Fixed { size: 5 }),
        }),
        random_data: false,
        insert_phase: Some(InsertPhaseConfig { key_count: None, value_size: None }),
        ..Default::default()
    }
}

/// A fixed SETEX over sequential keys with normal value sizes and random bytes.
fn redis_fixed_config() -> RedisConfig {
    RedisConfig {
        keys: KeysConfig::Sequential {
            start: 10,
            value_size: 4,
            prefix: "seq:".into(),
        },
        operations: Some(RedisOperationsConfig::Fixed { operation: "SETEX".into() }),
        value_size: Some(ValueSizeConfig::Normal { mean: 6.0, std_dev: 2.0, min: 3, max: 9 }),
        random_data: true,
        insert_phase: Some(InsertPhaseConfig { key_count: Some(1), value_size: None }),
        ..Default::default()
    }
}

/// Set one field of the first weighted command of `config`.
fn with_first_command(
    mut config: RedisConfig,
    update: impl FnOnce(&mut RedisCommandWeight),
) -> RedisConfig {
    let Some(RedisOperationsConfig::Weighted { commands }) = &mut config.operations else {
        panic!("expected weighted commands");
    };
    update(&mut commands[0]);
    config
}

/// Configs that both Redis constructors reject, with the error each one reports.
fn redis_config_errors() -> Vec<(RedisConfig, &'static str)> {
    vec![
        (
            with_first_command(redis_weighted_config(), |command| {
                command.keys = Some(KeysConfig::Random {
                    max: 10,
                    value_size: 6,
                    prefix: "other:".into(),
                });
            }),
            "Per-command key prefixes are unsupported",
        ),
        (
            with_first_command(redis_weighted_config(), |command| {
                command.keys = Some(KeysConfig::Random {
                    max: 10,
                    value_size: 7,
                    prefix: "app:".into(),
                });
            }),
            "Use value_size.per_command for per-command value sizes",
        ),
        (
            with_first_command(redis_weighted_config(), |command| {
                command.params = Some(RedisCommandParams::MGet { count: 2 });
            }),
            "Parameters do not match Redis command GET",
        ),
        (
            RedisConfig {
                operations: Some(RedisOperationsConfig::Fixed { operation: "DEL".into() }),
                ..Default::default()
            },
            "Unknown Redis command: DEL. Supported: GET, SET, INCR, MGET, WAIT, SETEX, \
             SETRANGE, GETRANGE, SCAN, MULTI, EXEC, DISCARD, CUSTOM",
        ),
        (
            RedisConfig {
                insert_phase: Some(InsertPhaseConfig { key_count: None, value_size: None }),
                ..Default::default()
            },
            "insert_phase.key_count is required for sequential keys",
        ),
        (
            RedisConfig {
                keys: KeysConfig::Random {
                    max: 0,
                    value_size: 6,
                    prefix: "key:".into(),
                },
                ..Default::default()
            },
            "Random max must be > 0",
        ),
        (
            RedisConfig {
                value_size: Some(ValueSizeConfig::Uniform { min: 9, max: 2 }),
                ..Default::default()
            },
            "UniformSize min (9) must be <= max (2)",
        ),
    ]
}

#[test]
fn redis_standalone_weighted_commands_with_value_sizes_and_insert_phase() {
    let mut protocol = RedisProtocol::from_config(&redis_weighted_config(), SEED).unwrap();
    assert_eq!(
        requests(&mut protocol, &CONNECTIONS),
        vec![
            Request::warmup(b"*3\r\n$3\r\nSET\r\n$7\r\napp:728\r\n$4\r\nm>}b\r\n".to_vec(), (0, 0)),
            Request::warmup(b"*3\r\n$3\r\nSET\r\n$7\r\napp:394\r\n$4\r\nkX,Y\r\n".to_vec(), (1, 0)),
            Request::measurement(
                b"*3\r\n$4\r\nMGET\r\n$7\r\napp:728\r\n$7\r\napp:729\r\n".to_vec(),
                (0, 1),
            ),
            Request::measurement(b"*2\r\n$3\r\nGET\r\n$7\r\napp:394\r\n".to_vec(), (1, 1)),
            Request::measurement(
                b"*3\r\n$4\r\nMGET\r\n$7\r\napp:704\r\n$7\r\napp:705\r\n".to_vec(),
                (1, 2),
            ),
            Request::measurement(
                b"*3\r\n$3\r\nSET\r\n$7\r\napp:393\r\n$8\r\n4xUp`'FT\r\n".to_vec(),
                (0, 2),
            ),
            Request::measurement(
                b"*3\r\n$3\r\nSET\r\n$7\r\napp:777\r\n$3\r\n5%W\r\n".to_vec(),
                (0, 3),
            ),
            Request::measurement(
                b"*3\r\n$3\r\nSET\r\n$7\r\napp:465\r\n$8\r\nL0r^Rm.I\r\n".to_vec(),
                (1, 3),
            ),
            Request::measurement(b"*2\r\n$3\r\nGET\r\n$6\r\napp:63\r\n".to_vec(), (0, 4)),
            Request::measurement(
                b"*3\r\n$3\r\nSET\r\n$7\r\napp:977\r\n$9\r\nzRr(\\d;}d\r\n".to_vec(),
                (1, 4),
            ),
        ]
    );
}

#[test]
fn redis_standalone_per_command_keys_and_value_sizes() {
    let mut protocol =
        RedisProtocol::from_config(&redis_per_command_keys_config(), Some(7)).unwrap();
    assert_eq!(
        requests(&mut protocol, &CONNECTIONS),
        vec![
            Request::warmup(b"*3\r\n$3\r\nSET\r\n$4\r\npc:0\r\n$5\r\nxxxxx\r\n".to_vec(), (0, 0)),
            Request::warmup(b"*3\r\n$3\r\nSET\r\n$4\r\npc:1\r\n$5\r\nxxxxx\r\n".to_vec(), (1, 0)),
            Request::warmup(b"*3\r\n$3\r\nSET\r\n$4\r\npc:2\r\n$5\r\nxxxxx\r\n".to_vec(), (0, 1)),
            Request::measurement(b"*2\r\n$3\r\nGET\r\n$4\r\npc:2\r\n".to_vec(), (1, 1)),
            Request::measurement(b"*2\r\n$3\r\nGET\r\n$5\r\npc:49\r\n".to_vec(), (1, 2)),
            Request::measurement(
                b"*3\r\n$3\r\nSET\r\n$4\r\npc:5\r\n$4\r\nxxxx\r\n".to_vec(),
                (0, 2)
            ),
            Request::measurement(b"*2\r\n$3\r\nGET\r\n$5\r\npc:13\r\n".to_vec(), (0, 3)),
            Request::measurement(b"*2\r\n$4\r\nINCR\r\n$4\r\npc:0\r\n".to_vec(), (1, 3)),
            Request::measurement(b"*2\r\n$3\r\nGET\r\n$4\r\npc:0\r\n".to_vec(), (0, 4)),
            Request::measurement(
                b"*3\r\n$3\r\nSET\r\n$5\r\npc:36\r\n$7\r\nxxxxxxx\r\n".to_vec(),
                (1, 4),
            ),
        ]
    );
}

#[test]
fn redis_standalone_fixed_command_with_sequential_keys_and_normal_value_sizes() {
    let mut protocol = RedisProtocol::from_config(&redis_fixed_config(), Some(3)).unwrap();
    assert_eq!(
        requests(&mut protocol, &CONNECTIONS),
        vec![
            Request::warmup(b"*3\r\n$3\r\nSET\r\n$6\r\nseq:10\r\n$4\r\n%]rp\r\n".to_vec(), (0, 0)),
            Request::measurement(
                b"*4\r\n$5\r\nSETEX\r\n$6\r\nseq:10\r\n$4\r\n3600\r\n$4\r\n[+1[\r\n".to_vec(),
                (1, 0),
            ),
            Request::measurement(
                b"*4\r\n$5\r\nSETEX\r\n$6\r\nseq:11\r\n$4\r\n3600\r\n$4\r\nU#W3\r\n".to_vec(),
                (0, 1),
            ),
            Request::measurement(
                b"*4\r\n$5\r\nSETEX\r\n$6\r\nseq:12\r\n$4\r\n3600\r\n$4\r\nBL&O\r\n".to_vec(),
                (1, 1),
            ),
            Request::measurement(
                b"*4\r\n$5\r\nSETEX\r\n$6\r\nseq:13\r\n$4\r\n3600\r\n$6\r\n3hgXA9\r\n".to_vec(),
                (1, 2),
            ),
            Request::measurement(
                b"*4\r\n$5\r\nSETEX\r\n$6\r\nseq:14\r\n$4\r\n3600\r\n$4\r\n-'oX\r\n".to_vec(),
                (0, 2),
            ),
            Request::measurement(
                b"*4\r\n$5\r\nSETEX\r\n$6\r\nseq:15\r\n$4\r\n3600\r\n$4\r\n{a5;\r\n".to_vec(),
                (0, 3),
            ),
            Request::measurement(
                b"*4\r\n$5\r\nSETEX\r\n$6\r\nseq:16\r\n$4\r\n3600\r\n$7\r\nzFe=m'9\r\n".to_vec(),
                (1, 3),
            ),
            Request::measurement(
                b"*4\r\n$5\r\nSETEX\r\n$6\r\nseq:17\r\n$4\r\n3600\r\n$7\r\n5;uQN!O\r\n".to_vec(),
                (0, 4),
            ),
            Request::measurement(
                b"*4\r\n$5\r\nSETEX\r\n$6\r\nseq:18\r\n$4\r\n3600\r\n$3\r\nVtg\r\n".to_vec(),
                (1, 4),
            ),
        ]
    );
}

#[test]
fn redis_standalone_rejects_unsupported_configs() {
    for (config, message) in redis_config_errors() {
        let error = RedisProtocol::from_config(&config, SEED).err().expect(message);
        assert_eq!(error.to_string(), message);
    }
}

/// SET over seeded random keys with a custom prefix and an insert phase with
/// its own count and value size.
fn memcached_set_config() -> MemcachedConfig {
    MemcachedConfig {
        keys: KeysConfig::Random {
            max: 100,
            value_size: 5,
            prefix: "mc:".into(),
        },
        operation: Some("set".into()),
        operations: None,
        value_size: None,
        insert_phase: Some(InsertPhaseConfig { key_count: Some(2), value_size: Some(3) }),
    }
}

/// GET over zipfian keys; the insert phase takes its count and size from the keys.
fn memcached_get_config() -> MemcachedConfig {
    MemcachedConfig {
        keys: KeysConfig::Zipfian {
            n: 3,
            theta: 0.99,
            value_size: 4,
            prefix: "z:".into(),
        },
        operation: Some("GET".into()),
        operations: None,
        value_size: None,
        insert_phase: Some(InsertPhaseConfig { key_count: None, value_size: None }),
    }
}

/// A sequential GET workload whose keys carry `prefix`.
fn memcached_prefix_config(prefix: String) -> MemcachedConfig {
    MemcachedConfig {
        keys: KeysConfig::Sequential { start: 0, value_size: 4, prefix },
        ..Default::default()
    }
}

/// Configs that both memcached constructors reject, with the error each one reports.
fn memcached_config_errors() -> Vec<(MemcachedConfig, &'static str)> {
    let prefix_error = "Memcached key prefix must contain no whitespace/control bytes and leave \
                        room for a u64 key within 250 bytes";
    vec![
        (
            MemcachedConfig {
                operation: Some("CAS".into()),
                ..Default::default()
            },
            "Unknown Memcached operation: CAS. Supported: GET, SET, ADD, REPLACE, DELETE, INCR, \
             DECR, TOUCH, MGET",
        ),
        (memcached_prefix_config("bad key:".into()), prefix_error),
        (memcached_prefix_config("bad\x01key:".into()), prefix_error),
        (memcached_prefix_config("p".repeat(231)), prefix_error),
        (
            MemcachedConfig {
                insert_phase: Some(InsertPhaseConfig { key_count: None, value_size: None }),
                ..Default::default()
            },
            "insert_phase.key_count is required for sequential keys",
        ),
        (
            MemcachedConfig {
                keys: KeysConfig::Sequential {
                    start: 0,
                    value_size: 0,
                    prefix: "k:".into(),
                },
                ..Default::default()
            },
            "value_size must be > 0",
        ),
    ]
}

#[test]
fn memcached_ascii_set_with_random_keys_prefix_and_insert_phase() {
    let mut protocol = MemcachedAsciiProtocol::from_config(&memcached_set_config(), SEED).unwrap();
    assert_eq!(
        requests(&mut protocol, &CONNECTIONS),
        vec![
            Request::warmup(b"set mc:72 0 0 3\r\nxxx\r\n".to_vec(), (0, 0)),
            Request::warmup(b"set mc:39 0 0 3\r\nxxx\r\n".to_vec(), (1, 0)),
            Request::measurement(b"set mc:72 0 0 5\r\nxxxxx\r\n".to_vec(), (0, 1)),
            Request::measurement(b"set mc:39 0 0 5\r\nxxxxx\r\n".to_vec(), (1, 1)),
            Request::measurement(b"set mc:70 0 0 5\r\nxxxxx\r\n".to_vec(), (1, 2)),
            Request::measurement(b"set mc:39 0 0 5\r\nxxxxx\r\n".to_vec(), (0, 2)),
            Request::measurement(b"set mc:77 0 0 5\r\nxxxxx\r\n".to_vec(), (0, 3)),
            Request::measurement(b"set mc:46 0 0 5\r\nxxxxx\r\n".to_vec(), (1, 3)),
            Request::measurement(b"set mc:6 0 0 5\r\nxxxxx\r\n".to_vec(), (0, 4)),
            Request::measurement(b"set mc:97 0 0 5\r\nxxxxx\r\n".to_vec(), (1, 4)),
        ]
    );
}

#[test]
fn memcached_ascii_get_with_zipfian_keys_and_default_insert_phase() {
    let mut protocol = MemcachedAsciiProtocol::from_config(&memcached_get_config(), SEED).unwrap();
    assert_eq!(
        requests(&mut protocol, &CONNECTIONS),
        vec![
            Request::warmup(b"set z:0 0 0 4\r\nxxxx\r\n".to_vec(), (0, 0)),
            Request::warmup(b"set z:0 0 0 4\r\nxxxx\r\n".to_vec(), (1, 0)),
            Request::warmup(b"set z:2 0 0 4\r\nxxxx\r\n".to_vec(), (0, 1)),
            Request::measurement(b"get z:0\r\n".to_vec(), (1, 1)),
            Request::measurement(b"get z:0\r\n".to_vec(), (1, 2)),
            Request::measurement(b"get z:2\r\n".to_vec(), (0, 2)),
            Request::measurement(b"get z:0\r\n".to_vec(), (0, 3)),
            Request::measurement(b"get z:0\r\n".to_vec(), (1, 3)),
            Request::measurement(b"get z:1\r\n".to_vec(), (0, 4)),
            Request::measurement(b"get z:0\r\n".to_vec(), (1, 4)),
        ]
    );
}

#[test]
fn memcached_ascii_accepts_the_longest_prefix() {
    let prefix = "p".repeat(230);
    let config = memcached_prefix_config(prefix.clone());
    let mut protocol = MemcachedAsciiProtocol::from_config(&config, SEED).unwrap();
    assert_eq!(
        requests(&mut protocol, &[1]),
        vec![Request::measurement(format!("get {prefix}0\r\n").into_bytes(), (1, 0))]
    );
}

#[test]
fn memcached_ascii_rejects_unsupported_configs() {
    for (config, message) in memcached_config_errors() {
        let error = MemcachedAsciiProtocol::from_config(&config, SEED).err().expect(message);
        assert_eq!(error.to_string(), message);
    }
}

#[test]
fn memcached_binary_set_with_random_keys_prefix_and_insert_phase() {
    let mut protocol = MemcachedBinaryProtocol::from_config(&memcached_set_config(), SEED).unwrap();
    assert_eq!(
        requests(&mut protocol, &CONNECTIONS),
        vec![
            Request::warmup(
                b"\x80\x01\x00\x05\x08\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00mc:72xxx".to_vec(),
                (0, 0),
            ),
            Request::warmup(
                b"\x80\x01\x00\x05\x08\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00mc:39xxx".to_vec(),
                (1, 0),
            ),
            Request::measurement(
                b"\x80\x01\x00\x05\x08\x00\x00\x00\x00\x00\x00\x12\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00mc:72xxxxx".to_vec(),
                (0, 1),
            ),
            Request::measurement(
                b"\x80\x01\x00\x05\x08\x00\x00\x00\x00\x00\x00\x12\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00mc:39xxxxx".to_vec(),
                (1, 1),
            ),
            Request::measurement(
                b"\x80\x01\x00\x05\x08\x00\x00\x00\x00\x00\x00\x12\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00mc:70xxxxx".to_vec(),
                (1, 2),
            ),
            Request::measurement(
                b"\x80\x01\x00\x05\x08\x00\x00\x00\x00\x00\x00\x12\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00mc:39xxxxx".to_vec(),
                (0, 2),
            ),
            Request::measurement(
                b"\x80\x01\x00\x05\x08\x00\x00\x00\x00\x00\x00\x12\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00mc:77xxxxx".to_vec(),
                (0, 3),
            ),
            Request::measurement(
                b"\x80\x01\x00\x05\x08\x00\x00\x00\x00\x00\x00\x12\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00mc:46xxxxx".to_vec(),
                (1, 3),
            ),
            Request::measurement(
                b"\x80\x01\x00\x04\x08\x00\x00\x00\x00\x00\x00\x11\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00mc:6xxxxx".to_vec(),
                (0, 4),
            ),
            Request::measurement(
                b"\x80\x01\x00\x05\x08\x00\x00\x00\x00\x00\x00\x12\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00mc:97xxxxx".to_vec(),
                (1, 4),
            ),
        ]
    );
}

#[test]
fn memcached_binary_get_with_zipfian_keys_and_default_insert_phase() {
    let mut protocol = MemcachedBinaryProtocol::from_config(&memcached_get_config(), SEED).unwrap();
    assert_eq!(
        requests(&mut protocol, &CONNECTIONS),
        vec![
            Request::warmup(
                b"\x80\x01\x00\x03\x08\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00z:0xxxx".to_vec(),
                (0, 0),
            ),
            Request::warmup(
                b"\x80\x01\x00\x03\x08\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00z:0xxxx".to_vec(),
                (1, 0),
            ),
            Request::warmup(
                b"\x80\x01\x00\x03\x08\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00z:2xxxx".to_vec(),
                (0, 1),
            ),
            Request::measurement(
                b"\x80\x0c\x00\x03\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00z:0".to_vec(),
                (1, 1),
            ),
            Request::measurement(
                b"\x80\x0c\x00\x03\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00z:0".to_vec(),
                (1, 2),
            ),
            Request::measurement(
                b"\x80\x0c\x00\x03\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00z:2".to_vec(),
                (0, 2),
            ),
            Request::measurement(
                b"\x80\x0c\x00\x03\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00z:0".to_vec(),
                (0, 3),
            ),
            Request::measurement(
                b"\x80\x0c\x00\x03\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00z:0".to_vec(),
                (1, 3),
            ),
            Request::measurement(
                b"\x80\x0c\x00\x03\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00z:1".to_vec(),
                (0, 4),
            ),
            Request::measurement(
                b"\x80\x0c\x00\x03\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00z:0".to_vec(),
                (1, 4),
            ),
        ]
    );
}

#[test]
fn memcached_binary_accepts_the_longest_prefix() {
    let prefix = "p".repeat(230);
    let config = memcached_prefix_config(prefix.clone());
    let mut protocol = MemcachedBinaryProtocol::from_config(&config, SEED).unwrap();
    // GETK header: key length and body length are both 231 (0xe7).
    let header: &[u8] = b"\x80\x0c\x00\xe7\x00\x00\x00\x00\x00\x00\x00\xe7\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
    let expected = [header, prefix.as_bytes(), b"0"].concat();
    assert_eq!(requests(&mut protocol, &[1]), vec![Request::measurement(expected, (1, 0))]);
}

#[test]
fn memcached_binary_rejects_unsupported_configs() {
    for (config, message) in memcached_config_errors() {
        let error = MemcachedBinaryProtocol::from_config(&config, SEED).err().expect(message);
        assert_eq!(error.to_string(), message);
    }
}

/// Build Masstree as main.rs does: the shared constructor wrapped for the CLI.
fn masstree_from_cli_path(config: &MasstreeConfig, seed: Option<u64>) -> MultiProtocol {
    MultiProtocol::Masstree(MasstreeProtocol::from_config(config, seed).unwrap())
}

/// Generate on `connections`, replying to each connection's first request (its
/// handshake) so that later requests carry the workload.
fn masstree_requests(
    protocol: &mut MultiProtocol,
    connections: &[usize],
) -> Vec<Request<(usize, u64)>> {
    let mut handshaken = HashSet::new();
    let mut requests = Vec::new();
    for &conn_id in connections {
        requests.push(protocol.next_request(conn_id));
        if handshaken.insert(conn_id) {
            let parsed = protocol.parse_response(conn_id, HANDSHAKE_REPLY).unwrap();
            assert_eq!(parsed, Parsed::ok(HANDSHAKE_REPLY.len(), (conn_id, 0)));
        }
    }
    requests
}

/// SET over seeded random keys with a custom prefix, a uniform value size
/// drawn per request, random value bytes, and an insert phase taking its
/// value size from the keys.
fn masstree_set_config() -> MasstreeConfig {
    MasstreeConfig {
        keys: KeysConfig::Random {
            max: 100,
            value_size: 9,
            prefix: "mt:".into(),
        },
        operation: Some("set".into()),
        value_size: Some(ValueSizeConfig::Uniform { min: 3, max: 8 }),
        random_data: true,
        insert_phase: Some(InsertPhaseConfig { key_count: Some(2), value_size: None }),
        ..Default::default()
    }
}

#[test]
fn masstree_set_with_value_size_random_data_and_insert_phase() {
    let mut protocol = masstree_from_cli_path(&masstree_set_config(), SEED);
    assert_eq!(
        masstree_requests(&mut protocol, &CONNECTIONS),
        vec![
            Request::setup(HANDSHAKE.to_vec(), (0, 0)),
            Request::setup(HANDSHAKE.to_vec(), (1, 0)),
            Request::warmup(
                b"\x94\xce\x00\x00\x00\x00\xcc\x08\xa4mt:0\xa9m>}bkX,Y4".to_vec(),
                (0, 0)
            ),
            Request::warmup(
                b"\x94\xce\x00\x00\x00\x00\xcc\x08\xa4mt:1\xa9xUp`'FT5%".to_vec(),
                (1, 0)
            ),
            Request::measurement(
                b"\x94\xce\x00\x00\x00\x01\xcc\x08\xa5mt:72\xa5WL0r^".to_vec(),
                (1, 1)
            ),
            Request::measurement(
                b"\x94\xce\x00\x00\x00\x01\xcc\x08\xa5mt:39\xa5Rm.Iz".to_vec(),
                (0, 1)
            ),
            Request::measurement(
                b"\x94\xce\x00\x00\x00\x02\xcc\x08\xa5mt:70\xa3Rr(".to_vec(),
                (0, 2)
            ),
            Request::measurement(
                b"\x94\xce\x00\x00\x00\x02\xcc\x08\xa5mt:39\xa7\\d;}dkG".to_vec(),
                (1, 2)
            ),
            Request::measurement(
                b"\x94\xce\x00\x00\x00\x03\xcc\x08\xa5mt:77\xa4Slq0".to_vec(),
                (0, 3)
            ),
            Request::measurement(
                b"\x94\xce\x00\x00\x00\x03\xcc\x08\xa5mt:46\xa7LX%G]Wh".to_vec(),
                (1, 3)
            ),
        ]
    );
}

/// Each remaining operation, with PUT columns of the value size and SCAN
/// settings that the constructor turns into the operation; a scan starts at
/// the generated key.
#[test]
fn masstree_operations_from_config() {
    let cases: [(&str, &[u8], &[u8]); 5] = [
        ("get", b"\x93\xce\x00\x00\x00\x00\xcc\x02\xa4op:0", b"\x93\xce\x00\x00\x00\x00\xcc\x02\xa4op:1"),
        (
            "PUT",
            b"\x97\xce\x00\x00\x00\x00\xcc\x06\xa4op:0\xce\x00\x00\x00\x00\xa3xxx\xce\x00\x00\x00\x01\xa3xxx",
            b"\x97\xce\x00\x00\x00\x00\xcc\x06\xa4op:1\xce\x00\x00\x00\x00\xa3xxx\xce\x00\x00\x00\x01\xa3xxx",
        ),
        ("remove", b"\x93\xce\x00\x00\x00\x00\xcc\n\xa4op:0", b"\x93\xce\x00\x00\x00\x00\xcc\n\xa4op:1"),
        (
            "scan",
            b"\x96\xce\x00\x00\x00\x00\xcc\x04\xa4op:0\xce\x00\x00\x00\x04\xce\x00\x00\x00\x01\xce\x00\x00\x00\x03",
            b"\x96\xce\x00\x00\x00\x00\xcc\x04\xa4op:1\xce\x00\x00\x00\x04\xce\x00\x00\x00\x01\xce\x00\x00\x00\x03",
        ),
        ("checkpoint", b"\x92\xce\x00\x00\x00\x00\xcc\x0c", b"\x92\xce\x00\x00\x00\x00\xcc\x0c"),
    ];
    for (operation, first, second) in cases {
        let config = MasstreeConfig {
            keys: KeysConfig::RoundRobin {
                max: 4,
                value_size: 3,
                prefix: "op:".into(),
            },
            operation: Some(operation.into()),
            put_columns: Some(2),
            scan: Some(MasstreeScanConfig {
                count: 4,
                fields: vec!["1".into(), "3".into()],
            }),
            ..Default::default()
        };
        let mut protocol = masstree_from_cli_path(&config, SEED);
        assert_eq!(
            masstree_requests(&mut protocol, &[0, 0, 1, 1]),
            vec![
                Request::setup(HANDSHAKE.to_vec(), (0, 0)),
                Request::measurement(first.to_vec(), (0, 0)),
                Request::setup(HANDSHAKE.to_vec(), (1, 0)),
                Request::measurement(second.to_vec(), (1, 0)),
            ],
            "{operation}"
        );
    }
}

#[test]
fn masstree_rejects_insert_phase_without_key_count_for_sequential_keys() {
    let config = MasstreeConfig {
        insert_phase: Some(InsertPhaseConfig { key_count: None, value_size: None }),
        ..Default::default()
    };
    let error = MasstreeProtocol::from_config(&config, SEED)
        .err()
        .expect("sequential keys have no keyspace size");
    assert_eq!(error.to_string(), "insert_phase.key_count is required for sequential keys");
}
