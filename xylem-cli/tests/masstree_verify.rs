//! End-to-end TCP loopback integration tests for Masstree opt-in deterministic
//! value verification.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

// Independent implementation of the documented FNV-1a + splitmix64 pattern
// formula (mirrors book/src/guide/protocols/masstree.md).
const FNV1A_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h = FNV1A_OFFSET_BASIS;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV1A_PRIME);
    }
    h
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn synthetic_pattern(seed: u64, key: &[u8], value_size: usize, col_idx: u32) -> Vec<u8> {
    let base = splitmix64(
        seed ^ fnv1a_64(key)
            ^ (value_size as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
            ^ u64::from(col_idx).wrapping_mul(0xbf58_476d_1ce4_e5b9),
    );
    let mut out = vec![0u8; value_size];
    let mut off = 0usize;
    let mut w = 0u64;
    while off < value_size {
        let word = splitmix64(base.wrapping_add(w.wrapping_mul(0x9e37_79b9_7f4a_7c15)));
        let bytes = word.to_le_bytes();
        let n = (value_size - off).min(8);
        out[off..off + n].copy_from_slice(&bytes[..n]);
        off += n;
        w += 1;
    }
    out
}

// Minimal MessagePack encoder/decoder for the in-process Masstree test server.

#[derive(Debug)]
enum MpValue {
    Int(i64),
    Bytes(Vec<u8>),
    Array(Vec<MpValue>),
    Map,
}

fn decode_mp(data: &[u8], pos: &mut usize) -> Option<MpValue> {
    let &b = data.get(*pos)?;
    *pos += 1;
    match b {
        0x00..=0x7f => Some(MpValue::Int(i64::from(b))),
        0xe0..=0xff => Some(MpValue::Int(i64::from(b as i8))),
        0xcc => {
            let v = *data.get(*pos)?;
            *pos += 1;
            Some(MpValue::Int(i64::from(v)))
        }
        0xcd => {
            let slice = data.get(*pos..*pos + 2)?;
            *pos += 2;
            Some(MpValue::Int(i64::from(u16::from_be_bytes([slice[0], slice[1]]))))
        }
        0xce => {
            let slice = data.get(*pos..*pos + 4)?;
            *pos += 4;
            Some(MpValue::Int(i64::from(u32::from_be_bytes([
                slice[0], slice[1], slice[2], slice[3],
            ]))))
        }
        0xcf => {
            let slice = data.get(*pos..*pos + 8)?;
            *pos += 8;
            let v = u64::from_be_bytes(slice.try_into().unwrap());
            Some(MpValue::Int(v as i64))
        }
        0xd0 => {
            let v = *data.get(*pos)? as i8;
            *pos += 1;
            Some(MpValue::Int(i64::from(v)))
        }
        0xd1 => {
            let slice = data.get(*pos..*pos + 2)?;
            *pos += 2;
            Some(MpValue::Int(i64::from(i16::from_be_bytes([slice[0], slice[1]]))))
        }
        0xd2 => {
            let slice = data.get(*pos..*pos + 4)?;
            *pos += 4;
            Some(MpValue::Int(i64::from(i32::from_be_bytes([
                slice[0], slice[1], slice[2], slice[3],
            ]))))
        }
        0xd3 => {
            let slice = data.get(*pos..*pos + 8)?;
            *pos += 8;
            Some(MpValue::Int(i64::from_be_bytes(slice.try_into().unwrap())))
        }
        0xa0..=0xbf => {
            let len = usize::from(b & 0x1f);
            let slice = data.get(*pos..*pos + len)?;
            *pos += len;
            Some(MpValue::Bytes(slice.to_vec()))
        }
        0xd9 | 0xc4 => {
            let len = usize::from(*data.get(*pos)?);
            *pos += 1;
            let slice = data.get(*pos..*pos + len)?;
            *pos += len;
            Some(MpValue::Bytes(slice.to_vec()))
        }
        0xda | 0xc5 => {
            let slice = data.get(*pos..*pos + 2)?;
            *pos += 2;
            let len = usize::from(u16::from_be_bytes([slice[0], slice[1]]));
            let payload = data.get(*pos..*pos + len)?;
            *pos += len;
            Some(MpValue::Bytes(payload.to_vec()))
        }
        0xdb | 0xc6 => {
            let slice = data.get(*pos..*pos + 4)?;
            *pos += 4;
            let len = u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]) as usize;
            let payload = data.get(*pos..*pos + len)?;
            *pos += len;
            Some(MpValue::Bytes(payload.to_vec()))
        }
        0x90..=0x9f => {
            let len = usize::from(b & 0x0f);
            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                items.push(decode_mp(data, pos)?);
            }
            Some(MpValue::Array(items))
        }
        0xdc => {
            let slice = data.get(*pos..*pos + 2)?;
            *pos += 2;
            let len = usize::from(u16::from_be_bytes([slice[0], slice[1]]));
            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                items.push(decode_mp(data, pos)?);
            }
            Some(MpValue::Array(items))
        }
        0x80..=0x8f => {
            let len = usize::from(b & 0x0f);
            for _ in 0..len {
                decode_mp(data, pos)?;
                decode_mp(data, pos)?;
            }
            Some(MpValue::Map)
        }
        other => panic!("unexpected msgpack marker 0x{other:02x}"),
    }
}

fn write_mp_array_len(out: &mut Vec<u8>, len: u8) {
    assert!(len < 16);
    out.push(0x90 | len);
}

fn write_mp_uint(out: &mut Vec<u8>, v: u64) {
    if v < 128 {
        out.push(v as u8);
    } else if v <= u64::from(u8::MAX) {
        out.push(0xcc);
        out.push(v as u8);
    } else if v <= u64::from(u16::MAX) {
        out.push(0xcd);
        out.extend_from_slice(&(v as u16).to_be_bytes());
    } else {
        out.push(0xce);
        out.extend_from_slice(&(v as u32).to_be_bytes());
    }
}

fn write_mp_str(out: &mut Vec<u8>, bytes: &[u8]) {
    if bytes.len() < 32 {
        out.push(0xa0 | (bytes.len() as u8));
    } else if bytes.len() <= usize::from(u8::MAX) {
        out.push(0xd9);
        out.push(bytes.len() as u8);
    } else if bytes.len() <= usize::from(u16::MAX) {
        out.push(0xda);
        out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    } else {
        out.push(0xdb);
        out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

#[derive(Clone)]
enum Fault {
    CorruptByte {
        target_key: Vec<u8>,
        offset: usize,
        xor_mask: u8,
    },
    Truncate {
        target_key: Vec<u8>,
        len: usize,
    },
    Miss {
        target_key: Vec<u8>,
    },
}

#[derive(Clone)]
enum ServerMode {
    Store,
    Synthetic {
        seed: u64,
        value_size: usize,
        fault: Option<Fault>,
    },
}

fn handle_get(mode: &ServerMode, store: &HashMap<Vec<u8>, Vec<u8>>, key: &[u8]) -> Vec<u8> {
    match mode {
        ServerMode::Store => store.get(key).cloned().unwrap_or_else(|| key.to_vec()),
        ServerMode::Synthetic { seed, value_size, fault } => {
            let mut payload = synthetic_pattern(*seed, key, *value_size, 0);
            match fault {
                Some(Fault::CorruptByte { target_key, offset, xor_mask })
                    if key == target_key.as_slice() =>
                {
                    payload[*offset] ^= *xor_mask;
                }
                Some(Fault::Truncate { target_key, len }) if key == target_key.as_slice() => {
                    payload.truncate(*len);
                }
                Some(Fault::Miss { target_key }) if key == target_key.as_slice() => {
                    return key.to_vec();
                }
                _ => {}
            }
            payload
        }
    }
}

fn handle_masstree_request(
    items: &[MpValue],
    mode: &ServerMode,
    store: &mut HashMap<Vec<u8>, Vec<u8>>,
    out: &mut Vec<u8>,
) {
    let MpValue::Int(seq) = items[0] else {
        panic!("seq must be int");
    };
    let MpValue::Int(cmd) = items[1] else {
        panic!("cmd must be int");
    };
    let seq = seq as u64;
    match cmd {
        // HANDSHAKE -> [0, 15, true, 0, "Bag"]
        14 => {
            write_mp_array_len(out, 5);
            write_mp_uint(out, 0);
            write_mp_uint(out, 15);
            out.push(0xc3); // true
            write_mp_uint(out, 0);
            write_mp_str(out, b"Bag");
        }
        // REPLACE (SET): [seq, 8, key, value] -> [seq, 9, 1]
        8 => {
            let (MpValue::Bytes(key), MpValue::Bytes(value)) = (&items[2], &items[3]) else {
                panic!("REPLACE key/value must be bytes");
            };
            if matches!(mode, ServerMode::Store) {
                store.insert(key.clone(), value.clone());
            }
            write_mp_array_len(out, 3);
            write_mp_uint(out, seq);
            write_mp_uint(out, 9);
            write_mp_uint(out, 1); // Inserted
        }
        // PUT: [seq, 6, key, col, val, ...] -> [seq, 7, 2]
        6 => {
            let MpValue::Bytes(key) = &items[2] else {
                panic!("PUT key must be bytes");
            };
            if matches!(mode, ServerMode::Store) {
                for [col, val] in items[3..].as_chunks::<2>().0 {
                    if let (MpValue::Int(0), MpValue::Bytes(val)) = (col, val) {
                        store.insert(key.clone(), val.clone());
                    }
                }
            }
            write_mp_array_len(out, 3);
            write_mp_uint(out, seq);
            write_mp_uint(out, 7);
            write_mp_uint(out, 2); // Updated
        }
        // GET: [seq, 2, key] -> [seq, 3, value] or [seq, 3, key] on miss
        2 => {
            let MpValue::Bytes(key) = &items[2] else {
                panic!("GET key must be bytes");
            };
            let reply_payload = handle_get(mode, store, key);
            write_mp_array_len(out, 3);
            write_mp_uint(out, seq);
            write_mp_uint(out, 3);
            write_mp_str(out, &reply_payload);
        }
        other => panic!("unexpected Masstree cmd {other}"),
    }
}

fn serve_masstree(listener: TcpListener, mode: ServerMode) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "CLI did not connect to Masstree stub");
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(e) => panic!("accept: {e}"),
            }
        };
        socket.set_nonblocking(false).unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

        let mut store: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut pending = Vec::new();
        let mut buf = [0u8; 16384];
        loop {
            let n = match socket.read(&mut buf) {
                Ok(0) => return,
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return,
                Err(e) => panic!("read: {e}"),
            };
            pending.extend_from_slice(&buf[..n]);
            let mut offset = 0usize;
            let mut out = Vec::new();
            while offset < pending.len() {
                let mut pos = offset;
                let Some(val) = decode_mp(&pending, &mut pos) else {
                    break;
                };
                offset = pos;
                let MpValue::Array(items) = val else {
                    panic!("request must be array");
                };
                handle_masstree_request(&items, &mode, &mut store, &mut out);
            }
            if offset > 0 {
                pending.drain(..offset);
            }
            if !out.is_empty() && socket.write_all(&out).is_err() {
                return;
            }
        }
    })
}

fn run_masstree_cli(
    protocol_config: serde_json::Value,
    mode: ServerMode,
    dir: &Path,
) -> (Output, serde_json::Value) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let target = listener.local_addr().unwrap().to_string();
    listener.set_nonblocking(true).unwrap();
    let server = serve_masstree(listener, mode);

    let profile = dir.join("profile.toml");
    let results_path = dir.join("results.json");
    let config = serde_json::json!({
        "experiment": { "name": "masstree-verify", "duration": "1s", "seed": 123 },
        "traffic_groups": [{
            "name": "mt",
            "threads": [0],
            "connections_per_thread": 1,
            "max_pending_per_connection": 4,
            "target": target,
            "protocol": "masstree",
            "traffic_policy": { "type": "closed-loop" },
            "protocol_config": protocol_config
        }],
        "output": { "format": "json", "file": results_path }
    });
    std::fs::write(&profile, toml::to_string(&config).unwrap()).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile)
        .env("RUST_LOG", "off")
        .output()
        .unwrap();
    server.join().expect("Masstree stub failed");

    let text = std::fs::read_to_string(&results_path).unwrap_or_else(|e| {
        panic!(
            "results JSON missing ({e}); status={:?}\nstdout={}\nstderr={}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    (output, json)
}

#[test]
fn insert_phase_and_mixed_ops_verify_against_real_store() {
    let dir = tempfile::tempdir().unwrap();
    let (output, json) = run_masstree_cli(
        serde_json::json!({
            "keys": { "strategy": "round-robin", "max": 16, "value_size": 128 },
            "insert_phase": {},
            "verify": true,
            "verify_seed": 42,
            "operations": {
                "strategy": "weighted",
                "commands": [
                    { "name": "get", "weight": 0.6 },
                    { "name": "set", "weight": 0.2 },
                    { "name": "put", "weight": 0.2 }
                ]
            }
        }),
        ServerMode::Store,
        dir.path(),
    );
    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(json["verdict"], "pass");
    let group = &json["traffic_groups"][0];
    assert_eq!(group["stats"]["requests"]["errors"], 0);
    assert!(group["verification"]["verified"].as_u64().unwrap() > 0);
    assert_eq!(group["verification"]["failures"], 0);
    assert!(group["verification"].get("first_mismatch").is_none());
}

#[test]
fn synthetic_responder_passes_for_71_512_and_4096_bytes() {
    for (seed, value_size) in [(0u64, 71usize), (7, 512), (999, 4096)] {
        let dir = tempfile::tempdir().unwrap();
        let (output, json) = run_masstree_cli(
            serde_json::json!({
                "keys": { "strategy": "round-robin", "max": 8, "value_size": value_size },
                "operation": "get",
                "verify": true,
                "verify_seed": seed
            }),
            ServerMode::Synthetic { seed, value_size, fault: None },
            dir.path(),
        );
        assert_eq!(
            output.status.code(),
            Some(0),
            "size={value_size} stdout={}\nstderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(json["verdict"], "pass", "size={value_size}");
        let group = &json["traffic_groups"][0];
        assert_eq!(group["stats"]["requests"]["errors"], 0, "size={value_size}");
        assert!(group["verification"]["verified"].as_u64().unwrap() > 0, "size={value_size}");
        assert_eq!(group["verification"]["failures"], 0, "size={value_size}");
    }
}

#[test]
fn fault_injection_corrupted_byte_fails_with_exit_3_and_exact_offset() {
    let dir = tempfile::tempdir().unwrap();
    let seed = 17u64;
    let value_size = 71usize;
    let bad_offset = 43usize;
    let xor_mask = 0x80u8;
    let expected_vec = synthetic_pattern(seed, b"key:2", value_size, 0);
    let expected_byte = expected_vec[bad_offset];
    let actual_byte = expected_byte ^ xor_mask;

    let (output, json) = run_masstree_cli(
        serde_json::json!({
            "keys": { "strategy": "round-robin", "max": 4, "value_size": value_size },
            "operation": "get",
            "verify": true,
            "verify_seed": seed
        }),
        ServerMode::Synthetic {
            seed,
            value_size,
            fault: Some(Fault::CorruptByte {
                target_key: b"key:2".to_vec(),
                offset: bad_offset,
                xor_mask,
            }),
        },
        dir.path(),
    );

    assert_eq!(
        output.status.code(),
        Some(3),
        "expected exit code 3 (InvalidRun)\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(json["verdict"], "invalid_run");
    let group = &json["traffic_groups"][0];
    assert!(group["stats"]["requests"]["errors_by_kind"]["ValueMismatch"].as_u64().unwrap() >= 1);
    let ver = &group["verification"];
    assert!(ver["failures"].as_u64().unwrap() >= 1);
    let first = &ver["first_mismatch"];
    assert_eq!(first["kind"], "ValueMismatch");
    assert_eq!(first["key"], "key:2");
    assert_eq!(first["offset"], bad_offset as u64);
    assert_eq!(first["expected"], u64::from(expected_byte));
    assert_eq!(first["actual"], u64::from(actual_byte));
    assert_eq!(first["expected_len"], value_size as u64);
    assert_eq!(first["actual_len"], value_size as u64);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("VERDICT: INVALID RUN"), "{stdout}");
    assert!(stdout.contains("ValueMismatch"), "{stdout}");
    assert!(stdout.contains("key=key:2"), "{stdout}");
    assert!(stdout.contains(&format!("offset={bad_offset}")), "{stdout}");
}

#[test]
fn fault_injection_short_value_fails_with_short_value_error() {
    let dir = tempfile::tempdir().unwrap();
    let seed = 23u64;
    let value_size = 128usize;
    let trunc_len = 37usize;

    let (output, json) = run_masstree_cli(
        serde_json::json!({
            "keys": { "strategy": "round-robin", "max": 4, "value_size": value_size },
            "operation": "get",
            "verify": true,
            "verify_seed": seed
        }),
        ServerMode::Synthetic {
            seed,
            value_size,
            fault: Some(Fault::Truncate {
                target_key: b"key:1".to_vec(),
                len: trunc_len,
            }),
        },
        dir.path(),
    );

    assert_eq!(output.status.code(), Some(3));
    assert_eq!(json["verdict"], "invalid_run");
    let group = &json["traffic_groups"][0];
    assert!(group["stats"]["requests"]["errors_by_kind"]["ShortValue"].as_u64().unwrap() >= 1);
    let first = &group["verification"]["first_mismatch"];
    assert_eq!(first["kind"], "ShortValue");
    assert_eq!(first["key"], "key:1");
    assert_eq!(first["offset"], trunc_len as u64);
    assert_eq!(first["expected_len"], value_size as u64);
    assert_eq!(first["actual_len"], trunc_len as u64);
}

#[test]
fn fault_injection_unexpected_miss_fails_with_unexpected_miss_error() {
    let dir = tempfile::tempdir().unwrap();
    let seed = 31u64;
    let value_size = 64usize;

    let (output, json) = run_masstree_cli(
        serde_json::json!({
            "keys": { "strategy": "round-robin", "max": 4, "value_size": value_size },
            "operation": "get",
            "verify": true,
            "verify_seed": seed
        }),
        ServerMode::Synthetic {
            seed,
            value_size,
            fault: Some(Fault::Miss { target_key: b"key:3".to_vec() }),
        },
        dir.path(),
    );

    assert_eq!(output.status.code(), Some(3));
    assert_eq!(json["verdict"], "invalid_run");
    let group = &json["traffic_groups"][0];
    assert!(group["stats"]["requests"]["errors_by_kind"]["UnexpectedMiss"].as_u64().unwrap() >= 1);
    let first = &group["verification"]["first_mismatch"];
    assert_eq!(first["kind"], "UnexpectedMiss");
    assert_eq!(first["key"], "key:3");
    assert_eq!(first["offset"], 0);
    assert_eq!(first["expected_len"], value_size as u64);
    assert_eq!(first["actual_len"], 5);
}
