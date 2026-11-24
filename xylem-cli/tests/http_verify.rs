//! End-to-end TCP loopback CLI tests for opt-in HTTP/1.1 GET response-body
//! verification (64 B, 16 KiB keyed, 96 KiB multi-segment streaming, and all
//! named error kinds + `on_error = "abort"`).

use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::Path;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// Independent implementation of the documented FNV-1a 64 + splitmix64 formula
// (mirrors book/src/guide/protocols/http.md).
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

fn synthetic_body(seed: u64, path: &[u8], body_size: usize) -> Vec<u8> {
    let base =
        splitmix64(seed ^ fnv1a_64(path) ^ (body_size as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    let mut out = vec![0u8; body_size];
    let mut off = 0usize;
    let mut w = 0u64;
    while off < body_size {
        let word = splitmix64(base.wrapping_add(w.wrapping_mul(0x9e37_79b9_7f4a_7c15)));
        let bytes = word.to_le_bytes();
        let n = (body_size - off).min(8);
        out[off..off + n].copy_from_slice(&bytes[..n]);
        off += n;
        w += 1;
    }
    out
}

#[derive(Clone)]
enum ServerFault {
    None,
    BadStatus { status: u16, reason: &'static str },
    WrongContentLength { actual_len: usize },
    CorruptByte { offset: usize, xor_mask: u8 },
    TruncateAndClose { declared_len: usize, send_len: usize },
}

#[derive(Clone)]
struct ServerConfig {
    seed: u64,
    body_size: usize,
    write_piece: usize,
    fault: ServerFault,
}

fn serve_connection(mut stream: TcpStream, cfg: ServerConfig) {
    stream.set_nodelay(true).ok();
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut pending = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        let n = match stream.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => n,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::TimedOut
                ) =>
            {
                return;
            }
            Err(_) => return,
        };
        pending.extend_from_slice(&buf[..n]);
        while let Some(end) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
            let req: Vec<u8> = pending.drain(..end + 4).collect();
            let first_line_end = req.windows(2).position(|w| w == b"\r\n").unwrap_or(req.len());
            let first_line = std::str::from_utf8(&req[..first_line_end]).unwrap();
            let target = first_line.split_whitespace().nth(1).expect("HTTP request target");

            match cfg.fault {
                ServerFault::None => {
                    let body = synthetic_body(cfg.seed, target.as_bytes(), cfg.body_size);
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                        body.len()
                    );
                    if write_in_pieces(&mut stream, head.as_bytes(), &body, cfg.write_piece)
                        .is_err()
                    {
                        return;
                    }
                }
                ServerFault::BadStatus { status, reason } => {
                    let body = b"error";
                    let head = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                        body.len()
                    );
                    if write_in_pieces(&mut stream, head.as_bytes(), body, cfg.write_piece).is_err()
                    {
                        return;
                    }
                }
                ServerFault::WrongContentLength { actual_len } => {
                    let body = synthetic_body(cfg.seed, target.as_bytes(), actual_len);
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                        body.len()
                    );
                    if write_in_pieces(&mut stream, head.as_bytes(), &body, cfg.write_piece)
                        .is_err()
                    {
                        return;
                    }
                }
                ServerFault::CorruptByte { offset, xor_mask } => {
                    let mut body = synthetic_body(cfg.seed, target.as_bytes(), cfg.body_size);
                    body[offset] ^= xor_mask;
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                        body.len()
                    );
                    if write_in_pieces(&mut stream, head.as_bytes(), &body, cfg.write_piece)
                        .is_err()
                    {
                        return;
                    }
                }
                ServerFault::TruncateAndClose { declared_len, send_len } => {
                    let body = synthetic_body(cfg.seed, target.as_bytes(), cfg.body_size);
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {declared_len}\r\nConnection: keep-alive\r\n\r\n",
                    );
                    let _ = write_in_pieces(
                        &mut stream,
                        head.as_bytes(),
                        &body[..send_len],
                        cfg.write_piece,
                    );
                    let _ = stream.flush();
                    let _ = stream.shutdown(Shutdown::Write);
                    return;
                }
            }
        }
    }
}

fn write_in_pieces(
    stream: &mut TcpStream,
    head: &[u8],
    body: &[u8],
    piece_size: usize,
) -> std::io::Result<()> {
    stream.write_all(head)?;
    if piece_size == 0 || piece_size >= body.len() {
        stream.write_all(body)?;
    } else {
        for chunk in body.chunks(piece_size) {
            stream.write_all(chunk)?;
        }
    }
    Ok(())
}

struct StubServer {
    addr: String,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl StubServer {
    fn start(cfg: ServerConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP stub server");
        let addr = listener.local_addr().unwrap().to_string();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let mut workers = Vec::new();
            while !stop_flag.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(false).ok();
                        let cfg = cfg.clone();
                        workers.push(thread::spawn(move || serve_connection(stream, cfg)));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
            for w in workers {
                let _ = w.join();
            }
        });
        Self { addr, stop, handle: Some(handle) }
    }
}

impl Drop for StubServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn run_http_cli(
    protocol_config: serde_json::Value,
    server_cfg: ServerConfig,
    connections: usize,
    on_error: Option<&str>,
    dir: &Path,
) -> (Output, Option<serde_json::Value>) {
    let server = StubServer::start(server_cfg);
    let profile = dir.join("profile.toml");
    let results_path = dir.join("results.json");

    let mut group = serde_json::json!({
        "name": "web",
        "threads": [0],
        "connections_per_thread": connections,
        "max_pending_per_connection": 1,
        "target": server.addr,
        "protocol": "http",
        "traffic_policy": { "type": "closed-loop" },
        "protocol_config": protocol_config
    });
    if let Some(on_error) = on_error {
        group["on_error"] = serde_json::Value::String(on_error.to_string());
    }

    let config = serde_json::json!({
        "experiment": { "name": "http-verify", "duration": "300ms", "seed": 123 },
        "traffic_groups": [group],
        "output": { "format": "json", "file": results_path }
    });
    std::fs::write(&profile, toml::to_string(&config).unwrap()).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile)
        .env("RUST_LOG", "off")
        .output()
        .unwrap();

    drop(server);

    let json = std::fs::read_to_string(&results_path)
        .ok()
        .map(|text| serde_json::from_str(&text).unwrap());
    (output, json)
}

#[test]
fn clean_pass_64b_fixed_16k_keyed_and_96k_streaming() {
    // 1. 64 B fixed path, N=2 connections
    {
        let dir = tempfile::tempdir().unwrap();
        let (output, json) = run_http_cli(
            serde_json::json!({
                "method": "GET",
                "path": "/small",
                "verify": true,
                "verify_seed": 7,
                "verify_body_size": 64
            }),
            ServerConfig {
                seed: 7,
                body_size: 64,
                write_piece: 64,
                fault: ServerFault::None,
            },
            2,
            None,
            dir.path(),
        );
        assert_eq!(
            output.status.code(),
            Some(0),
            "64B failed\nstdout={}\nstderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let json = json.expect("results.json missing");
        assert_eq!(json["verdict"], "pass");
        let group = &json["traffic_groups"][0];
        assert_eq!(group["stats"]["requests"]["errors"], 0);
        assert!(group["verification"]["verified"].as_u64().unwrap() > 0);
        assert_eq!(group["verification"]["failures"], 0);
        assert!(group["verification"].get("first_mismatch").is_none());
    }

    // 2. 16 KiB with keys, N=4 connections
    {
        let dir = tempfile::tempdir().unwrap();
        let (output, json) = run_http_cli(
            serde_json::json!({
                "method": "GET",
                "path": "/obj/",
                "keys": {
                    "strategy": "round-robin",
                    "max": 16,
                    "value_size": 64,
                    "prefix": "k"
                },
                "verify": true,
                "verify_seed": 42,
                "verify_body_size": 16384
            }),
            ServerConfig {
                seed: 42,
                body_size: 16384,
                write_piece: 4096,
                fault: ServerFault::None,
            },
            4,
            None,
            dir.path(),
        );
        assert_eq!(
            output.status.code(),
            Some(0),
            "16KiB failed\nstdout={}\nstderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let json = json.expect("results.json missing");
        assert_eq!(json["verdict"], "pass");
        let group = &json["traffic_groups"][0];
        assert_eq!(group["stats"]["requests"]["errors"], 0);
        assert!(group["verification"]["verified"].as_u64().unwrap() > 0);
        assert_eq!(group["verification"]["failures"], 0);
    }

    // 3. 96 KiB multi-segment (> 64 KiB MAX_PAYLOAD) written in 4096 B pieces over keep-alive, N=3
    {
        let dir = tempfile::tempdir().unwrap();
        let (output, json) = run_http_cli(
            serde_json::json!({
                "method": "GET",
                "path": "/large/",
                "keys": {
                    "strategy": "round-robin",
                    "max": 8,
                    "value_size": 64,
                    "prefix": "seg"
                },
                "verify": true,
                "verify_seed": 999,
                "verify_body_size": 98304
            }),
            ServerConfig {
                seed: 999,
                body_size: 98304,
                write_piece: 4096,
                fault: ServerFault::None,
            },
            3,
            None,
            dir.path(),
        );
        assert_eq!(
            output.status.code(),
            Some(0),
            "96KiB failed\nstdout={}\nstderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let json = json.expect("results.json missing");
        assert_eq!(json["verdict"], "pass");
        let group = &json["traffic_groups"][0];
        assert_eq!(group["stats"]["requests"]["errors"], 0);
        assert!(group["verification"]["verified"].as_u64().unwrap() > 0);
        assert_eq!(group["verification"]["failures"], 0);
    }
}

#[test]
fn bad_status_fails_with_exit_3_and_named_error() {
    for (status, reason) in [(404u16, "Not Found"), (500u16, "Internal Server Error")] {
        let dir = tempfile::tempdir().unwrap();
        let (output, json) = run_http_cli(
            serde_json::json!({
                "method": "GET",
                "path": "/missing",
                "verify": true,
                "verify_seed": 1,
                "verify_body_size": 64
            }),
            ServerConfig {
                seed: 1,
                body_size: 64,
                write_piece: 64,
                fault: ServerFault::BadStatus { status, reason },
            },
            2,
            None,
            dir.path(),
        );
        assert_eq!(
            output.status.code(),
            Some(3),
            "status={status}\nstdout={}\nstderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let json = json.expect("results.json missing");
        assert_eq!(json["verdict"], "invalid_run");
        let group = &json["traffic_groups"][0];
        assert!(group["stats"]["requests"]["errors_by_kind"]["BadStatus"].as_u64().unwrap() >= 1);
        assert!(group["verification"]["failures"].as_u64().unwrap() >= 1);
        assert_eq!(group["verification"]["first_mismatch"]["kind"], "BadStatus");
    }
}

#[test]
fn content_length_mismatch_shorter_and_longer_fails_with_exit_3() {
    for actual_len in [60usize, 128usize] {
        let dir = tempfile::tempdir().unwrap();
        let (output, json) = run_http_cli(
            serde_json::json!({
                "method": "GET",
                "path": "/item",
                "verify": true,
                "verify_seed": 5,
                "verify_body_size": 64
            }),
            ServerConfig {
                seed: 5,
                body_size: 64,
                write_piece: 64,
                fault: ServerFault::WrongContentLength { actual_len },
            },
            2,
            None,
            dir.path(),
        );
        assert_eq!(
            output.status.code(),
            Some(3),
            "actual_len={actual_len}\nstdout={}\nstderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let json = json.expect("results.json missing");
        assert_eq!(json["verdict"], "invalid_run");
        let group = &json["traffic_groups"][0];
        assert!(
            group["stats"]["requests"]["errors_by_kind"]["ContentLengthMismatch"]
                .as_u64()
                .unwrap()
                >= 1
        );
        assert!(group["verification"]["failures"].as_u64().unwrap() >= 1);
        let first = &group["verification"]["first_mismatch"];
        assert_eq!(first["kind"], "ContentLengthMismatch");
        assert_eq!(first["expected_len"], 64);
        assert_eq!(first["actual_len"], actual_len as u64);
    }
}

#[test]
fn body_mismatch_at_offset_70000_in_96k_body_fails_with_exit_3() {
    let dir = tempfile::tempdir().unwrap();
    let seed = 17u64;
    let body_size = 98_304usize;
    let bad_offset = 70_000usize;
    let xor_mask = 0x80u8;
    let expected_vec = synthetic_body(seed, b"/blob", body_size);
    let expected_byte = expected_vec[bad_offset];
    let actual_byte = expected_byte ^ xor_mask;

    let (output, json) = run_http_cli(
        serde_json::json!({
            "method": "GET",
            "path": "/blob",
            "verify": true,
            "verify_seed": seed,
            "verify_body_size": body_size
        }),
        ServerConfig {
            seed,
            body_size,
            write_piece: 4096,
            fault: ServerFault::CorruptByte { offset: bad_offset, xor_mask },
        },
        2,
        None,
        dir.path(),
    );

    assert_eq!(
        output.status.code(),
        Some(3),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let json = json.expect("results.json missing");
    assert_eq!(json["verdict"], "invalid_run");
    let group = &json["traffic_groups"][0];
    assert!(group["stats"]["requests"]["errors_by_kind"]["BodyMismatch"].as_u64().unwrap() >= 1);
    assert!(group["verification"]["failures"].as_u64().unwrap() >= 1);
    let first = &group["verification"]["first_mismatch"];
    assert_eq!(first["kind"], "BodyMismatch");
    assert_eq!(first["key"], "/blob");
    assert_eq!(first["offset"], bad_offset as u64);
    assert_eq!(first["expected"], u64::from(expected_byte));
    assert_eq!(first["actual"], u64::from(actual_byte));
    assert_eq!(first["expected_len"], body_size as u64);
    assert_eq!(first["actual_len"], body_size as u64);
}

#[test]
fn short_body_on_eof_fails_with_exit_3_and_exact_offset() {
    let dir = tempfile::tempdir().unwrap();
    let seed = 23u64;
    let body_size = 65_536usize;
    let send_len = 20_000usize;

    let (output, json) = run_http_cli(
        serde_json::json!({
            "method": "GET",
            "path": "/trunc",
            "verify": true,
            "verify_seed": seed,
            "verify_body_size": body_size
        }),
        ServerConfig {
            seed,
            body_size,
            write_piece: 4096,
            fault: ServerFault::TruncateAndClose { declared_len: body_size, send_len },
        },
        2,
        None,
        dir.path(),
    );

    assert_eq!(
        output.status.code(),
        Some(3),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let json = json.expect("results.json missing");
    assert_eq!(json["verdict"], "invalid_run");
    let group = &json["traffic_groups"][0];
    assert!(group["stats"]["requests"]["errors_by_kind"]["ShortBody"].as_u64().unwrap() >= 1);
    assert!(group["verification"]["failures"].as_u64().unwrap() >= 1);
    let first = &group["verification"]["first_mismatch"];
    assert_eq!(first["kind"], "ShortBody");
    assert_eq!(first["key"], "/trunc");
    assert_eq!(first["offset"], send_len as u64);
    assert_eq!(first["expected_len"], body_size as u64);
    assert_eq!(first["actual_len"], send_len as u64);
}

#[test]
fn on_error_abort_exits_1_loudly_on_body_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let (output, _json) = run_http_cli(
        serde_json::json!({
            "method": "GET",
            "path": "/abort-test",
            "verify": true,
            "verify_seed": 11,
            "verify_body_size": 64
        }),
        ServerConfig {
            seed: 11,
            body_size: 64,
            write_piece: 64,
            fault: ServerFault::CorruptByte { offset: 10, xor_mask: 0x01 },
        },
        2,
        Some("abort"),
        dir.path(),
    );

    assert_eq!(
        output.status.code(),
        Some(1),
        "expected exit 1 on abort\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("BodyMismatch") || stderr.contains("on_error = \"abort\""),
        "expected loud abort error in stderr, got: {stderr}"
    );
}
