//! End-to-end TCP loopback integration tests for:
//! - Bulk stream (`protocol = "stream"`) send by exact byte limit + warmup
//! - Bulk stream send by duration + warmup
//! - Reverse bulk stream (`direction = "recv"`) with payload verification (pass & fault)
//! - Post-run `experiment.idle_hold` before closing TCP connections

use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::time::{Duration, Instant};
use xylem_protocols::{check_stream_pattern, fill_stream_pattern};

fn accept_one(listener: &TcpListener) -> std::net::TcpStream {
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).unwrap();
                stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                stream.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
                return stream;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "xylem client did not connect in time");
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => panic!("accept failed: {e}"),
        }
    }
}

type SinkOutcome = (u64, Option<(u64, u8, u8)>);

/// Spawn a TCP sink server that reads until EOF, verifies every byte from
/// offset 0 against `check_stream_pattern(seed, ...)`, and returns
/// `(total_bytes_read, first_mismatch)`.
fn spawn_verifying_sink_server(
    listener: TcpListener,
    seed: u64,
) -> std::thread::JoinHandle<SinkOutcome> {
    std::thread::spawn(move || {
        let mut stream = accept_one(&listener);
        let mut buf = vec![0u8; 65536];
        let mut offset = 0u64;
        let mut first_mismatch = None;
        loop {
            match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if first_mismatch.is_none() {
                        first_mismatch = check_stream_pattern(seed, offset, &buf[..n]);
                    }
                    offset += n as u64;
                }
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => break,
                Err(e) => panic!("sink server read failed: {e}"),
            }
        }
        (offset, first_mismatch)
    })
}

/// Spawn a TCP server that streams deterministic pattern bytes starting from
/// offset 0, optionally flipping 1 byte at `corrupt_offset`.
fn spawn_streaming_source_server(
    listener: TcpListener,
    seed: u64,
    total_bytes: u64,
    corrupt_offset: Option<u64>,
) -> std::thread::JoinHandle<u64> {
    std::thread::spawn(move || {
        let mut stream = accept_one(&listener);
        let mut buf = vec![0u8; 65536];
        let mut offset = 0u64;
        while offset < total_bytes {
            let len = (buf.len() as u64).min(total_bytes - offset) as usize;
            fill_stream_pattern(seed, offset, &mut buf[..len]);
            if let Some(bad_off) = corrupt_offset {
                if bad_off >= offset && bad_off < offset + len as u64 {
                    buf[(bad_off - offset) as usize] ^= 0x01;
                }
            }
            match stream.write_all(&buf[..len]) {
                Ok(()) => offset += len as u64,
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
                    ) =>
                {
                    break;
                }
                Err(e) => panic!("source server write failed: {e}"),
            }
        }
        offset
    })
}

fn run_xylem(profile_toml: &str) -> (std::process::Output, serde_json::Value) {
    let dir = tempfile::tempdir().unwrap();
    let profile_path = dir.path().join("profile.toml");
    let results_path = dir.path().join("results.json");
    let rendered = profile_toml.replace("__RESULTS__", results_path.to_str().unwrap());
    std::fs::write(&profile_path, rendered).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile_path)
        .output()
        .unwrap();

    let json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&results_path).unwrap_or_else(|e| {
            panic!(
                "results.json not written ({e}); status={:?}, stderr={}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            )
        }))
        .unwrap();
    (output, json)
}

#[test]
fn stream_send_by_bytes_with_warmup_and_omitted_duration() {
    let seed = 0xCAFE_BABEu64;
    let quota: u64 = 4 * 1024 * 1024; // 4 MiB
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let target = listener.local_addr().unwrap().to_string();
    let server = spawn_verifying_sink_server(listener, seed);

    let profile = format!(
        r#"
[experiment]
name = "stream-send-bytes"
warmup = "100ms"

[[traffic_groups]]
name = "sink"
protocol = "stream"
target = "{target}"
transport = "tcp"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1

[traffic_groups.traffic_policy]
type = "closed-loop"

[traffic_groups.protocol_config]
direction = "send"
chunk_size = 65536
bytes = {quota}
seed = {seed}

[output]
format = "json"
file = "__RESULTS__"
"#
    );

    let (output, json) = run_xylem(&profile);
    let (server_bytes, mismatch) = server.join().unwrap();

    assert_eq!(
        output.status.code(),
        Some(0),
        "expected exit 0, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(json["verdict"], "pass");
    assert_eq!(json["global"]["requests"]["errors"], 0);
    assert_eq!(mismatch, None, "server detected stream byte mismatch");
    assert!(
        server_bytes > quota,
        "server should receive warmup + {quota} bytes, got {server_bytes}"
    );
    assert_eq!(json["global"]["tx_bytes"].as_u64(), Some(quota));
    assert!(json["global"]["tx_mbps"].as_f64().unwrap() > 0.0);
    assert_eq!(json["traffic_groups"][0]["stats"]["tx_bytes"].as_u64(), Some(quota));
}

#[test]
fn stream_send_by_duration_with_warmup() {
    let seed = 1337u64;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let target = listener.local_addr().unwrap().to_string();
    let server = spawn_verifying_sink_server(listener, seed);

    let profile = format!(
        r#"
[experiment]
name = "stream-send-duration"
warmup = "100ms"
duration = "300ms"

[[traffic_groups]]
name = "sink"
protocol = "stream"
target = "{target}"
transport = "tcp"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1

[traffic_groups.traffic_policy]
type = "closed-loop"

[traffic_groups.protocol_config]
direction = "send"
chunk_size = 65536
seed = {seed}

[output]
format = "json"
file = "__RESULTS__"
"#
    );

    let (output, json) = run_xylem(&profile);
    let (server_bytes, mismatch) = server.join().unwrap();

    assert_eq!(
        output.status.code(),
        Some(0),
        "expected exit 0, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(json["verdict"], "pass");
    assert_eq!(mismatch, None, "server detected stream byte mismatch");
    assert!(server_bytes > 0);
    let tx_bytes = json["global"]["tx_bytes"].as_u64().unwrap();
    assert!(tx_bytes > 0, "expected tx_bytes > 0");
    assert!(json["global"]["tx_mbps"].as_f64().unwrap() > 0.0);
    assert!(
        server_bytes >= tx_bytes,
        "server_bytes ({server_bytes}) must include at least measured tx_bytes ({tx_bytes})"
    );
}

#[test]
fn stream_recv_verify_pass_and_fault() {
    let seed = 0xDEAD_BEEFu64;
    let quota: u64 = 2 * 1024 * 1024; // 2 MiB

    // 1. Clean stream -> Pass, verification.failures == 0
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let target = listener.local_addr().unwrap().to_string();
        // Stream slightly more than quota so client reaches byte_limit before EOF
        let server = spawn_streaming_source_server(listener, seed, quota + 256 * 1024, None);

        let profile = format!(
            r#"
[experiment]
name = "stream-recv-pass"

[[traffic_groups]]
name = "reverse"
protocol = "stream"
target = "{target}"
transport = "tcp"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1

[traffic_groups.traffic_policy]
type = "closed-loop"

[traffic_groups.protocol_config]
direction = "recv"
bytes = {quota}
seed = {seed}
verify = true

[output]
format = "json"
file = "__RESULTS__"
"#
        );

        let (output, json) = run_xylem(&profile);
        let _ = server.join().unwrap();

        assert_eq!(
            output.status.code(),
            Some(0),
            "expected exit 0, stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(json["verdict"], "pass");
        let rx_bytes = json["global"]["rx_bytes"].as_u64().unwrap();
        assert!(rx_bytes >= quota, "expected rx_bytes >= {quota}, got {rx_bytes}");
        assert!(json["global"]["rx_mbps"].as_f64().unwrap() > 0.0);
        let verification = &json["traffic_groups"][0]["verification"];
        assert_eq!(verification["failures"].as_u64(), Some(0));
        assert!(verification["verified"].as_u64().unwrap() >= quota);
    }

    // 2. Corrupted byte at exact offset 123456 -> exit 3, InvalidRun, ValueMismatch
    {
        let bad_offset = 123_456u64;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let target = listener.local_addr().unwrap().to_string();
        let server =
            spawn_streaming_source_server(listener, seed, quota + 256 * 1024, Some(bad_offset));

        let profile = format!(
            r#"
[experiment]
name = "stream-recv-fault"

[[traffic_groups]]
name = "reverse"
protocol = "stream"
target = "{target}"
transport = "tcp"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1

[traffic_groups.traffic_policy]
type = "closed-loop"

[traffic_groups.protocol_config]
direction = "recv"
bytes = {quota}
seed = {seed}
verify = true

[output]
format = "json"
file = "__RESULTS__"
"#
        );

        let (output, json) = run_xylem(&profile);
        let _ = server.join().unwrap();

        assert_eq!(
            output.status.code(),
            Some(3),
            "expected exit 3 (InvalidRun), got {:?}, stderr={}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(json["verdict"], "invalid_run");
        assert!(
            json["global"]["requests"]["errors_by_kind"]["ValueMismatch"]
                .as_u64()
                .unwrap_or(0)
                >= 1,
            "expected ValueMismatch in errors_by_kind: {}",
            json["global"]["requests"]
        );
        let verification = &json["traffic_groups"][0]["verification"];
        assert!(verification["failures"].as_u64().unwrap() >= 1);
        assert_eq!(verification["first_mismatch"]["offset"].as_u64(), Some(bad_offset));
    }
}

#[test]
fn idle_hold_keeps_connection_open_after_last_response() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let target = listener.local_addr().unwrap().to_string();

    // Minimal xylem-echo server (16-byte echo) that records last response timestamp and client EOF timestamp
    let server = std::thread::spawn(move || serve_echo_and_measure_hold(listener));

    let profile = format!(
        r#"
[experiment]
name = "idle-hold-test"
duration = "150ms"
idle_hold = "300ms"

[[traffic_groups]]
name = "echo"
protocol = "xylem-echo"
target = "{target}"
transport = "tcp"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "__RESULTS__"
"#
    );

    let (output, json) = run_xylem(&profile);
    let (replies, last_response_at, close_at) = server.join().unwrap();

    assert_eq!(
        output.status.code(),
        Some(0),
        "expected exit 0, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(json["verdict"], "pass");
    assert_eq!(json["global"]["requests"]["errors"], 0);
    assert!(replies > 0, "expected echo replies");

    let hold_duration = close_at.duration_since(last_response_at.expect("at least one reply"));
    assert!(
        hold_duration >= Duration::from_millis(250),
        "expected idle_hold >= 250ms before client EOF, observed {hold_duration:?}"
    );
}

fn serve_echo_and_measure_hold(listener: TcpListener) -> (u64, Option<Instant>, Instant) {
    let mut stream = accept_one(&listener);
    let mut buf = [0u8; 4096];
    let mut pending = Vec::new();
    let mut replies = 0u64;
    let mut last_response_at: Option<Instant> = None;
    loop {
        let n = match stream.read(&mut buf) {
            Ok(0) => return (replies, last_response_at, Instant::now()),
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                return (replies, last_response_at, Instant::now());
            }
            Err(e) => panic!("echo server read failed: {e}"),
        };
        pending.extend_from_slice(&buf[..n]);
        while pending.len() >= 16 {
            let req: Vec<u8> = pending.drain(..16).collect();
            if stream.write_all(&req).is_err() {
                return (replies, last_response_at, Instant::now());
            }
            replies += 1;
            last_response_at = Some(Instant::now());
        }
    }
}
