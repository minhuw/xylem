//! Servers that drop or never answer connection setup (a ClickHouse Hello,
//! Redis AUTH, the masstree handshake, NVMe/TCP ICReq, the HTTP/2 preface)
//! fail the run with an error naming the connection, through the real CLI.
//! The servers are stubs in the test: no Docker.
use std::io::Read;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

/// What the stub server does with each connection it accepts
#[derive(Clone, Copy)]
enum Stub {
    /// Close it before reading anything
    CloseAtOnce,
    /// Close it once the client's first bytes arrived, without answering
    CloseAfterHello,
    /// Read and never answer
    Silent,
}

/// A stub server on a free local port, until dropped
struct StubServer {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl StubServer {
    fn start(stub: Stub) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            // Silent connections stay open until the server stops
            let mut held = Vec::new();
            while !stopped.load(Ordering::Relaxed) {
                let mut socket = match listener.accept() {
                    Ok((socket, _)) => socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(error) => panic!("accept: {error}"),
                };
                socket.set_nonblocking(false).unwrap();
                match stub {
                    Stub::CloseAtOnce => {}
                    Stub::CloseAfterHello => {
                        socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                        let n = socket.read(&mut [0; 4096]).expect("the client's setup request");
                        assert!(n > 0, "the client closed before its setup request");
                    }
                    Stub::Silent => held.push(socket),
                }
            }
        });
        Self { addr, stop, thread: Some(thread) }
    }
}

impl Drop for StubServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let result = thread.join();
            if !std::thread::panicking() {
                result.expect("stub server failed");
            }
        }
    }
}

/// Run one connection of `protocol` with `protocol_config` and
/// `traffic_policy` against `target` for a second, with JSON results in `dir`
fn run_cli(
    protocol: &str,
    protocol_config: serde_json::Value,
    traffic_policy: serde_json::Value,
    target: SocketAddr,
    dir: &Path,
) -> Output {
    let profile = dir.join("profile.toml");
    let config = serde_json::json!({
        "experiment": { "name": "setup-failure", "duration": "1s", "seed": 7 },
        "traffic_groups": [{
            "name": "stub", "threads": [0], "connections_per_thread": 1,
            "max_pending_per_connection": 1, "target": target.to_string(),
            "transport": "tcp", "protocol": protocol, "traffic_policy": traffic_policy,
            "protocol_config": protocol_config
        }],
        "output": { "format": "json", "file": dir.join("results.json") }
    });
    std::fs::write(&profile, toml::to_string(&config).unwrap()).unwrap();
    Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile)
        .env("RUST_LOG", "warn")
        .output()
        .unwrap()
}

/// The protocols with connection setup, with a protocol_config that sets
/// connections up
fn setup_protocols() -> [(&'static str, serde_json::Value); 5] {
    [
        ("clickhouse", serde_json::json!({ "query": "SELECT 1" })),
        ("redis", serde_json::json!({ "auth": { "password": "secret" } })),
        ("masstree", serde_json::json!({})),
        ("nvme-tcp", serde_json::json!({ "subnqn": "nqn.2016-06.io.spdk:cnode1" })),
        ("http2", serde_json::json!({})),
    ]
}

/// Run every setup protocol against a server doing `stub`, and check that
/// each run fails with exit 1 and an error ending in `failure`, without
/// results
fn assert_setup_fails(stub: Stub, failure: &str) {
    for (protocol, protocol_config) in setup_protocols() {
        let server = StubServer::start(stub);
        let dir = tempfile::tempdir().unwrap();
        let closed_loop = serde_json::json!({ "type": "closed-loop" });
        let output = run_cli(protocol, protocol_config, closed_loop, server.addr, dir.path());
        drop(server);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(1), "{protocol}: {stderr}");
        assert!(!stderr.contains("panicked"), "{protocol}: {stderr}");
        let error = format!(
            "Error: Connection error: Traffic group 0, connection 0 ({protocol}): {failure}"
        );
        assert!(stderr.contains(&error), "{protocol}: {stderr}");
        assert!(!dir.path().join("results.json").exists(), "{protocol}: {stderr}");
    }
}

#[test]
fn a_server_closing_before_reading_setup_fails_the_run() {
    assert_setup_fails(Stub::CloseAtOnce, "connection closed during setup: ");
}

#[test]
fn a_server_closing_after_reading_setup_fails_the_run() {
    assert_setup_fails(Stub::CloseAfterHello, "connection closed during setup: ");
}

#[test]
fn a_server_never_answering_setup_fails_the_run() {
    assert_setup_fails(Stub::Silent, "connection setup did not finish by the end of the run");
}

/// A rate-driven group that sends nothing in the run (no setup, a rate far
/// below one request per run) is left out of the results, which makes the
/// run invalid
#[test]
fn a_group_that_sends_nothing_makes_the_run_invalid() {
    let server = StubServer::start(Stub::Silent);
    let dir = tempfile::tempdir().unwrap();
    // Seed 7 draws its first arrival long after the end of the run
    let rare = serde_json::json!({ "type": "poisson", "rate": 0.001 });
    let output = run_cli("http", serde_json::json!({}), rare, server.addr, dir.path());
    drop(server);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(3), "{stderr}");
    let text = std::fs::read_to_string(dir.path().join("results.json")).unwrap();
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(json["verdict"], "invalid_run");
    assert_eq!(json["traffic_groups"], serde_json::json!([]));
    assert_eq!(json["global"]["requests"]["sent"], 0);
    let validity = json["validity"].as_array().unwrap();
    assert!(!validity.is_empty());
    for check in validity {
        assert_eq!(check["ok"], false, "{check}");
        assert_eq!(check["reason"], "the group recorded no requests", "{check}");
    }
}
