//! The executable counts error replies instead of failing the run, unless a
//! traffic group sets `on_error = "abort"`.
use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

/// Answer the HTTP requests of one keep-alive connection, every third with a
/// 503 and the others with a 200. Returns how many requests it answered.
fn serve_http(listener: TcpListener) -> std::thread::JoinHandle<u64> {
    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "CLI did not connect");
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("accept: {error}"),
            }
        };
        socket.set_nonblocking(false).unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let (mut pending, mut buf, mut answered) = (Vec::new(), [0; 4096], 0);
        loop {
            let n = match socket.read(&mut buf) {
                Ok(0) => return answered,
                Ok(n) => n,
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {
                    return answered
                }
                Err(error) => panic!("read: {error}"),
            };
            pending.extend_from_slice(&buf[..n]);
            while let Some(end) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
                pending.drain(..end + 4);
                let response: &[u8] = if answered % 3 == 2 {
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 4\r\n\r\nbusy"
                } else {
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"
                };
                answered += 1;
                if socket.write_all(response).is_err() {
                    return answered;
                }
            }
        }
    })
}

/// Run a short closed-loop HTTP profile with `on_error`, writing JSON results
/// to `dir`. Returns the process output and the requests the stub answered.
fn run_cli(on_error: &str, dir: &std::path::Path) -> (Output, u64) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let target = listener.local_addr().unwrap().to_string();
    listener.set_nonblocking(true).unwrap();
    let server = serve_http(listener);
    let profile = dir.join("profile.toml");
    let config = serde_json::json!({
        "experiment": { "name": "error-replies", "duration": "1s", "seed": 7 },
        "traffic_groups": [{
            "name": "web", "threads": [0], "connections_per_thread": 1,
            "max_pending_per_connection": 4, "target": target, "protocol": "http",
            "on_error": on_error, "traffic_policy": { "type": "closed-loop" }
        }],
        "output": { "format": "json", "file": dir.join("results.json") }
    });
    std::fs::write(&profile, toml::to_string(&config).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile)
        .env("RUST_LOG", "off")
        .output()
        .unwrap();
    let answered = server.join().expect("HTTP stub failed");
    (output, answered)
}

#[test]
fn error_replies_are_counted_by_kind_with_their_own_latency() {
    let dir = tempfile::tempdir().unwrap();
    let (output, answered) = run_cli("count", dir.path());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(output.status.code(), Some(0), "{}", String::from_utf8_lossy(&output.stderr));
    let text = std::fs::read_to_string(dir.path().join("results.json")).unwrap();
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    let group = &json["traffic_groups"][0]["stats"];
    let count = |key: &str| group["requests"][key].as_u64().unwrap();
    // Every request the stub answered completed, every third an error
    assert_eq!(count("completed"), answered);
    assert_eq!(count("errors"), answered / 3);
    assert!(count("errors") > 10, "{answered} requests answered");
    assert_eq!(group["requests"]["errors_by_kind"], serde_json::json!({ "503": answered / 3 }));
    assert_eq!(count("sent"), count("completed") + count("timed_out") + count("failed"));
    // Error reply latencies stay out of the latency distribution
    let successful = count("completed") - count("errors");
    assert_eq!(group["latency"]["sample_count"].as_u64().unwrap(), successful);
    assert_eq!(group["error_latency"]["sample_count"].as_u64().unwrap(), count("errors"));
    assert!(group["error_latency"]["p50_us"].as_f64().unwrap() > 0.0);
    assert_eq!(json["global"]["requests"], group["requests"]);
    assert_eq!(json["global"]["error_latency"], group["error_latency"]);
    // The console names the errors and warns about them
    let errors = format!("  Errors:        {} (503: {})", count("errors"), count("errors"));
    assert!(stdout.contains(&errors), "{stdout}");
    assert!(stdout.contains(&format!("  WARNING: {} requests got an error reply", count("errors"))));
    let (global_text, group_text) = stdout.split_once("TRAFFIC GROUP 0: web").unwrap();
    for (text, indent, latency) in [
        (global_text, "", &json["global"]["error_latency"]),
        (group_text, "  ", &group["error_latency"]),
    ] {
        let error_summary = text
            .split_once(&format!("{indent}Error latency (μs):\n"))
            .unwrap()
            .1
            .split_once("\n\n")
            .unwrap()
            .0;
        assert!(error_summary.contains(&format!(
            "{indent}  Percentile samples: {}",
            latency["percentile_sample_count"].as_u64().unwrap()
        )));
    }
}

#[test]
fn on_error_abort_fails_the_run_naming_the_reply() {
    let dir = tempfile::tempdir().unwrap();
    let (output, answered) = run_cli("abort", dir.path());
    assert_ne!(output.status.code(), Some(0));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(
            "Traffic group 0, connection 0: error reply 503 (on_error = \"abort\"): \
             HTTP/1.1 503 Service Unavailable"
        ),
        "{stderr}"
    );
    // The first error reply is the third response; no results are written
    assert!(answered >= 3);
    assert!(!dir.path().join("results.json").exists());
}
