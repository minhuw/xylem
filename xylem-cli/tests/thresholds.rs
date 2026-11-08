//! The real executable must exit with the status of its verdict, after
//! writing every result file, and reject an invalid threshold before the run.
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

/// Answer every 16-byte xylem-echo request of one connection with itself,
/// after `delay`
fn serve_echo(listener: TcpListener, delay: Duration) -> std::thread::JoinHandle<()> {
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
        let mut request = [0; 16];
        while socket.read_exact(&mut request).is_ok() {
            std::thread::sleep(delay);
            if socket.write_all(&request).is_err() {
                break;
            }
        }
    })
}

/// Write a one-second xylem-echo profile with one connection at pipeline
/// depth 1 to `dir`, with `traffic_policy` and the `[thresholds]` table
fn write_profile(
    dir: &Path,
    target: &str,
    traffic_policy: serde_json::Value,
    thresholds: serde_json::Value,
) -> std::path::PathBuf {
    let profile = dir.join("profile.toml");
    let config = serde_json::json!({
        "experiment": { "name": "thresholds", "duration": "1s", "seed": 7 },
        "traffic_groups": [{
            "name": "echo", "threads": [0], "connections_per_thread": 1,
            "max_pending_per_connection": 1, "target": target, "protocol": "xylem-echo",
            "traffic_policy": traffic_policy
        }],
        "output": { "format": "both", "file": dir.join("results") },
        "thresholds": thresholds
    });
    std::fs::write(&profile, toml::to_string(&config).unwrap()).unwrap();
    profile
}

fn run_profile(profile: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(profile)
        .env("RUST_LOG", "off")
        .output()
        .unwrap()
}

/// Run a profile against an echo server that answers after `delay`.
/// Returns the process output and the JSON results.
fn run_against_echo(
    delay: Duration,
    traffic_policy: serde_json::Value,
    thresholds: serde_json::Value,
) -> (Output, serde_json::Value) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let target = listener.local_addr().unwrap().to_string();
    listener.set_nonblocking(true).unwrap();
    let server = serve_echo(listener, delay);
    let dir = tempfile::tempdir().unwrap();
    let profile = write_profile(dir.path(), &target, traffic_policy, thresholds);
    let output = run_profile(&profile);
    if server.join().is_err() {
        panic!("echo server failed; CLI stderr: {}", String::from_utf8_lossy(&output.stderr));
    }
    // Both result files are written whatever the verdict
    let html = std::fs::read_to_string(dir.path().join("results.html"))
        .unwrap_or_else(|e| panic!("{e}; stderr: {}", String::from_utf8_lossy(&output.stderr)));
    let json = std::fs::read_to_string(dir.path().join("results.json")).unwrap();
    let json: serde_json::Value = serde_json::from_str(&json).unwrap();
    let label = json["verdict"].as_str().unwrap().replace('_', " ").to_uppercase();
    assert!(html.contains(&format!("\">{label}</div>")), "HTML verdict {label}");
    (output, json)
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).unwrap()
}

#[test]
fn a_run_within_its_thresholds_exits_0() {
    let (output, json) = run_against_echo(
        Duration::ZERO,
        serde_json::json!({ "type": "closed-loop" }),
        serde_json::json!({
            "latency": ["p99 < 10s", "max < 10s"],
            "requests.completed{group=echo}": ["> 0"],
            "requests.failed": ["== 0", "rate < 0.01"],
        }),
    );
    assert_eq!(output.status.code(), Some(0), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(json["verdict"], "pass");
    let thresholds = json["thresholds"].as_array().unwrap();
    assert_eq!(thresholds.len(), 5);
    assert!(thresholds.iter().all(|t| t["ok"] == true), "{thresholds:?}");
    assert_eq!(thresholds[2]["group"], "echo");
    assert_eq!(
        thresholds[2]["observed"],
        json["global"]["requests"]["completed"].as_f64().unwrap()
    );
    // A closed loop has no schedule to check
    assert_eq!(json["validity"], serde_json::json!([]));
    let stdout = stdout(&output);
    assert!(stdout.contains("VERDICT: PASS\n"), "{stdout}");
    assert!(stdout.contains("  ok    requests.completed{group=echo}: > 0 (observed "));
}

#[test]
fn a_failed_threshold_exits_2_after_writing_the_results() {
    let (output, json) = run_against_echo(
        Duration::ZERO,
        serde_json::json!({ "type": "closed-loop" }),
        serde_json::json!({ "latency": ["p99 < 10s"], "requests.completed": ["== 0"] }),
    );
    assert_eq!(output.status.code(), Some(2), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(json["verdict"], "thresholds_failed");
    let ok: Vec<&serde_json::Value> =
        json["thresholds"].as_array().unwrap().iter().map(|t| &t["ok"]).collect();
    assert_eq!(ok, [true, false]);
    let stdout = stdout(&output);
    assert!(stdout.contains("VERDICT: THRESHOLDS FAILED\n"), "{stdout}");
    assert!(stdout.contains("  FAIL  requests.completed: == 0 (observed "), "{stdout}");
    // The summary comes before the lines that name the written files
    assert!(stdout.trim_end().ends_with("results.html"), "{stdout}");
}

#[test]
fn a_run_that_misses_its_rate_is_invalid_and_exits_3() {
    // One connection at depth 1 with a 5 ms server sustains 200 requests per
    // second, a tenth of the rate, so most intended sends never go out.
    let (output, json) = run_against_echo(
        Duration::from_millis(5),
        serde_json::json!({ "type": "fixed-rate", "rate": 2000.0 }),
        serde_json::json!({ "requests.completed": ["== 0"] }),
    );
    // Invalid takes precedence over the failed threshold
    assert_eq!(output.status.code(), Some(3), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(json["verdict"], "invalid_run");
    assert_eq!(json["thresholds"][0]["ok"], false);
    let unsent = &json["validity"][0];
    assert_eq!(
        (&unsent["check"], &unsent["group"], &unsent["ok"]),
        (&"unsent".into(), &"echo".into(), &false.into())
    );
    assert!(unsent["observed"].as_f64().unwrap() > 0.5, "{unsent}");
    assert_eq!(unsent["limit"], 0.001);
    assert!(
        unsent["reason"].as_str().unwrap().contains("intended sends never went out"),
        "{unsent}"
    );
    assert_eq!(json["validity"][1]["check"], "late_sends");
    let stdout = stdout(&output);
    assert!(stdout.contains("VERDICT: INVALID RUN\n"), "{stdout}");
    assert!(stdout.contains("  FAIL  unsent{group=echo}: "), "{stdout}");
}

#[test]
fn a_misspelled_metric_fails_before_the_run() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let target = listener.local_addr().unwrap().to_string();
    listener.set_nonblocking(true).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let profile = write_profile(
        dir.path(),
        &target,
        serde_json::json!({ "type": "closed-loop" }),
        serde_json::json!({ "latncy": ["p99 < 1ms"], "latency": ["p99 < 1"] }),
    );
    let output = run_profile(&profile);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr.clone()).unwrap();
    assert!(stderr.starts_with("Error: Invalid thresholds:\n"), "{stderr}");
    assert!(stderr.contains("\"latncy\": unknown metric 'latncy'; metrics: "), "{stderr}");
    assert!(
        stderr.contains("\"latency\" = \"p99 < 1\": latency p99 is a duration"),
        "{stderr}"
    );
    // Nothing ran: no connection and no result files
    assert_eq!(listener.accept().unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
    assert!(!dir.path().join("results.json").exists());
    assert_eq!(stdout(&output), "");
}

#[test]
fn help_lists_the_exit_statuses() {
    let output = Command::new(env!("CARGO_BIN_EXE_xylem")).arg("--help").output().unwrap();
    assert_eq!(output.status.code(), Some(0));
    let help = stdout(&output);
    for line in [
        "Exit status:",
        "  0  The run passed its validity checks and thresholds, or none applied",
        "  2  Thresholds failed: a [thresholds] expression did not hold",
        "  3  Invalid run: a [validity] check failed, so the results do not describe",
    ] {
        assert!(help.lines().any(|actual| actual == line), "{line}: {help}");
    }
}
