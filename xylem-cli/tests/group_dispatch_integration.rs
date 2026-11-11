//! Group dispatch keeps an HTTP/1.1 run open-loop with one request in flight
//! per connection.
//!
//! Nginx answers `/delay` after about 1 ms. At 4000 req/s Poisson over 32
//! connections, per-connection dispatch gives each connection its own
//! 125 req/s process: whenever its next arrival comes within the service time
//! of its previous one, the arrival waits and goes out late. Group dispatch
//! sends every arrival on an idle connection; about 5 of the 32 are busy at a
//! time, so none waits.
//!
//! Requires Docker:
//! ```bash
//! cargo nextest run --profile integration -E 'binary(group_dispatch_integration)'
//! ```

use std::process::Command;

mod common;

const RATE: f64 = 4000.0;
const DURATION_SECS: f64 = 4.0;
/// Largest late-send fraction of an open-loop run: the default
/// `validity.max_late_send_ratio`. A release build stays near 0.1%; the
/// debug build these tests run stalls once for ~25 ms in either dispatch
/// mode, which makes ~0.7% of the sends late.
const MAX_LATE: f64 = 0.01;

/// The worker thread's CPU: the last one, since CPU 0 tends to be the busiest
/// with interrupts and other work, which would make sends late regardless
fn worker_cpu() -> usize {
    std::thread::available_parallelism().map_or(1, |cpus| cpus.get()) - 1
}

/// Run the `xylem` executable with one HTTP group of 32 connections, one
/// request in flight each, against `/delay`; returns its exit status (0, or
/// 3 for a run its validity checks reject) and its JSON results
fn run(target: &str, policy: serde_json::Value) -> (i32, serde_json::Value) {
    let dir = tempfile::tempdir().unwrap();
    let profile = dir.path().join("profile.toml");
    let results = dir.path().join("results.json");
    let config = serde_json::json!({
        "experiment": { "name": "group-dispatch", "duration": "4s", "seed": 7 },
        "traffic_groups": [{
            "name": "web", "threads": [worker_cpu()], "connections_per_thread": 32,
            "max_pending_per_connection": 1, "target": target, "protocol": "http",
            "protocol_config": { "method": "GET", "path": "/delay" },
            "traffic_policy": policy
        }],
        "output": { "format": "json", "file": results }
    });
    std::fs::write(&profile, toml::to_string(&config).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile)
        .env("RUST_LOG", "off")
        .output()
        .unwrap();
    let code = output.status.code().unwrap();
    assert!(
        matches!(code, 0 | 3),
        "exit {code}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    (code, serde_json::from_str(&std::fs::read_to_string(results).unwrap()).unwrap())
}

/// The fields Stackbench reads, printed for the test log
fn summary(json: &serde_json::Value) -> (f64, f64, u64, u64) {
    let global = &json["global"];
    let requests = &global["requests"];
    let throughput = global["throughput_rps"].as_f64().unwrap();
    let late_fraction =
        requests["late_sends"].as_u64().unwrap() as f64 / requests["sent"].as_u64().unwrap() as f64;
    println!(
        "throughput {throughput:.0} req/s, late {:.3}%, requests {}, latency p50 {} p99 {} p999 {} us",
        late_fraction * 100.0,
        requests,
        global["latency"]["p50_us"],
        global["latency"]["p99_us"],
        global["latency"]["p999_us"],
    );
    (
        throughput,
        late_fraction,
        requests["unsent"].as_u64().unwrap(),
        requests["failed"].as_u64().unwrap() + requests["errors"].as_u64().unwrap(),
    )
}

#[test]
fn group_dispatch_integration_poisson_http_has_no_late_sends_where_per_connection_has() {
    let nginx = common::nginx::NginxGuard::new().expect("Failed to start nginx");
    let target = nginx.addr().to_string();

    let (_, per_connection) = run(&target, serde_json::json!({ "type": "poisson", "rate": RATE }));
    let (group_code, group) = run(
        &target,
        serde_json::json!({ "type": "poisson", "rate": RATE, "dispatch": "group" }),
    );
    assert_eq!(per_connection["traffic_groups"][0]["dispatch"], "connection");
    assert_eq!(group["traffic_groups"][0]["dispatch"], "group");

    let (_, connection_late, _, connection_failed) = summary(&per_connection);
    let (throughput, group_late, unsent, group_failed) = summary(&group);
    assert_eq!((connection_failed, group_failed), (0, 0));
    // Per connection, an arrival within ~1 ms of the previous one on its
    // connection waits: about 1 - e^(-125/s · 1 ms) ≈ 12% of them
    assert!(connection_late > 0.03, "per-connection late fraction {connection_late}");
    assert!(group_late < MAX_LATE, "group late fraction {group_late}");
    assert!(group_late < connection_late / 5.0, "{group_late} vs {connection_late}");
    // The achieved rate is the offered one
    assert!(
        (throughput - RATE).abs() < RATE * 0.05,
        "group throughput {throughput} for {RATE} offered"
    );
    assert!(unsent < (RATE * DURATION_SECS * 0.005) as u64, "{unsent} unsent");
    // The validity checks accept the group-dispatched run
    assert_eq!(group_code, 0, "verdict {}", group["verdict"]);
}

#[test]
fn group_dispatch_integration_fixed_rate_http_keeps_its_rate() {
    let nginx = common::nginx::NginxGuard::new().expect("Failed to start nginx");
    let (code, group) = run(
        &nginx.addr().to_string(),
        serde_json::json!({ "type": "fixed-rate", "rate": RATE, "dispatch": "group" }),
    );
    let (throughput, late, unsent, failed) = summary(&group);
    assert_eq!(failed, 0);
    assert!(late < MAX_LATE, "late fraction {late}");
    assert!((throughput - RATE).abs() < RATE * 0.05, "throughput {throughput}");
    assert!(unsent < (RATE * DURATION_SECS * 0.005) as u64, "{unsent} unsent");
    // Nearly every arrival completed in the window
    let completed = group["traffic_groups"][0]["stats"]["requests"]["completed"].as_u64().unwrap();
    assert!(completed as f64 > RATE * DURATION_SECS * 0.95, "{completed} completed");
    assert_eq!(code, 0, "verdict {}", group["verdict"]);
}
