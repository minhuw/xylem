//! A busy-polling closed-loop HTTP run against Nginx: it completes its
//! requests like a run that waits in its polls, records its budget in the
//! JSON results, and keeps its worker's core busy for the whole window.
//!
//! Requires Docker:
//! ```bash
//! cargo nextest run --profile integration -E 'binary(busy_poll_integration)'
//! ```

use std::process::Command;

mod common;

const DURATION_SECS: f64 = 2.0;
/// Longer than the run, so the worker never waits in a poll
const BUSY_POLL_US: u64 = 10_000_000;

/// The worker thread's CPU: the last one, since CPU 0 tends to be the busiest
fn worker_cpu() -> usize {
    std::thread::available_parallelism().map_or(1, |cpus| cpus.get()) - 1
}

/// CPU seconds, user and system, of this process's waited-for children
/// (`cutime` and `cstime` of /proc/self/stat, in USER_HZ = 100 ticks)
fn children_cpu_secs() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
    // The fields after the parenthesised command name, from field 3 (state)
    let fields: Vec<&str> = stat[stat.rfind(')').unwrap() + 2..].split(' ').collect();
    let ticks = |field: usize| fields[field - 3].parse::<u64>().unwrap();
    (ticks(16) + ticks(17)) as f64 / 100.0
}

/// Run the `xylem` executable with one closed-loop HTTP connection, one
/// request in flight, and `busy_poll_us`; returns its JSON results and the
/// CPU seconds it used
fn run(target: &str, busy_poll_us: Option<u64>) -> (serde_json::Value, f64) {
    let dir = tempfile::tempdir().unwrap();
    let profile = dir.path().join("profile.toml");
    let results = dir.path().join("results.json");
    let mut config = serde_json::json!({
        "experiment": { "name": "busy-poll", "duration": "2s", "seed": 7 },
        "traffic_groups": [{
            "name": "web", "threads": [worker_cpu()], "connections_per_thread": 1,
            "max_pending_per_connection": 1, "target": target, "protocol": "http",
            "protocol_config": { "method": "GET", "path": "/" },
            "traffic_policy": { "type": "closed-loop" }
        }],
        "output": { "format": "json", "file": results }
    });
    if let Some(budget) = busy_poll_us {
        config["experiment"]["busy_poll_us"] = budget.into();
    }
    std::fs::write(&profile, toml::to_string(&config).unwrap()).unwrap();
    let cpu_before = children_cpu_secs();
    let output = Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile)
        .env("RUST_LOG", "off")
        .output()
        .unwrap();
    let cpu = children_cpu_secs() - cpu_before;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let json = serde_json::from_str(&std::fs::read_to_string(results).unwrap()).unwrap();
    (json, cpu)
}

#[test]
fn busy_poll_integration_closed_loop_http_completes_and_spins_its_core() {
    let nginx = common::nginx::NginxGuard::new().expect("Failed to start nginx");
    let target = nginx.addr().to_string();
    let (waiting, waiting_cpu) = run(&target, None);
    let (busy, busy_cpu) = run(&target, Some(BUSY_POLL_US));

    assert_eq!(waiting["experiment"]["busy_poll_us"], serde_json::Value::Null);
    assert_eq!(busy["experiment"]["busy_poll_us"], BUSY_POLL_US);
    for (json, cpu) in [(&waiting, waiting_cpu), (&busy, busy_cpu)] {
        let requests = &json["global"]["requests"];
        let count = |key: &str| requests[key].as_u64().unwrap();
        println!(
            "busy_poll_us {}: {:.0} req/s, latency mean {} p99 {} us, CPU {cpu:.2} s",
            json["experiment"]["busy_poll_us"],
            json["global"]["throughput_rps"].as_f64().unwrap(),
            json["global"]["latency"]["mean_us"],
            json["global"]["latency"]["p99_us"],
        );
        assert!(count("completed") > 100, "{requests}");
        assert_eq!(count("sent"), count("completed"), "{requests}");
        assert_eq!((count("timed_out"), count("failed"), count("errors")), (0, 0, 0));
        assert_eq!(json["verdict"], "pass");
    }
    // The spinning worker keeps its core busy through the window
    assert!(
        busy_cpu > DURATION_SECS * 0.8,
        "{busy_cpu} CPU seconds in a {DURATION_SECS} s run"
    );
}
