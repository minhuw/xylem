//! DNS against Knot DNS through the real CLI
//!
//! Uses the Docker Compose Knot fixture, which serves the zones of
//! Stackbench's knot benchmark (example.test, other.invalid) and a large TXT
//! RRset at bulk.example.test that does not fit an unextended UDP response.

use std::path::Path;
use std::process::Command;

mod common;

/// Run a one-second DNS profile and return its JSON results
fn run_dns(port: u16, transport: &str, protocol_config: serde_json::Value) -> serde_json::Value {
    let dir = tempfile::tempdir().unwrap();
    let profile = dir.path().join("profile.toml");
    let results = dir.path().join("results.json");
    let config = serde_json::json!({
        "experiment": { "name": "dns", "duration": "1s", "seed": 1 },
        "traffic_groups": [{
            "name": "knot", "threads": [0], "connections_per_thread": 2,
            "max_pending_per_connection": 16, "target": format!("127.0.0.1:{port}"),
            "transport": transport, "protocol": "dns",
            "traffic_policy": { "type": "closed-loop" },
            "protocol_config": protocol_config
        }],
        "output": { "format": "json", "file": results }
    });
    std::fs::write(&profile, toml::to_string(&config).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile)
        .env("RUST_LOG", "warn")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "xylem failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    read_json(&results)
}

fn read_json(path: &Path) -> serde_json::Value {
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn count(json: &serde_json::Value, key: &str) -> u64 {
    json["global"]["requests"][key].as_u64().unwrap()
}

#[test]
fn dns_tcp_pipelined_query_mix_completes() {
    let knot = common::knot::KnotGuard::new().expect("Failed to start Knot");
    // The names and types knot_client.py checks, NXDOMAIN included, and the
    // 3.4 KB TXT RRset, which arrives over several reads
    let json = run_dns(
        knot.get_port(),
        "tcp",
        serde_json::json!({
            "names": [
                { "name": "www.example.test.", "weight": 4 },
                { "name": "alias.example.test." },
                { "name": "text.example.test." },
                { "name": "absent.example.test." },
                { "name": "www.other.invalid." },
                { "name": "bulk.example.test." },
                { "pattern": "h{key}.wild.example.test.",
                  "keys": { "strategy": "random", "max": 1000 } }
            ],
            "qtypes": [
                { "qtype": "A", "weight": 4 }, { "qtype": "AAAA" }, { "qtype": "TXT" },
                { "qtype": "MX" }, { "qtype": "NS" }, { "qtype": "SOA" }, { "qtype": "ANY" }
            ],
            "edns": { "udp_size": 1232, "dnssec_ok": true },
            "nxdomain": "success"
        }),
    );
    let requests = &json["global"]["requests"];
    assert_eq!(json["target"]["transport"], "tcp");
    assert!(count(&json, "completed") > 1000, "{requests}");
    assert_eq!(count(&json, "sent"), count(&json, "completed"), "{requests}");
    assert_eq!(count(&json, "errors"), 0, "{requests}");
    assert_eq!(count(&json, "failed"), 0, "{requests}");
}

#[test]
fn dns_rcodes_are_error_replies_by_name() {
    let knot = common::knot::KnotGuard::new().expect("Failed to start Knot");
    // Knot answers NXDOMAIN in its zones and REFUSED outside them
    let json = run_dns(
        knot.get_port(),
        "tcp",
        serde_json::json!({
            "names": [{ "name": "absent.example.test." }, { "name": "www.example.org." }],
            "recursion_desired": false
        }),
    );
    let requests = &json["global"]["requests"];
    assert!(count(&json, "completed") > 100, "{requests}");
    assert_eq!(count(&json, "errors"), count(&json, "completed"), "{requests}");
    let kinds = requests["errors_by_kind"].as_object().unwrap();
    let kind = |name: &str| kinds.get(name).and_then(serde_json::Value::as_u64).unwrap_or(0);
    assert!(kind("NXDOMAIN") > 0 && kind("REFUSED") > 0, "{requests}");
    assert_eq!(kind("NXDOMAIN") + kind("REFUSED"), count(&json, "errors"), "{requests}");
}

#[test]
fn dns_udp_counts_truncated_responses() {
    let knot = common::knot::KnotGuard::new().expect("Failed to start Knot");
    // Without EDNS a UDP response holds 512 bytes: bulk.example.test's TXT
    // RRset comes back truncated, text.example.test's does not
    let json = run_dns(
        knot.get_udp_port(),
        "udp",
        serde_json::json!({
            "names": [{ "name": "text.example.test." }, { "name": "bulk.example.test." }],
            "qtypes": [{ "qtype": "TXT" }]
        }),
    );
    let requests = &json["global"]["requests"];
    assert_eq!(json["target"]["transport"], "udp");
    assert!(count(&json, "completed") > 100, "{requests}");
    assert_eq!(
        count(&json, "sent"),
        count(&json, "completed") + count(&json, "timed_out") + count(&json, "failed")
    );
    assert_eq!(count(&json, "failed"), 0, "{requests}");
    // Loopback loses few datagrams, if any
    assert!(count(&json, "timed_out") * 100 <= count(&json, "sent"), "{requests}");
    let truncated = requests["errors_by_kind"]["TC"].as_u64().unwrap();
    assert_eq!(truncated, count(&json, "errors"), "{requests}");
    assert!(truncated > 0 && truncated < count(&json, "completed"), "{requests}");
}
