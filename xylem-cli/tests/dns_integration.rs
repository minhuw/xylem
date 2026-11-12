//! DNS against Knot DNS through the real CLI
//!
//! Uses the Docker Compose Knot fixture, which serves the zones of
//! Stackbench's knot benchmark (example.test, other.invalid) and a large TXT
//! RRset at bulk.example.test that does not fit an unextended UDP response.

use std::path::Path;
use std::process::Command;

mod common;

use std::net::SocketAddr;

/// Run a one-second DNS profile and return its JSON results
fn run_dns(
    target: SocketAddr,
    transport: &str,
    protocol_config: serde_json::Value,
) -> serde_json::Value {
    let dir = tempfile::tempdir().unwrap();
    let profile = dir.path().join("profile.toml");
    let results = dir.path().join("results.json");
    let config = serde_json::json!({
        "experiment": { "name": "dns", "duration": "1s", "seed": 1 },
        "traffic_groups": [{
            "name": "knot", "threads": [0], "connections_per_thread": 2,
            "max_pending_per_connection": 16, "target": target.to_string(),
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
        knot.addr(),
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
        knot.addr(),
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
        knot.addr(),
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

#[test]
fn dns_tcp_query_pairs_keep_name_and_type_together() {
    let knot = common::knot::KnotGuard::new().expect("Failed to start Knot");
    // The (name, type) pairs knot_client.py checks, as one weighted mix: of
    // them only absent.example.test is NXDOMAIN
    let json = run_dns(
        knot.addr(),
        "tcp",
        serde_json::json!({
            "queries": [
                { "name": "absent.example.test.", "qtype": "A" },
                { "name": "www.example.test.", "qtype": "AAAA" },
                { "name": "alias.example.test.", "qtype": "A" },
                { "pattern": "h{key}.wild.example.test.", "qtype": "A",
                  "keys": { "strategy": "random", "max": 1000 } },
                { "name": "www.other.invalid.", "qtype": "A" },
                { "name": "text.example.test.", "qtype": "TXT", "weight": 3 }
            ],
            "edns": { "udp_size": 1232, "dnssec_ok": false }
        }),
    );
    let requests = &json["global"]["requests"];
    let completed = count(&json, "completed");
    assert!(completed > 1000, "{requests}");
    assert_eq!(count(&json, "sent"), completed, "{requests}");
    assert_eq!(count(&json, "failed"), 0, "{requests}");
    let nxdomain = requests["errors_by_kind"]["NXDOMAIN"].as_u64().unwrap();
    assert_eq!(nxdomain, count(&json, "errors"), "{requests}");
    // 1 of 8 by weight
    assert!((completed / 16..completed / 5).contains(&nxdomain), "{requests}");
}

#[test]
fn dns_udp_query_pairs_draw_name_and_type_together() {
    let knot = common::knot::KnotGuard::new().expect("Failed to start Knot");
    // Only bulk.example.test's TXT RRset is truncated without EDNS. As a
    // pair it is half of the queries; drawn independently of its name, TXT
    // would be asked of bulk in a quarter of them.
    let json = run_dns(
        knot.addr(),
        "udp",
        serde_json::json!({
            "queries": [
                { "name": "bulk.example.test.", "qtype": "TXT" },
                { "name": "text.example.test.", "qtype": "A" }
            ]
        }),
    );
    let requests = &json["global"]["requests"];
    let completed = count(&json, "completed");
    assert!(completed > 100, "{requests}");
    assert_eq!(count(&json, "failed"), 0, "{requests}");
    let truncated = requests["errors_by_kind"]["TC"].as_u64().unwrap();
    assert_eq!(truncated, count(&json, "errors"), "{requests}");
    assert!((completed * 2 / 5..completed * 3 / 5).contains(&truncated), "{requests}");
}
