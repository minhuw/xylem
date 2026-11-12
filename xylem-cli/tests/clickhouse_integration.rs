//! ClickHouse native protocol against clickhouse-server through the real CLI
//!
//! Uses the Docker Compose ClickHouse fixture: the version Stackbench builds,
//! with a 100,000-row table `bench.items`. Over TCP only: ClickHouse has no
//! Unix socket listener for its native protocol.

use std::process::Command;

mod common;

use common::clickhouse::{ClickHouseGuard, ITEMS, PASSWORD, USER};

/// A ClickHouse profile of `duration` with `connections` closed-loop
/// connections to `target`
fn profile(
    target: std::net::SocketAddr,
    duration: &str,
    connections: usize,
    protocol_config: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "experiment": { "name": "clickhouse", "duration": duration, "seed": 1 },
        "traffic_groups": [{
            "name": "clickhouse", "threads": [0], "connections_per_thread": connections,
            "max_pending_per_connection": 1, "target": target.to_string(),
            "transport": "tcp", "protocol": "clickhouse",
            "traffic_policy": { "type": "closed-loop" },
            "protocol_config": protocol_config
        }]
    })
}

/// The protocol_config of the fixture's database and user with `queries`
fn login(queries: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "database": "bench", "user": USER, "password": PASSWORD, "queries": queries
    })
}

fn count(json: &serde_json::Value, key: &str) -> u64 {
    json["global"]["requests"][key].as_u64().unwrap()
}

#[test]
fn clickhouse_query_mix_completes() {
    let clickhouse = ClickHouseGuard::new().expect("Failed to start ClickHouse");
    let json = common::run_profile(profile(
        clickhouse.addr(),
        "2s",
        4,
        login(serde_json::json!([
            { "sql": "SELECT name, price, tags, note FROM items WHERE id = 4242", "weight": 4 },
            { "sql": "SELECT 1" },
            { "sql": "SELECT category, count(), avg(price), max(created), groupArray(3)(name) \
                      FROM items GROUP BY category ORDER BY category" },
            { "sql": "SELECT number % 7 AS k, sum(number) FROM numbers(1000) GROUP BY k \
                      WITH TOTALS ORDER BY k SETTINGS extremes = 1" },
            { "sql": "SELECT * FROM items ORDER BY id DESC LIMIT 100" }
        ])),
    ));
    let requests = &json["global"]["requests"];
    assert_eq!(json["target"]["protocol"], "clickhouse");
    assert!(count(&json, "completed") > 100, "{requests}");
    assert_eq!(count(&json, "sent"), count(&json, "completed"), "{requests}");
    assert_eq!(count(&json, "errors"), 0, "{requests}");
    assert_eq!(count(&json, "failed"), 0, "{requests}");
}

#[test]
fn clickhouse_large_results_stream() {
    let clickhouse = ClickHouseGuard::new().expect("Failed to start ClickHouse");
    // Every row of the table, in many blocks, with settings
    let mut config = login(serde_json::json!([{ "sql": "SELECT * FROM items" }]));
    config["settings"] = serde_json::json!({ "max_block_size": 8192, "max_threads": 2 });
    let json = common::run_profile(profile(clickhouse.addr(), "2s", 2, config));
    let global = &json["global"];
    let requests = &global["requests"];
    let completed = count(&json, "completed");
    assert!(completed >= 2, "{requests}");
    assert_eq!(count(&json, "errors"), 0, "{requests}");
    // Each response carries the whole table: over 30 bytes per row
    let seconds = json["experiment"]["measured_duration_secs"].as_f64().unwrap();
    let bytes = global["throughput_mbps"].as_f64().unwrap() * 1e6 / 8.0 * seconds;
    let per_response = bytes / global["total_requests"].as_f64().unwrap();
    assert!(per_response > 30.0 * ITEMS as f64, "{per_response} bytes per response");
}

#[test]
fn clickhouse_exceptions_count_by_error_code() {
    let clickhouse = ClickHouseGuard::new().expect("Failed to start ClickHouse");
    let json = common::run_profile(profile(
        clickhouse.addr(),
        "1s",
        2,
        login(serde_json::json!([
            { "sql": "SELECT count() FROM items" },
            { "sql": "SELECT * FROM missing_table" },
            { "sql": "SELECT throwIf(1, 'boom')" }
        ])),
    ));
    let requests = &json["global"]["requests"];
    let kinds = requests["errors_by_kind"].as_object().unwrap();
    let kind = |name: &str| kinds.get(name).and_then(serde_json::Value::as_u64).unwrap_or(0);
    // UNKNOWN_TABLE and FUNCTION_THROW_IF_VALUE_IS_NON_ZERO
    assert!(kind("60") > 0 && kind("395") > 0, "{requests}");
    assert_eq!(kind("60") + kind("395"), count(&json, "errors"), "{requests}");
    assert!(count(&json, "errors") < count(&json, "completed"), "{requests}");
    assert_eq!(count(&json, "sent"), count(&json, "completed"), "{requests}");
}

#[test]
fn clickhouse_wrong_password_fails_setup() {
    let clickhouse = ClickHouseGuard::new().expect("Failed to start ClickHouse");
    let mut config = profile(
        clickhouse.addr(),
        "1s",
        1,
        serde_json::json!({ "user": USER, "password": "not-the-password-x7", "query": "SELECT 1" }),
    );
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("profile.toml");
    config["output"] = serde_json::json!({ "format": "json", "file": dir.path().join("r.json") });
    std::fs::write(&path, toml::to_string(&config).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&path)
        .env("RUST_LOG", "warn")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains("ClickHouse refused the connection"), "{stderr}");
    assert!(stderr.contains("(code 516)"), "{stderr}");
    assert!(!stderr.contains("not-the-password-x7"), "{stderr}");
}
