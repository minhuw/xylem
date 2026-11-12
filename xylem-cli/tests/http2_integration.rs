//! HTTP/2 (h2c with prior knowledge) against nginx through the real CLI,
//! over TCP and a Unix socket
//!
//! nginx serves `tests/http2/nginx.conf` on a plain listener with
//! `http2 on`, so it speaks HTTP/2 from the first byte.

mod common;

use common::h2c::H2cGuard;

/// Run a one-second profile of `protocol` against `guard` over `transport`
fn run(
    guard: &H2cGuard,
    transport: &str,
    protocol: &str,
    protocol_config: serde_json::Value,
) -> serde_json::Value {
    let target = match transport {
        "unix" => guard.socket().display().to_string(),
        _ => format!("127.0.0.1:{}", guard.port()),
    };
    common::run_profile(serde_json::json!({
        "experiment": { "name": protocol, "duration": "1s", "seed": 1 },
        "traffic_groups": [{
            "name": "h2", "threads": [0], "connections_per_thread": 2,
            "max_pending_per_connection": 16, "target": target,
            "transport": transport, "protocol": protocol,
            "traffic_policy": { "type": "closed-loop" },
            "protocol_config": protocol_config
        }]
    }))
}

fn count(json: &serde_json::Value, key: &str) -> u64 {
    json["global"]["requests"][key].as_u64().unwrap()
}

/// Assert that `json` completed `at_least` requests, all successful
fn assert_clean(json: &serde_json::Value, at_least: u64) {
    let requests = &json["global"]["requests"];
    assert!(count(json, "completed") >= at_least, "{requests}");
    assert_eq!(count(json, "sent"), count(json, "completed"), "{requests}");
    assert_eq!(count(json, "errors"), 0, "{requests}");
    assert_eq!(count(json, "failed"), 0, "{requests}");
}

#[test]
fn http2_get_and_post_over_tcp_and_unix() {
    let nginx = H2cGuard::nginx().expect("Failed to start nginx");
    for transport in ["tcp", "unix"] {
        let json = run(&nginx, transport, "http2", serde_json::json!({}));
        assert_eq!(json["target"]["transport"], transport);
        assert_clean(&json, 1000);
        let post = serde_json::json!({ "method": "POST", "body_size": 1000 });
        assert_clean(&run(&nginx, transport, "http2", post), 1000);
    }
}

#[test]
fn http2_large_responses_give_their_windows_back() {
    let nginx = H2cGuard::nginx().expect("Failed to start nginx");
    // 300 KB each: 32 streams in flight take far more than the connection's
    // 16 MiB window within a second, so it must be given back
    for transport in ["tcp", "unix"] {
        let json = run(&nginx, transport, "http2", serde_json::json!({ "path": "/big" }));
        assert_clean(&json, 100);
        let bytes = json["global"]["throughput_mbps"].as_f64().unwrap() * 1e6 / 8.0;
        let per_response = bytes / json["global"]["throughput_rps"].as_f64().unwrap();
        assert!(per_response > 300_000.0, "{per_response} bytes per response");
    }
}

#[test]
fn http2_statuses_other_than_200_are_error_replies() {
    let nginx = H2cGuard::nginx().expect("Failed to start nginx");
    let json = run(&nginx, "tcp", "http2", serde_json::json!({ "path": "/missing" }));
    let requests = &json["global"]["requests"];
    assert!(count(&json, "completed") > 100, "{requests}");
    assert_eq!(count(&json, "errors"), count(&json, "completed"), "{requests}");
    assert_eq!(
        requests["errors_by_kind"]["404"].as_u64(),
        Some(count(&json, "errors")),
        "{requests}"
    );
}
