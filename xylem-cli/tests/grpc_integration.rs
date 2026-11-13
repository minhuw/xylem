//! gRPC unary calls through the real CLI, over TCP and a Unix socket
//!
//! The gRPC server is `tests/grpc/server.py` (grpcio). With
//! `XYLEM_TFS_MODEL` set to a SavedModel base directory, such as
//! Stackbench's ranking model, and `XYLEM_TFS_REQUESTS` to PredictRequests
//! for it in JSON, such as Stackbench's validation.json, one test also calls
//! TF Serving's Predict on the official tensorflow/serving image.

mod common;

use common::h2c::H2cGuard;
use std::process::Command;

/// Run a one-second gRPC profile against `target` over `transport`
fn run(target: &str, transport: &str, protocol_config: serde_json::Value) -> serde_json::Value {
    common::run_profile(serde_json::json!({
        "experiment": { "name": "grpc", "duration": "1s", "seed": 1 },
        "traffic_groups": [{
            "name": "grpc", "threads": [0], "connections_per_thread": 2,
            "max_pending_per_connection": 8, "target": target,
            "transport": transport, "protocol": "grpc",
            "traffic_policy": { "type": "closed-loop" },
            "protocol_config": protocol_config
        }]
    }))
}

fn targets(guard: &H2cGuard) -> [(String, &'static str); 2] {
    [
        (guard.addr().to_string(), "tcp"),
        (guard.socket().display().to_string(), "unix"),
    ]
}

fn count(json: &serde_json::Value, key: &str) -> u64 {
    json["global"]["requests"][key].as_u64().unwrap()
}

/// Assert that `json` completed `at_least` calls, all successful
fn assert_clean(json: &serde_json::Value, at_least: u64) {
    let requests = &json["global"]["requests"];
    assert!(count(json, "completed") >= at_least, "{requests}");
    assert_eq!(count(json, "sent"), count(json, "completed"), "{requests}");
    assert_eq!(count(json, "errors"), 0, "{requests}");
    assert_eq!(count(json, "failed"), 0, "{requests}");
}

#[test]
fn grpc_unary_calls_over_tcp_and_unix() {
    let server = H2cGuard::grpc().expect("Failed to start the gRPC server");
    for (target, transport) in targets(&server) {
        let echo =
            serde_json::json!({ "call": "xylem.Test/Echo", "request_hex": "0a0568656c6c6f" });
        let json = run(&target, transport, echo);
        assert_eq!(json["target"]["transport"], transport);
        assert_clean(&json, 200);
        // 300 KB responses: the receive windows must be given back
        let big = serde_json::json!({ "call": "/xylem.Test/Big" });
        assert_clean(&run(&target, transport, big), 50);
    }
}

#[test]
fn grpc_statuses_are_error_replies() {
    let server = H2cGuard::grpc().expect("Failed to start the gRPC server");
    for (call, kind) in [("xylem.Test/Fail", "14"), ("xylem.Test/Absent", "12")] {
        let json = run(&server.addr().to_string(), "tcp", serde_json::json!({ "call": call }));
        let requests = &json["global"]["requests"];
        assert!(count(&json, "completed") > 100, "{requests}");
        assert_eq!(count(&json, "errors"), count(&json, "completed"), "{requests}");
        assert_eq!(
            requests["errors_by_kind"][kind].as_u64(),
            Some(count(&json, "errors")),
            "{requests}"
        );
    }
}

/// TF Serving's Predict on the model at `XYLEM_TFS_MODEL`, with the
/// requests in `XYLEM_TFS_REQUESTS` turned into PredictRequest bytes by
/// `tools/tfs-predict-request.py`
#[test]
fn grpc_tensorflow_serving_predict() {
    let (Ok(model), Ok(requests)) =
        (std::env::var("XYLEM_TFS_MODEL"), std::env::var("XYLEM_TFS_REQUESTS"))
    else {
        eprintln!(
            "Skipped: set XYLEM_TFS_MODEL to a SavedModel base directory and \
             XYLEM_TFS_REQUESTS to PredictRequests for it in JSON"
        );
        return;
    };
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let messages = dir.path().join("predict.bin");
    let status = Command::new("python3")
        .arg("-I")
        .arg(root.join("tools/tfs-predict-request.py"))
        .arg("--framed")
        .arg(requests)
        .arg(&messages)
        .status()
        .expect("run python3, which the flake's devShell provides");
    assert!(status.success(), "tfs-predict-request.py failed");
    let server = common::tfs::TfsGuard::new(&model).expect("Failed to start TF Serving");
    let json = run(
        &server.addr().to_string(),
        "tcp",
        serde_json::json!({
            "call": "tensorflow.serving.PredictionService/Predict",
            "messages_file": messages
        }),
    );
    assert_clean(&json, 100);
}
