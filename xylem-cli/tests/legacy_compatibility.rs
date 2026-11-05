//! Deprecated public paths retain their original wire formats during migration.
#![allow(deprecated)]

use serde_json::json;
use xylem_cli::config::OperationsConfig;
use xylem_cli::output::ExperimentResults;

#[test]
fn legacy_operations_keep_flattened_command_parameters() {
    let config: OperationsConfig = serde_json::from_value(json!({
        "strategy": "weighted",
        "commands": [{"name": "mget", "weight": 1.0, "count": 2}]
    }))
    .unwrap();
    let serialized = serde_json::to_value(&config).unwrap();
    assert_eq!(serialized["commands"][0]["count"], 2);
    assert!(serialized["commands"][0].get("params").is_none());
    config.to_redis_selector(Some(42)).unwrap();
}

#[test]
fn legacy_results_keep_flat_json_and_public_writer() {
    let fixture = json!({
        "protocol": "redis", "target": "127.0.0.1:6379", "duration_secs": 1.0,
        "total_requests": 100, "total_tx_bytes": 128, "total_rx_bytes": 256,
        "throughput_rps": 100.0, "throughput_mbps": 0.1,
        "latency": {
            "min_us": 1.0, "max_us": 9.0, "mean_us": 4.0,
            "p50_us": 3.0, "p95_us": 5.0, "p99_us": 6.0,
            "p999_us": 7.0, "p9999_us": 8.0, "p99999_us": 9.0,
            "std_dev_us": 1.0, "confidence_interval_us": 0.5, "sample_count": 100
        }
    });
    let results: ExperimentResults = serde_json::from_value(fixture.clone()).unwrap();
    let output = tempfile::NamedTempFile::new().unwrap();
    results.write_json(output.path().to_str().unwrap()).unwrap();
    let saved: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.path()).unwrap()).unwrap();
    assert_eq!(saved, fixture);
}
