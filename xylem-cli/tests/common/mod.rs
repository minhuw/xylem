//! Common test utilities for integration tests
//!
//! This module provides guards for Docker-based test services and shared test helpers.

#![allow(dead_code)]

// Docker compose fixture shared by the service guards
mod compose;

// Docker-based service guards
pub mod knot;
pub mod masstree;
pub mod memcached;
pub mod multi_protocol;
pub mod nginx;
pub mod redis;
pub mod redis_acl;
pub mod redis_auth;

use std::time::Duration;
use xylem_core::stats::{SamplingPolicy, TupleStatsCollector};

/// Create a TupleStatsCollector for testing with a single group (group_id=0)
///
/// The global view uses the default sampling policy; group 0 keeps up to
/// 1,000,000 latency samples per entry at full rate.
pub fn create_test_stats() -> TupleStatsCollector {
    let mut stats = TupleStatsCollector::new(SamplingPolicy::default(), Duration::from_secs(1));
    stats.register_group_policy(0, SamplingPolicy::Limited { max_samples: 1_000_000, rate: 1.0 });
    stats
}

/// Run the xylem executable on the profile `config` (with its `output`
/// replaced by JSON to a temporary file) and return the JSON results.
/// Fails the test if the run fails.
pub fn run_profile(mut config: serde_json::Value) -> serde_json::Value {
    let dir = tempfile::tempdir().unwrap();
    let profile = dir.path().join("profile.toml");
    let results = dir.path().join("results.json");
    config["output"] = serde_json::json!({ "format": "json", "file": results });
    std::fs::write(&profile, toml::to_string(&config).unwrap()).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile)
        .env("RUST_LOG", "warn")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "xylem failed ({}): {}{}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_str(&std::fs::read_to_string(&results).unwrap()).unwrap()
}
