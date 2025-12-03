//! Integration tests for rich output system

use std::time::Duration;
use xylem_cli::output::{html, DetailedExperimentResults};
use xylem_core::stats::{GroupStatsCollector, SamplingPolicy};
use xylem_core::traffic_group::{PolicyConfig, TrafficGroupConfig};

#[test]
fn test_detailed_json_output_generation() {
    // Create mock group stats collector with data
    let mut group_stats = GroupStatsCollector::new();

    let policy1 = SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 };
    let policy2 = SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 };

    group_stats.register_group(0, &policy1);
    group_stats.register_group(1, &policy2);

    // Add latency samples for group 0
    for _ in 0..500 {
        group_stats.record_latency(0, Duration::from_micros(100));
    }

    // Add latency samples for group 1
    for _ in 0..300 {
        group_stats.record_latency(1, Duration::from_micros(250));
    }

    // Add metadata for both groups
    group_stats.set_group_metadata(
        0,
        serde_json::json!({
            "commands": {
                "GET": 400,
                "SET": 100
            }
        }),
    );

    group_stats.set_group_metadata(
        1,
        serde_json::json!({
            "commands": {
                "GET": 200,
                "SET": 100
            },
            "redirects": {
                "moved": 5
            }
        }),
    );

    // Create traffic group configs with required protocol_config
    let protocol_config = Some(serde_json::json!({
        "keys": {"strategy": "sequential", "start": 0, "value_size": 64}
    }));

    let traffic_groups = vec![
        TrafficGroupConfig {
            name: "fast-group".to_string(),
            threads: vec![0, 1],
            connections_per_thread: 10,
            max_pending_per_connection: 100,
            protocol: "redis".to_string(),
            target: "127.0.0.1:6379".to_string(),
            transport: "tcp".to_string(),
            traffic_policy: PolicyConfig::ClosedLoop,
            sampling_policy: policy1,
            protocol_config: protocol_config.clone(),
        },
        TrafficGroupConfig {
            name: "slow-group".to_string(),
            threads: vec![2],
            connections_per_thread: 5,
            max_pending_per_connection: 50,
            protocol: "redis-cluster".to_string(),
            target: "127.0.0.1:6380".to_string(),
            transport: "tcp".to_string(),
            traffic_policy: PolicyConfig::FixedRate { rate: 1000.0 },
            sampling_policy: policy2,
            protocol_config: protocol_config.clone(),
        },
    ];

    // Generate detailed results
    let results = DetailedExperimentResults::from_group_stats(
        "Integration Test".to_string(),
        Some("Testing detailed JSON output".to_string()),
        Some(42),
        "127.0.0.1:6379".to_string(),
        "redis".to_string(),
        "tcp".to_string(),
        Duration::from_secs(10),
        &group_stats,
        &traffic_groups,
    );

    // Verify experiment metadata
    assert_eq!(results.experiment.name, "Integration Test");
    assert_eq!(results.experiment.description, Some("Testing detailed JSON output".to_string()));
    assert_eq!(results.experiment.seed, Some(42));
    assert_eq!(results.experiment.duration_secs, 10.0);

    // Verify target metadata
    assert_eq!(results.target.address, "127.0.0.1:6379");
    assert_eq!(results.target.protocol, "redis");
    assert_eq!(results.target.transport, "tcp");

    // Verify global stats
    assert_eq!(results.global.total_requests, 800); // 500 + 300
    assert!(results.global.throughput_rps > 0.0);
    assert!(results.global.latency.mean_us > 0.0);

    // Verify traffic groups
    assert_eq!(results.traffic_groups.len(), 2);

    let group0 = &results.traffic_groups[0];
    assert_eq!(group0.id, 0);
    assert_eq!(group0.name, "fast-group");
    assert_eq!(group0.protocol, "redis");
    assert_eq!(group0.threads, vec![0, 1]);
    assert_eq!(group0.connections, 20); // 2 threads * 10 conns
    assert_eq!(group0.policy, "closed-loop");
    assert_eq!(group0.stats.total_requests, 500);
    assert!(group0.protocol_metadata.is_some());

    let group1 = &results.traffic_groups[1];
    assert_eq!(group1.id, 1);
    assert_eq!(group1.name, "slow-group");
    assert_eq!(group1.protocol, "redis-cluster");
    assert_eq!(group1.stats.total_requests, 300);
    assert!(group1.protocol_metadata.is_some());

    // Verify metadata content
    let metadata0 = group0.protocol_metadata.as_ref().unwrap();
    assert_eq!(metadata0["commands"]["GET"], 400);
    assert_eq!(metadata0["commands"]["SET"], 100);

    let metadata1 = group1.protocol_metadata.as_ref().unwrap();
    assert_eq!(metadata1["commands"]["GET"], 200);
    assert_eq!(metadata1["redirects"]["moved"], 5);
}

#[test]
fn test_json_serialization_deserialization() {
    let mut group_stats = GroupStatsCollector::new();
    let policy = SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 };

    group_stats.register_group(0, &policy);

    for _ in 0..100 {
        group_stats.record_latency(0, Duration::from_micros(100));
    }

    group_stats.set_group_metadata(0, serde_json::json!({"test": "data"}));

    let protocol_config = Some(serde_json::json!({
        "keys": {"strategy": "sequential", "start": 0, "value_size": 64}
    }));

    let traffic_groups = vec![TrafficGroupConfig {
        name: "test".to_string(),
        threads: vec![0],
        connections_per_thread: 1,
        max_pending_per_connection: 1,
        protocol: "redis".to_string(),
        target: "127.0.0.1:6379".to_string(),
        transport: "tcp".to_string(),
        traffic_policy: PolicyConfig::ClosedLoop,
        sampling_policy: policy,
        protocol_config,
    }];

    let results = DetailedExperimentResults::from_group_stats(
        "Test".to_string(),
        None,
        None,
        "127.0.0.1:6379".to_string(),
        "redis".to_string(),
        "tcp".to_string(),
        Duration::from_secs(1),
        &group_stats,
        &traffic_groups,
    );

    // Serialize to JSON
    let json = serde_json::to_string(&results).expect("Failed to serialize");

    // Deserialize back
    let deserialized: DetailedExperimentResults =
        serde_json::from_str(&json).expect("Failed to deserialize");

    // Verify key fields
    assert_eq!(deserialized.experiment.name, "Test");
    assert_eq!(deserialized.traffic_groups.len(), 1);
    assert_eq!(deserialized.traffic_groups[0].name, "test");
}

#[test]
fn test_html_report_generation() {
    let mut group_stats = GroupStatsCollector::new();
    let policy = SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 };

    group_stats.register_group(0, &policy);

    for _ in 0..100 {
        group_stats.record_latency(0, Duration::from_micros(150));
    }

    let protocol_config = Some(serde_json::json!({
        "keys": {"strategy": "sequential", "start": 0, "value_size": 64}
    }));

    let traffic_groups = vec![TrafficGroupConfig {
        name: "test-group".to_string(),
        threads: vec![0],
        connections_per_thread: 10,
        max_pending_per_connection: 100,
        protocol: "redis".to_string(),
        target: "127.0.0.1:6379".to_string(),
        transport: "tcp".to_string(),
        traffic_policy: PolicyConfig::ClosedLoop,
        sampling_policy: policy,
        protocol_config,
    }];

    let results = DetailedExperimentResults::from_group_stats(
        "HTML Test".to_string(),
        Some("Testing HTML generation".to_string()),
        Some(123),
        "127.0.0.1:6379".to_string(),
        "redis".to_string(),
        "tcp".to_string(),
        Duration::from_secs(5),
        &group_stats,
        &traffic_groups,
    );

    // Generate HTML to temp file
    let temp_path = "/tmp/xylem_test_report.html";
    let result = html::generate_html_report(&results, temp_path);

    assert!(result.is_ok(), "HTML generation failed: {:?}", result.err());

    // Verify file was created
    assert!(std::path::Path::new(temp_path).exists(), "HTML file was not created");

    // Read and verify HTML content
    let html_content = std::fs::read_to_string(temp_path).expect("Failed to read generated HTML");

    // Verify essential HTML elements
    assert!(html_content.contains("<!DOCTYPE html>"));
    assert!(html_content.contains("<title>Xylem Report: HTML Test</title>"));
    assert!(html_content.contains("HTML Test"));
    assert!(html_content.contains("Testing HTML generation"));
    assert!(html_content.contains("127.0.0.1:6379"));
    assert!(html_content.contains("test-group"));
    assert!(html_content.contains("chart.js")); // Chart library included
    assert!(html_content.contains("canvas")); // Chart canvases present

    // Clean up
    std::fs::remove_file(temp_path).ok();
}

#[test]
fn test_per_group_statistics_isolation() {
    let mut group_stats = GroupStatsCollector::new();

    let policy1 = SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 };
    let policy2 = SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 };

    group_stats.register_group(0, &policy1);
    group_stats.register_group(1, &policy2);

    // Group 0: Fast responses
    for _ in 0..1000 {
        group_stats.record_latency(0, Duration::from_micros(50));
    }

    // Group 1: Slow responses
    for _ in 0..500 {
        group_stats.record_latency(1, Duration::from_micros(500));
    }

    let protocol_config = Some(serde_json::json!({
        "keys": {"strategy": "sequential", "start": 0, "value_size": 64}
    }));

    let traffic_groups = vec![
        TrafficGroupConfig {
            name: "fast".to_string(),
            threads: vec![0],
            connections_per_thread: 10,
            max_pending_per_connection: 100,
            protocol: "redis".to_string(),
            target: "127.0.0.1:6379".to_string(),
            transport: "tcp".to_string(),
            traffic_policy: PolicyConfig::ClosedLoop,
            sampling_policy: policy1,
            protocol_config: protocol_config.clone(),
        },
        TrafficGroupConfig {
            name: "slow".to_string(),
            threads: vec![1],
            connections_per_thread: 5,
            max_pending_per_connection: 50,
            protocol: "redis".to_string(),
            target: "127.0.0.1:6379".to_string(),
            transport: "tcp".to_string(),
            traffic_policy: PolicyConfig::ClosedLoop,
            sampling_policy: policy2,
            protocol_config,
        },
    ];

    let results = DetailedExperimentResults::from_group_stats(
        "Isolation Test".to_string(),
        None,
        None,
        "127.0.0.1:6379".to_string(),
        "redis".to_string(),
        "tcp".to_string(),
        Duration::from_secs(10),
        &group_stats,
        &traffic_groups,
    );

    // Verify groups have independent statistics
    let fast_group = &results.traffic_groups[0];
    let slow_group = &results.traffic_groups[1];

    // Fast group should have ~50μs latency
    assert!(fast_group.stats.latency.mean_us < 100.0);

    // Slow group should have ~500μs latency
    assert!(slow_group.stats.latency.mean_us > 400.0);

    // Fast group should have more requests
    assert_eq!(fast_group.stats.total_requests, 1000);
    assert_eq!(slow_group.stats.total_requests, 500);

    // Global stats should aggregate both
    assert_eq!(results.global.total_requests, 1500);
}

#[test]
fn test_metadata_merging_across_groups() {
    let mut c1 = GroupStatsCollector::new();
    let mut c2 = GroupStatsCollector::new();

    let policy = SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 };

    c1.register_group(0, &policy);
    c2.register_group(0, &policy);

    // Simulate two threads working on same group
    c1.set_group_metadata(0, serde_json::json!({"requests": 100, "errors": 5}));
    c2.set_group_metadata(0, serde_json::json!({"requests": 150, "errors": 3}));

    // Merge collectors
    let merged = GroupStatsCollector::merge(vec![c1, c2]);

    // Verify metadata was merged correctly
    let metadata = merged.get_group_metadata(0).unwrap();
    assert_eq!(metadata["requests"], 250); // 100 + 150
    assert_eq!(metadata["errors"], 8); // 5 + 3
}
