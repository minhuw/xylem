//! Integration tests for rich output system

use std::time::Duration;
use xylem_cli::output::{html, DetailedExperimentResults};
use xylem_core::stats::{SamplingPolicy, TupleStatsCollector};
use xylem_core::traffic_group::{PolicyConfig, TrafficGroupConfig};

#[test]
fn test_detailed_json_output_generation() {
    // Create mock tuple stats collector with data
    let policy = SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 };
    let mut stats = TupleStatsCollector::new(policy.clone(), Duration::from_secs(1));

    // Add latency samples for group 0
    for i in 0..500 {
        stats.record_latency(0, i % 10, Duration::from_micros(100));
    }

    // Add latency samples for group 1
    for i in 0..300 {
        stats.record_latency(1, i % 5, Duration::from_micros(250));
    }

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
            sampling_policy: policy.clone(),
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
            sampling_policy: policy.clone(),
            protocol_config: protocol_config.clone(),
        },
    ];

    // Generate detailed results with records included
    let results = DetailedExperimentResults::from_tuple_stats(
        "Integration Test".to_string(),
        Some("Testing detailed JSON output".to_string()),
        Some(42),
        "127.0.0.1:6379".to_string(),
        "redis".to_string(),
        "tcp".to_string(),
        Duration::from_secs(10),
        &stats,
        &traffic_groups,
        true, // include_records
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

    let group1 = &results.traffic_groups[1];
    assert_eq!(group1.id, 1);
    assert_eq!(group1.name, "slow-group");
    assert_eq!(group1.protocol, "redis-cluster");
    assert_eq!(group1.stats.total_requests, 300);

    // Verify records contain per-connection entries
    assert!(!results.records.is_empty());
    // Should have entries for both groups
    let group_ids: std::collections::HashSet<_> =
        results.records.iter().map(|r| r.group_id).collect();
    assert!(group_ids.contains(&0));
    assert!(group_ids.contains(&1));
}

#[test]
fn test_json_serialization_deserialization() {
    let policy = SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 };
    let mut stats = TupleStatsCollector::new(policy.clone(), Duration::from_secs(1));

    for i in 0..100 {
        stats.record_latency(0, i % 5, Duration::from_micros(100));
    }

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

    let results = DetailedExperimentResults::from_tuple_stats(
        "Test".to_string(),
        None,
        None,
        "127.0.0.1:6379".to_string(),
        "redis".to_string(),
        "tcp".to_string(),
        Duration::from_secs(1),
        &stats,
        &traffic_groups,
        true, // include_records
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
    let policy = SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 };
    let mut stats = TupleStatsCollector::new(policy.clone(), Duration::from_secs(1));

    for i in 0..100 {
        stats.record_latency(0, i % 5, Duration::from_micros(150));
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

    let results = DetailedExperimentResults::from_tuple_stats(
        "HTML Test".to_string(),
        Some("Testing HTML generation".to_string()),
        Some(123),
        "127.0.0.1:6379".to_string(),
        "redis".to_string(),
        "tcp".to_string(),
        Duration::from_secs(5),
        &stats,
        &traffic_groups,
        false, // include_records - not needed for HTML
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
    let policy = SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 };
    let mut stats = TupleStatsCollector::new(policy.clone(), Duration::from_secs(1));

    // Group 0: Fast responses
    for i in 0..1000 {
        stats.record_latency(0, i % 10, Duration::from_micros(50));
    }

    // Group 1: Slow responses
    for i in 0..500 {
        stats.record_latency(1, i % 5, Duration::from_micros(500));
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
            sampling_policy: policy.clone(),
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
            sampling_policy: policy.clone(),
            protocol_config,
        },
    ];

    let results = DetailedExperimentResults::from_tuple_stats(
        "Isolation Test".to_string(),
        None,
        None,
        "127.0.0.1:6379".to_string(),
        "redis".to_string(),
        "tcp".to_string(),
        Duration::from_secs(10),
        &stats,
        &traffic_groups,
        true, // include_records
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
fn test_tuple_stats_merging_across_threads() {
    let policy = SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 };

    // Simulate two threads working on same group
    let mut c1 = TupleStatsCollector::new(policy.clone(), Duration::from_secs(1));
    let mut c2 = TupleStatsCollector::new(policy.clone(), Duration::from_secs(1));

    // Thread 1: records latencies for connections 0-4
    for i in 0..100 {
        c1.record_latency(0, i % 5, Duration::from_micros(50));
    }

    // Thread 2: records latencies for connections 5-9
    for i in 0..150 {
        c2.record_latency(0, 5 + (i % 5), Duration::from_micros(60));
    }

    // Merge collectors
    let merged = TupleStatsCollector::merge(vec![c1, c2]);

    // Verify stats were merged correctly
    let aggregated = merged.aggregate_global(Duration::from_secs(1));
    assert_eq!(aggregated.total_requests, 250); // 100 + 150
}
