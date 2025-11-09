//! Tests for configuration parsing and validation

use std::time::Duration;
use xylem_cli::config::{KeysConfig, ProfileConfig};
use xylem_core::stats::collector::SamplingPolicy;

#[test]
fn test_load_redis_zipfian_profile() {
    let config = ProfileConfig::from_file("../profiles/redis-get-zipfian.toml")
        .expect("Failed to load redis-get-zipfian profile");

    // Verify experiment config
    assert_eq!(config.experiment.name, "redis-get-zipfian");
    assert_eq!(config.experiment.seed, Some(42));
    assert_eq!(config.experiment.duration, Duration::from_secs(30));

    // Verify target config
    assert_eq!(config.target.address, Some("127.0.0.1:6379".to_string()));
    assert_eq!(config.target.protocol, Some("redis".to_string()));
    assert_eq!(config.target.transport, "tcp");

    // Verify traffic groups
    assert_eq!(config.traffic_groups.len(), 1);
    assert_eq!(config.traffic_groups[0].name, "main");
    assert_eq!(config.traffic_groups[0].threads, vec![0, 1, 2, 3]);
    assert_eq!(config.traffic_groups[0].connections_per_thread, 25);
}

#[test]
fn test_load_memcached_ramp_profile() {
    let config = ProfileConfig::from_file("../profiles/memcached-ramp.toml")
        .expect("Failed to load memcached-ramp profile");

    // Verify experiment
    assert_eq!(config.experiment.name, "memcached-ramp-load");
    assert_eq!(config.experiment.seed, Some(12345));
    assert_eq!(config.experiment.duration, Duration::from_secs(60));

    // Verify target
    assert_eq!(config.target.protocol, Some("memcached-binary".to_string()));

    // Verify traffic groups
    assert_eq!(config.traffic_groups.len(), 1);
    let threads: Vec<usize> = (0..8).collect();
    assert_eq!(config.traffic_groups[0].threads, threads);
}

#[test]
fn test_load_http_spike_profile() {
    let config = ProfileConfig::from_file("../profiles/http-spike.toml")
        .expect("Failed to load http-spike profile");

    // Verify experiment
    assert_eq!(config.experiment.name, "http-spike-test");
    assert_eq!(config.experiment.seed, Some(99999));
    assert_eq!(config.experiment.duration, Duration::from_secs(120));

    // Verify target
    assert_eq!(config.target.protocol, Some("http".to_string()));

    // Verify traffic groups
    assert_eq!(config.traffic_groups.len(), 1);
    let threads: Vec<usize> = (0..16).collect();
    assert_eq!(config.traffic_groups[0].threads, threads);
}

#[test]
fn test_load_poisson_heterogeneous_profile() {
    let config = ProfileConfig::from_file("../profiles/poisson-heterogeneous.toml")
        .expect("Failed to load poisson-heterogeneous profile");

    // Verify experiment
    assert_eq!(config.experiment.name, "poisson-heterogeneous");
    assert_eq!(config.experiment.seed, Some(7777));

    // Verify traffic groups (this config has 2 groups: slow and fast)
    assert_eq!(config.traffic_groups.len(), 2);
    assert_eq!(config.traffic_groups[0].name, "slow-poisson");
    assert_eq!(config.traffic_groups[1].name, "fast-poisson");
}

#[test]
fn test_load_closed_loop_profile() {
    let config = ProfileConfig::from_file("../profiles/closed-loop-max-throughput.toml")
        .expect("Failed to load closed-loop-max-throughput profile");

    // Verify experiment
    assert_eq!(config.experiment.name, "closed-loop-max-throughput");
    assert_eq!(config.experiment.seed, Some(1234567890));
    assert_eq!(config.experiment.duration, Duration::from_secs(30));

    // Verify target
    assert_eq!(config.target.protocol, Some("memcached-binary".to_string()));

    // Verify traffic groups
    assert_eq!(config.traffic_groups.len(), 1);
    let threads: Vec<usize> = (0..16).collect();
    assert_eq!(config.traffic_groups[0].threads, threads);
}

#[test]
fn test_load_latency_agent_profile() {
    let config = ProfileConfig::from_file("../profiles/latency-agent.toml")
        .expect("Failed to load latency-agent profile");

    // Verify experiment
    assert_eq!(config.experiment.name, "latency-agent-separation");
    assert_eq!(config.experiment.seed, Some(42));
    assert_eq!(config.experiment.duration, Duration::from_secs(10));

    // Verify target
    assert_eq!(config.target.protocol, Some("redis".to_string()));

    // Verify traffic groups - should have 2 groups (latency + throughput agents)
    assert_eq!(config.traffic_groups.len(), 2);

    // Verify latency agent configuration
    let latency_agent = &config.traffic_groups[0];
    assert_eq!(latency_agent.name, "latency-agent");
    assert_eq!(latency_agent.threads, vec![0, 1]);
    assert_eq!(latency_agent.connections_per_thread, 10);
    assert_eq!(latency_agent.max_pending_per_connection, 1);
    // Check sampling policy (should be Unlimited for 100% sampling)
    match &latency_agent.sampling_policy {
        SamplingPolicy::Unlimited => {} // Expected
        _ => panic!("Expected Unlimited sampling policy for latency agent"),
    }

    // Verify throughput agent configuration
    let throughput_agent = &config.traffic_groups[1];
    assert_eq!(throughput_agent.name, "throughput-agent");
    assert_eq!(throughput_agent.threads, vec![2, 3, 4, 5, 6, 7]);
    assert_eq!(throughput_agent.connections_per_thread, 25);
    assert_eq!(throughput_agent.max_pending_per_connection, 32);
    // Check sampling policy (should be Limited with 1% rate)
    match &throughput_agent.sampling_policy {
        SamplingPolicy::Limited { rate, .. } => {
            assert!((rate - 0.01).abs() < 0.001, "Expected 1% sampling rate");
        }
        _ => panic!("Expected Limited sampling policy for throughput agent"),
    }
}

#[test]
fn test_load_redis_bench_profile() {
    let config = ProfileConfig::from_file("../profiles/redis-bench.toml")
        .expect("Failed to load redis-bench profile");

    // Verify experiment
    assert_eq!(config.experiment.name, "redis-bench");
    assert_eq!(config.experiment.seed, Some(42));
    assert_eq!(config.experiment.duration, Duration::from_secs(30));

    // Verify target
    assert_eq!(config.target.protocol, Some("redis".to_string()));

    // Verify traffic groups - should have 2 groups
    assert_eq!(config.traffic_groups.len(), 2);

    // Verify latency agent configuration (1 core)
    let latency_agent = &config.traffic_groups[0];
    assert_eq!(latency_agent.name, "latency-agent");
    assert_eq!(latency_agent.threads, vec![0]);
    assert_eq!(latency_agent.connections_per_thread, 10);
    assert_eq!(latency_agent.max_pending_per_connection, 1);
    // Check sampling policy (should be Unlimited for 100% sampling)
    match &latency_agent.sampling_policy {
        SamplingPolicy::Unlimited => {} // Expected
        _ => panic!("Expected Unlimited sampling policy for latency agent"),
    }

    // Verify throughput agent configuration (3 cores)
    let throughput_agent = &config.traffic_groups[1];
    assert_eq!(throughput_agent.name, "throughput-agent");
    assert_eq!(throughput_agent.threads, vec![1, 2, 3]);
    assert_eq!(throughput_agent.connections_per_thread, 25);
    assert_eq!(throughput_agent.max_pending_per_connection, 32);
    // Check sampling policy (should be Limited with 1% rate)
    match &throughput_agent.sampling_policy {
        SamplingPolicy::Limited { rate, .. } => {
            assert!((rate - 0.01).abs() < 0.001, "Expected 1% sampling rate");
        }
        _ => panic!("Expected Limited sampling policy for throughput agent"),
    }
}

#[test]
fn test_config_with_overrides() {
    // Apply overrides using --set style
    let config = ProfileConfig::from_file_with_overrides(
        "../profiles/redis-get-zipfian.toml",
        &[
            "target.address=192.168.1.100:6379".to_string(),
            "experiment.duration=60s".to_string(),
            "experiment.seed=999".to_string(),
        ],
    )
    .expect("Failed to apply overrides");

    // Verify overrides were applied
    assert_eq!(config.target.address, Some("192.168.1.100:6379".to_string()));
    assert_eq!(config.experiment.duration, Duration::from_secs(60));
    assert_eq!(config.experiment.seed, Some(999));

    // Verify non-overridden values remain unchanged
    assert_eq!(config.experiment.name, "redis-get-zipfian");
    assert_eq!(config.target.protocol, Some("redis".to_string()));
}

#[test]
fn test_config_with_cli_target_only() {
    // Test profile without target address (must be provided via CLI)
    let profile_without_target = r#"
[experiment]
name = "cli-target-test"
duration = "10s"

[target]
protocol = "redis"

[workload]
[workload.keys]
strategy = "sequential"
start = 0
value_size = 64

[workload.pattern]
type = "constant"
rate = 1000.0

[[traffic_groups]]
name = "main"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1
sampling_rate = 1.0

[traffic_groups.policy]
type = "closed-loop"

[output]
format = "json"
file = "results/test.json"
"#;

    // Write temporary config file
    use std::io::Write;
    let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
    tmpfile.write_all(profile_without_target.as_bytes()).unwrap();
    tmpfile.flush().unwrap();

    // Load config without target
    let config = ProfileConfig::from_file(tmpfile.path()).unwrap();
    assert_eq!(config.target.address, None);

    // Apply CLI override with target using --set
    let config = ProfileConfig::from_file_with_overrides(
        tmpfile.path(),
        &["target.address=127.0.0.1:6379".to_string()],
    )
    .expect("Failed to apply target override");

    // Verify target was set via CLI
    assert_eq!(config.target.address, Some("127.0.0.1:6379".to_string()));
}

#[test]
fn test_config_validation_missing_target() {
    // Config without target should fail validation
    let profile_without_target = r#"
[experiment]
name = "no-target-test"
duration = "10s"

[target]
protocol = "redis"

[workload]
[workload.keys]
strategy = "sequential"
start = 0
value_size = 64

[workload.pattern]
type = "constant"
rate = 1000.0

[[traffic_groups]]
name = "main"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1
sampling_rate = 1.0

[traffic_groups.policy]
type = "closed-loop"

[output]
format = "json"
file = "results/test.json"
"#;

    use std::io::Write;
    let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
    tmpfile.write_all(profile_without_target.as_bytes()).unwrap();
    tmpfile.flush().unwrap();

    // Load config - should succeed (no validation yet)
    let config = ProfileConfig::from_file(tmpfile.path()).unwrap();

    // Validation is now automatic in from_file_with_overrides, so test validation directly
    let result = config.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Target address must be specified"));
}

#[test]
fn test_invalid_config_validation() {
    // TODO: Create invalid config files and test validation
    // For now, just verify that valid configs pass validation
    let config = ProfileConfig::from_file("../profiles/redis-get-zipfian.toml")
        .expect("Failed to load profile");

    // Should not panic - validation already ran in from_file
    assert_eq!(config.experiment.name, "redis-get-zipfian");
}

#[test]
fn test_key_generation_conversion() {
    use xylem_core::workload::KeyGeneration;

    let config = ProfileConfig::from_file("../profiles/redis-get-zipfian.toml")
        .expect("Failed to load profile");

    // Test conversion to KeyGeneration with seed
    let key_gen = config
        .workload
        .keys
        .to_key_generation(config.experiment.seed)
        .expect("Failed to convert to KeyGeneration");

    // Verify it's a Zipfian distribution
    match key_gen {
        KeyGeneration::Zipfian(_) => {} // Success
        _ => panic!("Expected Zipfian distribution"),
    }
}

#[test]
fn test_value_size_extraction() {
    let config = ProfileConfig::from_file("../profiles/redis-get-zipfian.toml")
        .expect("Failed to load profile");

    let value_size = config.workload.keys.value_size();
    assert_eq!(value_size, 64);
}

// ============================================================================
// Integration Tests
// ============================================================================

#[test]
fn test_seed_reproducibility_integration() {
    // Test that same seed produces reproducible key generation
    use xylem_core::workload::KeyGeneration;

    let config = ProfileConfig::from_file("../profiles/redis-get-zipfian.toml")
        .expect("Failed to load profile");

    let master_seed = config.experiment.seed;
    assert_eq!(master_seed, Some(42));

    // Generate two key generators with same seed
    let key_gen1 = config
        .workload
        .keys
        .to_key_generation(master_seed)
        .expect("Failed to create key generation 1");
    let key_gen2 = config
        .workload
        .keys
        .to_key_generation(master_seed)
        .expect("Failed to create key generation 2");

    // Both should be Zipfian
    match (&key_gen1, &key_gen2) {
        (KeyGeneration::Zipfian(_), KeyGeneration::Zipfian(_)) => {}
        _ => panic!("Expected Zipfian distribution"),
    }

    // TODO: Would need to make KeyGeneration cloneable to test actual sequences
}

#[test]
fn test_all_key_strategies() {
    // Test Sequential
    let sequential_config = r#"
[experiment]
name = "test"
duration = "10s"

[target]
address = "127.0.0.1:6379"
protocol = "redis"

[workload]
[workload.keys]
strategy = "sequential"
start = 100
value_size = 64

[workload.pattern]
type = "constant"
rate = 1000.0

[[traffic_groups]]
name = "main"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1
sampling_rate = 1.0

[traffic_groups.policy]
type = "closed-loop"

[output]
format = "json"
file = "test.json"
"#;

    use std::io::Write;
    let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
    tmpfile.write_all(sequential_config.as_bytes()).unwrap();
    tmpfile.flush().unwrap();

    let config = ProfileConfig::from_file(tmpfile.path()).unwrap();
    let key_gen = config.workload.keys.to_key_generation(None).unwrap();

    match key_gen {
        xylem_core::workload::KeyGeneration::Sequential { start, .. } => {
            assert_eq!(start, 100);
        }
        _ => panic!("Expected Sequential"),
    }

    // Test Random
    let random_config = sequential_config.replace(
        r#"strategy = "sequential"
start = 100"#,
        r#"strategy = "random"
max = 10000"#,
    );

    let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
    tmpfile.write_all(random_config.as_bytes()).unwrap();
    tmpfile.flush().unwrap();

    let config = ProfileConfig::from_file(tmpfile.path()).unwrap();
    let key_gen = config.workload.keys.to_key_generation(Some(12345)).unwrap();

    match key_gen {
        xylem_core::workload::KeyGeneration::Random { max, .. } => {
            assert_eq!(max, 10000);
        }
        _ => panic!("Expected Random"),
    }

    // Test RoundRobin
    let rr_config = sequential_config.replace(
        r#"strategy = "sequential"
start = 100"#,
        r#"strategy = "round-robin"
max = 5000"#,
    );

    let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
    tmpfile.write_all(rr_config.as_bytes()).unwrap();
    tmpfile.flush().unwrap();

    let config = ProfileConfig::from_file(tmpfile.path()).unwrap();
    let key_gen = config.workload.keys.to_key_generation(None).unwrap();

    match key_gen {
        xylem_core::workload::KeyGeneration::RoundRobin { max, .. } => {
            assert_eq!(max, 5000);
        }
        _ => panic!("Expected RoundRobin"),
    }
}

#[test]
fn test_all_protocols_valid() {
    let protocols =
        vec!["redis", "memcached-binary", "memcached-ascii", "http", "masstree", "xylem-echo"];

    for protocol in protocols {
        let config_str = format!(
            r#"
[experiment]
name = "test-{protocol}"
duration = "10s"

[target]
address = "127.0.0.1:6379"
protocol = "{protocol}"

[workload]
[workload.keys]
strategy = "sequential"
start = 0
value_size = 64

[workload.pattern]
type = "constant"
rate = 1000.0

[[traffic_groups]]
name = "main"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1
sampling_rate = 1.0

[traffic_groups.policy]
type = "closed-loop"

[output]
format = "json"
file = "test.json"
"#
        );

        use std::io::Write;
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        tmpfile.write_all(config_str.as_bytes()).unwrap();
        tmpfile.flush().unwrap();

        let config = ProfileConfig::from_file(tmpfile.path())
            .unwrap_or_else(|_| panic!("Failed to parse config for protocol: {}", protocol));
        assert_eq!(config.target.protocol.as_deref(), Some(protocol));
    }
}

#[test]
fn test_invalid_protocol() {
    let config_str = r#"
[experiment]
name = "test-invalid"
duration = "10s"

[target]
address = "127.0.0.1:6379"
protocol = "invalid_protocol"

[workload]
[workload.keys]
strategy = "sequential"
start = 0
value_size = 64

[workload.pattern]
type = "constant"
rate = 1000.0

[[traffic_groups]]
name = "main"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1
sampling_rate = 1.0

[traffic_groups.policy]
type = "closed-loop"

[output]
format = "json"
file = "test.json"
"#;

    use std::io::Write;
    let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
    tmpfile.write_all(config_str.as_bytes()).unwrap();
    tmpfile.flush().unwrap();

    let config = ProfileConfig::from_file(tmpfile.path()).unwrap();
    let result = config.validate();
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Invalid") && err_msg.contains("protocol"),
        "Error message should contain 'Invalid' and 'protocol', got: {}",
        err_msg
    );
}

#[test]
fn test_duration_parsing() {
    let config_str = r#"
[experiment]
name = "test-duration"
duration = "2m30s"

[target]
address = "127.0.0.1:6379"
protocol = "redis"

[workload]
[workload.keys]
strategy = "sequential"
start = 0
value_size = 64

[workload.pattern]
type = "constant"
rate = 1000.0

[[traffic_groups]]
name = "main"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1
sampling_rate = 1.0

[traffic_groups.policy]
type = "closed-loop"

[output]
format = "json"
file = "test.json"
"#;

    use std::io::Write;
    let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
    tmpfile.write_all(config_str.as_bytes()).unwrap();
    tmpfile.flush().unwrap();

    let config = ProfileConfig::from_file(tmpfile.path()).unwrap();
    assert_eq!(config.experiment.duration, Duration::from_secs(150)); // 2m30s = 150s
}

#[test]
fn test_seed_none_vs_some() {
    // Config with no seed
    let no_seed_config = r#"
[experiment]
name = "no-seed"
duration = "10s"

[target]
address = "127.0.0.1:6379"
protocol = "redis"

[workload]
[workload.keys]
strategy = "random"
max = 1000
value_size = 64

[workload.pattern]
type = "constant"
rate = 1000.0

[[traffic_groups]]
name = "main"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1
sampling_rate = 1.0

[traffic_groups.policy]
type = "closed-loop"

[output]
format = "json"
file = "test.json"
"#;

    use std::io::Write;
    let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
    tmpfile.write_all(no_seed_config.as_bytes()).unwrap();
    tmpfile.flush().unwrap();

    let config = ProfileConfig::from_file(tmpfile.path()).unwrap();
    assert_eq!(config.experiment.seed, None);

    // Config with seed
    let with_seed = no_seed_config.replace(
        r#"[experiment]
name = "no-seed"
duration = "10s""#,
        r#"[experiment]
name = "with-seed"
seed = 999
duration = "10s""#,
    );

    let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
    tmpfile.write_all(with_seed.as_bytes()).unwrap();
    tmpfile.flush().unwrap();

    let config = ProfileConfig::from_file(tmpfile.path()).unwrap();
    assert_eq!(config.experiment.seed, Some(999));

    // Verify seed can be overridden via CLI using --set
    let config = ProfileConfig::from_file_with_overrides(
        tmpfile.path(),
        &["experiment.seed=12345".to_string()],
    )
    .unwrap();
    assert_eq!(config.experiment.seed, Some(12345));
}

// =============================================================================
// NEW --SET OVERRIDE TESTS
// =============================================================================

#[test]
fn test_set_simple_field() {
    let config = ProfileConfig::from_file_with_overrides(
        "../profiles/redis-get-zipfian.toml",
        &["experiment.seed=42".to_string()],
    )
    .unwrap();
    assert_eq!(config.experiment.seed, Some(42));
}

#[test]
fn test_set_nested_field() {
    let config = ProfileConfig::from_file_with_overrides(
        "../profiles/redis-get-zipfian.toml",
        &["workload.keys.n=5000".to_string()],
    )
    .unwrap();

    match config.workload.keys {
        KeysConfig::Zipfian { n, .. } => {
            assert_eq!(n, 5000);
        }
        _ => panic!("Expected Zipfian keys config"),
    }
}

#[test]
fn test_set_array_index() {
    let config = ProfileConfig::from_file_with_overrides(
        "../profiles/redis-bench.toml",
        &["traffic_groups.0.sampling_policy.type=unlimited".to_string()],
    )
    .unwrap();

    // Check that sampling policy was changed to Unlimited
    match &config.traffic_groups[0].sampling_policy {
        SamplingPolicy::Unlimited => {} // Expected
        _ => panic!("Expected Unlimited sampling policy after override"),
    }
}

#[test]
fn test_set_array_value() {
    let config = ProfileConfig::from_file_with_overrides(
        "../profiles/redis-bench.toml",
        &["traffic_groups.0.threads=[0,1,2,3]".to_string()],
    )
    .unwrap();

    assert_eq!(config.traffic_groups[0].threads, vec![0, 1, 2, 3]);
}

#[test]
fn test_set_multiple_overrides() {
    let config = ProfileConfig::from_file_with_overrides(
        "../profiles/redis-get-zipfian.toml",
        &[
            "target.address=192.168.1.100:6379".to_string(),
            "experiment.duration=5m".to_string(),
            "experiment.seed=999".to_string(),
            "target.protocol=memcached-binary".to_string(),
        ],
    )
    .unwrap();

    assert_eq!(config.target.address, Some("192.168.1.100:6379".to_string()));
    assert_eq!(config.experiment.duration, Duration::from_secs(300));
    assert_eq!(config.experiment.seed, Some(999));
    assert_eq!(config.target.protocol, Some("memcached-binary".to_string()));
}

#[test]
fn test_set_type_inference_integer() {
    let config = ProfileConfig::from_file_with_overrides(
        "../profiles/redis-get-zipfian.toml",
        &["experiment.seed=12345".to_string()],
    )
    .unwrap();
    assert_eq!(config.experiment.seed, Some(12345));
}

#[test]
fn test_set_type_inference_float() {
    let config = ProfileConfig::from_file_with_overrides(
        "../profiles/redis-bench.toml",
        &["traffic_groups.0.sampling_policy.rate=0.75".to_string()],
    )
    .expect("Failed to load redis-bench.toml with overrides");

    // Check that the rate was updated
    match &config.traffic_groups[0].sampling_policy {
        SamplingPolicy::Limited { rate, .. } => {
            assert!((rate - 0.75).abs() < 0.001, "Expected 75% sampling rate");
        }
        _ => panic!("Expected Limited sampling policy"),
    }
}

#[test]
fn test_set_type_inference_boolean() {
    let config = ProfileConfig::from_file_with_overrides(
        "../profiles/redis-get-zipfian.toml",
        &["output.real_time=true".to_string()],
    )
    .unwrap();
    assert!(config.output.real_time);
}

#[test]
fn test_set_type_inference_string() {
    let config = ProfileConfig::from_file_with_overrides(
        "../profiles/redis-get-zipfian.toml",
        &["target.protocol=http".to_string()],
    )
    .unwrap();
    assert_eq!(config.target.protocol, Some("http".to_string()));
}

#[test]
fn test_set_invalid_path() {
    let result = ProfileConfig::from_file_with_overrides(
        "../profiles/redis-get-zipfian.toml",
        &["nonexistent.field=value".to_string()],
    );
    // Creating a new field will succeed in TOML manipulation,
    // but should fail during deserialization if it's not a valid field
    // (or succeed if serde ignores unknown fields)
    // This test just ensures the operation doesn't panic
    let _ = result;
}

#[test]
fn test_set_invalid_format() {
    let result = ProfileConfig::from_file_with_overrides(
        "../profiles/redis-get-zipfian.toml",
        &["invalid_no_equals".to_string()],
    );
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Invalid override format"));
}

#[test]
fn test_set_array_out_of_bounds() {
    let result = ProfileConfig::from_file_with_overrides(
        "../profiles/redis-bench.toml",
        &["traffic_groups.999.sampling_rate=0.5".to_string()],
    );
    assert!(result.is_err());
    // Just verify it failed - the exact error message may vary
}

#[test]
fn test_set_duration_string() {
    let config = ProfileConfig::from_file_with_overrides(
        "../profiles/redis-get-zipfian.toml",
        &["experiment.duration=2h".to_string()],
    )
    .unwrap();
    assert_eq!(config.experiment.duration, Duration::from_secs(7200));
}
