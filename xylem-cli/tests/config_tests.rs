//! Tests for configuration parsing and validation

use std::time::Duration;
use xylem_cli::config::ProfileConfig;

#[test]
fn test_load_redis_zipfian_profile() {
    let config = ProfileConfig::from_file("profiles/redis-get-zipfian.toml")
        .expect("Failed to load redis-get-zipfian profile");

    // Verify experiment config
    assert_eq!(config.experiment.name, "redis-get-zipfian");
    assert_eq!(config.experiment.seed, Some(42));
    assert_eq!(config.experiment.duration, Duration::from_secs(30));

    // Verify target config
    assert_eq!(config.target.address, Some("127.0.0.1:6379".to_string()));
    assert_eq!(config.target.protocol, "redis");
    assert_eq!(config.target.transport, "tcp");

    // Verify threading
    assert_eq!(config.threading.threads, 4);
    assert_eq!(config.threading.connections_per_thread, 25);
    assert!(config.threading.pin_cpus);

    // Verify statistics
    assert_eq!(config.statistics.percentiles, vec![50.0, 90.0, 95.0, 99.0, 99.9]);
    assert_eq!(config.statistics.confidence_level, 0.95);
}

#[test]
fn test_load_memcached_ramp_profile() {
    let config = ProfileConfig::from_file("profiles/memcached-ramp.toml")
        .expect("Failed to load memcached-ramp profile");

    // Verify experiment
    assert_eq!(config.experiment.name, "memcached-ramp-load");
    assert_eq!(config.experiment.seed, Some(12345));
    assert_eq!(config.experiment.duration, Duration::from_secs(60));

    // Verify target
    assert_eq!(config.target.protocol, "memcached-binary");

    // Verify threading
    assert_eq!(config.threading.threads, 8);

    // Verify connections
}

#[test]
fn test_load_http_spike_profile() {
    let config = ProfileConfig::from_file("profiles/http-spike.toml")
        .expect("Failed to load http-spike profile");

    // Verify experiment
    assert_eq!(config.experiment.name, "http-spike-test");
    assert_eq!(config.experiment.seed, Some(99999));
    assert_eq!(config.experiment.duration, Duration::from_secs(120));

    // Verify target
    assert_eq!(config.target.protocol, "http");

    // Verify threading
    assert_eq!(config.threading.threads, 16);

    // Verify connections

    // Verify statistics (more percentiles for spike test)
    assert_eq!(config.statistics.percentiles.len(), 7);
    assert_eq!(config.statistics.confidence_level, 0.99);
}

#[test]
fn test_load_poisson_heterogeneous_profile() {
    let config = ProfileConfig::from_file("profiles/poisson-heterogeneous.toml")
        .expect("Failed to load poisson-heterogeneous profile");

    // Verify experiment
    assert_eq!(config.experiment.name, "poisson-heterogeneous");
    assert_eq!(config.experiment.seed, Some(7777));

    // Verify connections

    // Verify threading
    assert_eq!(config.threading.threads, 8);
}

#[test]
fn test_load_closed_loop_profile() {
    let config = ProfileConfig::from_file("profiles/closed-loop-max-throughput.toml")
        .expect("Failed to load closed-loop-max-throughput profile");

    // Verify experiment
    assert_eq!(config.experiment.name, "closed-loop-max-throughput");
    assert_eq!(config.experiment.seed, Some(1234567890));
    assert_eq!(config.experiment.duration, Duration::from_secs(30));

    // Verify target
    assert_eq!(config.target.protocol, "memcached-binary");

    // Verify connections (many for max throughput)

    // Verify threading (maximize CPU)
    assert_eq!(config.threading.threads, 16);
}

#[test]
fn test_config_with_overrides() {
    let config = ProfileConfig::from_file("profiles/redis-get-zipfian.toml")
        .expect("Failed to load profile");

    // Apply overrides
    let config = config
        .with_overrides(
            Some("192.168.1.100:6379".to_string()),
            Some("60s".to_string()),
            None,
            Some(999),
        )
        .expect("Failed to apply overrides");

    // Verify overrides were applied
    assert_eq!(config.target.address, Some("192.168.1.100:6379".to_string()));
    assert_eq!(config.experiment.duration, Duration::from_secs(60));
    assert_eq!(config.experiment.seed, Some(999));

    // Verify non-overridden values remain unchanged
    assert_eq!(config.experiment.name, "redis-get-zipfian");
    assert_eq!(config.target.protocol, "redis");
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

[connections]
[connections.policy]
scheduler = "uniform"
type = "closed-loop"

[threading]
threads = 1
connections_per_thread = 1
max_pending_per_connection = 1
pin_cpus = false
cpu_start = 0

[statistics]
percentiles = [50, 99]
confidence_level = 0.95

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

    // Apply CLI override with target
    let config = config
        .with_overrides(Some("127.0.0.1:6379".to_string()), None, None, None)
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

[connections]
[connections.policy]
scheduler = "uniform"
type = "closed-loop"

[threading]
threads = 1
connections_per_thread = 1
max_pending_per_connection = 1
pin_cpus = false
cpu_start = 0

[statistics]
percentiles = [50, 99]
confidence_level = 0.95

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

    // Try to apply overrides WITHOUT target - should fail validation
    let result = config.with_overrides(None, None, None, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Target address must be specified"));
}

#[test]
fn test_invalid_config_validation() {
    // TODO: Create invalid config files and test validation
    // For now, just verify that valid configs pass validation
    let config = ProfileConfig::from_file("profiles/redis-get-zipfian.toml")
        .expect("Failed to load profile");

    // Should not panic - validation already ran in from_file
    assert_eq!(config.experiment.name, "redis-get-zipfian");
}

#[test]
fn test_key_generation_conversion() {
    use xylem_core::workload::KeyGeneration;

    let config = ProfileConfig::from_file("profiles/redis-get-zipfian.toml")
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
    let config = ProfileConfig::from_file("profiles/redis-get-zipfian.toml")
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

    let config = ProfileConfig::from_file("profiles/redis-get-zipfian.toml")
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

[connections]
[connections.policy]
scheduler = "uniform"
type = "closed-loop"

[threading]
threads = 1
connections_per_thread = 1
max_pending_per_connection = 1

[statistics]
percentiles = [50, 99]
confidence_level = 0.95

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

[connections]
[connections.policy]
scheduler = "uniform"
type = "closed-loop"

[threading]
threads = 1
connections_per_thread = 1
max_pending_per_connection = 1

[statistics]
percentiles = [50, 99]
confidence_level = 0.95

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
        assert_eq!(config.target.protocol, protocol);
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

[connections]
[connections.policy]
scheduler = "uniform"
type = "closed-loop"

[threading]
threads = 1
connections_per_thread = 1
max_pending_per_connection = 1

[statistics]
percentiles = [50, 99]
confidence_level = 0.95

[output]
format = "json"
file = "test.json"
"#;

    use std::io::Write;
    let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
    tmpfile.write_all(config_str.as_bytes()).unwrap();
    tmpfile.flush().unwrap();

    let config = ProfileConfig::from_file(tmpfile.path()).unwrap();
    let result = config.with_overrides(None, None, None, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Invalid protocol"));
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

[connections]
[connections.policy]
scheduler = "uniform"
type = "closed-loop"

[threading]
threads = 1
connections_per_thread = 1
max_pending_per_connection = 1

[statistics]
percentiles = [50, 99]
confidence_level = 0.95

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

[connections]
[connections.policy]
scheduler = "uniform"
type = "closed-loop"

[threading]
threads = 1
connections_per_thread = 1
max_pending_per_connection = 1

[statistics]
percentiles = [50, 99]
confidence_level = 0.95

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

    // Verify seed can be overridden via CLI
    let config = config.with_overrides(None, None, None, Some(12345)).unwrap();
    assert_eq!(config.experiment.seed, Some(12345));
}
