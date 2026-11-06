//! Integration tests for key selection distributions, value size distributions,
//! and key prefix/random data features for Redis and Masstree protocols.
//!
//! These tests verify that the distribution implementations produce
//! statistically correct outputs.

use std::collections::HashMap;

mod common;

// =============================================================================
// Key Selection Distribution Tests
// =============================================================================

mod key_selection {
    use xylem_protocols::workload::KeyGeneration;

    /// Test sequential key generation
    #[test]
    fn test_sequential_keys() {
        let mut keygen = KeyGeneration::sequential(100);

        // Should produce strictly increasing keys
        let keys: Vec<u64> = (0..1000).map(|_| keygen.next_key()).collect();

        for i in 1..keys.len() {
            assert_eq!(keys[i], keys[i - 1] + 1, "Sequential keys should increment by 1");
        }
        assert_eq!(keys[0], 100, "Should start from specified value");
    }

    /// Test random key generation stays within bounds
    #[test]
    fn test_random_keys_bounds() {
        let mut keygen = KeyGeneration::random(10000);

        for _ in 0..10000 {
            let key = keygen.next_key();
            assert!(key < 10000, "Random key {} should be < max 10000", key);
        }
    }

    /// Test random key generation produces variety
    #[test]
    fn test_random_keys_distribution() {
        let mut keygen = KeyGeneration::random(1000);

        let keys: Vec<u64> = (0..10000).map(|_| keygen.next_key()).collect();
        let unique_keys: std::collections::HashSet<_> = keys.iter().collect();

        // With 10000 samples from range [0, 1000), we should see most values
        assert!(
            unique_keys.len() > 900,
            "Random should cover most of keyspace, got {} unique keys",
            unique_keys.len()
        );
    }

    /// Test round-robin key generation
    #[test]
    fn test_round_robin_keys() {
        let mut keygen = KeyGeneration::round_robin(5);

        let keys: Vec<u64> = (0..15).map(|_| keygen.next_key()).collect();

        // Should cycle through 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, ...
        assert_eq!(keys, vec![0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
    }

    /// Test Zipfian distribution produces skewed access pattern
    #[test]
    fn test_zipfian_skew() {
        let mut keygen = KeyGeneration::zipfian(1000, 0.99).expect("Failed to create Zipfian");

        let mut counts: std::collections::HashMap<u64, usize> = std::collections::HashMap::new();
        for _ in 0..100000 {
            let key = keygen.next_key();
            *counts.entry(key).or_insert(0) += 1;
        }

        // Find the most accessed key
        let max_count = *counts.values().max().unwrap();
        let min_count = *counts.values().min().unwrap_or(&0);

        // Zipfian should show significant skew - hot keys accessed much more
        assert!(
            max_count > min_count * 10,
            "Zipfian should show significant skew: max={}, min={}",
            max_count,
            min_count
        );

        // Top 10% of keys should handle majority of accesses
        let mut sorted_counts: Vec<_> = counts.values().collect();
        sorted_counts.sort_by(|a, b| b.cmp(a));
        let top_10_pct = sorted_counts.len() / 10;
        let top_10_accesses: usize = sorted_counts[..top_10_pct].iter().copied().sum();
        let total_accesses: usize = sorted_counts.iter().copied().sum();

        assert!(
            top_10_accesses > total_accesses / 2,
            "Top 10% should handle >50% of accesses: top_10={}, total={}",
            top_10_accesses,
            total_accesses
        );
    }

    /// Test Gaussian distribution centers around mean
    #[test]
    fn test_gaussian_centered() {
        // Mean at 50%, std_dev at 10%, max 10000
        let mut keygen =
            KeyGeneration::gaussian(0.5, 0.1, 10000).expect("Failed to create Gaussian");

        let keys: Vec<u64> = (0..100000).map(|_| keygen.next_key()).collect();

        // Calculate actual mean
        let sum: u64 = keys.iter().sum();
        let mean = sum as f64 / keys.len() as f64;

        // Mean should be close to 5000 (50% of 10000)
        assert!((mean - 5000.0).abs() < 200.0, "Gaussian mean {} should be close to 5000", mean);

        // Count keys in different ranges
        let in_center = keys.iter().filter(|&&k| (4000..=6000).contains(&k)).count();

        // ~68% should be within 1 std dev (1000 units) of mean
        let center_pct = in_center as f64 / keys.len() as f64;
        assert!(
            center_pct > 0.5 && center_pct < 0.85,
            "About 68% should be within 1 std dev, got {}%",
            center_pct * 100.0
        );
    }

    /// Test Gaussian with seed reproducibility
    #[test]
    fn test_gaussian_reproducibility() {
        let mut keygen1 =
            KeyGeneration::gaussian_with_seed(0.5, 0.1, 10000, Some(42)).expect("Failed");
        let mut keygen2 =
            KeyGeneration::gaussian_with_seed(0.5, 0.1, 10000, Some(42)).expect("Failed");

        let keys1: Vec<u64> = (0..100).map(|_| keygen1.next_key()).collect();
        let keys2: Vec<u64> = (0..100).map(|_| keygen2.next_key()).collect();

        assert_eq!(keys1, keys2, "Same seed should produce same sequence");
    }
}

// =============================================================================
// Value Size Distribution Tests
// =============================================================================

mod value_size {
    use xylem_protocols::workload::{FixedSize, NormalSize, UniformSize, ValueSizeGenerator};

    /// Test fixed size always returns same value
    #[test]
    fn test_fixed_size() {
        let mut gen = FixedSize::new(128);

        for _ in 0..1000 {
            assert_eq!(gen.next_size(), 128);
        }
    }

    /// Test uniform size stays within bounds
    #[test]
    fn test_uniform_size_bounds() {
        let mut gen = UniformSize::new(100, 200).expect("Failed to create UniformSize");

        for _ in 0..10000 {
            let size = gen.next_size();
            assert!((100..=200).contains(&size), "Uniform size {} should be in [100, 200]", size);
        }
    }

    /// Test uniform size distribution is roughly even
    #[test]
    fn test_uniform_size_distribution() {
        // Note: Uniform distribution is [min, max) - exclusive of max
        let mut gen = UniformSize::new(100, 110).expect("Failed to create UniformSize");

        let mut counts = [0usize; 10];
        for _ in 0..100000 {
            let size = gen.next_size();
            // Map size 100-109 to index 0-9
            if (100..110).contains(&size) {
                counts[size - 100] += 1;
            }
        }

        // Each bucket should have roughly 10000 samples (10%)
        // Allow wider margin since uniform distribution has variance
        for (i, &count) in counts.iter().enumerate() {
            assert!(
                count > 7000 && count < 13000,
                "Bucket {} should have ~10000, got {}",
                i,
                count
            );
        }
    }

    /// Test normal size distribution centers around mean
    #[test]
    fn test_normal_size_centered() {
        let mut gen = NormalSize::new(500.0, 50.0, 100, 900).expect("Failed to create NormalSize");

        let sizes: Vec<usize> = (0..100000).map(|_| gen.next_size()).collect();

        // Calculate mean
        let sum: usize = sizes.iter().sum();
        let mean = sum as f64 / sizes.len() as f64;

        assert!((mean - 500.0).abs() < 10.0, "Normal mean {} should be close to 500", mean);

        // Most values should be within 2 std devs
        let within_2_std = sizes.iter().filter(|&&s| (400..=600).contains(&s)).count();
        let pct = within_2_std as f64 / sizes.len() as f64;

        assert!(pct > 0.9, "~95% should be within 2 std devs, got {}%", pct * 100.0);
    }

    /// Test normal size respects min/max bounds
    #[test]
    fn test_normal_size_clamping() {
        // Very wide std dev to test clamping
        let mut gen = NormalSize::new(500.0, 500.0, 100, 900).expect("Failed to create NormalSize");

        for _ in 0..10000 {
            let size = gen.next_size();
            assert!(
                (100..=900).contains(&size),
                "Normal size {} should be clamped to [100, 900]",
                size
            );
        }
    }
}

// =============================================================================
// Redis Protocol Distribution Integration Tests
// =============================================================================

mod redis_distributions {
    use super::*;
    use xylem_protocols::redis::command_selector::FixedCommandSelector;
    use xylem_protocols::redis::{RedisOp, RedisProtocol};
    use xylem_protocols::workload::KeyGeneration;
    use xylem_protocols::Protocol;

    /// Test Redis with custom key prefix
    #[test]
    fn test_redis_custom_key_prefix() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let key_gen = KeyGeneration::sequential(0);
        let mut protocol = RedisProtocol::with_workload_and_options(
            selector,
            key_gen,
            64,
            "myapp:user:".to_string(),
            false,
            None,
        );

        for i in 0..100 {
            let request = protocol.next_request(0);
            let request_str = String::from_utf8_lossy(&request.data);

            assert!(
                request_str.contains(&format!("myapp:user:{}", i)),
                "Request should contain key with custom prefix"
            );
        }
    }

    /// Test Redis with random data generation
    #[test]
    fn test_redis_random_data() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let key_gen = KeyGeneration::sequential(0);
        let mut protocol = RedisProtocol::with_workload_and_options(
            selector,
            key_gen,
            100,
            "key:".to_string(),
            true, // random data
            Some(42),
        );

        let request = protocol.next_request(0);

        // Count 'x' characters - random data should have very few
        let x_count = request.data.iter().filter(|&&b| b == b'x').count();
        assert!(x_count < 20, "Random data should not contain many 'x' chars, found {}", x_count);
    }

    /// Test Redis with Zipfian key distribution
    #[test]
    fn test_redis_zipfian_distribution() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let key_gen = KeyGeneration::zipfian(1000, 0.99).expect("Failed to create Zipfian");
        let mut protocol = RedisProtocol::with_workload(selector, key_gen, 64);

        let mut key_counts: HashMap<String, usize> = HashMap::new();

        for _ in 0..10000 {
            let request = protocol.next_request(0);
            let request_str = String::from_utf8_lossy(&request.data);

            // Extract key from request
            if let Some(start) = request_str.find("key:") {
                let key_part = &request_str[start..];
                if let Some(end) = key_part.find("\r\n") {
                    let key = key_part[..end].to_string();
                    *key_counts.entry(key).or_insert(0) += 1;
                }
            }
        }

        // Verify Zipfian skew - most popular key should be accessed much more
        let max_count = *key_counts.values().max().unwrap_or(&0);
        let min_count = *key_counts.values().filter(|&&c| c > 0).min().unwrap_or(&1);

        assert!(
            max_count > min_count * 5,
            "Zipfian should show skew: max={}, min={}",
            max_count,
            min_count
        );
    }

    /// Test Redis with Gaussian key distribution
    #[test]
    fn test_redis_gaussian_distribution() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let key_gen = KeyGeneration::gaussian(0.5, 0.1, 10000).expect("Failed to create Gaussian");
        let mut protocol = RedisProtocol::with_workload(selector, key_gen, 64);

        let mut keys: Vec<u64> = Vec::new();

        for _ in 0..10000 {
            let request = protocol.next_request(0);
            let request_str = String::from_utf8_lossy(&request.data);

            // Extract key number from request
            if let Some(key_num) = extract_redis_key_number(&request_str) {
                keys.push(key_num);
            }
        }

        // Calculate mean
        let sum: u64 = keys.iter().sum();
        let mean = sum as f64 / keys.len() as f64;

        // Mean should be close to 5000 (50% of 10000)
        assert!((mean - 5000.0).abs() < 300.0, "Gaussian mean {} should be close to 5000", mean);
    }

    /// Helper to extract key number from Redis request string
    fn extract_redis_key_number(s: &str) -> Option<u64> {
        let start = s.find("key:")?;
        let key_part = &s[start + 4..];
        let end = key_part.find("\r\n")?;
        key_part[..end].parse().ok()
    }

    /// Test Redis deterministic with seed
    #[test]
    fn test_redis_reproducibility() {
        let selector1 = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let key_gen1 = KeyGeneration::zipfian_with_seed(1000, 0.99, Some(12345)).expect("Failed");
        let mut proto1 = RedisProtocol::with_workload_and_options(
            selector1,
            key_gen1,
            50,
            "key:".to_string(),
            true,
            Some(12345),
        );

        let selector2 = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let key_gen2 = KeyGeneration::zipfian_with_seed(1000, 0.99, Some(12345)).expect("Failed");
        let mut proto2 = RedisProtocol::with_workload_and_options(
            selector2,
            key_gen2,
            50,
            "key:".to_string(),
            true,
            Some(12345),
        );

        for _ in 0..100 {
            let req1 = proto1.next_request(0);
            let req2 = proto2.next_request(0);
            assert_eq!(req1.data, req2.data, "Same seeds should produce identical requests");
        }
    }
}

// =============================================================================
// Masstree Protocol Distribution Integration Tests
// =============================================================================

mod masstree_distributions {
    use super::*;
    use xylem_protocols::masstree::{MasstreeOp, MasstreeProtocol};
    use xylem_protocols::workload::KeyGeneration;
    use xylem_protocols::Protocol;

    /// Test Masstree with custom key prefix
    #[test]
    fn test_masstree_custom_key_prefix() {
        let key_gen = KeyGeneration::sequential(0);
        let mut protocol = MasstreeProtocol::with_workload_and_options(
            MasstreeOp::Get,
            key_gen,
            64,
            "mydb:record:".to_string(),
            false,
            None,
            None, // no insert phase
        );
        protocol.mark_handshake_done(0);

        for i in 0..100 {
            let request = protocol.next_request(0);
            let request_str = String::from_utf8_lossy(&request.data);

            assert!(
                request_str.contains(&format!("mydb:record:{}", i)),
                "Request should contain key with custom prefix, got: {}",
                request_str
            );
        }
    }

    /// Test Masstree with random data generation
    #[test]
    fn test_masstree_random_data() {
        let key_gen = KeyGeneration::sequential(0);
        let mut protocol = MasstreeProtocol::with_workload_and_options(
            MasstreeOp::Set,
            key_gen,
            100,
            "key:".to_string(),
            true, // random data
            Some(42),
            None, // no insert phase
        );
        protocol.mark_handshake_done(0);

        let request = protocol.next_request(0);

        // Count 'x' characters - random data should have very few
        let x_count = request.data.iter().filter(|&&b| b == b'x').count();
        assert!(x_count < 20, "Random data should not contain many 'x' chars, found {}", x_count);
    }

    /// Test Masstree with Zipfian key distribution
    #[test]
    fn test_masstree_zipfian_distribution() {
        let key_gen = KeyGeneration::zipfian(1000, 0.99).expect("Failed to create Zipfian");
        let mut protocol = MasstreeProtocol::with_workload(MasstreeOp::Get, key_gen, 64);
        protocol.mark_handshake_done(0);

        let mut key_counts: HashMap<String, usize> = HashMap::new();

        for _ in 0..10000 {
            let request = protocol.next_request(0);
            let request_str = String::from_utf8_lossy(&request.data);

            // Extract key from request (format: key:N)
            if let Some(start) = request_str.find("key:") {
                // Find the end of the key (msgpack string ends with non-printable or next field)
                let key_start = start;
                let remaining = &request_str[key_start..];
                // Take characters until we hit something that's not part of the key
                let key: String = remaining
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == ':')
                    .collect();
                if !key.is_empty() {
                    *key_counts.entry(key).or_insert(0) += 1;
                }
            }
        }

        // Verify Zipfian skew
        let max_count = *key_counts.values().max().unwrap_or(&0);
        let min_count = *key_counts.values().filter(|&&c| c > 0).min().unwrap_or(&1);

        assert!(
            max_count > min_count * 5,
            "Zipfian should show skew: max={}, min={}",
            max_count,
            min_count
        );
    }

    /// Test Masstree with Gaussian key distribution
    #[test]
    fn test_masstree_gaussian_distribution() {
        let key_gen = KeyGeneration::gaussian(0.5, 0.1, 10000).expect("Failed to create Gaussian");
        let mut protocol = MasstreeProtocol::with_workload(MasstreeOp::Get, key_gen, 64);
        protocol.mark_handshake_done(0);

        let mut keys: Vec<u64> = Vec::new();

        for _ in 0..10000 {
            let request = protocol.next_request(0);
            let request_str = String::from_utf8_lossy(&request.data);

            // Extract key number
            if let Some(start) = request_str.find("key:") {
                let key_part = &request_str[start + 4..];
                // Parse digits
                let num_str: String = key_part.chars().take_while(|c| c.is_ascii_digit()).collect();
                if let Ok(key_num) = num_str.parse::<u64>() {
                    keys.push(key_num);
                }
            }
        }

        // Calculate mean
        let sum: u64 = keys.iter().sum();
        let mean = sum as f64 / keys.len() as f64;

        // Mean should be close to 5000
        assert!((mean - 5000.0).abs() < 300.0, "Gaussian mean {} should be close to 5000", mean);
    }

    /// Test Masstree deterministic with seed
    #[test]
    fn test_masstree_reproducibility() {
        let key_gen1 = KeyGeneration::zipfian_with_seed(1000, 0.99, Some(12345)).expect("Failed");
        let mut proto1 = MasstreeProtocol::with_workload_and_options(
            MasstreeOp::Set,
            key_gen1,
            50,
            "key:".to_string(),
            true,
            Some(12345),
            None, // no insert phase
        );
        proto1.mark_handshake_done(0);

        let key_gen2 = KeyGeneration::zipfian_with_seed(1000, 0.99, Some(12345)).expect("Failed");
        let mut proto2 = MasstreeProtocol::with_workload_and_options(
            MasstreeOp::Set,
            key_gen2,
            50,
            "key:".to_string(),
            true,
            Some(12345),
            None, // no insert phase
        );
        proto2.mark_handshake_done(0);

        for _ in 0..100 {
            let req1 = proto1.next_request(0);
            let req2 = proto2.next_request(0);
            assert_eq!(req1.data, req2.data, "Same seeds should produce identical requests");
        }
    }

    /// Test Masstree round-robin key distribution
    #[test]
    fn test_masstree_round_robin() {
        let key_gen = KeyGeneration::round_robin(10);
        let mut protocol = MasstreeProtocol::with_workload(MasstreeOp::Get, key_gen, 64);
        protocol.mark_handshake_done(0);

        let mut key_sequence: Vec<String> = Vec::new();

        for _ in 0..30 {
            let request = protocol.next_request(0);
            let request_str = String::from_utf8_lossy(&request.data);

            if let Some(start) = request_str.find("key:") {
                let key_part = &request_str[start + 4..];
                let num_str: String = key_part.chars().take_while(|c| c.is_ascii_digit()).collect();
                key_sequence.push(num_str);
            }
        }

        // Should cycle: 0,1,2,3,4,5,6,7,8,9,0,1,2,...
        for (i, key) in key_sequence.iter().enumerate().take(30) {
            assert_eq!(key, &(i % 10).to_string(), "Round-robin should cycle through keys");
        }
    }
}

// =============================================================================
// Cross-Protocol Consistency Tests
// =============================================================================

mod cross_protocol {
    use xylem_protocols::masstree::{MasstreeOp, MasstreeProtocol};
    use xylem_protocols::redis::command_selector::FixedCommandSelector;
    use xylem_protocols::redis::{RedisOp, RedisProtocol};
    use xylem_protocols::workload::KeyGeneration;
    use xylem_protocols::Protocol;

    /// Test that same seed produces same key sequence for both protocols
    #[test]
    fn test_same_key_sequence() {
        // Create key generators with same seed
        let redis_keygen = KeyGeneration::zipfian_with_seed(1000, 0.99, Some(42)).expect("Failed");
        let masstree_keygen =
            KeyGeneration::zipfian_with_seed(1000, 0.99, Some(42)).expect("Failed");

        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut redis = RedisProtocol::with_workload(selector, redis_keygen, 64);

        let mut masstree = MasstreeProtocol::with_workload(MasstreeOp::Get, masstree_keygen, 64);
        masstree.mark_handshake_done(0);

        // Extract keys from both and compare
        for _ in 0..100 {
            let redis_req = redis.next_request(0);
            let masstree_req = masstree.next_request(0);

            let redis_str = String::from_utf8_lossy(&redis_req.data);
            let masstree_str = String::from_utf8_lossy(&masstree_req.data);

            // Both should contain the same key number
            let redis_key = extract_key_number(&redis_str);
            let masstree_key = extract_key_number(&masstree_str);

            assert_eq!(redis_key, masstree_key, "Same seed should produce same key sequence");
        }
    }

    fn extract_key_number(s: &str) -> Option<u64> {
        if let Some(start) = s.find("key:") {
            let key_part = &s[start + 4..];
            let num_str: String = key_part.chars().take_while(|c| c.is_ascii_digit()).collect();
            num_str.parse().ok()
        } else {
            None
        }
    }

    /// Test that key prefix is applied consistently
    #[test]
    fn test_key_prefix_consistency() {
        let prefix = "benchmark:test:";

        let redis_keygen = KeyGeneration::sequential(0);
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut redis = RedisProtocol::with_workload_and_options(
            selector,
            redis_keygen,
            64,
            prefix.to_string(),
            false,
            None,
        );

        let masstree_keygen = KeyGeneration::sequential(0);
        let mut masstree = MasstreeProtocol::with_workload_and_options(
            MasstreeOp::Get,
            masstree_keygen,
            64,
            prefix.to_string(),
            false,
            None,
            None, // no insert phase
        );
        masstree.mark_handshake_done(0);

        // Both should use the same prefix
        let redis_req = redis.next_request(0);
        let masstree_req = masstree.next_request(0);

        let redis_str = String::from_utf8_lossy(&redis_req.data);
        let masstree_str = String::from_utf8_lossy(&masstree_req.data);

        assert!(redis_str.contains("benchmark:test:0"), "Redis should use custom prefix");
        assert!(masstree_str.contains("benchmark:test:0"), "Masstree should use custom prefix");
    }
}

// =============================================================================
// Config Parsing Tests
// =============================================================================

mod config_parsing {
    use xylem_protocols::configs::{KeysConfig, MasstreeConfig, RedisConfig, ValueSizeConfig};

    /// Test KeysConfig parsing with all distributions
    #[test]
    fn test_keys_config_sequential() {
        let json = r#"{"strategy": "sequential", "start": 100, "value_size": 64}"#;
        let config: KeysConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.value_size(), 64);
        assert_eq!(config.prefix(), "key:");
    }

    #[test]
    fn test_keys_config_random() {
        let json = r#"{"strategy": "random", "max": 10000, "value_size": 128}"#;
        let config: KeysConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.value_size(), 128);
    }

    #[test]
    fn test_keys_config_zipfian() {
        let json = r#"{"strategy": "zipfian", "n": 100000, "theta": 0.99, "value_size": 256}"#;
        let config: KeysConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.value_size(), 256);
    }

    #[test]
    fn test_keys_config_gaussian() {
        let json = r#"{"strategy": "gaussian", "mean_pct": 0.5, "std_dev_pct": 0.1, "max": 1000000, "value_size": 512}"#;
        let config: KeysConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.value_size(), 512);
    }

    #[test]
    fn test_keys_config_custom_prefix() {
        let json =
            r#"{"strategy": "sequential", "start": 0, "value_size": 64, "prefix": "memtier-"}"#;
        let config: KeysConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.prefix(), "memtier-");
    }

    /// Test ValueSizeConfig parsing
    #[test]
    fn test_value_size_config_fixed() {
        let json = r#"{"strategy": "fixed", "size": 128}"#;
        let config: ValueSizeConfig = serde_json::from_str(json).unwrap();
        let mut gen = config.to_generator(None).unwrap();
        assert_eq!(gen.next_size(), 128);
    }

    #[test]
    fn test_value_size_config_uniform() {
        let json = r#"{"strategy": "uniform", "min": 64, "max": 256}"#;
        let config: ValueSizeConfig = serde_json::from_str(json).unwrap();
        let mut gen = config.to_generator(None).unwrap();
        let size = gen.next_size();
        assert!((64..=256).contains(&size));
    }

    #[test]
    fn test_value_size_config_normal() {
        let json =
            r#"{"strategy": "normal", "mean": 128.0, "std_dev": 32.0, "min": 64, "max": 256}"#;
        let config: ValueSizeConfig = serde_json::from_str(json).unwrap();
        let mut gen = config.to_generator(None).unwrap();
        let size = gen.next_size();
        assert!((64..=256).contains(&size));
    }

    /// Test RedisConfig with all options
    #[test]
    fn test_redis_config_full() {
        let json = r#"{
            "keys": {
                "strategy": "zipfian",
                "n": 100000,
                "theta": 0.99,
                "value_size": 128,
                "prefix": "redis:"
            },
            "random_data": true,
            "value_size": {
                "strategy": "uniform",
                "min": 64,
                "max": 512
            }
        }"#;
        let config: RedisConfig = serde_json::from_str(json).unwrap();
        assert!(config.random_data);
        assert!(config.value_size.is_some());
        assert_eq!(config.keys.prefix(), "redis:");
    }

    /// Test MasstreeConfig with all options
    #[test]
    fn test_masstree_config_full() {
        let json = r#"{
            "keys": {
                "strategy": "gaussian",
                "mean_pct": 0.5,
                "std_dev_pct": 0.15,
                "max": 500000,
                "value_size": 256,
                "prefix": "mt:"
            },
            "operation": "set",
            "random_data": true,
            "value_size": {
                "strategy": "normal",
                "mean": 256.0,
                "std_dev": 64.0,
                "min": 128,
                "max": 512
            }
        }"#;
        let config: MasstreeConfig = serde_json::from_str(json).unwrap();
        assert!(config.random_data);
        assert!(config.value_size.is_some());
        assert_eq!(config.keys.prefix(), "mt:");
        assert_eq!(config.operation, "set");
    }
}
