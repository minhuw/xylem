//! Cryptographic seed derivation for reproducible randomness
//!
//! This module provides a secure way to derive component-specific seeds from a master seed.
//! Uses SHA-256 to ensure:
//! - Deterministic: Same master + component = same derived seed
//! - Collision-resistant: Different components get independent seeds
//! - Uniform distribution: Derived seeds are uniformly distributed
//!
//! # Example
//!
//! ```
//! use xylem_core::seed::derive_seed;
//!
//! let master_seed = 42;
//! let zipf_seed = derive_seed(master_seed, "zipfian_distribution");
//! let poisson_seed = derive_seed(master_seed, "poisson_policy");
//!
//! // Same inputs always produce same output
//! assert_eq!(derive_seed(42, "test"), derive_seed(42, "test"));
//!
//! // Different components get different seeds
//! assert_ne!(derive_seed(42, "comp1"), derive_seed(42, "comp2"));
//! ```

use sha2::{Digest, Sha256};

/// Derive a component-specific seed from a master seed using SHA-256
///
/// This function takes a master seed and a component identifier and produces
/// a deterministic, uniformly-distributed u64 seed for that component.
///
/// # Parameters
/// - `master_seed`: The master seed (e.g., from CLI `--seed` argument)
/// - `component`: Component identifier (e.g., "zipfian_dist", "poisson_policy")
///
/// # Returns
/// A deterministic u64 seed derived from the inputs
///
/// # Example
///
/// ```
/// use xylem_core::seed::derive_seed;
///
/// // Derive independent seeds for different components
/// let master = 12345;
/// let seed1 = derive_seed(master, "workload_generator");
/// let seed2 = derive_seed(master, "poisson_timing");
///
/// // Seeds are deterministic
/// assert_eq!(seed1, derive_seed(master, "workload_generator"));
/// assert_ne!(seed1, seed2);  // Different components get different seeds
/// ```
pub fn derive_seed(master_seed: u64, component: &str) -> u64 {
    let mut hasher = Sha256::new();

    // Hash the master seed (8 bytes, big-endian for consistency)
    hasher.update(master_seed.to_be_bytes());

    // Hash the component identifier
    hasher.update(component.as_bytes());

    // Get the hash result (32 bytes)
    let result = hasher.finalize();

    // Take first 8 bytes and convert to u64 (big-endian)
    u64::from_be_bytes([
        result[0], result[1], result[2], result[3], result[4], result[5], result[6], result[7],
    ])
}

/// Standard component names for seed derivation
///
/// Using constants ensures consistent naming across the codebase
pub mod components {
    pub const ZIPFIAN_DIST: &str = "zipfian_distribution";
    pub const EXPONENTIAL_DIST: &str = "exponential_distribution";
    pub const UNIFORM_DIST: &str = "uniform_distribution";
    pub const NORMAL_DIST: &str = "normal_distribution";
    pub const GAUSSIAN_DIST: &str = "gaussian_distribution";
    pub const RANDOM_KEYS: &str = "random_key_generation";
    pub const POISSON_POLICY: &str = "poisson_policy";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_seed_deterministic() {
        // Same inputs should produce same output
        let seed1 = derive_seed(42, "test_component");
        let seed2 = derive_seed(42, "test_component");
        assert_eq!(seed1, seed2);
    }

    #[test]
    fn test_derive_seed_different_components() {
        // Different components should produce different seeds
        let master = 12345;
        let seed1 = derive_seed(master, "component_a");
        let seed2 = derive_seed(master, "component_b");
        assert_ne!(seed1, seed2);
    }

    #[test]
    fn test_derive_seed_different_masters() {
        // Different master seeds should produce different results
        let seed1 = derive_seed(100, "test");
        let seed2 = derive_seed(200, "test");
        assert_ne!(seed1, seed2);
    }

    #[test]
    fn test_derive_seed_collision_resistance() {
        // Similar component names should produce very different seeds
        let master = 999;
        let seed1 = derive_seed(master, "test1");
        let seed2 = derive_seed(master, "test2");

        // Seeds should be very different (not just off by 1)
        let diff = seed1.abs_diff(seed2);
        assert!(diff > 1000, "Seeds too similar: {} vs {}", seed1, seed2);
    }

    #[test]
    fn test_component_constants() {
        // Verify component names are unique
        use components::*;

        let master = 42;
        let seeds = [
            derive_seed(master, ZIPFIAN_DIST),
            derive_seed(master, EXPONENTIAL_DIST),
            derive_seed(master, UNIFORM_DIST),
            derive_seed(master, NORMAL_DIST),
            derive_seed(master, RANDOM_KEYS),
            derive_seed(master, POISSON_POLICY),
        ];

        // All seeds should be unique
        for i in 0..seeds.len() {
            for j in (i + 1)..seeds.len() {
                assert_ne!(seeds[i], seeds[j], "Seeds {} and {} are not unique", i, j);
            }
        }
    }

    #[test]
    fn test_seed_distribution() {
        // Derived seeds should be well-distributed
        let master = 777;
        let mut seeds = Vec::new();

        for i in 0..100 {
            let component = format!("component_{}", i);
            seeds.push(derive_seed(master, &component));
        }

        // Check that seeds use full u64 range (at least some high-bit seeds)
        let high_bit_count = seeds.iter().filter(|&&s| s > u64::MAX / 2).count();
        assert!(
            high_bit_count > 30,
            "Seeds not well distributed: only {} high-bit seeds",
            high_bit_count
        );
    }

    #[test]
    fn test_end_to_end_reproducibility() {
        // Test that using the same master seed produces identical results across all components
        use crate::scheduler::policy::{PoissonPolicy, Policy};
        use crate::workload::generator::KeyGeneration;
        use components::*;
        use xylem_common::{Distribution, ExponentialDistribution, ZipfianDistribution};

        let master_seed = 12345;

        // Run 1: Generate sequences with derived seeds
        let mut zipf1 = ZipfianDistribution::with_seed(
            1000,
            0.99,
            Some(derive_seed(master_seed, ZIPFIAN_DIST)),
        )
        .unwrap();
        let mut exp1 = ExponentialDistribution::with_seed(
            1000.0,
            Some(derive_seed(master_seed, EXPONENTIAL_DIST)),
        )
        .unwrap();
        let mut rand_keys1 =
            KeyGeneration::random_with_seed(10000, Some(derive_seed(master_seed, RANDOM_KEYS)));
        let mut poisson1 =
            PoissonPolicy::with_seed(1000.0, Some(derive_seed(master_seed, POISSON_POLICY)))
                .unwrap();

        // Generate samples from run 1
        let zipf_samples1: Vec<u64> = (0..10).map(|_| zipf1.sample_key()).collect();
        let exp_samples1: Vec<f64> = (0..10).map(|_| exp1.sample()).collect();
        let key_samples1: Vec<u64> = (0..10).map(|_| rand_keys1.next_key()).collect();

        // Poisson policy needs send times
        poisson1.on_request_sent(1000);
        let poisson_time1 = poisson1.next_send_time(1000);

        // Run 2: Generate sequences with same derived seeds
        let mut zipf2 = ZipfianDistribution::with_seed(
            1000,
            0.99,
            Some(derive_seed(master_seed, ZIPFIAN_DIST)),
        )
        .unwrap();
        let mut exp2 = ExponentialDistribution::with_seed(
            1000.0,
            Some(derive_seed(master_seed, EXPONENTIAL_DIST)),
        )
        .unwrap();
        let mut rand_keys2 =
            KeyGeneration::random_with_seed(10000, Some(derive_seed(master_seed, RANDOM_KEYS)));
        let mut poisson2 =
            PoissonPolicy::with_seed(1000.0, Some(derive_seed(master_seed, POISSON_POLICY)))
                .unwrap();

        // Generate samples from run 2
        let zipf_samples2: Vec<u64> = (0..10).map(|_| zipf2.sample_key()).collect();
        let exp_samples2: Vec<f64> = (0..10).map(|_| exp2.sample()).collect();
        let key_samples2: Vec<u64> = (0..10).map(|_| rand_keys2.next_key()).collect();

        poisson2.on_request_sent(1000);
        let poisson_time2 = poisson2.next_send_time(1000);

        // Verify all sequences are identical
        assert_eq!(zipf_samples1, zipf_samples2, "Zipfian samples should be identical");
        assert_eq!(exp_samples1, exp_samples2, "Exponential samples should be identical");
        assert_eq!(key_samples1, key_samples2, "Random key samples should be identical");
        assert_eq!(poisson_time1, poisson_time2, "Poisson timing should be identical");

        println!("✓ Reproducibility verified across all components:");
        println!("  Zipfian: {:?}", &zipf_samples1[..3]);
        println!("  Exponential: {:?}", &exp_samples1[..3]);
        println!("  Random keys: {:?}", &key_samples1[..3]);
        println!("  Poisson next time: {:?}", poisson_time1);
    }
}
