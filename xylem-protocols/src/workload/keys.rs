//! Key generation strategies for KV protocols
//!
//! This module provides key generation strategies used by key-value protocols
//! like Redis and Memcached.

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use xylem_common::{Distribution, NormalDistribution, ZipfianDistribution};

/// Trait for key generation (to enable per-command key generators)
pub trait KeyGeneratorTrait: Send {
    fn next_key(&mut self) -> u64;
    fn reset(&mut self);
}

/// Key generation strategy
#[derive(Debug)]
pub enum KeyGeneration {
    /// Sequential keys starting from a value
    Sequential { start: u64, current: u64 },
    /// Random keys in range [0, max)
    Random { max: u64, rng: SmallRng },
    /// Round-robin over a range
    RoundRobin { max: u64, current: u64 },
    /// Zipfian distribution (hot-key pattern)
    Zipfian(ZipfianDistribution),
    /// Gaussian (Normal) distribution (bell curve pattern)
    Gaussian {
        mean_pct: f64,
        std_dev_pct: f64,
        max: u64,
        dist: NormalDistribution,
    },
}

impl KeyGeneratorTrait for KeyGeneration {
    fn next_key(&mut self) -> u64 {
        match self {
            Self::Sequential { current, .. } => {
                let key = *current;
                *current = current.wrapping_add(1);
                key
            }
            Self::Random { max, rng } => rng.random_range(0..*max),
            Self::RoundRobin { max, current } => {
                let key = *current;
                *current = (*current + 1) % *max;
                key
            }
            Self::Zipfian(dist) => dist.sample_key(),
            Self::Gaussian { max, dist, .. } => {
                let sample = dist.sample();
                let clamped = sample.max(0.0).min(*max as f64 - 1.0);
                clamped as u64
            }
        }
    }

    fn reset(&mut self) {
        match self {
            Self::Sequential { start, current } => {
                *current = *start;
            }
            Self::Random { .. } => {}
            Self::RoundRobin { current, .. } => {
                *current = 0;
            }
            Self::Zipfian(dist) => {
                dist.reset();
            }
            Self::Gaussian { dist, .. } => {
                dist.reset();
            }
        }
    }
}

impl Clone for KeyGeneration {
    fn clone(&self) -> Self {
        match self {
            Self::Sequential { start, current } => {
                Self::Sequential { start: *start, current: *current }
            }
            Self::Random { max, .. } => Self::Random { max: *max, rng: SmallRng::from_os_rng() },
            Self::RoundRobin { max, current } => Self::RoundRobin { max: *max, current: *current },
            Self::Zipfian(dist) => Self::Zipfian(
                ZipfianDistribution::new(dist.n(), dist.exponent())
                    .expect("Invalid Zipf parameters"),
            ),
            Self::Gaussian { mean_pct, std_dev_pct, max, .. } => {
                let mean = (*max as f64) * mean_pct;
                let std_dev = (*max as f64) * std_dev_pct;
                let dist =
                    NormalDistribution::new(mean, std_dev).expect("Invalid Gaussian parameters");
                Self::Gaussian {
                    mean_pct: *mean_pct,
                    std_dev_pct: *std_dev_pct,
                    max: *max,
                    dist,
                }
            }
        }
    }
}

impl KeyGeneration {
    /// Create a sequential key generator
    pub fn sequential(start: u64) -> Self {
        Self::Sequential { start, current: start }
    }

    /// Create a round-robin key generator
    pub fn round_robin(max: u64) -> Self {
        Self::RoundRobin { max, current: 0 }
    }

    /// Create a random key generator with entropy-based seed
    pub fn random(max: u64) -> Self {
        Self::random_with_seed(max, None)
    }

    /// Create a random key generator with explicit seed
    pub fn random_with_seed(max: u64, seed: Option<u64>) -> Self {
        let rng = match seed {
            Some(s) => SmallRng::seed_from_u64(s),
            None => SmallRng::from_os_rng(),
        };
        Self::Random { max, rng }
    }

    /// Create a Zipfian key generator with entropy-based seed
    pub fn zipfian(n: u64, s: f64) -> anyhow::Result<Self> {
        Self::zipfian_with_seed(n, s, None)
    }

    /// Create a Zipfian key generator with explicit seed
    pub fn zipfian_with_seed(n: u64, s: f64, seed: Option<u64>) -> anyhow::Result<Self> {
        let dist = ZipfianDistribution::with_seed(n, s, seed)?;
        Ok(Self::Zipfian(dist))
    }

    /// Create a Gaussian (Normal) key generator with entropy-based seed
    pub fn gaussian(mean_pct: f64, std_dev_pct: f64, max: u64) -> anyhow::Result<Self> {
        Self::gaussian_with_seed(mean_pct, std_dev_pct, max, None)
    }

    /// Create a Gaussian (Normal) key generator with explicit seed
    pub fn gaussian_with_seed(
        mean_pct: f64,
        std_dev_pct: f64,
        max: u64,
        seed: Option<u64>,
    ) -> anyhow::Result<Self> {
        if max == 0 {
            anyhow::bail!("Gaussian max must be > 0");
        }
        if !(0.0..=1.0).contains(&mean_pct) {
            anyhow::bail!("Gaussian mean_pct must be in range [0.0, 1.0]");
        }
        if !(0.0..=1.0).contains(&std_dev_pct) {
            anyhow::bail!("Gaussian std_dev_pct must be in range [0.0, 1.0]");
        }

        let mean = (max as f64) * mean_pct;
        let std_dev = (max as f64) * std_dev_pct;

        let dist = NormalDistribution::with_seed(mean, std_dev, seed)?;
        Ok(Self::Gaussian { mean_pct, std_dev_pct, max, dist })
    }

    /// Generate the next key (inherent method, calls trait method)
    pub fn next_key(&mut self) -> u64 {
        KeyGeneratorTrait::next_key(self)
    }

    /// Reset the generator (inherent method, calls trait method)
    pub fn reset(&mut self) {
        KeyGeneratorTrait::reset(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequential_keys() {
        let mut keygen = KeyGeneration::sequential(100);
        assert_eq!(keygen.next_key(), 100);
        assert_eq!(keygen.next_key(), 101);
        assert_eq!(keygen.next_key(), 102);
    }

    #[test]
    fn test_sequential_reset() {
        let mut keygen = KeyGeneration::sequential(50);
        keygen.next_key();
        keygen.next_key();
        keygen.reset();
        assert_eq!(keygen.next_key(), 50);
    }

    #[test]
    fn test_round_robin() {
        let mut keygen = KeyGeneration::round_robin(3);
        assert_eq!(keygen.next_key(), 0);
        assert_eq!(keygen.next_key(), 1);
        assert_eq!(keygen.next_key(), 2);
        assert_eq!(keygen.next_key(), 0);
    }

    #[test]
    fn test_random_keys() {
        let mut keygen = KeyGeneration::random(10000);
        let k1 = keygen.next_key();
        let k2 = keygen.next_key();
        assert!(k1 < 10000);
        assert!(k2 < 10000);
        assert_ne!(k1, k2);
    }

    #[test]
    fn test_zipfian_basic() {
        let mut keygen = KeyGeneration::zipfian(100, 0.99).expect("Failed to create Zipfian");
        for _ in 0..1000 {
            let key = keygen.next_key();
            assert!(key < 100, "Key {} out of range [0, 100)", key);
        }
    }

    #[test]
    fn test_zipfian_parameter_validation() {
        let result = KeyGeneration::zipfian(0, 0.99);
        assert!(result.is_err());

        let result = KeyGeneration::zipfian(100, -0.5);
        assert!(result.is_err());

        let result = KeyGeneration::zipfian(100, 0.99);
        assert!(result.is_ok());
    }
}
