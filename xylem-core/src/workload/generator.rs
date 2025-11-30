//! Request generation

use super::distributions::{Distribution, NormalDistribution, ZipfianDistribution};
use super::value_size::ValueSizeGenerator;
use crate::timing::time_ns;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::time::Duration;

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
        /// Mean as percentage of keyspace (0.0 to 1.0)
        mean_pct: f64,
        /// Standard deviation as percentage of keyspace (0.0 to 1.0)
        std_dev_pct: f64,
        /// Maximum key value (keyspace size)
        max: u64,
        /// Underlying normal distribution
        dist: NormalDistribution,
    },
}

// Implement KeyGeneratorTrait for KeyGeneration
impl KeyGeneratorTrait for KeyGeneration {
    fn next_key(&mut self) -> u64 {
        // Call the inherent next_key method
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
            Self::Random { .. } => {
                // RNG state cannot be easily reset
            }
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

// Manual Clone implementation
impl Clone for KeyGeneration {
    fn clone(&self) -> Self {
        match self {
            Self::Sequential { start, current } => {
                Self::Sequential { start: *start, current: *current }
            }
            Self::Random { max, .. } => {
                // Create new RNG with entropy (cloning doesn't preserve RNG state)
                Self::Random { max: *max, rng: SmallRng::from_os_rng() }
            }
            Self::RoundRobin { max, current } => Self::RoundRobin { max: *max, current: *current },
            Self::Zipfian(dist) => {
                // Recreate distribution with same parameters
                Self::Zipfian(
                    ZipfianDistribution::new(dist.n(), dist.exponent())
                        .expect("Invalid Zipf parameters"),
                )
            }
            Self::Gaussian { mean_pct, std_dev_pct, max, .. } => {
                // Recreate distribution with same parameters
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
    ///
    /// # Parameters
    /// - `max`: Maximum key value (keys will be in range [0, max))
    pub fn random(max: u64) -> Self {
        Self::random_with_seed(max, None)
    }

    /// Create a random key generator with explicit seed
    ///
    /// # Parameters
    /// - `max`: Maximum key value (keys will be in range [0, max))
    /// - `seed`: Optional seed for reproducibility (None = use entropy)
    pub fn random_with_seed(max: u64, seed: Option<u64>) -> Self {
        let rng = match seed {
            Some(s) => SmallRng::seed_from_u64(s),
            None => SmallRng::from_os_rng(),
        };
        Self::Random { max, rng }
    }

    /// Create a Zipfian key generator with entropy-based seed
    ///
    /// # Parameters
    /// - `n`: Number of unique keys in the range [0, n-1]
    /// - `s`: Exponent (theta) controlling skewness. Common values:
    ///   - s = 0.0: Uniform distribution
    ///   - s = 0.99: Typical database workload (YCSB default)
    ///   - s = 1.0: Classic Zipf (1/rank)
    ///   - s > 1.0: More skewed toward low keys
    ///
    /// # Returns
    /// Returns an error if n == 0 or s < 0.0
    pub fn zipfian(n: u64, s: f64) -> anyhow::Result<Self> {
        Self::zipfian_with_seed(n, s, None)
    }

    /// Create a Zipfian key generator with explicit seed
    ///
    /// # Parameters
    /// - `n`: Number of unique keys in the range [0, n-1]
    /// - `s`: Exponent (theta) controlling skewness
    /// - `seed`: Optional seed for reproducibility (None = use entropy)
    ///
    /// # Returns
    /// Returns an error if n == 0 or s < 0.0
    pub fn zipfian_with_seed(n: u64, s: f64, seed: Option<u64>) -> anyhow::Result<Self> {
        let dist = ZipfianDistribution::with_seed(n, s, seed)?;
        Ok(Self::Zipfian(dist))
    }

    /// Create a Gaussian (Normal) key generator with entropy-based seed
    ///
    /// # Parameters
    /// - `mean_pct`: Mean as percentage of keyspace (0.0 to 1.0)
    /// - `std_dev_pct`: Standard deviation as percentage of keyspace (0.0 to 1.0)
    /// - `max`: Maximum key value (keyspace size)
    ///
    /// # Returns
    /// Returns an error if max == 0, mean_pct or std_dev_pct are out of bounds
    pub fn gaussian(mean_pct: f64, std_dev_pct: f64, max: u64) -> anyhow::Result<Self> {
        Self::gaussian_with_seed(mean_pct, std_dev_pct, max, None)
    }

    /// Create a Gaussian (Normal) key generator with explicit seed
    ///
    /// # Parameters
    /// - `mean_pct`: Mean as percentage of keyspace (0.0 to 1.0)
    /// - `std_dev_pct`: Standard deviation as percentage of keyspace (0.0 to 1.0)
    /// - `max`: Maximum key value (keyspace size)
    /// - `seed`: Optional seed for reproducibility (None = use entropy)
    ///
    /// # Returns
    /// Returns an error if max == 0, mean_pct or std_dev_pct are out of bounds
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

        // Convert percentages to absolute values
        let mean = (max as f64) * mean_pct;
        let std_dev = (max as f64) * std_dev_pct;

        let dist = NormalDistribution::with_seed(mean, std_dev, seed)?;
        Ok(Self::Gaussian { mean_pct, std_dev_pct, max, dist })
    }

    /// Generate the next key
    pub fn next_key(&mut self) -> u64 {
        match self {
            Self::Sequential { current, .. } => {
                let key = *current;
                *current = current.wrapping_add(1);
                key
            }
            Self::Random { max, rng } => {
                // Generate random key in range [0, max)
                rng.random_range(0..*max)
            }
            Self::RoundRobin { max, current } => {
                let key = *current;
                *current = (*current + 1) % *max;
                key
            }
            Self::Zipfian(dist) => dist.sample_key(),
            Self::Gaussian { max, dist, .. } => {
                // Sample from normal distribution and clamp to [0, max)
                let sample = dist.sample();
                let clamped = sample.max(0.0).min(*max as f64 - 1.0);
                clamped as u64
            }
        }
    }

    /// Reset the generator
    pub fn reset(&mut self) {
        match self {
            Self::Sequential { start, current } => {
                *current = *start;
            }
            Self::Random { .. } => {
                // RNG state cannot be easily reset, continues from current state
            }
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

/// Request rate control
#[derive(Debug, Clone)]
pub enum RateControl {
    /// No rate control (closed-loop, max throughput)
    ClosedLoop,
    /// Fixed rate (requests per second)
    Fixed { rate: f64 },
}

/// Request generator
pub struct RequestGenerator {
    key_gen: KeyGeneration,
    rate_control: RateControl,
    value_size_gen: Box<dyn ValueSizeGenerator>,
    /// Last request time in nanoseconds (from time_ns())
    last_request_time_ns: Option<u64>,
    request_count: u64,
    /// Optional data importer for testing with real data
    data_importer: Option<super::data_import::DataImporter>,
}

impl RequestGenerator {
    /// Create a new request generator with a value size generator
    pub fn new(
        key_gen: KeyGeneration,
        rate_control: RateControl,
        value_size_gen: Box<dyn ValueSizeGenerator>,
    ) -> Self {
        Self {
            key_gen,
            rate_control,
            value_size_gen,
            last_request_time_ns: None,
            request_count: 0,
            data_importer: None,
        }
    }

    /// Create a request generator with imported data
    pub fn with_imported_data(
        data_importer: super::data_import::DataImporter,
        rate_control: RateControl,
        value_size_gen: Box<dyn ValueSizeGenerator>,
    ) -> Self {
        Self {
            key_gen: KeyGeneration::sequential(0), // Unused when importing
            rate_control,
            value_size_gen,
            last_request_time_ns: None,
            request_count: 0,
            data_importer: Some(data_importer),
        }
    }

    /// Check if this generator is using imported data
    pub fn is_using_imported_data(&self) -> bool {
        self.data_importer.is_some()
    }

    /// Generate the next request parameters (key, value_size)
    pub fn next_request(&mut self) -> (u64, usize) {
        let key = self.key_gen.next_key();
        let value_size = self.value_size_gen.next_size();
        self.request_count += 1;
        (key, value_size)
    }

    /// Generate the next request with command-specific size
    pub fn next_request_for_command(&mut self, command: &str) -> (u64, usize) {
        let key = self.key_gen.next_key();
        let value_size = self.value_size_gen.next_size_for_command(command);
        self.request_count += 1;
        (key, value_size)
    }

    /// Generate request from imported data (returns key as string and value as bytes)
    ///
    /// # Returns
    /// - `(key, value_bytes)` tuple where key is the string key and value_bytes is the data
    ///
    /// # Panics
    /// Panics if called when no data importer is configured
    pub fn next_request_from_import(&mut self) -> (String, Vec<u8>) {
        let importer = self
            .data_importer
            .as_mut()
            .expect("next_request_from_import called but no data importer configured");

        let entry = importer.next_random();
        self.request_count += 1;

        (entry.key.clone(), entry.value_bytes())
    }

    /// Calculate delay until next request should be sent
    pub fn delay_until_next(&mut self) -> Option<Duration> {
        match self.rate_control {
            RateControl::ClosedLoop => None,
            RateControl::Fixed { rate } => {
                let inter_arrival_ns = (1_000_000_000.0 / rate) as u64;

                if let Some(last_time_ns) = self.last_request_time_ns {
                    let now_ns = time_ns();
                    let elapsed_ns = now_ns.saturating_sub(last_time_ns);
                    if elapsed_ns < inter_arrival_ns {
                        Some(Duration::from_nanos(inter_arrival_ns - elapsed_ns))
                    } else {
                        Some(Duration::ZERO)
                    }
                } else {
                    self.last_request_time_ns = Some(time_ns());
                    Some(Duration::ZERO)
                }
            }
        }
    }

    /// Mark that a request was sent (for rate control)
    pub fn mark_request_sent(&mut self) {
        self.last_request_time_ns = Some(time_ns());
    }

    /// Get total request count
    pub fn request_count(&self) -> u64 {
        self.request_count
    }

    /// Reset the generator
    pub fn reset(&mut self) {
        self.key_gen.reset();
        self.value_size_gen.reset();
        if let Some(importer) = &mut self.data_importer {
            importer.reset();
        }
        self.last_request_time_ns = None;
        self.request_count = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::FixedSize;

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

        // Keys should be in range
        assert!(k1 < 10000);
        assert!(k2 < 10000);

        // They should be different (very high probability)
        assert_ne!(k1, k2);
    }

    #[test]
    fn test_request_generator_closed_loop() {
        let keygen = KeyGeneration::sequential(0);
        let mut gen =
            RequestGenerator::new(keygen, RateControl::ClosedLoop, Box::new(FixedSize::new(64)));

        let (key1, size1) = gen.next_request();
        assert_eq!(key1, 0);
        assert_eq!(size1, 64);

        let (key2, size2) = gen.next_request();
        assert_eq!(key2, 1);
        assert_eq!(size2, 64);

        // Closed loop should have no delay
        assert_eq!(gen.delay_until_next(), None);
    }

    #[test]
    fn test_request_generator_fixed_rate() {
        let keygen = KeyGeneration::sequential(0);
        let rate = 1000.0; // 1000 req/s = 1ms per request
        let mut gen = RequestGenerator::new(
            keygen,
            RateControl::Fixed { rate },
            Box::new(FixedSize::new(64)),
        );

        let (key, size) = gen.next_request();
        assert_eq!(key, 0);
        assert_eq!(size, 64);

        gen.mark_request_sent();

        // Should have a delay for fixed rate
        let delay = gen.delay_until_next();
        assert!(delay.is_some());
    }

    #[test]
    fn test_request_count() {
        let keygen = KeyGeneration::sequential(0);
        let mut gen =
            RequestGenerator::new(keygen, RateControl::ClosedLoop, Box::new(FixedSize::new(64)));

        assert_eq!(gen.request_count(), 0);

        gen.next_request();
        assert_eq!(gen.request_count(), 1);

        gen.next_request();
        assert_eq!(gen.request_count(), 2);
    }

    #[test]
    fn test_reset() {
        let keygen = KeyGeneration::sequential(10);
        let mut gen =
            RequestGenerator::new(keygen, RateControl::ClosedLoop, Box::new(FixedSize::new(64)));

        gen.next_request();
        gen.next_request();

        gen.reset();

        assert_eq!(gen.request_count(), 0);
        let (key, _) = gen.next_request();
        assert_eq!(key, 10);
    }

    #[test]
    fn test_zipfian_basic() {
        let mut keygen = KeyGeneration::zipfian(100, 0.99).expect("Failed to create Zipfian");

        // Generate some keys and verify they're in valid range
        for _ in 0..1000 {
            let key = keygen.next_key();
            assert!(key < 100, "Key {} out of range [0, 100)", key);
        }
    }

    #[test]
    fn test_zipfian_parameter_validation() {
        // n must be > 0
        let result = KeyGeneration::zipfian(0, 0.99);
        assert!(result.is_err());

        // s must be >= 0
        let result = KeyGeneration::zipfian(100, -0.5);
        assert!(result.is_err());

        // Valid parameters should succeed
        let result = KeyGeneration::zipfian(100, 0.99);
        assert!(result.is_ok());
    }

    #[test]
    fn test_zipfian_distribution_properties() {
        let mut keygen = KeyGeneration::zipfian(1000, 0.99).expect("Failed to create Zipfian");

        // Collect 10000 samples
        let mut counts = vec![0u32; 1000];
        for _ in 0..10000 {
            let key = keygen.next_key();
            counts[key as usize] += 1;
        }

        // Check that lower keys are accessed more frequently
        // Top 10% of keys (0-99) should get significantly more hits than bottom 10% (900-999)
        let top_10_percent: u32 = counts[0..100].iter().sum();
        let bottom_10_percent: u32 = counts[900..1000].iter().sum();

        // With theta=0.99, top 10% should get at least 2x more requests than bottom 10%
        assert!(
            top_10_percent > bottom_10_percent * 2,
            "Top 10% hits: {}, Bottom 10% hits: {}",
            top_10_percent,
            bottom_10_percent
        );

        // Most frequent key should be 0
        let max_count = counts.iter().max().unwrap();
        assert_eq!(counts[0], *max_count, "Key 0 should be most frequent");
    }

    #[test]
    fn test_zipfian_uniform_limit() {
        // s=0 should behave close to uniform
        let mut keygen = KeyGeneration::zipfian(100, 0.0).expect("Failed to create Zipfian");

        let mut counts = vec![0u32; 100];
        for _ in 0..10000 {
            let key = keygen.next_key();
            counts[key as usize] += 1;
        }

        // With uniform distribution, each key should get ~100 hits
        // Allow reasonable variance (50-150)
        for (key, count) in counts.iter().enumerate() {
            assert!(
                *count >= 50 && *count <= 200,
                "Key {} count {} outside expected range for uniform distribution",
                key,
                count
            );
        }
    }

    #[test]
    fn test_zipfian_clone() {
        let keygen = KeyGeneration::zipfian(100, 0.99).expect("Failed to create Zipfian");
        let mut cloned = keygen.clone();

        // Clone should work and produce valid keys
        for _ in 0..100 {
            let key = cloned.next_key();
            assert!(key < 100);
        }
    }

    #[test]
    fn test_zipfian_with_request_generator() {
        let keygen = KeyGeneration::zipfian(1000, 0.99).expect("Failed to create Zipfian");
        let mut gen =
            RequestGenerator::new(keygen, RateControl::ClosedLoop, Box::new(FixedSize::new(64)));

        // Generate requests and verify keys are valid
        for _ in 0..100 {
            let (key, size) = gen.next_request();
            assert!(key < 1000);
            assert_eq!(size, 64);
        }
    }
}
