//! Request generation

use super::distributions::{Distribution, ZipfianDistribution};
use std::time::{Duration, Instant};

/// Key generation strategy
#[derive(Debug)]
pub enum KeyGeneration {
    /// Sequential keys starting from a value
    Sequential { start: u64, current: u64 },
    /// Random keys
    Random,
    /// Round-robin over a range
    RoundRobin { max: u64, current: u64 },
    /// Zipfian distribution (hot-key pattern)
    Zipfian(ZipfianDistribution),
}

// Manual Clone implementation
impl Clone for KeyGeneration {
    fn clone(&self) -> Self {
        match self {
            Self::Sequential { start, current } => {
                Self::Sequential { start: *start, current: *current }
            }
            Self::Random => Self::Random,
            Self::RoundRobin { max, current } => Self::RoundRobin { max: *max, current: *current },
            Self::Zipfian(dist) => {
                // Recreate distribution with same parameters
                Self::Zipfian(
                    ZipfianDistribution::new(dist.n(), dist.exponent())
                        .expect("Invalid Zipf parameters"),
                )
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

    /// Create a random key generator
    pub fn random() -> Self {
        Self::Random
    }

    /// Create a Zipfian key generator
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
        let dist = ZipfianDistribution::new(n, s)?;
        Ok(Self::Zipfian(dist))
    }

    /// Generate the next key
    pub fn next_key(&mut self) -> u64 {
        match self {
            Self::Sequential { current, .. } => {
                let key = *current;
                *current = current.wrapping_add(1);
                key
            }
            Self::Random => {
                // Simple pseudo-random using current time
                // TODO: Use proper RNG (rand crate)
                let now = Instant::now();
                now.elapsed().as_nanos() as u64
            }
            Self::RoundRobin { max, current } => {
                let key = *current;
                *current = (*current + 1) % *max;
                key
            }
            Self::Zipfian(dist) => dist.sample_key(),
        }
    }

    /// Reset the generator
    pub fn reset(&mut self) {
        match self {
            Self::Sequential { start, current } => {
                *current = *start;
            }
            Self::Random => {}
            Self::RoundRobin { current, .. } => {
                *current = 0;
            }
            Self::Zipfian(dist) => {
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
    value_size: usize,
    last_request_time: Option<Instant>,
    request_count: u64,
}

impl RequestGenerator {
    /// Create a new request generator
    pub fn new(key_gen: KeyGeneration, rate_control: RateControl, value_size: usize) -> Self {
        Self {
            key_gen,
            rate_control,
            value_size,
            last_request_time: None,
            request_count: 0,
        }
    }

    /// Generate the next request parameters (key, value_size)
    pub fn next_request(&mut self) -> (u64, usize) {
        let key = self.key_gen.next_key();
        self.request_count += 1;
        (key, self.value_size)
    }

    /// Calculate delay until next request should be sent
    pub fn delay_until_next(&mut self) -> Option<Duration> {
        match self.rate_control {
            RateControl::ClosedLoop => None,
            RateControl::Fixed { rate } => {
                let inter_arrival_time = Duration::from_secs_f64(1.0 / rate);

                if let Some(last_time) = self.last_request_time {
                    let elapsed = last_time.elapsed();
                    if elapsed < inter_arrival_time {
                        Some(inter_arrival_time - elapsed)
                    } else {
                        Some(Duration::ZERO)
                    }
                } else {
                    self.last_request_time = Some(Instant::now());
                    Some(Duration::ZERO)
                }
            }
        }
    }

    /// Mark that a request was sent (for rate control)
    pub fn mark_request_sent(&mut self) {
        self.last_request_time = Some(Instant::now());
    }

    /// Get total request count
    pub fn request_count(&self) -> u64 {
        self.request_count
    }

    /// Reset the generator
    pub fn reset(&mut self) {
        self.key_gen.reset();
        self.last_request_time = None;
        self.request_count = 0;
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
        let mut keygen = KeyGeneration::random();

        let k1 = keygen.next_key();
        let k2 = keygen.next_key();

        // They should be different (very high probability)
        assert_ne!(k1, k2);
    }

    #[test]
    fn test_request_generator_closed_loop() {
        let keygen = KeyGeneration::sequential(0);
        let mut gen = RequestGenerator::new(keygen, RateControl::ClosedLoop, 64);

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
        let mut gen = RequestGenerator::new(keygen, RateControl::Fixed { rate }, 64);

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
        let mut gen = RequestGenerator::new(keygen, RateControl::ClosedLoop, 64);

        assert_eq!(gen.request_count(), 0);

        gen.next_request();
        assert_eq!(gen.request_count(), 1);

        gen.next_request();
        assert_eq!(gen.request_count(), 2);
    }

    #[test]
    fn test_reset() {
        let keygen = KeyGeneration::sequential(10);
        let mut gen = RequestGenerator::new(keygen, RateControl::ClosedLoop, 64);

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
        let mut gen = RequestGenerator::new(keygen, RateControl::ClosedLoop, 64);

        // Generate requests and verify keys are valid
        for _ in 0..100 {
            let (key, size) = gen.next_request();
            assert!(key < 1000);
            assert_eq!(size, 64);
        }
    }
}
