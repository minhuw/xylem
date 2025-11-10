//! Statistical distributions for workload generation
//!
//! Provides reusable distribution implementations used by:
//! - Key generation (Zipfian for hot-key workloads)
//! - Inter-arrival timing (Exponential for Poisson process)
//! - Value size variation (Normal, Pareto)

use rand::rngs::SmallRng;
use rand::SeedableRng;
use rand_distr::{Distribution as RandDistribution, Exp, Normal, Uniform, Zipf};

/// Trait for all distributions
pub trait Distribution: Send {
    /// Sample a value from this distribution
    fn sample(&mut self) -> f64;

    /// Reset the distribution state
    fn reset(&mut self) {
        // Default: no-op (most distributions are stateless except RNG)
    }

    /// Get distribution name
    fn name(&self) -> &'static str;
}

/// Zipfian distribution (power law, hot-key pattern)
///
/// Used for:
/// - Key selection (hot keys accessed more frequently)
/// - Connection selection (some connections handle more traffic)
///
/// The Zipfian distribution follows a power law where the frequency of an item
/// is inversely proportional to its rank. With exponent s (theta):
/// - s = 0.0: Uniform distribution
/// - s = 0.99: Typical database workload (YCSB default)
/// - s = 1.0: Classic Zipf (frequency ∝ 1/rank)
/// - s > 1.0: More skewed toward low-rank items
#[derive(Debug)]
pub struct ZipfianDistribution {
    /// Number of items (range: 0..n-1)
    n: u64,
    /// Exponent (theta): higher = more skewed
    s: f64,
    /// Random number generator
    rng: SmallRng,
    /// Underlying Zipf distribution
    dist: Zipf<f64>,
}

impl ZipfianDistribution {
    /// Create a new Zipfian distribution with entropy-based seed
    ///
    /// # Parameters
    /// - `n`: Number of unique items in range [0, n-1]
    /// - `s`: Exponent (theta) controlling skewness. Common values:
    ///   - s = 0.0: Uniform distribution
    ///   - s = 0.99: Typical database workload (YCSB default)
    ///   - s = 1.0: Classic Zipf (1/rank)
    ///   - s > 1.0: More skewed toward low values
    ///
    /// # Returns
    /// Error if n == 0 or s < 0.0
    pub fn new(n: u64, s: f64) -> anyhow::Result<Self> {
        Self::with_seed(n, s, None)
    }

    /// Create a new Zipfian distribution with explicit seed
    ///
    /// # Parameters
    /// - `n`: Number of unique items in range [0, n-1]
    /// - `s`: Exponent (theta) controlling skewness
    /// - `seed`: Optional seed for reproducibility (None = use entropy)
    ///
    /// # Returns
    /// Error if n == 0 or s < 0.0
    pub fn with_seed(n: u64, s: f64, seed: Option<u64>) -> anyhow::Result<Self> {
        if n == 0 {
            anyhow::bail!("Zipfian n must be > 0");
        }
        if s < 0.0 {
            anyhow::bail!("Zipfian s (exponent) must be >= 0.0");
        }

        let rng = match seed {
            Some(s) => SmallRng::seed_from_u64(s),
            None => SmallRng::from_os_rng(),
        };
        let dist = Zipf::new(n as f64, s)?;

        Ok(Self { n, s, rng, dist })
    }

    /// Sample and return 0-indexed value (0..n-1)
    ///
    /// The underlying Zipf distribution returns values in [1, n],
    /// this method adjusts to [0, n-1] for array indexing.
    pub fn sample_key(&mut self) -> u64 {
        let sample = self.dist.sample(&mut self.rng);
        (sample as u64).saturating_sub(1) // Zipf returns 1..=n, convert to 0..n-1
    }

    /// Get the number of items
    pub fn n(&self) -> u64 {
        self.n
    }

    /// Get the exponent (theta)
    pub fn exponent(&self) -> f64 {
        self.s
    }
}

impl Distribution for ZipfianDistribution {
    fn sample(&mut self) -> f64 {
        self.dist.sample(&mut self.rng)
    }

    fn reset(&mut self) {
        // RNG continues from current state, no reset needed
        // This maintains statistical properties
    }

    fn name(&self) -> &'static str {
        "Zipfian"
    }
}

/// Exponential distribution (Poisson inter-arrival times)
///
/// Used for:
/// - Request inter-arrival times (realistic bursty traffic)
/// - Modeling time between events in a Poisson process
///
/// The exponential distribution is the continuous probability distribution
/// that describes the time between events in a Poisson process.
pub struct ExponentialDistribution {
    /// Rate parameter (arrivals per second)
    lambda: f64,
    /// Random number generator
    rng: SmallRng,
    /// Underlying exponential distribution
    dist: Exp<f64>,
}

impl ExponentialDistribution {
    /// Create a new exponential distribution with entropy-based seed
    ///
    /// # Parameters
    /// - `lambda`: Rate parameter (arrivals per second). Must be > 0.
    ///
    /// # Returns
    /// Error if lambda <= 0
    pub fn new(lambda: f64) -> anyhow::Result<Self> {
        Self::with_seed(lambda, None)
    }

    /// Create a new exponential distribution with explicit seed
    ///
    /// # Parameters
    /// - `lambda`: Rate parameter (arrivals per second). Must be > 0.
    /// - `seed`: Optional seed for reproducibility (None = use entropy)
    ///
    /// # Returns
    /// Error if lambda <= 0
    pub fn with_seed(lambda: f64, seed: Option<u64>) -> anyhow::Result<Self> {
        if lambda <= 0.0 {
            anyhow::bail!("Exponential lambda must be > 0");
        }

        let rng = match seed {
            Some(s) => SmallRng::seed_from_u64(s),
            None => SmallRng::from_os_rng(),
        };
        let dist = Exp::new(lambda)?;

        Ok(Self { lambda, rng, dist })
    }

    /// Sample inter-arrival time in seconds
    ///
    /// Returns the time until the next event, in seconds.
    /// For a rate of λ arrivals/second, the average inter-arrival time is 1/λ.
    pub fn sample_inter_arrival(&mut self) -> f64 {
        self.dist.sample(&mut self.rng)
    }

    /// Get the rate parameter (lambda)
    pub fn lambda(&self) -> f64 {
        self.lambda
    }
}

impl Distribution for ExponentialDistribution {
    fn sample(&mut self) -> f64 {
        self.dist.sample(&mut self.rng)
    }

    fn reset(&mut self) {
        // Continue from current RNG state
    }

    fn name(&self) -> &'static str {
        "Exponential"
    }
}

/// Uniform distribution (all values equally likely)
///
/// Used for:
/// - Random value sizes within a range
/// - Baseline comparison for other distributions
pub struct UniformDistribution {
    /// Minimum value (inclusive)
    min: f64,
    /// Maximum value (exclusive)
    max: f64,
    /// Random number generator
    rng: SmallRng,
    /// Underlying uniform distribution
    dist: Uniform<f64>,
}

impl UniformDistribution {
    /// Create a new uniform distribution with entropy-based seed
    ///
    /// # Parameters
    /// - `min`: Minimum value (inclusive)
    /// - `max`: Maximum value (exclusive)
    ///
    /// # Returns
    /// Error if min >= max
    pub fn new(min: f64, max: f64) -> anyhow::Result<Self> {
        Self::with_seed(min, max, None)
    }

    /// Create a new uniform distribution with explicit seed
    ///
    /// # Parameters
    /// - `min`: Minimum value (inclusive)
    /// - `max`: Maximum value (exclusive)
    /// - `seed`: Optional seed for reproducibility (None = use entropy)
    ///
    /// # Returns
    /// Error if min >= max
    pub fn with_seed(min: f64, max: f64, seed: Option<u64>) -> anyhow::Result<Self> {
        if min >= max {
            anyhow::bail!("Uniform min must be < max");
        }

        let rng = match seed {
            Some(s) => SmallRng::seed_from_u64(s),
            None => SmallRng::from_os_rng(),
        };
        let dist = Uniform::new(min, max)?;

        Ok(Self { min, max, rng, dist })
    }

    /// Get the minimum value
    pub fn min(&self) -> f64 {
        self.min
    }

    /// Get the maximum value
    pub fn max(&self) -> f64 {
        self.max
    }
}

impl Distribution for UniformDistribution {
    fn sample(&mut self) -> f64 {
        self.dist.sample(&mut self.rng)
    }

    fn name(&self) -> &'static str {
        "Uniform"
    }
}

/// Normal (Gaussian) distribution
///
/// Used for:
/// - Value sizes around a mean with variation
/// - Modeling natural phenomena with central tendency
#[derive(Debug)]
pub struct NormalDistribution {
    /// Mean (μ)
    mean: f64,
    /// Standard deviation (σ)
    std_dev: f64,
    /// Random number generator
    rng: SmallRng,
    /// Underlying normal distribution
    dist: Normal<f64>,
}

impl NormalDistribution {
    /// Create a new normal distribution with entropy-based seed
    ///
    /// # Parameters
    /// - `mean`: Mean value (μ)
    /// - `std_dev`: Standard deviation (σ). Must be > 0.
    ///
    /// # Returns
    /// Error if std_dev <= 0
    pub fn new(mean: f64, std_dev: f64) -> anyhow::Result<Self> {
        Self::with_seed(mean, std_dev, None)
    }

    /// Create a new normal distribution with explicit seed
    ///
    /// # Parameters
    /// - `mean`: Mean value (μ)
    /// - `std_dev`: Standard deviation (σ). Must be > 0.
    /// - `seed`: Optional seed for reproducibility (None = use entropy)
    ///
    /// # Returns
    /// Error if std_dev <= 0
    pub fn with_seed(mean: f64, std_dev: f64, seed: Option<u64>) -> anyhow::Result<Self> {
        if std_dev <= 0.0 {
            anyhow::bail!("Normal std_dev must be > 0");
        }

        let rng = match seed {
            Some(s) => SmallRng::seed_from_u64(s),
            None => SmallRng::from_os_rng(),
        };
        let dist = Normal::new(mean, std_dev)?;

        Ok(Self { mean, std_dev, rng, dist })
    }

    /// Get the mean
    pub fn mean(&self) -> f64 {
        self.mean
    }

    /// Get the standard deviation
    pub fn std_dev(&self) -> f64 {
        self.std_dev
    }
}

impl Distribution for NormalDistribution {
    fn sample(&mut self) -> f64 {
        self.dist.sample(&mut self.rng)
    }

    fn name(&self) -> &'static str {
        "Normal"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zipfian_basic() {
        let mut dist = ZipfianDistribution::new(100, 0.99).expect("Failed to create Zipfian");

        // Generate some keys and verify they're in valid range
        for _ in 0..1000 {
            let key = dist.sample_key();
            assert!(key < 100, "Key {} out of range [0, 100)", key);
        }
    }

    #[test]
    fn test_zipfian_distribution_properties() {
        let mut dist = ZipfianDistribution::new(1000, 0.99).expect("Failed to create Zipfian");

        // Collect 10000 samples
        let mut counts = vec![0u32; 1000];
        for _ in 0..10000 {
            let key = dist.sample_key();
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
        let mut dist = ZipfianDistribution::new(100, 0.0).expect("Failed to create Zipfian");

        let mut counts = vec![0u32; 100];
        for _ in 0..10000 {
            let key = dist.sample_key();
            counts[key as usize] += 1;
        }

        // With uniform distribution, each key should get ~100 hits
        // Allow reasonable variance (50-200)
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
    fn test_zipfian_parameter_validation() {
        // n must be > 0
        let result = ZipfianDistribution::new(0, 0.99);
        assert!(result.is_err());

        // s must be >= 0
        let result = ZipfianDistribution::new(100, -0.5);
        assert!(result.is_err());

        // Valid parameters should succeed
        let result = ZipfianDistribution::new(100, 0.99);
        assert!(result.is_ok());
    }

    #[test]
    fn test_exponential_basic() {
        let mut dist = ExponentialDistribution::new(1000.0).expect("Failed to create Exponential");

        // Generate samples and verify they're reasonable
        for _ in 0..100 {
            let sample = dist.sample_inter_arrival();
            assert!(sample >= 0.0, "Sample should be non-negative");
            assert!(sample < 1.0, "Sample should be less than 1 second for 1000 req/s rate");
        }
    }

    #[test]
    fn test_exponential_mean() {
        let lambda = 100.0; // 100 arrivals/sec
        let mut dist = ExponentialDistribution::new(lambda).expect("Failed to create Exponential");

        // Collect samples and verify mean is approximately 1/lambda
        let samples: Vec<f64> = (0..10000).map(|_| dist.sample_inter_arrival()).collect();

        let mean: f64 = samples.iter().sum::<f64>() / samples.len() as f64;
        let expected_mean = 1.0 / lambda;

        // Mean should be within 10% of expected
        assert!(
            (mean - expected_mean).abs() / expected_mean < 0.1,
            "Mean {} not close to expected {}",
            mean,
            expected_mean
        );
    }

    #[test]
    fn test_uniform_basic() {
        let mut dist = UniformDistribution::new(10.0, 20.0).expect("Failed to create Uniform");

        // All samples should be in [10, 20)
        for _ in 0..1000 {
            let sample = dist.sample();
            assert!(sample >= 10.0, "Sample {} below minimum", sample);
            assert!(sample < 20.0, "Sample {} above maximum", sample);
        }
    }

    #[test]
    fn test_uniform_distribution() {
        let mut dist = UniformDistribution::new(0.0, 100.0).expect("Failed to create Uniform");

        // Collect samples and verify uniform distribution
        let samples: Vec<f64> = (0..10000).map(|_| dist.sample()).collect();

        let mean: f64 = samples.iter().sum::<f64>() / samples.len() as f64;
        let expected_mean = 50.0; // (0 + 100) / 2

        // Mean should be around 50
        assert!(
            (mean - expected_mean).abs() < 2.0,
            "Mean {} not close to expected {}",
            mean,
            expected_mean
        );
    }

    #[test]
    fn test_normal_basic() {
        let mut dist = NormalDistribution::new(100.0, 10.0).expect("Failed to create Normal");

        // Most samples should be within a few standard deviations
        let samples: Vec<f64> = (0..1000).map(|_| dist.sample()).collect();

        for sample in &samples {
            assert!(
                *sample > 50.0 && *sample < 150.0,
                "Sample {} far from mean (should be within ~5 std devs)",
                sample
            );
        }
    }

    #[test]
    fn test_normal_mean_and_stddev() {
        let expected_mean = 50.0;
        let expected_stddev = 10.0;
        let mut dist = NormalDistribution::new(expected_mean, expected_stddev)
            .expect("Failed to create Normal");

        // Collect samples
        let samples: Vec<f64> = (0..10000).map(|_| dist.sample()).collect();

        let mean: f64 = samples.iter().sum::<f64>() / samples.len() as f64;
        let variance: f64 =
            samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / samples.len() as f64;
        let stddev = variance.sqrt();

        // Mean should be within 1% of expected
        assert!(
            (mean - expected_mean).abs() / expected_mean < 0.01,
            "Mean {} not close to expected {}",
            mean,
            expected_mean
        );

        // Std dev should be within 10% of expected
        assert!(
            (stddev - expected_stddev).abs() / expected_stddev < 0.1,
            "Std dev {} not close to expected {}",
            stddev,
            expected_stddev
        );
    }

    #[test]
    fn test_distribution_trait() {
        let mut zipfian: Box<dyn Distribution> =
            Box::new(ZipfianDistribution::new(100, 0.99).unwrap());
        let mut exponential: Box<dyn Distribution> =
            Box::new(ExponentialDistribution::new(1000.0).unwrap());
        let mut uniform: Box<dyn Distribution> =
            Box::new(UniformDistribution::new(0.0, 100.0).unwrap());
        let mut normal: Box<dyn Distribution> =
            Box::new(NormalDistribution::new(50.0, 10.0).unwrap());

        // All should implement the trait
        assert_eq!(zipfian.name(), "Zipfian");
        assert_eq!(exponential.name(), "Exponential");
        assert_eq!(uniform.name(), "Uniform");
        assert_eq!(normal.name(), "Normal");

        // All should be able to sample
        let _ = zipfian.sample();
        let _ = exponential.sample();
        let _ = uniform.sample();
        let _ = normal.sample();
    }
}
