//! Statistical distributions for workload generation
//!
//! Provides reusable distribution implementations used by:
//! - Key generation (Zipfian for hot-key workloads)
//! - Inter-arrival timing (Exponential for Poisson process)
//! - Value size variation (Normal, Pareto)

use rand::rngs::SmallRng;
use rand::SeedableRng;
use rand_distr::{
    Distribution as RandDistribution, Exp, Gamma, LogNormal, Normal, Pareto, Uniform, Zipf,
};

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

/// Generalized Pareto distribution (heavy-tailed)
///
/// Used for:
/// - Modeling heavy-tailed inter-arrival times (bursty traffic)
/// - Value sizes with long tails (Facebook workloads)
///
/// The Generalized Pareto distribution is defined by location (μ), scale (σ), and shape (ξ).
/// Standard Pareto in rand_distr uses scale and shape only (location = scale).
///
/// CDF: F(x) = 1 - (1 + ξ(x-μ)/σ)^(-1/ξ) for ξ ≠ 0
///
/// Note: This wraps rand_distr::Pareto which uses scale (x_m) and shape (α) parameters.
/// The relationship is: α = 1/ξ when ξ > 0
pub struct ParetoDistribution {
    /// Scale parameter (x_m, minimum value)
    scale: f64,
    /// Shape parameter (α, tail index)
    shape: f64,
    /// Random number generator
    rng: SmallRng,
    /// Underlying Pareto distribution
    dist: Pareto<f64>,
}

impl ParetoDistribution {
    /// Create a new Pareto distribution with entropy-based seed
    ///
    /// # Parameters
    /// - `scale`: Scale parameter (x_m), the minimum possible value. Must be > 0.
    /// - `shape`: Shape parameter (α), controls tail heaviness. Must be > 0.
    ///   - Lower α = heavier tail (more extreme values)
    ///   - α ≤ 1: infinite mean
    ///   - α ≤ 2: infinite variance
    pub fn new(scale: f64, shape: f64) -> anyhow::Result<Self> {
        Self::with_seed(scale, shape, None)
    }

    /// Create a new Pareto distribution with explicit seed
    pub fn with_seed(scale: f64, shape: f64, seed: Option<u64>) -> anyhow::Result<Self> {
        if scale <= 0.0 {
            anyhow::bail!("Pareto scale must be > 0");
        }
        if shape <= 0.0 {
            anyhow::bail!("Pareto shape must be > 0");
        }

        let rng = match seed {
            Some(s) => SmallRng::seed_from_u64(s),
            None => SmallRng::from_os_rng(),
        };
        let dist = Pareto::new(scale, shape)?;

        Ok(Self { scale, shape, rng, dist })
    }

    /// Get the scale parameter
    pub fn scale(&self) -> f64 {
        self.scale
    }

    /// Get the shape parameter
    pub fn shape(&self) -> f64 {
        self.shape
    }
}

impl Distribution for ParetoDistribution {
    fn sample(&mut self) -> f64 {
        self.dist.sample(&mut self.rng)
    }

    fn name(&self) -> &'static str {
        "Pareto"
    }
}

/// Generalized Pareto distribution with location parameter
///
/// This is the full 3-parameter GPD as used in Lancet (gpar:loc:scale:shape).
/// F(x) = 1 - (1 + shape*(x-loc)/scale)^(-1/shape)
pub struct GeneralizedParetoDistribution {
    /// Location parameter (μ)
    location: f64,
    /// Scale parameter (σ)
    scale: f64,
    /// Shape parameter (ξ)
    shape: f64,
    /// Random number generator
    rng: SmallRng,
}

impl GeneralizedParetoDistribution {
    /// Create a new Generalized Pareto distribution
    ///
    /// # Parameters
    /// - `location`: Location parameter (μ), shift
    /// - `scale`: Scale parameter (σ). Must be > 0.
    /// - `shape`: Shape parameter (ξ), controls tail behavior
    pub fn new(location: f64, scale: f64, shape: f64) -> anyhow::Result<Self> {
        Self::with_seed(location, scale, shape, None)
    }

    /// Create with explicit seed
    pub fn with_seed(
        location: f64,
        scale: f64,
        shape: f64,
        seed: Option<u64>,
    ) -> anyhow::Result<Self> {
        if scale <= 0.0 {
            anyhow::bail!("Generalized Pareto scale must be > 0");
        }

        let rng = match seed {
            Some(s) => SmallRng::seed_from_u64(s),
            None => SmallRng::from_os_rng(),
        };

        Ok(Self { location, scale, shape, rng })
    }

    /// Get location parameter
    pub fn location(&self) -> f64 {
        self.location
    }

    /// Get scale parameter
    pub fn scale(&self) -> f64 {
        self.scale
    }

    /// Get shape parameter
    pub fn shape(&self) -> f64 {
        self.shape
    }

    /// Sample using inverse CDF method
    fn inv_cdf(&self, u: f64) -> f64 {
        if self.shape.abs() < 1e-10 {
            // Exponential limit when shape -> 0
            self.location - self.scale * (1.0 - u).ln()
        } else {
            self.location + self.scale * ((1.0 - u).powf(-self.shape) - 1.0) / self.shape
        }
    }
}

impl Distribution for GeneralizedParetoDistribution {
    fn sample(&mut self) -> f64 {
        use rand::Rng;
        let u: f64 = self.rng.random();
        self.inv_cdf(u)
    }

    fn name(&self) -> &'static str {
        "GeneralizedPareto"
    }
}

/// Generalized Extreme Value (GEV) distribution
///
/// Used for modeling extreme values (maxima/minima), such as:
/// - Key sizes in Facebook workloads (fb_key)
/// - Maximum latencies
///
/// The GEV distribution unifies three extreme value distributions:
/// - Type I (Gumbel): ξ = 0
/// - Type II (Frechet): ξ > 0
/// - Type III (Weibull): ξ < 0
///
/// CDF: F(x) = exp(-(1 + ξ(x-μ)/σ)^(-1/ξ)) for ξ ≠ 0
pub struct GevDistribution {
    /// Location parameter (μ)
    location: f64,
    /// Scale parameter (σ)
    scale: f64,
    /// Shape parameter (ξ)
    shape: f64,
    /// Random number generator
    rng: SmallRng,
}

impl GevDistribution {
    /// Create a new GEV distribution
    ///
    /// # Parameters
    /// - `location`: Location parameter (μ)
    /// - `scale`: Scale parameter (σ). Must be > 0.
    /// - `shape`: Shape parameter (ξ), determines distribution type
    pub fn new(location: f64, scale: f64, shape: f64) -> anyhow::Result<Self> {
        Self::with_seed(location, scale, shape, None)
    }

    /// Create with explicit seed
    pub fn with_seed(
        location: f64,
        scale: f64,
        shape: f64,
        seed: Option<u64>,
    ) -> anyhow::Result<Self> {
        if scale <= 0.0 {
            anyhow::bail!("GEV scale must be > 0");
        }

        let rng = match seed {
            Some(s) => SmallRng::seed_from_u64(s),
            None => SmallRng::from_os_rng(),
        };

        Ok(Self { location, scale, shape, rng })
    }

    /// Get location parameter
    pub fn location(&self) -> f64 {
        self.location
    }

    /// Get scale parameter
    pub fn scale(&self) -> f64 {
        self.scale
    }

    /// Get shape parameter
    pub fn shape(&self) -> f64 {
        self.shape
    }

    /// Sample using inverse CDF method
    fn inv_cdf(&self, u: f64) -> f64 {
        let y = -u.ln();
        if self.shape.abs() < 1e-10 {
            // Gumbel limit when shape -> 0
            self.location - self.scale * y.ln()
        } else {
            self.location + self.scale * (y.powf(-self.shape) - 1.0) / self.shape
        }
    }
}

impl Distribution for GevDistribution {
    fn sample(&mut self) -> f64 {
        use rand::Rng;
        let u: f64 = self.rng.random();
        self.inv_cdf(u)
    }

    fn name(&self) -> &'static str {
        "GEV"
    }
}

/// Bimodal distribution (two distinct modes)
///
/// Used for:
/// - Modeling workloads with two distinct request types
/// - Service times with fast/slow paths
///
/// Returns `low` with probability `prob`, otherwise returns `high`.
pub struct BimodalDistribution {
    /// Low value
    low: f64,
    /// High value
    high: f64,
    /// Probability of returning low value (0.0 to 1.0)
    prob: f64,
    /// Random number generator
    rng: SmallRng,
}

impl BimodalDistribution {
    /// Create a new bimodal distribution
    ///
    /// # Parameters
    /// - `low`: First mode value
    /// - `high`: Second mode value
    /// - `prob`: Probability of returning `low` (0.0 to 1.0)
    pub fn new(low: f64, high: f64, prob: f64) -> anyhow::Result<Self> {
        Self::with_seed(low, high, prob, None)
    }

    /// Create with explicit seed
    pub fn with_seed(low: f64, high: f64, prob: f64, seed: Option<u64>) -> anyhow::Result<Self> {
        if !(0.0..=1.0).contains(&prob) {
            anyhow::bail!("Bimodal prob must be in [0.0, 1.0]");
        }

        let rng = match seed {
            Some(s) => SmallRng::seed_from_u64(s),
            None => SmallRng::from_os_rng(),
        };

        Ok(Self { low, high, prob, rng })
    }

    /// Get low value
    pub fn low(&self) -> f64 {
        self.low
    }

    /// Get high value
    pub fn high(&self) -> f64 {
        self.high
    }

    /// Get probability
    pub fn prob(&self) -> f64 {
        self.prob
    }
}

impl Distribution for BimodalDistribution {
    fn sample(&mut self) -> f64 {
        use rand::Rng;
        let u: f64 = self.rng.random();
        if u <= self.prob {
            self.low
        } else {
            self.high
        }
    }

    fn name(&self) -> &'static str {
        "Bimodal"
    }
}

/// Log-normal distribution
///
/// Used for:
/// - Modeling values that are products of many factors
/// - Response times, file sizes, income distributions
///
/// If X ~ Normal(μ, σ), then exp(X) ~ LogNormal(μ, σ)
pub struct LognormalDistribution {
    /// Mean of underlying normal (μ)
    mu: f64,
    /// Std dev of underlying normal (σ)
    sigma: f64,
    /// Random number generator
    rng: SmallRng,
    /// Underlying log-normal distribution
    dist: LogNormal<f64>,
}

impl LognormalDistribution {
    /// Create a new log-normal distribution
    ///
    /// # Parameters
    /// - `mu`: Mean of the underlying normal distribution
    /// - `sigma`: Standard deviation of the underlying normal distribution. Must be > 0.
    pub fn new(mu: f64, sigma: f64) -> anyhow::Result<Self> {
        Self::with_seed(mu, sigma, None)
    }

    /// Create with explicit seed
    pub fn with_seed(mu: f64, sigma: f64, seed: Option<u64>) -> anyhow::Result<Self> {
        if sigma <= 0.0 {
            anyhow::bail!("Lognormal sigma must be > 0");
        }

        let rng = match seed {
            Some(s) => SmallRng::seed_from_u64(s),
            None => SmallRng::from_os_rng(),
        };
        let dist = LogNormal::new(mu, sigma)?;

        Ok(Self { mu, sigma, rng, dist })
    }

    /// Get mu parameter
    pub fn mu(&self) -> f64 {
        self.mu
    }

    /// Get sigma parameter
    pub fn sigma(&self) -> f64 {
        self.sigma
    }
}

impl Distribution for LognormalDistribution {
    fn sample(&mut self) -> f64 {
        self.dist.sample(&mut self.rng)
    }

    fn name(&self) -> &'static str {
        "Lognormal"
    }
}

/// Gamma distribution
///
/// Used for:
/// - Modeling waiting times
/// - Service time distributions
/// - Sum of exponential random variables
///
/// The gamma distribution is defined by shape (k) and scale (θ) parameters.
/// Mean = k*θ, Variance = k*θ²
pub struct GammaDistribution {
    /// Shape parameter (k)
    shape: f64,
    /// Scale parameter (θ)
    scale: f64,
    /// Random number generator
    rng: SmallRng,
    /// Underlying gamma distribution
    dist: Gamma<f64>,
}

impl GammaDistribution {
    /// Create a new gamma distribution
    ///
    /// # Parameters
    /// - `shape`: Shape parameter (k). Must be > 0.
    /// - `scale`: Scale parameter (θ). Must be > 0.
    pub fn new(shape: f64, scale: f64) -> anyhow::Result<Self> {
        Self::with_seed(shape, scale, None)
    }

    /// Create with explicit seed
    pub fn with_seed(shape: f64, scale: f64, seed: Option<u64>) -> anyhow::Result<Self> {
        if shape <= 0.0 {
            anyhow::bail!("Gamma shape must be > 0");
        }
        if scale <= 0.0 {
            anyhow::bail!("Gamma scale must be > 0");
        }

        let rng = match seed {
            Some(s) => SmallRng::seed_from_u64(s),
            None => SmallRng::from_os_rng(),
        };
        let dist = Gamma::new(shape, scale)?;

        Ok(Self { shape, scale, rng, dist })
    }

    /// Get shape parameter
    pub fn shape(&self) -> f64 {
        self.shape
    }

    /// Get scale parameter
    pub fn scale(&self) -> f64 {
        self.scale
    }
}

impl Distribution for GammaDistribution {
    fn sample(&mut self) -> f64 {
        self.dist.sample(&mut self.rng)
    }

    fn name(&self) -> &'static str {
        "Gamma"
    }
}

/// Facebook inter-arrival time distribution (fb_ia)
///
/// Pre-fitted Generalized Pareto distribution for Facebook memcached traffic.
/// Parameters from Lancet: gpar:0:16.0292:0.154971
pub struct FacebookInterArrivalDistribution {
    inner: GeneralizedParetoDistribution,
}

impl FacebookInterArrivalDistribution {
    /// Create a new Facebook inter-arrival distribution
    pub fn new() -> anyhow::Result<Self> {
        Self::with_seed(None)
    }

    /// Create with explicit seed
    pub fn with_seed(seed: Option<u64>) -> anyhow::Result<Self> {
        let inner = GeneralizedParetoDistribution::with_seed(0.0, 16.0292, 0.154971, seed)?;
        Ok(Self { inner })
    }
}

impl Default for FacebookInterArrivalDistribution {
    fn default() -> Self {
        Self::new().expect("Facebook IA distribution should always be valid")
    }
}

impl Distribution for FacebookInterArrivalDistribution {
    fn sample(&mut self) -> f64 {
        self.inner.sample()
    }

    fn name(&self) -> &'static str {
        "FacebookIA"
    }
}

/// Facebook key size distribution (fb_key)
///
/// Pre-fitted GEV distribution for Facebook memcached key sizes.
/// Parameters from Lancet: gev:30.7984:8.20449:0.078688
pub struct FacebookKeySizeDistribution {
    inner: GevDistribution,
}

impl FacebookKeySizeDistribution {
    /// Create a new Facebook key size distribution
    pub fn new() -> anyhow::Result<Self> {
        Self::with_seed(None)
    }

    /// Create with explicit seed
    pub fn with_seed(seed: Option<u64>) -> anyhow::Result<Self> {
        let inner = GevDistribution::with_seed(30.7984, 8.20449, 0.078688, seed)?;
        Ok(Self { inner })
    }
}

impl Default for FacebookKeySizeDistribution {
    fn default() -> Self {
        Self::new().expect("Facebook key size distribution should always be valid")
    }
}

impl Distribution for FacebookKeySizeDistribution {
    fn sample(&mut self) -> f64 {
        self.inner.sample()
    }

    fn name(&self) -> &'static str {
        "FacebookKeySize"
    }
}

/// Facebook value size distribution (fb_val)
///
/// Pre-fitted Generalized Pareto distribution for Facebook memcached value sizes.
/// Parameters from Lancet: gpar:15.0:214.476:0.348238
pub struct FacebookValueSizeDistribution {
    inner: GeneralizedParetoDistribution,
}

impl FacebookValueSizeDistribution {
    /// Create a new Facebook value size distribution
    pub fn new() -> anyhow::Result<Self> {
        Self::with_seed(None)
    }

    /// Create with explicit seed
    pub fn with_seed(seed: Option<u64>) -> anyhow::Result<Self> {
        let inner = GeneralizedParetoDistribution::with_seed(15.0, 214.476, 0.348238, seed)?;
        Ok(Self { inner })
    }
}

impl Default for FacebookValueSizeDistribution {
    fn default() -> Self {
        Self::new().expect("Facebook value size distribution should always be valid")
    }
}

impl Distribution for FacebookValueSizeDistribution {
    fn sample(&mut self) -> f64 {
        self.inner.sample()
    }

    fn name(&self) -> &'static str {
        "FacebookValueSize"
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

    #[test]
    fn test_pareto_basic() {
        let mut dist = ParetoDistribution::new(1.0, 2.0).expect("Failed to create Pareto");

        // All samples should be >= scale (minimum value)
        for _ in 0..1000 {
            let sample = dist.sample();
            assert!(sample >= 1.0, "Sample {} below scale", sample);
        }
    }

    #[test]
    fn test_pareto_parameter_validation() {
        // scale must be > 0
        let result = ParetoDistribution::new(0.0, 2.0);
        assert!(result.is_err());

        // shape must be > 0
        let result = ParetoDistribution::new(1.0, 0.0);
        assert!(result.is_err());

        // Valid parameters
        let result = ParetoDistribution::new(1.0, 2.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_generalized_pareto_basic() {
        let mut dist =
            GeneralizedParetoDistribution::new(0.0, 16.0, 0.15).expect("Failed to create GPD");

        // All samples should be >= location
        for _ in 0..1000 {
            let sample = dist.sample();
            assert!(sample >= 0.0, "Sample {} below location", sample);
        }
    }

    #[test]
    fn test_gev_basic() {
        let mut dist = GevDistribution::new(30.0, 8.0, 0.08).expect("Failed to create GEV");

        // GEV samples should be reasonable
        let samples: Vec<f64> = (0..1000).map(|_| dist.sample()).collect();
        let mean: f64 = samples.iter().sum::<f64>() / samples.len() as f64;

        // Mean should be roughly around location parameter
        assert!(mean > 20.0 && mean < 50.0, "Mean {} outside expected range", mean);
    }

    #[test]
    fn test_bimodal_basic() {
        let mut dist =
            BimodalDistribution::new(10.0, 100.0, 0.7).expect("Failed to create Bimodal");

        let mut low_count = 0;

        for _ in 0..10000 {
            let sample = dist.sample();
            if (sample - 10.0).abs() < 0.01 {
                low_count += 1;
            } else if (sample - 100.0).abs() < 0.01 {
                // high value, counted implicitly
            } else {
                panic!("Bimodal sample {} is neither low nor high", sample);
            }
        }

        // With prob=0.7, low should be ~70%, high should be ~30%
        let low_ratio = low_count as f64 / 10000.0;
        assert!((low_ratio - 0.7).abs() < 0.05, "Low ratio {} not close to 0.7", low_ratio);
    }

    #[test]
    fn test_bimodal_parameter_validation() {
        // prob must be in [0, 1]
        let result = BimodalDistribution::new(10.0, 100.0, 1.5);
        assert!(result.is_err());

        let result = BimodalDistribution::new(10.0, 100.0, -0.1);
        assert!(result.is_err());

        // Valid parameters
        let result = BimodalDistribution::new(10.0, 100.0, 0.5);
        assert!(result.is_ok());
    }

    #[test]
    fn test_lognormal_basic() {
        let mut dist = LognormalDistribution::new(0.0, 1.0).expect("Failed to create Lognormal");

        // All samples should be positive
        for _ in 0..1000 {
            let sample = dist.sample();
            assert!(sample > 0.0, "Lognormal sample {} should be positive", sample);
        }
    }

    #[test]
    fn test_lognormal_mean() {
        // For LogNormal(mu, sigma), the mean is exp(mu + sigma^2/2)
        let mu = 1.0;
        let sigma = 0.5;
        let mut dist = LognormalDistribution::new(mu, sigma).expect("Failed to create Lognormal");

        let samples: Vec<f64> = (0..10000).map(|_| dist.sample()).collect();
        let mean: f64 = samples.iter().sum::<f64>() / samples.len() as f64;
        let expected_mean = (mu + sigma * sigma / 2.0).exp();

        assert!(
            (mean - expected_mean).abs() / expected_mean < 0.1,
            "Mean {} not close to expected {}",
            mean,
            expected_mean
        );
    }

    #[test]
    fn test_gamma_basic() {
        let mut dist = GammaDistribution::new(2.0, 3.0).expect("Failed to create Gamma");

        // All samples should be positive
        for _ in 0..1000 {
            let sample = dist.sample();
            assert!(sample > 0.0, "Gamma sample {} should be positive", sample);
        }
    }

    #[test]
    fn test_gamma_mean() {
        // For Gamma(shape, scale), mean = shape * scale
        let shape = 2.0;
        let scale = 3.0;
        let mut dist = GammaDistribution::new(shape, scale).expect("Failed to create Gamma");

        let samples: Vec<f64> = (0..10000).map(|_| dist.sample()).collect();
        let mean: f64 = samples.iter().sum::<f64>() / samples.len() as f64;
        let expected_mean = shape * scale;

        assert!(
            (mean - expected_mean).abs() / expected_mean < 0.1,
            "Mean {} not close to expected {}",
            mean,
            expected_mean
        );
    }

    #[test]
    fn test_gamma_parameter_validation() {
        // shape must be > 0
        let result = GammaDistribution::new(0.0, 1.0);
        assert!(result.is_err());

        // scale must be > 0
        let result = GammaDistribution::new(1.0, 0.0);
        assert!(result.is_err());

        // Valid parameters
        let result = GammaDistribution::new(2.0, 3.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_facebook_distributions() {
        // Test Facebook inter-arrival distribution
        let mut fb_ia = FacebookInterArrivalDistribution::new().expect("Failed to create fb_ia");
        for _ in 0..100 {
            let sample = fb_ia.sample();
            assert!(sample >= 0.0, "fb_ia sample {} should be >= 0", sample);
        }
        assert_eq!(fb_ia.name(), "FacebookIA");

        // Test Facebook key size distribution
        let mut fb_key = FacebookKeySizeDistribution::new().expect("Failed to create fb_key");
        for _ in 0..100 {
            let sample = fb_key.sample();
            assert!(sample > 0.0, "fb_key sample {} should be > 0", sample);
        }
        assert_eq!(fb_key.name(), "FacebookKeySize");

        // Test Facebook value size distribution
        let mut fb_val = FacebookValueSizeDistribution::new().expect("Failed to create fb_val");
        for _ in 0..100 {
            let sample = fb_val.sample();
            assert!(sample >= 15.0, "fb_val sample {} should be >= 15 (location)", sample);
        }
        assert_eq!(fb_val.name(), "FacebookValueSize");
    }

    #[test]
    fn test_new_distributions_trait() {
        let mut pareto: Box<dyn Distribution> =
            Box::new(ParetoDistribution::new(1.0, 2.0).unwrap());
        let mut gpd: Box<dyn Distribution> =
            Box::new(GeneralizedParetoDistribution::new(0.0, 16.0, 0.15).unwrap());
        let mut gev: Box<dyn Distribution> =
            Box::new(GevDistribution::new(30.0, 8.0, 0.08).unwrap());
        let mut bimodal: Box<dyn Distribution> =
            Box::new(BimodalDistribution::new(10.0, 100.0, 0.7).unwrap());
        let mut lognormal: Box<dyn Distribution> =
            Box::new(LognormalDistribution::new(0.0, 1.0).unwrap());
        let mut gamma: Box<dyn Distribution> = Box::new(GammaDistribution::new(2.0, 3.0).unwrap());

        assert_eq!(pareto.name(), "Pareto");
        assert_eq!(gpd.name(), "GeneralizedPareto");
        assert_eq!(gev.name(), "GEV");
        assert_eq!(bimodal.name(), "Bimodal");
        assert_eq!(lognormal.name(), "Lognormal");
        assert_eq!(gamma.name(), "Gamma");

        // All should be able to sample
        let _ = pareto.sample();
        let _ = gpd.sample();
        let _ = gev.sample();
        let _ = bimodal.sample();
        let _ = lognormal.sample();
        let _ = gamma.sample();
    }
}
