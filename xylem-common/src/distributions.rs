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
    pub fn new(n: u64, s: f64) -> anyhow::Result<Self> {
        Self::with_seed(n, s, None)
    }

    /// Create a new Zipfian distribution with explicit seed
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
    pub fn sample_key(&mut self) -> u64 {
        let sample = self.dist.sample(&mut self.rng);
        (sample as u64).saturating_sub(1)
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

    fn reset(&mut self) {}

    fn name(&self) -> &'static str {
        "Zipfian"
    }
}

impl Clone for ZipfianDistribution {
    fn clone(&self) -> Self {
        // Clone RNG state and recreate distribution with same parameters
        Self {
            n: self.n,
            s: self.s,
            rng: self.rng.clone(),
            dist: Zipf::new(self.n as f64, self.s).expect("Zipf params validated in constructor"),
        }
    }
}

/// Exponential distribution (Poisson inter-arrival times)
pub struct ExponentialDistribution {
    lambda: f64,
    rng: SmallRng,
    dist: Exp<f64>,
}

impl ExponentialDistribution {
    pub fn new(lambda: f64) -> anyhow::Result<Self> {
        Self::with_seed(lambda, None)
    }

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

    pub fn sample_inter_arrival(&mut self) -> f64 {
        self.dist.sample(&mut self.rng)
    }

    pub fn lambda(&self) -> f64 {
        self.lambda
    }
}

impl Distribution for ExponentialDistribution {
    fn sample(&mut self) -> f64 {
        self.dist.sample(&mut self.rng)
    }

    fn reset(&mut self) {}

    fn name(&self) -> &'static str {
        "Exponential"
    }
}

/// Uniform distribution
pub struct UniformDistribution {
    min: f64,
    max: f64,
    rng: SmallRng,
    dist: Uniform<f64>,
}

impl UniformDistribution {
    pub fn new(min: f64, max: f64) -> anyhow::Result<Self> {
        Self::with_seed(min, max, None)
    }

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

    pub fn min(&self) -> f64 {
        self.min
    }

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
#[derive(Debug)]
pub struct NormalDistribution {
    mean: f64,
    std_dev: f64,
    rng: SmallRng,
    dist: Normal<f64>,
}

impl NormalDistribution {
    pub fn new(mean: f64, std_dev: f64) -> anyhow::Result<Self> {
        Self::with_seed(mean, std_dev, None)
    }

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

    pub fn mean(&self) -> f64 {
        self.mean
    }

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

impl Clone for NormalDistribution {
    fn clone(&self) -> Self {
        // Clone RNG state and recreate distribution with same parameters
        Self {
            mean: self.mean,
            std_dev: self.std_dev,
            rng: self.rng.clone(),
            dist: Normal::new(self.mean, self.std_dev)
                .expect("Normal params validated in constructor"),
        }
    }
}

/// Pareto distribution (heavy-tailed)
pub struct ParetoDistribution {
    scale: f64,
    shape: f64,
    rng: SmallRng,
    dist: Pareto<f64>,
}

impl ParetoDistribution {
    pub fn new(scale: f64, shape: f64) -> anyhow::Result<Self> {
        Self::with_seed(scale, shape, None)
    }

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

    pub fn scale(&self) -> f64 {
        self.scale
    }

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
pub struct GeneralizedParetoDistribution {
    location: f64,
    scale: f64,
    shape: f64,
    rng: SmallRng,
}

impl GeneralizedParetoDistribution {
    pub fn new(location: f64, scale: f64, shape: f64) -> anyhow::Result<Self> {
        Self::with_seed(location, scale, shape, None)
    }

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

    pub fn location(&self) -> f64 {
        self.location
    }

    pub fn scale(&self) -> f64 {
        self.scale
    }

    pub fn shape(&self) -> f64 {
        self.shape
    }

    fn inv_cdf(&self, u: f64) -> f64 {
        if self.shape.abs() < 1e-10 {
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
pub struct GevDistribution {
    location: f64,
    scale: f64,
    shape: f64,
    rng: SmallRng,
}

impl GevDistribution {
    pub fn new(location: f64, scale: f64, shape: f64) -> anyhow::Result<Self> {
        Self::with_seed(location, scale, shape, None)
    }

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

    pub fn location(&self) -> f64 {
        self.location
    }

    pub fn scale(&self) -> f64 {
        self.scale
    }

    pub fn shape(&self) -> f64 {
        self.shape
    }

    fn inv_cdf(&self, u: f64) -> f64 {
        let y = -u.ln();
        if self.shape.abs() < 1e-10 {
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
pub struct BimodalDistribution {
    low: f64,
    high: f64,
    prob: f64,
    rng: SmallRng,
}

impl BimodalDistribution {
    pub fn new(low: f64, high: f64, prob: f64) -> anyhow::Result<Self> {
        Self::with_seed(low, high, prob, None)
    }

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

    pub fn low(&self) -> f64 {
        self.low
    }

    pub fn high(&self) -> f64 {
        self.high
    }

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
pub struct LognormalDistribution {
    mu: f64,
    sigma: f64,
    rng: SmallRng,
    dist: LogNormal<f64>,
}

impl LognormalDistribution {
    pub fn new(mu: f64, sigma: f64) -> anyhow::Result<Self> {
        Self::with_seed(mu, sigma, None)
    }

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

    pub fn mu(&self) -> f64 {
        self.mu
    }

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
pub struct GammaDistribution {
    shape: f64,
    scale: f64,
    rng: SmallRng,
    dist: Gamma<f64>,
}

impl GammaDistribution {
    pub fn new(shape: f64, scale: f64) -> anyhow::Result<Self> {
        Self::with_seed(shape, scale, None)
    }

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

    pub fn shape(&self) -> f64 {
        self.shape
    }

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
pub struct FacebookInterArrivalDistribution {
    inner: GeneralizedParetoDistribution,
}

impl FacebookInterArrivalDistribution {
    pub fn new() -> anyhow::Result<Self> {
        Self::with_seed(None)
    }

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
pub struct FacebookKeySizeDistribution {
    inner: GevDistribution,
}

impl FacebookKeySizeDistribution {
    pub fn new() -> anyhow::Result<Self> {
        Self::with_seed(None)
    }

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
pub struct FacebookValueSizeDistribution {
    inner: GeneralizedParetoDistribution,
}

impl FacebookValueSizeDistribution {
    pub fn new() -> anyhow::Result<Self> {
        Self::with_seed(None)
    }

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
        for _ in 0..1000 {
            let key = dist.sample_key();
            assert!(key < 100, "Key {} out of range [0, 100)", key);
        }
    }

    #[test]
    fn test_exponential_basic() {
        let mut dist = ExponentialDistribution::new(1000.0).expect("Failed to create Exponential");
        for _ in 0..100 {
            let sample = dist.sample_inter_arrival();
            assert!(sample >= 0.0, "Sample should be non-negative");
        }
    }

    #[test]
    fn test_uniform_basic() {
        let mut dist = UniformDistribution::new(10.0, 20.0).expect("Failed to create Uniform");
        for _ in 0..1000 {
            let sample = dist.sample();
            assert!((10.0..20.0).contains(&sample), "Sample {} out of range", sample);
        }
    }

    #[test]
    fn test_normal_basic() {
        let mut dist = NormalDistribution::new(100.0, 10.0).expect("Failed to create Normal");
        let samples: Vec<f64> = (0..1000).map(|_| dist.sample()).collect();
        for sample in &samples {
            assert!(*sample > 50.0 && *sample < 150.0, "Sample {} far from mean", sample);
        }
    }
}
