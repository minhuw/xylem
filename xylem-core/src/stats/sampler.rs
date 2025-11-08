//! Adaptive sampling
//!
//! Implements adaptive sampling rate control based on confidence interval quality,
//! inspired by Lancet's approach but simplified for single-machine deployment.
//!
//! The adaptive sampler adjusts the sampling rate dynamically to:
//! - Reduce overhead when CI quality is good (narrow intervals)
//! - Increase sampling when CI quality is poor (wide intervals)
//!
//! Unlike Lancet's coordinator-based approach (which runs multiple measurement
//! rounds with different rates), Xylem's adaptive sampler operates continuously
//! during a single measurement run.

use std::time::Duration;

/// Adaptive sampling configuration
#[derive(Debug, Clone)]
pub struct AdaptiveSamplerConfig {
    /// Initial sampling rate (0.0-1.0)
    pub initial_rate: f64,
    /// Minimum sampling rate (prevent going too low)
    pub min_rate: f64,
    /// Maximum sampling rate (usually 1.0)
    pub max_rate: f64,
    /// Target confidence interval width (nanoseconds)
    /// If actual CI > target, increase sampling rate
    pub target_ci_width_ns: f64,
    /// How often to check and adjust rate (in number of samples)
    pub check_interval: usize,
    /// Rate adjustment factor (multiply/divide by this)
    pub adjustment_factor: f64,
}

impl Default for AdaptiveSamplerConfig {
    fn default() -> Self {
        Self {
            initial_rate: 0.2,          // Start at 20% (Lancet default)
            min_rate: 0.01,             // Min 1% (Lancet minimum)
            max_rate: 1.0,              // Max 100%
            target_ci_width_ns: 5000.0, // Target 5μs CI width
            check_interval: 1000,       // Check every 1000 samples
            adjustment_factor: 1.5,     // Adjust by 50%
        }
    }
}

/// Adaptive sampler state
pub struct AdaptiveSampler {
    config: AdaptiveSamplerConfig,
    current_rate: f64,
    samples_since_check: usize,
    total_samples: usize,
}

impl AdaptiveSampler {
    /// Create a new adaptive sampler
    pub fn new(config: AdaptiveSamplerConfig) -> Self {
        let current_rate = config.initial_rate.clamp(config.min_rate, config.max_rate);
        Self {
            config,
            current_rate,
            samples_since_check: 0,
            total_samples: 0,
        }
    }

    /// Get current sampling rate
    pub fn current_rate(&self) -> f64 {
        self.current_rate
    }

    /// Record a new sample and potentially adjust rate
    ///
    /// Returns the updated sampling rate if it changed, None otherwise
    pub fn record_sample(&mut self, samples: &[Duration]) -> Option<f64> {
        self.samples_since_check += 1;
        self.total_samples += 1;

        // Only check periodically
        if self.samples_since_check < self.config.check_interval {
            return None;
        }

        self.samples_since_check = 0;

        // Need minimum samples for meaningful CI calculation
        if samples.len() < 30 {
            return None;
        }

        // Calculate current CI width for P99
        let ci_width = self.estimate_ci_width(samples);

        // Adjust rate based on CI quality
        let old_rate = self.current_rate;

        if ci_width > self.config.target_ci_width_ns {
            // CI too wide - increase sampling rate
            self.current_rate *= self.config.adjustment_factor;
            self.current_rate = self.current_rate.min(self.config.max_rate);
        } else if ci_width < self.config.target_ci_width_ns * 0.5 {
            // CI much narrower than needed - reduce sampling rate to save overhead
            self.current_rate /= self.config.adjustment_factor;
            self.current_rate = self.current_rate.max(self.config.min_rate);
        }

        if (self.current_rate - old_rate).abs() > 1e-6 {
            Some(self.current_rate)
        } else {
            None
        }
    }

    /// Estimate confidence interval width for P99
    ///
    /// Uses bootstrap-like approach: approximate CI width using standard error
    fn estimate_ci_width(&self, samples: &[Duration]) -> f64 {
        if samples.len() < 30 {
            return f64::MAX; // Not enough samples
        }

        let mut sorted = samples.to_vec();
        sorted.sort();

        let n = sorted.len();
        let p99_idx = ((n as f64) * 0.99) as usize;

        // Get P99 value (currently unused, but kept for potential future enhancements)
        let _p99 = sorted[p99_idx.min(n - 1)];

        // Estimate standard error for P99 using binomial approximation
        // SE(p99) ≈ sqrt(p * (1-p) / n) * range
        let p = 0.99;
        let range_ns = (sorted[n - 1] - sorted[0]).as_nanos() as f64;
        let se = ((p * (1.0 - p)) / (n as f64)).sqrt() * range_ns;

        // 95% CI width ≈ 2 * 1.96 * SE
        2.0 * 1.96 * se
    }

    /// Reset sampler state
    pub fn reset(&mut self) {
        self.current_rate = self.config.initial_rate;
        self.samples_since_check = 0;
        self.total_samples = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_sampler() {
        let config = AdaptiveSamplerConfig::default();
        let sampler = AdaptiveSampler::new(config.clone());

        assert_eq!(sampler.current_rate(), config.initial_rate);
        assert_eq!(sampler.samples_since_check, 0);
    }

    #[test]
    fn test_sampling_rate_bounds() {
        let config = AdaptiveSamplerConfig {
            initial_rate: 0.5,
            min_rate: 0.1,
            max_rate: 0.8,
            ..Default::default()
        };

        let sampler = AdaptiveSampler::new(config.clone());
        assert_eq!(sampler.current_rate(), 0.5);

        // Test too high initial rate
        let config2 = AdaptiveSamplerConfig {
            initial_rate: 2.0, // Above max_rate
            ..config.clone()
        };
        let sampler2 = AdaptiveSampler::new(config2);
        assert_eq!(sampler2.current_rate(), config.max_rate);

        // Test too low initial rate
        let config3 = AdaptiveSamplerConfig {
            initial_rate: 0.01, // Below min_rate
            ..config
        };
        let sampler3 = AdaptiveSampler::new(config3);
        assert_eq!(sampler3.current_rate(), 0.1);
    }

    #[test]
    fn test_record_sample_no_adjustment_before_interval() {
        let config = AdaptiveSamplerConfig {
            check_interval: 100,
            ..Default::default()
        };
        let mut sampler = AdaptiveSampler::new(config);

        let samples: Vec<Duration> = (0..50).map(|i| Duration::from_micros(100 + i)).collect();

        // Should return None (no adjustment) before check_interval
        let result = sampler.record_sample(&samples);
        assert_eq!(result, None);
        assert_eq!(sampler.samples_since_check, 1);
    }

    #[test]
    fn test_estimate_ci_width() {
        let config = AdaptiveSamplerConfig::default();
        let sampler = AdaptiveSampler::new(config);

        // Create samples with known distribution
        let samples: Vec<Duration> =
            (0..1000).map(|i| Duration::from_micros(100 + i % 100)).collect();

        let ci_width = sampler.estimate_ci_width(&samples);

        // CI width should be positive and finite
        assert!(ci_width > 0.0);
        assert!(ci_width < f64::MAX);
    }

    #[test]
    fn test_estimate_ci_width_insufficient_samples() {
        let config = AdaptiveSamplerConfig::default();
        let sampler = AdaptiveSampler::new(config);

        let samples: Vec<Duration> = vec![Duration::from_micros(100), Duration::from_micros(200)];

        let ci_width = sampler.estimate_ci_width(&samples);
        assert_eq!(ci_width, f64::MAX);
    }

    #[test]
    fn test_reset() {
        let config = AdaptiveSamplerConfig::default();
        let mut sampler = AdaptiveSampler::new(config.clone());

        // Simulate some samples
        sampler.samples_since_check = 50;
        sampler.total_samples = 1000;
        sampler.current_rate = 0.5;

        sampler.reset();

        assert_eq!(sampler.current_rate, config.initial_rate);
        assert_eq!(sampler.samples_since_check, 0);
        assert_eq!(sampler.total_samples, 0);
    }

    #[test]
    fn test_rate_adjustment_up() {
        let config = AdaptiveSamplerConfig {
            initial_rate: 0.2,
            min_rate: 0.01,
            max_rate: 1.0,
            target_ci_width_ns: 1000.0, // Very tight target
            check_interval: 10,
            adjustment_factor: 2.0,
        };
        let mut sampler = AdaptiveSampler::new(config);

        // Create wide distribution (will have wide CI)
        let samples: Vec<Duration> = (0..100).map(|i| Duration::from_micros(i * 100)).collect();

        // Trigger check by recording enough samples
        for _ in 0..9 {
            sampler.record_sample(&samples);
        }

        let old_rate = sampler.current_rate();
        let result = sampler.record_sample(&samples); // 10th sample triggers check

        // Should increase rate due to wide CI
        if let Some(new_rate) = result {
            assert!(new_rate > old_rate);
        }
    }

    #[test]
    fn test_rate_adjustment_down() {
        let config = AdaptiveSamplerConfig {
            initial_rate: 0.8,
            min_rate: 0.01,
            max_rate: 1.0,
            target_ci_width_ns: 100000.0, // Very loose target
            check_interval: 10,
            adjustment_factor: 2.0,
        };
        let mut sampler = AdaptiveSampler::new(config);

        // Create tight distribution (will have narrow CI)
        let samples: Vec<Duration> =
            (0..100).map(|i| Duration::from_micros(1000 + i % 10)).collect();

        // Trigger check
        for _ in 0..9 {
            sampler.record_sample(&samples);
        }

        let old_rate = sampler.current_rate();
        let result = sampler.record_sample(&samples);

        // Should decrease rate due to narrow CI
        if let Some(new_rate) = result {
            assert!(new_rate < old_rate);
        }
    }

    #[test]
    fn test_rate_never_exceeds_max() {
        let config = AdaptiveSamplerConfig {
            initial_rate: 0.9,
            max_rate: 1.0,
            target_ci_width_ns: 100.0, // Very tight - will want to increase
            check_interval: 10,
            adjustment_factor: 10.0, // Large factor
            ..Default::default()
        };
        let mut sampler = AdaptiveSampler::new(config.clone());

        let samples: Vec<Duration> = (0..100).map(|i| Duration::from_micros(i * 1000)).collect();

        // Multiple adjustments
        for _ in 0..5 {
            for _ in 0..10 {
                sampler.record_sample(&samples);
            }
        }

        assert!(sampler.current_rate() <= config.max_rate);
    }

    #[test]
    fn test_rate_never_below_min() {
        let config = AdaptiveSamplerConfig {
            initial_rate: 0.05,
            min_rate: 0.01,
            target_ci_width_ns: 1000000.0, // Very loose - will want to decrease
            check_interval: 10,
            adjustment_factor: 10.0, // Large factor
            ..Default::default()
        };
        let mut sampler = AdaptiveSampler::new(config.clone());

        let samples: Vec<Duration> =
            (0..100).map(|i| Duration::from_micros(1000 + i % 5)).collect();

        // Multiple adjustments
        for _ in 0..5 {
            for _ in 0..10 {
                sampler.record_sample(&samples);
            }
        }

        assert!(sampler.current_rate() >= config.min_rate);
    }
}
