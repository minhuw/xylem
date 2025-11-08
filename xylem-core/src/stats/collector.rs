//! Statistics collection

use super::sampler::{AdaptiveSampler, AdaptiveSamplerConfig};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::time::Duration;

/// Statistics collector (thread-local)
pub struct StatsCollector {
    samples: Vec<Duration>,
    max_samples: usize,
    sampling_rate: f64,
    tx_bytes: u64,
    rx_bytes: u64,
    tx_requests: u64,
    rx_requests: u64,
    sample_count: u64,
    rng: SmallRng,
    adaptive_sampler: Option<AdaptiveSampler>,
}

impl StatsCollector {
    /// Merge multiple stats collectors into one
    pub fn merge(collectors: Vec<StatsCollector>) -> Self {
        let mut merged = StatsCollector::default();

        for collector in collectors {
            // Merge samples
            for sample in collector.samples {
                if merged.samples.len() < merged.max_samples {
                    merged.samples.push(sample);
                }
            }

            // Aggregate counters
            merged.tx_bytes += collector.tx_bytes;
            merged.rx_bytes += collector.rx_bytes;
            merged.tx_requests += collector.tx_requests;
            merged.rx_requests += collector.rx_requests;
            merged.sample_count += collector.sample_count;
        }

        // Note: Adaptive sampler state is not merged (each thread has independent sampler)

        merged
    }

    /// Create a new statistics collector
    pub fn new(max_samples: usize, sampling_rate: f64) -> Self {
        Self {
            samples: Vec::with_capacity(max_samples),
            max_samples,
            sampling_rate: sampling_rate.clamp(0.0, 1.0),
            tx_bytes: 0,
            rx_bytes: 0,
            tx_requests: 0,
            rx_requests: 0,
            sample_count: 0,
            rng: SmallRng::from_os_rng(),
            adaptive_sampler: None,
        }
    }

    /// Create a new statistics collector with adaptive sampling
    pub fn with_adaptive_sampling(
        max_samples: usize,
        adaptive_config: AdaptiveSamplerConfig,
    ) -> Self {
        let initial_rate = adaptive_config.initial_rate;
        Self {
            samples: Vec::with_capacity(max_samples),
            max_samples,
            sampling_rate: initial_rate,
            tx_bytes: 0,
            rx_bytes: 0,
            tx_requests: 0,
            rx_requests: 0,
            sample_count: 0,
            rng: SmallRng::from_os_rng(),
            adaptive_sampler: Some(AdaptiveSampler::new(adaptive_config)),
        }
    }

    /// Record a latency sample
    pub fn record_latency(&mut self, latency: Duration) {
        self.tx_requests += 1;
        self.rx_requests += 1;
        self.sample_count += 1;

        // Check if adaptive sampler wants to adjust rate
        if let Some(ref mut adaptive_sampler) = self.adaptive_sampler {
            if let Some(new_rate) = adaptive_sampler.record_sample(&self.samples) {
                self.sampling_rate = new_rate;
            }
        }

        // Probabilistic sampling using RNG
        // Implement the TODO from line 68
        let should_sample = if self.sampling_rate >= 1.0 {
            true
        } else if self.sampling_rate <= 0.0 {
            false
        } else {
            // Random sampling based on sampling_rate
            self.rng.random::<f64>() < self.sampling_rate
        };

        if should_sample && self.samples.len() < self.max_samples {
            self.samples.push(latency);
        }
    }

    /// Record transmitted bytes
    pub fn record_tx_bytes(&mut self, bytes: usize) {
        self.tx_bytes += bytes as u64;
    }

    /// Record received bytes
    pub fn record_rx_bytes(&mut self, bytes: usize) {
        self.rx_bytes += bytes as u64;
    }

    /// Get all collected samples
    pub fn samples(&self) -> &[Duration] {
        &self.samples
    }

    /// Get total transmitted bytes
    pub fn tx_bytes(&self) -> u64 {
        self.tx_bytes
    }

    /// Get total received bytes
    pub fn rx_bytes(&self) -> u64 {
        self.rx_bytes
    }

    /// Get total transmitted requests
    pub fn tx_requests(&self) -> u64 {
        self.tx_requests
    }

    /// Get total received requests
    pub fn rx_requests(&self) -> u64 {
        self.rx_requests
    }

    /// Get total sample count (including those not stored due to sampling)
    pub fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Reset the collector
    pub fn reset(&mut self) {
        self.samples.clear();
        self.tx_bytes = 0;
        self.rx_bytes = 0;
        self.tx_requests = 0;
        self.rx_requests = 0;
        self.sample_count = 0;

        if let Some(ref mut adaptive_sampler) = self.adaptive_sampler {
            adaptive_sampler.reset();
            self.sampling_rate = adaptive_sampler.current_rate();
        }
    }

    /// Calculate basic statistics
    pub fn calculate_basic_stats(&self) -> BasicStats {
        if self.samples.is_empty() {
            return BasicStats::default();
        }

        let mut sorted = self.samples.clone();
        sorted.sort();

        let count = sorted.len();
        let sum: Duration = sorted.iter().sum();
        let mean = sum / count as u32;

        let min = sorted[0];
        let max = sorted[count - 1];

        BasicStats { count, min, max, mean }
    }
}

impl Default for StatsCollector {
    fn default() -> Self {
        // Default: store up to 128K samples at 100% sampling rate
        Self {
            samples: Vec::with_capacity(128 * 1024),
            max_samples: 128 * 1024,
            sampling_rate: 1.0,
            tx_bytes: 0,
            rx_bytes: 0,
            tx_requests: 0,
            rx_requests: 0,
            sample_count: 0,
            rng: SmallRng::from_os_rng(),
            adaptive_sampler: None,
        }
    }
}

/// Basic statistics summary
#[derive(Debug, Clone, Default)]
pub struct BasicStats {
    pub count: usize,
    pub min: Duration,
    pub max: Duration,
    pub mean: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_collector() {
        let collector = StatsCollector::new(1000, 0.5);
        assert_eq!(collector.samples().len(), 0);
        assert_eq!(collector.tx_bytes(), 0);
        assert_eq!(collector.rx_bytes(), 0);
        assert_eq!(collector.tx_requests(), 0);
        assert_eq!(collector.rx_requests(), 0);
    }

    #[test]
    fn test_record_latency() {
        let mut collector = StatsCollector::new(100, 1.0);

        collector.record_latency(Duration::from_millis(10));
        collector.record_latency(Duration::from_millis(20));
        collector.record_latency(Duration::from_millis(15));

        assert_eq!(collector.samples().len(), 3);
        assert_eq!(collector.tx_requests(), 3);
        assert_eq!(collector.rx_requests(), 3);
        assert_eq!(collector.sample_count(), 3);
    }

    #[test]
    fn test_record_bytes() {
        let mut collector = StatsCollector::new(100, 1.0);

        collector.record_tx_bytes(100);
        collector.record_tx_bytes(200);
        collector.record_rx_bytes(150);

        assert_eq!(collector.tx_bytes(), 300);
        assert_eq!(collector.rx_bytes(), 150);
    }

    #[test]
    fn test_max_samples() {
        let mut collector = StatsCollector::new(5, 1.0);

        for i in 0..10 {
            collector.record_latency(Duration::from_millis(i));
        }

        // Should only store up to max_samples
        assert_eq!(collector.samples().len(), 5);
        // But all requests should be counted
        assert_eq!(collector.tx_requests(), 10);
    }

    #[test]
    fn test_reset() {
        let mut collector = StatsCollector::new(100, 1.0);

        collector.record_latency(Duration::from_millis(10));
        collector.record_tx_bytes(100);
        collector.record_rx_bytes(50);

        collector.reset();

        assert_eq!(collector.samples().len(), 0);
        assert_eq!(collector.tx_bytes(), 0);
        assert_eq!(collector.rx_bytes(), 0);
        assert_eq!(collector.tx_requests(), 0);
        assert_eq!(collector.rx_requests(), 0);
    }

    #[test]
    fn test_basic_stats() {
        let mut collector = StatsCollector::new(100, 1.0);

        collector.record_latency(Duration::from_millis(10));
        collector.record_latency(Duration::from_millis(20));
        collector.record_latency(Duration::from_millis(30));

        let stats = collector.calculate_basic_stats();

        assert_eq!(stats.count, 3);
        assert_eq!(stats.min, Duration::from_millis(10));
        assert_eq!(stats.max, Duration::from_millis(30));
        assert_eq!(stats.mean, Duration::from_millis(20));
    }

    #[test]
    fn test_basic_stats_empty() {
        let collector = StatsCollector::new(100, 1.0);
        let stats = collector.calculate_basic_stats();

        assert_eq!(stats.count, 0);
        assert_eq!(stats.min, Duration::ZERO);
        assert_eq!(stats.max, Duration::ZERO);
        assert_eq!(stats.mean, Duration::ZERO);
    }

    #[test]
    fn test_default() {
        let collector = StatsCollector::default();
        assert_eq!(collector.max_samples, 128 * 1024);
        assert_eq!(collector.sampling_rate, 1.0);
    }
}
