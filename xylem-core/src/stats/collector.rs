//! Statistics collection

use super::sampler::{AdaptiveSampler, AdaptiveSamplerConfig};
use hdrhistogram::Histogram;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use sketches_ddsketch::{Config as DDSketchConfig, DDSketch};
use std::sync::Mutex;
use std::time::Duration;
use tdigest::TDigest;

/// Sampling policy configuration for latency measurement
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum SamplingPolicy {
    /// Limited sampling with fixed rate and max samples
    Limited {
        /// Maximum number of samples to store
        #[serde(default = "default_max_samples")]
        max_samples: usize,
        /// Sampling rate (0.0 to 1.0)
        #[serde(default = "default_rate")]
        rate: f64,
    },
    /// Zero sampling: record every latency (unlimited storage)
    /// WARNING: Can consume large amounts of memory for high-throughput workloads
    Unlimited,
    /// Adaptive sampling: dynamically adjust rate to maintain target CI width
    Adaptive {
        /// Maximum number of samples to store
        #[serde(default = "default_max_samples")]
        max_samples: usize,
        /// Initial sampling rate
        #[serde(default = "default_rate")]
        initial_rate: f64,
        /// Target confidence interval width (in microseconds)
        target_ci_width_us: u64,
        /// Minimum sampling rate
        #[serde(default = "default_min_rate")]
        min_rate: f64,
        /// Maximum sampling rate
        #[serde(default = "default_max_rate")]
        max_rate: f64,
    },
    /// DDSketch: Distributed quantile sketch with relative-error guarantees
    /// Memory-efficient and fully mergeable across traffic groups
    DdSketch {
        /// Relative accuracy parameter (e.g., 0.01 = 1% error)
        #[serde(default = "default_ddsketch_alpha")]
        alpha: f64,
        /// Maximum number of bins (in steps of 128)
        #[serde(default = "default_ddsketch_max_bins")]
        max_bins: u32,
    },
    /// T-Digest: Adaptive clustering sketch optimized for extreme tail percentiles
    /// Better accuracy than DDSketch for p999+, fully mergeable
    TDigest {
        /// Compression parameter (higher = more accurate, more memory)
        /// Typical values: 100-1000
        #[serde(default = "default_tdigest_compression")]
        compression: usize,
    },
    /// HDR Histogram: High dynamic range histogram for maximum accuracy
    /// Higher memory usage but excellent for all percentiles
    HdrHistogram {
        /// Number of significant digits (1-5)
        /// Higher = more accurate but more memory
        #[serde(default = "default_hdr_sigfigs")]
        sigfigs: u8,
        /// Maximum expected value (in microseconds)
        #[serde(default = "default_hdr_max_value")]
        max_value_us: u64,
    },
}

fn default_max_samples() -> usize {
    128 * 1024
}

fn default_rate() -> f64 {
    1.0
}

fn default_min_rate() -> f64 {
    0.001
}

fn default_max_rate() -> f64 {
    1.0
}

fn default_ddsketch_alpha() -> f64 {
    0.01 // 1% relative error
}

fn default_ddsketch_max_bins() -> u32 {
    1024 // 1024 bins (optimal for most use cases)
}

fn default_tdigest_compression() -> usize {
    1000 // Good balance of accuracy and memory
}

fn default_hdr_sigfigs() -> u8 {
    3 // 3 significant figures (0.1% precision)
}

fn default_hdr_max_value() -> u64 {
    3_600_000_000 // 1 hour in microseconds
}

impl Default for SamplingPolicy {
    fn default() -> Self {
        SamplingPolicy::Limited {
            max_samples: default_max_samples(),
            rate: default_rate(),
        }
    }
}

/// Sampling mode for statistics collection
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SamplingMode {
    /// Limited sampling: store up to max_samples with given sampling rate
    Limited { max_samples: usize, rate: f64 },
    /// Zero sampling: store every single latency (no limit)
    /// WARNING: This can consume large amounts of memory for high-throughput workloads
    Unlimited,
    /// DDSketch: Use sketch-based quantile estimation
    Sketch,
}

/// Statistics collector (thread-local)
pub struct StatsCollector {
    samples: Vec<Duration>,
    mode: SamplingMode,
    sampling_rate: f64,
    tx_bytes: u64,
    rx_bytes: u64,
    tx_requests: u64,
    rx_requests: u64,
    sample_count: u64,
    rng: SmallRng,
    adaptive_sampler: Option<AdaptiveSampler>,
    ddsketch: Option<DDSketch>,
    ddsketch_config: Option<DDSketchConfig>,
    tdigest: Option<Mutex<TDigest>>,
    tdigest_compression: Option<usize>,
    hdrhistogram: Option<Mutex<Histogram<u64>>>,
}

impl StatsCollector {
    /// Merge multiple stats collectors into one
    pub fn merge(collectors: Vec<StatsCollector>) -> Self {
        let mut merged = StatsCollector::default();

        for collector in collectors {
            // Merge DDSketch if present
            if let Some(sketch) = collector.ddsketch {
                if let Some(ref mut merged_sketch) = merged.ddsketch {
                    // Merge sketches
                    let _ = merged_sketch.merge(&sketch);
                } else {
                    // First sketch - initialize merged
                    merged.ddsketch = Some(sketch);
                    merged.mode = SamplingMode::Sketch;
                }
            }

            // Merge T-Digest if present
            if let Some(tdigest_mutex) = collector.tdigest {
                let tdigest = tdigest_mutex.into_inner().unwrap();
                if let Some(ref merged_tdigest_mutex) = merged.tdigest {
                    // Merge into existing - extract both digests and merge
                    let existing = merged_tdigest_mutex.lock().unwrap();
                    let new_merged = TDigest::merge_digests(vec![existing.clone(), tdigest]);
                    drop(existing);
                    *merged_tdigest_mutex.lock().unwrap() = new_merged;
                } else {
                    // First T-Digest - initialize merged
                    merged.tdigest = Some(Mutex::new(tdigest));
                    merged.tdigest_compression = collector.tdigest_compression;
                    merged.mode = SamplingMode::Sketch;
                }
            }

            // Merge HDR Histogram if present
            if let Some(hdr_mutex) = collector.hdrhistogram {
                let hdr = hdr_mutex.into_inner().unwrap();
                if let Some(ref merged_hdr_mutex) = merged.hdrhistogram {
                    // Merge into existing
                    let mut merged_hdr = merged_hdr_mutex.lock().unwrap();
                    let _ = merged_hdr.add(&hdr);
                } else {
                    // First HDR - initialize merged
                    merged.hdrhistogram = Some(Mutex::new(hdr));
                    merged.mode = SamplingMode::Sketch;
                }
            }

            // Merge samples
            for sample in collector.samples {
                let should_add = match merged.mode {
                    SamplingMode::Unlimited => true,
                    SamplingMode::Limited { max_samples, .. } => merged.samples.len() < max_samples,
                    SamplingMode::Sketch => false,
                };
                if should_add {
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

    /// Create a new statistics collector from a sampling policy
    pub fn from_policy(policy: &SamplingPolicy) -> Self {
        match policy {
            SamplingPolicy::Limited { max_samples, rate } => Self::new(*max_samples, *rate),
            SamplingPolicy::Unlimited => Self::new_unlimited(),
            SamplingPolicy::Adaptive {
                max_samples,
                initial_rate,
                target_ci_width_us,
                min_rate,
                max_rate,
            } => {
                let config = AdaptiveSamplerConfig {
                    initial_rate: *initial_rate,
                    target_ci_width_ns: (*target_ci_width_us as f64) * 1000.0, // Convert microseconds to nanoseconds
                    min_rate: *min_rate,
                    max_rate: *max_rate,
                    check_interval: 1000,
                    adjustment_factor: 1.5, // Adjust by 50%
                };
                Self::with_adaptive_sampling(*max_samples, config)
            }
            SamplingPolicy::DdSketch { alpha, max_bins } => Self::with_ddsketch(*alpha, *max_bins),
            SamplingPolicy::TDigest { compression } => Self::with_tdigest(*compression),
            SamplingPolicy::HdrHistogram { sigfigs, max_value_us } => {
                Self::with_hdrhistogram(*sigfigs, *max_value_us)
            }
        }
    }

    /// Create a new statistics collector
    pub fn new(max_samples: usize, sampling_rate: f64) -> Self {
        Self {
            samples: Vec::with_capacity(max_samples),
            mode: SamplingMode::Limited {
                max_samples,
                rate: sampling_rate.clamp(0.0, 1.0),
            },
            sampling_rate: sampling_rate.clamp(0.0, 1.0),
            tx_bytes: 0,
            rx_bytes: 0,
            tx_requests: 0,
            rx_requests: 0,
            sample_count: 0,
            rng: SmallRng::from_os_rng(),
            adaptive_sampler: None,
            ddsketch: None,
            ddsketch_config: None,
            tdigest: None,
            tdigest_compression: None,
            hdrhistogram: None,
        }
    }

    /// Create a new statistics collector with zero sampling (unlimited storage)
    /// WARNING: This stores every single latency and can consume large amounts of memory
    pub fn new_unlimited() -> Self {
        Self {
            samples: Vec::new(),
            mode: SamplingMode::Unlimited,
            sampling_rate: 1.0,
            tx_bytes: 0,
            rx_bytes: 0,
            tx_requests: 0,
            rx_requests: 0,
            sample_count: 0,
            rng: SmallRng::from_os_rng(),
            adaptive_sampler: None,
            ddsketch: None,
            ddsketch_config: None,
            tdigest: None,
            tdigest_compression: None,
            hdrhistogram: None,
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
            mode: SamplingMode::Limited { max_samples, rate: initial_rate },
            sampling_rate: initial_rate,
            tx_bytes: 0,
            rx_bytes: 0,
            tx_requests: 0,
            rx_requests: 0,
            sample_count: 0,
            rng: SmallRng::from_os_rng(),
            adaptive_sampler: Some(AdaptiveSampler::new(adaptive_config)),
            ddsketch: None,
            ddsketch_config: None,
            tdigest: None,
            tdigest_compression: None,
            hdrhistogram: None,
        }
    }

    /// Create a new statistics collector with DDSketch
    pub fn with_ddsketch(alpha: f64, max_bins: u32) -> Self {
        let config = DDSketchConfig::new(alpha, max_bins, 1.0); // min_value=1.0 nanosecond
        let ddsketch = DDSketch::new(config);
        Self {
            samples: Vec::new(),
            mode: SamplingMode::Sketch,
            sampling_rate: 1.0,
            tx_bytes: 0,
            rx_bytes: 0,
            tx_requests: 0,
            rx_requests: 0,
            sample_count: 0,
            rng: SmallRng::from_os_rng(),
            adaptive_sampler: None,
            ddsketch: Some(ddsketch),
            ddsketch_config: Some(config),
            tdigest: None,
            tdigest_compression: None,
            hdrhistogram: None,
        }
    }

    /// Create a new statistics collector with T-Digest
    pub fn with_tdigest(compression: usize) -> Self {
        let tdigest = TDigest::new_with_size(compression);
        Self {
            samples: Vec::new(),
            mode: SamplingMode::Sketch,
            sampling_rate: 1.0,
            tx_bytes: 0,
            rx_bytes: 0,
            tx_requests: 0,
            rx_requests: 0,
            sample_count: 0,
            rng: SmallRng::from_os_rng(),
            adaptive_sampler: None,
            ddsketch: None,
            ddsketch_config: None,
            tdigest: Some(Mutex::new(tdigest)),
            tdigest_compression: Some(compression),
            hdrhistogram: None,
        }
    }

    /// Create a new statistics collector with HDR Histogram
    pub fn with_hdrhistogram(sigfigs: u8, max_value_us: u64) -> Self {
        // Convert microseconds to nanoseconds for internal storage
        let max_value_ns = max_value_us * 1000;
        let histogram = Histogram::<u64>::new_with_max(max_value_ns, sigfigs)
            .expect("Failed to create HDR histogram");
        Self {
            samples: Vec::new(),
            mode: SamplingMode::Sketch,
            sampling_rate: 1.0,
            tx_bytes: 0,
            rx_bytes: 0,
            tx_requests: 0,
            rx_requests: 0,
            sample_count: 0,
            rng: SmallRng::from_os_rng(),
            adaptive_sampler: None,
            ddsketch: None,
            ddsketch_config: None,
            tdigest: None,
            tdigest_compression: None,
            hdrhistogram: Some(Mutex::new(histogram)),
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

        // Determine if we should store this sample
        match self.mode {
            SamplingMode::Sketch => {
                // Sketch mode: add to whichever sketch is present
                if let Some(ref mut sketch) = self.ddsketch {
                    sketch.add(latency.as_nanos() as f64);
                }
                if let Some(ref tdigest_mutex) = self.tdigest {
                    let mut tdigest = tdigest_mutex.lock().unwrap();
                    *tdigest = tdigest.merge_unsorted(vec![latency.as_nanos() as f64]);
                }
                if let Some(ref hdr_mutex) = self.hdrhistogram {
                    let mut hdr = hdr_mutex.lock().unwrap();
                    let _ = hdr.record(latency.as_nanos() as u64);
                }
            }
            SamplingMode::Unlimited => {
                // Zero sampling: always record
                self.samples.push(latency);
            }
            SamplingMode::Limited { max_samples, .. } => {
                // Limited mode: check sampling rate and capacity
                let should_sample = if self.samples.len() >= max_samples {
                    // Already at capacity
                    false
                } else if self.sampling_rate >= 1.0 {
                    true
                } else if self.sampling_rate <= 0.0 {
                    false
                } else {
                    // Probabilistic sampling
                    self.rng.random::<f64>() < self.sampling_rate
                };

                if should_sample {
                    self.samples.push(latency);
                }
            }
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

    /// Get reference to DDSketch if in sketch mode
    pub fn ddsketch(&self) -> Option<&DDSketch> {
        self.ddsketch.as_ref()
    }

    /// Get reference to T-Digest if in sketch mode
    pub fn tdigest(&self) -> Option<&Mutex<TDigest>> {
        self.tdigest.as_ref()
    }

    /// Get reference to HDR Histogram if in sketch mode
    pub fn hdrhistogram(&self) -> Option<&Mutex<Histogram<u64>>> {
        self.hdrhistogram.as_ref()
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

        if let Some(ref config) = self.ddsketch_config {
            // Reset DDSketch by creating a new one with the same config
            self.ddsketch = Some(DDSketch::new(*config));
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
        let max_samples = 128 * 1024;
        Self {
            samples: Vec::with_capacity(max_samples),
            mode: SamplingMode::Limited { max_samples, rate: 1.0 },
            sampling_rate: 1.0,
            tx_bytes: 0,
            rx_bytes: 0,
            tx_requests: 0,
            rx_requests: 0,
            sample_count: 0,
            rng: SmallRng::from_os_rng(),
            adaptive_sampler: None,
            ddsketch: None,
            ddsketch_config: None,
            tdigest: None,
            tdigest_compression: None,
            hdrhistogram: None,
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
        assert_eq!(collector.mode, SamplingMode::Limited { max_samples: 128 * 1024, rate: 1.0 });
        assert_eq!(collector.sampling_rate, 1.0);
    }

    #[test]
    fn test_unlimited_sampling() {
        let mut collector = StatsCollector::new_unlimited();
        assert_eq!(collector.mode, SamplingMode::Unlimited);
        assert_eq!(collector.sampling_rate, 1.0);

        // Record many samples - should store all of them
        for i in 0..10000 {
            collector.record_latency(Duration::from_millis(i));
        }

        // Should have stored ALL samples (no limit)
        assert_eq!(collector.samples().len(), 10000);
        assert_eq!(collector.sample_count(), 10000);
    }

    #[test]
    fn test_limited_vs_unlimited() {
        let mut limited = StatsCollector::new(100, 1.0);
        let mut unlimited = StatsCollector::new_unlimited();

        // Record 200 samples to both
        for i in 0..200 {
            limited.record_latency(Duration::from_millis(i));
            unlimited.record_latency(Duration::from_millis(i));
        }

        // Limited should cap at 100
        assert_eq!(limited.samples().len(), 100);
        assert_eq!(limited.sample_count(), 200);

        // Unlimited should store all 200
        assert_eq!(unlimited.samples().len(), 200);
        assert_eq!(unlimited.sample_count(), 200);
    }

    #[test]
    fn test_merge_unlimited() {
        let mut collector1 = StatsCollector::new_unlimited();
        let mut collector2 = StatsCollector::new_unlimited();

        for i in 0..100 {
            collector1.record_latency(Duration::from_millis(i));
        }
        for i in 100..200 {
            collector2.record_latency(Duration::from_millis(i));
        }

        let merged = StatsCollector::merge(vec![collector1, collector2]);

        // Should have merged all samples
        assert_eq!(merged.samples().len(), 200);
        assert_eq!(merged.sample_count(), 200);
    }

    #[test]
    fn test_ddsketch_creation() {
        let collector = StatsCollector::with_ddsketch(0.01, 2048);
        assert_eq!(collector.mode, SamplingMode::Sketch);
        assert!(collector.ddsketch().is_some());
        assert_eq!(collector.samples().len(), 0); // DDSketch doesn't use samples
    }

    #[test]
    fn test_ddsketch_record() {
        let mut collector = StatsCollector::with_ddsketch(0.01, 2048);

        // Record 1000 samples
        for i in 1..=1000 {
            collector.record_latency(Duration::from_millis(i as u64));
        }

        assert_eq!(collector.sample_count(), 1000);
        assert_eq!(collector.samples().len(), 0); // Sketches don't store samples
        assert!(collector.ddsketch().is_some());
    }

    #[test]
    fn test_ddsketch_merge() {
        let mut collector1 = StatsCollector::with_ddsketch(0.01, 2048);
        let mut collector2 = StatsCollector::with_ddsketch(0.01, 2048);

        // Add different ranges to each collector
        for i in 1..=500 {
            collector1.record_latency(Duration::from_millis(i));
        }
        for i in 501..=1000 {
            collector2.record_latency(Duration::from_millis(i));
        }

        let merged = StatsCollector::merge(vec![collector1, collector2]);

        assert_eq!(merged.mode, SamplingMode::Sketch);
        assert_eq!(merged.sample_count(), 1000);
        assert!(merged.ddsketch().is_some());
    }

    #[test]
    fn test_from_policy_ddsketch() {
        let policy = SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 2048 };

        let collector = StatsCollector::from_policy(&policy);
        assert_eq!(collector.mode, SamplingMode::Sketch);
        assert!(collector.ddsketch().is_some());
    }

    #[test]
    fn test_from_policy_unlimited() {
        let policy = SamplingPolicy::Unlimited;
        let collector = StatsCollector::from_policy(&policy);
        assert_eq!(collector.mode, SamplingMode::Unlimited);
    }

    #[test]
    fn test_from_policy_limited() {
        let policy = SamplingPolicy::Limited { max_samples: 1000, rate: 0.5 };
        let collector = StatsCollector::from_policy(&policy);
        assert_eq!(collector.mode, SamplingMode::Limited { max_samples: 1000, rate: 0.5 });
    }
}
