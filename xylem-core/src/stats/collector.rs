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
    /// Disable latency sampling entirely (still counts bytes/requests)
    None,
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
    /// Latency sampling disabled
    None,
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
    /// Merge collectors with identical storage configurations.
    ///
    /// Retains the first collector's mode, capacity, sketch configuration, and
    /// adaptive sampler state. Limited sampling keeps at most its configured
    /// capacity, in input order; Unlimited keeps every stored sample. Empty input
    /// produces the default collector.
    ///
    /// # Panics
    /// Panics on incompatible configurations or a sketch merge error. Use
    /// [`Self::try_merge`] to handle incompatible inputs explicitly.
    pub fn merge(collectors: Vec<StatsCollector>) -> Self {
        Self::try_merge(collectors).expect("Cannot merge incompatible statistics collectors")
    }

    /// Fallible version of [`Self::merge`]. Inputs must have the same sampling
    /// mode and storage settings; heterogeneous sketches cannot be converted
    /// back into their original samples without losing information.
    pub fn try_merge(collectors: Vec<StatsCollector>) -> Result<Self, &'static str> {
        let mut collectors = collectors.into_iter();
        let Some(mut merged) = collectors.next() else {
            return Ok(Self::default());
        };

        for collector in collectors {
            if merged.mode != collector.mode
                || merged.ddsketch_config != collector.ddsketch_config
                || merged.tdigest_compression != collector.tdigest_compression
                || merged.adaptive_sampler.as_ref().map(AdaptiveSampler::config)
                    != collector.adaptive_sampler.as_ref().map(AdaptiveSampler::config)
            {
                return Err("sampling configurations differ");
            }
            match (&mut merged.ddsketch, collector.ddsketch) {
                (Some(target), Some(source)) => {
                    target.merge(&source).map_err(|_| "DDSketch merge failed")?;
                }
                (None, None) => {}
                _ => return Err("sketch types differ"),
            }
            match (&mut merged.tdigest, collector.tdigest) {
                (Some(target), Some(source)) => {
                    let target = target.get_mut().unwrap();
                    *target =
                        TDigest::merge_digests(vec![target.clone(), source.into_inner().unwrap()]);
                }
                (None, None) => {}
                _ => return Err("sketch types differ"),
            }
            match (&mut merged.hdrhistogram, collector.hdrhistogram) {
                (Some(target), Some(source)) => {
                    let target = target.get_mut().unwrap();
                    let source = source.into_inner().unwrap();
                    if (target.low(), target.high(), target.sigfig())
                        != (source.low(), source.high(), source.sigfig())
                    {
                        return Err("HDR histogram configurations differ");
                    }
                    target.add(&source).map_err(|_| "HDR histogram merge failed")?;
                }
                (None, None) => {}
                _ => return Err("sketch types differ"),
            }

            let available = match merged.mode {
                SamplingMode::Unlimited => usize::MAX,
                SamplingMode::Limited { max_samples, .. } => {
                    max_samples.saturating_sub(merged.samples.len())
                }
                SamplingMode::Sketch | SamplingMode::None => 0,
            };
            merged.samples.extend(collector.samples.into_iter().take(available));
            merged.tx_bytes += collector.tx_bytes;
            merged.rx_bytes += collector.rx_bytes;
            merged.tx_requests += collector.tx_requests;
            merged.rx_requests += collector.rx_requests;
            merged.sample_count += collector.sample_count;
        }
        Ok(merged)
    }

    /// Create a new statistics collector from a sampling policy
    pub fn from_policy(policy: &SamplingPolicy) -> Self {
        match policy {
            SamplingPolicy::None => Self::new_none(),
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

    /// Create a statistics collector that skips latency recording
    pub fn new_none() -> Self {
        Self {
            samples: Vec::new(),
            mode: SamplingMode::None,
            sampling_rate: 0.0,
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
        // Always count requests/bytes even if latency sampling is disabled
        self.tx_requests += 1;
        self.rx_requests += 1;

        // Skip latency storage when sampling is disabled
        if matches!(self.mode, SamplingMode::None) {
            return;
        }

        // Check if adaptive sampler wants to adjust rate
        if let Some(ref mut adaptive_sampler) = self.adaptive_sampler {
            if let Some(new_rate) = adaptive_sampler.record_sample(&self.samples) {
                self.sampling_rate = new_rate;
            }
        }

        self.sample_count += 1;

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
            SamplingMode::None => {
                // Already handled by early return
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

    /// Sampling mode accessor
    pub fn sampling_mode(&self) -> SamplingMode {
        self.mode
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
    fn merge_unlimited_keeps_large_distinct_distributions() {
        let mut low = StatsCollector::new_unlimited();
        let mut high = StatsCollector::new_unlimited();
        for _ in 0..70_000 {
            low.record_latency(Duration::from_micros(10));
            high.record_latency(Duration::from_micros(100));
        }
        let merged = StatsCollector::merge(vec![low, high]);
        assert_eq!(merged.sampling_mode(), SamplingMode::Unlimited);
        assert_eq!(merged.samples().len(), 140_000);
        assert_eq!(merged.sample_count(), 140_000);
        assert_eq!(merged.tx_requests(), 140_000);
        assert_eq!(merged.calculate_basic_stats().mean, Duration::from_micros(55));
        assert_eq!(merged.samples()[139_999], Duration::from_micros(100));
    }

    #[test]
    fn merge_none_preserves_throughput() {
        let collectors = (0..2)
            .map(|_| {
                let mut c = StatsCollector::new_none();
                c.record_latency(Duration::from_micros(10));
                c.record_tx_bytes(100);
                c.record_rx_bytes(200);
                c
            })
            .collect();
        let merged = StatsCollector::merge(collectors);
        assert_eq!(merged.sampling_mode(), SamplingMode::None);
        assert_eq!(merged.tx_requests(), 2);
        assert_eq!(merged.rx_requests(), 2);
        assert_eq!((merged.tx_bytes(), merged.rx_bytes()), (200, 400));
        let stats = crate::stats::aggregate_stats(&merged, Duration::from_secs(2), 0.95);
        assert_eq!(stats.total_requests, 2);
        assert_eq!(stats.throughput_rps, 1.0);
        assert_eq!(stats.throughput_mbps, 0.0024);
        assert_eq!(stats.latency_p99, Duration::ZERO);
    }

    #[test]
    fn merge_preserves_custom_capacity_and_adaptive_settings() {
        let mut first = StatsCollector::new(3, 1.0);
        let mut second = StatsCollector::new(3, 1.0);
        for i in 0..2 {
            first.record_latency(Duration::from_micros(i));
            second.record_latency(Duration::from_micros(i + 10));
        }
        let merged = StatsCollector::merge(vec![first, second]);
        assert_eq!(merged.sampling_mode(), SamplingMode::Limited { max_samples: 3, rate: 1.0 });
        assert_eq!(merged.samples().len(), 3);
        assert_eq!(merged.sample_count(), 4);
        assert_eq!(merged.samples()[2], Duration::from_micros(10));
        let config = AdaptiveSamplerConfig { initial_rate: 0.7, ..Default::default() };
        let merged = StatsCollector::merge(vec![
            StatsCollector::with_adaptive_sampling(19, config.clone()),
            StatsCollector::with_adaptive_sampling(19, config.clone()),
        ]);
        assert_eq!(merged.sampling_mode(), SamplingMode::Limited { max_samples: 19, rate: 0.7 });
        assert_eq!(merged.adaptive_sampler.as_ref().unwrap().config(), &config);
    }

    #[test]
    fn merge_preserves_sketch_settings_and_distributions() {
        for policy in [
            SamplingPolicy::DdSketch { alpha: 0.02, max_bins: 2048 },
            SamplingPolicy::TDigest { compression: 123 },
            SamplingPolicy::HdrHistogram { sigfigs: 4, max_value_us: 100_000 },
        ] {
            let mut low = StatsCollector::from_policy(&policy);
            let mut high = StatsCollector::from_policy(&policy);
            for _ in 0..10 {
                low.record_latency(Duration::from_micros(10));
                high.record_latency(Duration::from_micros(100));
            }
            let mut merged = StatsCollector::merge(vec![low, high]);
            assert_eq!(merged.sampling_mode(), SamplingMode::Sketch);
            assert_eq!(merged.sample_count(), 20);
            let stats = crate::stats::aggregate_stats(&merged, Duration::from_secs(1), 0.95);
            assert!((98_000..=102_000).contains(&(stats.latency_p99.as_nanos() as u64)));
            match policy {
                SamplingPolicy::DdSketch { alpha, max_bins } => {
                    assert_eq!(
                        merged.ddsketch_config,
                        Some(DDSketchConfig::new(alpha, max_bins, 1.0))
                    );
                    assert_eq!(merged.ddsketch().unwrap().count(), 20);
                    merged.reset();
                    assert_eq!(merged.ddsketch().unwrap().count(), 0);
                }
                SamplingPolicy::TDigest { compression } => {
                    assert_eq!(merged.tdigest_compression, Some(compression));
                    assert_eq!(merged.tdigest().unwrap().lock().unwrap().count(), 20.0);
                }
                SamplingPolicy::HdrHistogram { sigfigs, max_value_us } => {
                    let hist = merged.hdrhistogram().unwrap().lock().unwrap();
                    assert_eq!(hist.sigfig(), sigfigs);
                    assert_eq!(hist.high(), max_value_us * 1000);
                    assert_eq!(hist.len(), 20);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn merge_reports_incompatible_policies() {
        let pairs = [
            (SamplingPolicy::None, SamplingPolicy::Unlimited),
            (
                SamplingPolicy::Limited { max_samples: 2, rate: 1.0 },
                SamplingPolicy::Limited { max_samples: 3, rate: 1.0 },
            ),
            (
                SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 1024 },
                SamplingPolicy::DdSketch { alpha: 0.02, max_bins: 1024 },
            ),
            (
                SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 1024 },
                SamplingPolicy::TDigest { compression: 100 },
            ),
            (
                SamplingPolicy::TDigest { compression: 100 },
                SamplingPolicy::TDigest { compression: 200 },
            ),
            (
                SamplingPolicy::HdrHistogram { sigfigs: 2, max_value_us: 1000 },
                SamplingPolicy::HdrHistogram { sigfigs: 3, max_value_us: 1000 },
            ),
        ];
        for (a, b) in pairs {
            assert!(StatsCollector::try_merge(vec![
                StatsCollector::from_policy(&a),
                StatsCollector::from_policy(&b)
            ])
            .is_err());
        }
    }

    #[test]
    #[should_panic(expected = "Cannot merge incompatible statistics collectors")]
    fn infallible_merge_panics_instead_of_dropping_incompatible_samples() {
        StatsCollector::merge(vec![StatsCollector::new_none(), StatsCollector::new_unlimited()]);
    }

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
