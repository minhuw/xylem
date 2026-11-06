//! Sampling policy configuration

use serde::{Deserialize, Serialize};

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
