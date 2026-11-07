//! Sampling policy configuration

use serde::{Deserialize, Serialize};

/// Sampling policy configuration for latency measurement
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum SamplingPolicy {
    /// Disable latency sampling entirely (still counts bytes/requests)
    None,
    /// Uniform random sample of at most `max_samples` latencies per entry.
    /// Each latency is first kept with probability `rate`; the kept latencies
    /// are reservoir-sampled, so the stored samples are a uniform sample of
    /// every kept latency, not the first ones. Mean, min, max and standard
    /// deviation are computed from every kept latency.
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
    /// HDR Histogram: records every latency with bounded relative error.
    /// This is the default policy.
    HdrHistogram {
        /// Number of significant digits (1-5)
        /// Higher = more accurate but more memory
        #[serde(default = "default_hdr_sigfigs")]
        sigfigs: u8,
        /// Optional upper bound (in microseconds). When unset, the histogram
        /// grows to fit every recorded value. When set, larger values are
        /// recorded as this bound and counted in the `clamped_samples` output.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_value_us: Option<u64>,
    },
}

fn default_max_samples() -> usize {
    128 * 1024
}

fn default_rate() -> f64 {
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

impl Default for SamplingPolicy {
    /// An auto-resizing HDR histogram with 3 significant figures, which keeps
    /// every latency and merges independently of order.
    fn default() -> Self {
        SamplingPolicy::HdrHistogram {
            sigfigs: default_hdr_sigfigs(),
            max_value_us: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_auto_resizing_hdr_histogram() {
        assert_eq!(
            SamplingPolicy::default(),
            SamplingPolicy::HdrHistogram { sigfigs: 3, max_value_us: None }
        );
        let parsed: SamplingPolicy = serde_json::from_str(r#"{"type": "hdr-histogram"}"#).unwrap();
        assert_eq!(parsed, SamplingPolicy::default());
    }

    #[test]
    fn explicit_policies_keep_parsing() {
        let limited: SamplingPolicy =
            serde_json::from_str(r#"{"type": "limited", "rate": 0.5, "max_samples": 10}"#).unwrap();
        assert_eq!(limited, SamplingPolicy::Limited { max_samples: 10, rate: 0.5 });
        let bounded: SamplingPolicy = serde_json::from_str(
            r#"{"type": "hdr-histogram", "sigfigs": 2, "max_value_us": 1000}"#,
        )
        .unwrap();
        assert_eq!(bounded, SamplingPolicy::HdrHistogram { sigfigs: 2, max_value_us: Some(1000) });
    }
}
