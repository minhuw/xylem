//! Statistics collection and analysis

use serde::{Deserialize, Serialize};
use std::time::Duration;

pub mod policy;
pub mod tuple_collector;

// Re-export main types
pub use policy::SamplingPolicy;
pub use tuple_collector::{
    ConnectionStats, LatencyStorage, StatsEntry, StatsKey, TimeSeriesPoint, TupleStatsCollector,
};

/// Trait for statistics collectors used by Worker
///
/// `TupleStatsCollector` implements this trait and groups stats by
/// (time_bucket, group_id, connection_id).
pub trait StatsRecorder: Send {
    /// Record a latency sample
    ///
    /// # Arguments
    /// * `group_id` - Traffic group ID
    /// * `connection_id` - Connection ID within the group
    /// * `latency` - Measured latency
    fn record_latency(&mut self, group_id: usize, connection_id: usize, latency: Duration);

    /// Record transmitted bytes
    fn record_tx_bytes(&mut self, group_id: usize, connection_id: usize, bytes: usize);

    /// Record received bytes
    fn record_rx_bytes(&mut self, group_id: usize, connection_id: usize, bytes: usize);
}

// Implement StatsRecorder for TupleStatsCollector (uses all dimensions)
impl StatsRecorder for TupleStatsCollector {
    fn record_latency(&mut self, group_id: usize, connection_id: usize, latency: Duration) {
        TupleStatsCollector::record_latency(self, group_id, connection_id, latency);
    }

    fn record_tx_bytes(&mut self, group_id: usize, connection_id: usize, bytes: usize) {
        TupleStatsCollector::record_tx_bytes(self, group_id, connection_id, bytes);
    }

    fn record_rx_bytes(&mut self, group_id: usize, connection_id: usize, bytes: usize) {
        TupleStatsCollector::record_rx_bytes(self, group_id, connection_id, bytes);
    }
}

/// Aggregated statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedStats {
    pub latency_p50: Duration,
    pub latency_p95: Duration,
    pub latency_p99: Duration,
    pub latency_p999: Duration,
    pub latency_p9999: Duration,
    pub latency_p99999: Duration,
    /// Exact mean of every recorded latency; zero when none were recorded.
    pub mean_latency: Duration,
    /// Smallest recorded latency; `None` when none were recorded.
    pub min_latency: Option<Duration>,
    /// Largest recorded latency; `None` when none were recorded.
    pub max_latency: Option<Duration>,
    /// Sample standard deviation; `None` with fewer than two latencies.
    pub std_dev: Option<Duration>,
    /// Half-width of the 95% confidence interval of the mean
    /// (1.96 * std_dev / sqrt(n)); `None` with fewer than two latencies.
    pub confidence_interval: Option<Duration>,
    /// Number of latencies recorded, after any sampling rate.
    pub latency_sample_count: u64,
    /// Latencies a bounded HDR histogram recorded at its `max_value_us`;
    /// `None` unless the policy sets that bound.
    pub clamped_samples: Option<u64>,
    pub throughput_rps: f64,
    pub throughput_mbps: f64,
    pub total_requests: u64,
}
