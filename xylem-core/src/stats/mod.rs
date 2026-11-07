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
    pub mean_latency: Duration,
    pub std_dev: Duration,
    pub confidence_interval: Duration,
    pub throughput_rps: f64,
    pub throughput_mbps: f64,
    pub total_requests: u64,
}
