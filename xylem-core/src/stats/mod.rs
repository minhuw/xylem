//! Statistics collection and analysis

use serde::{Deserialize, Serialize};
use std::time::Duration;

pub mod analysis;
pub mod collector;
pub mod dumper;
pub mod group_collector;
pub mod metadata;
pub mod sampler;
pub mod tuple_collector;

// Re-export main types
pub use analysis::{aggregate_stats, aggregate_stats_per_group};
pub use collector::{BasicStats, SamplingMode, SamplingPolicy, StatsCollector};
pub use dumper::{StatsDumper, StatsDumperError, StatsRow, StreamingConfig};
pub use group_collector::GroupStatsCollector;
pub use metadata::{GroupMetadataCollector, StatsMetadata};
pub use sampler::{AdaptiveSampler, AdaptiveSamplerConfig};
pub use tuple_collector::{
    ConnectionStats, LatencyStorage, StatsEntry, StatsKey, TimeSeriesPoint, TupleStatsCollector,
};

/// Trait for statistics collectors used by Worker
///
/// This trait abstracts over different statistics collection strategies:
/// - `GroupStatsCollector`: Groups stats by traffic group only
/// - `TupleStatsCollector`: Groups stats by (time_bucket, group_id, connection_id)
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

// Implement StatsRecorder for GroupStatsCollector (ignores connection_id for backward compatibility)
impl StatsRecorder for GroupStatsCollector {
    fn record_latency(&mut self, group_id: usize, _connection_id: usize, latency: Duration) {
        GroupStatsCollector::record_latency(self, group_id, latency);
    }

    fn record_tx_bytes(&mut self, group_id: usize, _connection_id: usize, bytes: usize) {
        GroupStatsCollector::record_tx_bytes(self, group_id, bytes);
    }

    fn record_rx_bytes(&mut self, group_id: usize, _connection_id: usize, bytes: usize) {
        GroupStatsCollector::record_rx_bytes(self, group_id, bytes);
    }
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
