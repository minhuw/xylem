//! Statistics collection and analysis

use serde::{Deserialize, Serialize};
use std::time::Duration;

pub mod analysis;
pub mod collector;
pub mod sampler;

// Re-export main types
pub use analysis::aggregate_stats;
pub use collector::{BasicStats, StatsCollector};

/// Aggregated statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedStats {
    pub latency_p50: Duration,
    pub latency_p95: Duration,
    pub latency_p99: Duration,
    pub latency_p999: Duration,
    pub mean_latency: Duration,
    pub std_dev: Duration,
    pub confidence_interval: Duration,
    pub throughput_rps: f64,
    pub throughput_mbps: f64,
    pub total_requests: u64,
}
