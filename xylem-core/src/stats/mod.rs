//! Statistics collection and analysis

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Statistics collector (thread-local)
#[allow(dead_code)]
pub struct StatsCollector {
    samples: Vec<Duration>,
    max_samples: usize,
    sampling_rate: f64,
    tx_bytes: u64,
    rx_bytes: u64,
    tx_requests: u64,
    rx_requests: u64,
}

impl StatsCollector {
    pub fn new(max_samples: usize, sampling_rate: f64) -> Self {
        Self {
            samples: Vec::with_capacity(max_samples),
            max_samples,
            sampling_rate,
            tx_bytes: 0,
            rx_bytes: 0,
            tx_requests: 0,
            rx_requests: 0,
        }
    }

    pub fn record_sample(&mut self, latency: Duration) {
        if self.samples.len() < self.max_samples {
            self.samples.push(latency);
        }
    }

    pub fn record_request(&mut self, tx_bytes: usize, rx_bytes: usize) {
        self.tx_bytes += tx_bytes as u64;
        self.rx_bytes += rx_bytes as u64;
        self.tx_requests += 1;
        self.rx_requests += 1;
    }
}

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

pub mod analysis;
pub mod collector;
pub mod sampler;
