//! Results output formatting

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::time::Duration;
use xylem_core::stats::{AggregatedStats, BasicStats};

/// Experiment results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentResults {
    pub protocol: String,
    pub target: String,
    pub duration_secs: f64,
    pub total_requests: u64,
    pub total_tx_bytes: u64,
    pub total_rx_bytes: u64,
    pub throughput_rps: f64,
    pub throughput_mbps: f64,
    pub latency: LatencyStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyStats {
    pub min_us: f64,
    pub max_us: f64,
    pub mean_us: f64,
    pub p50_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub p9999_us: f64,
    pub p99999_us: f64,
    pub std_dev_us: f64,
    pub confidence_interval_us: f64,
    pub sample_count: usize,
}

impl ExperimentResults {
    /// Create results from aggregated stats
    pub fn from_aggregated_stats(
        protocol: String,
        target: String,
        duration: Duration,
        stats: AggregatedStats,
    ) -> Self {
        let duration_secs = duration.as_secs_f64();

        Self {
            protocol,
            target,
            duration_secs,
            total_requests: stats.total_requests,
            total_tx_bytes: 0, // Not tracked in AggregatedStats currently
            total_rx_bytes: 0, // Not tracked in AggregatedStats currently
            throughput_rps: stats.throughput_rps,
            throughput_mbps: stats.throughput_mbps,
            latency: LatencyStats {
                min_us: 0.0, // Could be tracked separately if needed
                max_us: 0.0, // Could be tracked separately if needed
                mean_us: stats.mean_latency.as_secs_f64() * 1_000_000.0,
                p50_us: stats.latency_p50.as_secs_f64() * 1_000_000.0,
                p95_us: stats.latency_p95.as_secs_f64() * 1_000_000.0,
                p99_us: stats.latency_p99.as_secs_f64() * 1_000_000.0,
                p999_us: stats.latency_p999.as_secs_f64() * 1_000_000.0,
                p9999_us: stats.latency_p9999.as_secs_f64() * 1_000_000.0,
                p99999_us: stats.latency_p99999.as_secs_f64() * 1_000_000.0,
                std_dev_us: stats.std_dev.as_secs_f64() * 1_000_000.0,
                confidence_interval_us: stats.confidence_interval.as_secs_f64() * 1_000_000.0,
                sample_count: 0, // Not tracked in AggregatedStats currently
            },
        }
    }

    /// Create results from basic stats (legacy)
    #[allow(dead_code)]
    pub fn from_stats(
        protocol: String,
        target: String,
        duration: Duration,
        tx_requests: u64,
        tx_bytes: u64,
        rx_bytes: u64,
        stats: BasicStats,
    ) -> Self {
        let duration_secs = duration.as_secs_f64();
        let throughput_rps = if duration_secs > 0.0 {
            tx_requests as f64 / duration_secs
        } else {
            0.0
        };
        let throughput_mbps = if duration_secs > 0.0 {
            (rx_bytes as f64 / 1_000_000.0) / duration_secs
        } else {
            0.0
        };

        Self {
            protocol,
            target,
            duration_secs,
            total_requests: tx_requests,
            total_tx_bytes: tx_bytes,
            total_rx_bytes: rx_bytes,
            throughput_rps,
            throughput_mbps,
            latency: LatencyStats {
                min_us: stats.min.as_secs_f64() * 1_000_000.0,
                max_us: stats.max.as_secs_f64() * 1_000_000.0,
                mean_us: stats.mean.as_secs_f64() * 1_000_000.0,
                p50_us: 0.0,
                p95_us: 0.0,
                p99_us: 0.0,
                p999_us: 0.0,
                p9999_us: 0.0,
                p99999_us: 0.0,
                std_dev_us: 0.0,
                confidence_interval_us: 0.0,
                sample_count: stats.count,
            },
        }
    }

    /// Print results to stdout in human-readable format
    pub fn print_human(&self) {
        println!("\n{}", "=".repeat(60));
        println!("Xylem Latency Measurement Results");
        println!("{}", "=".repeat(60));
        println!();
        println!("Configuration:");
        println!("  Protocol:        {}", self.protocol);
        println!("  Target:          {}", self.target);
        println!("  Duration:        {:.2}s", self.duration_secs);
        println!();
        println!("Throughput:");
        println!("  Requests:        {} total", self.total_requests);
        println!("  Rate:            {:.2} req/s", self.throughput_rps);
        println!("  Bandwidth:       {:.2} MB/s", self.throughput_mbps);
        if self.total_tx_bytes > 0 || self.total_rx_bytes > 0 {
            println!("  TX Bytes:        {}", self.total_tx_bytes);
            println!("  RX Bytes:        {}", self.total_rx_bytes);
        }
        println!();
        println!("Latency (microseconds):");
        if self.latency.sample_count > 0 {
            println!("  Samples:         {}", self.latency.sample_count);
        }
        if self.latency.min_us > 0.0 {
            println!("  Min:             {:.2} μs", self.latency.min_us);
        }
        println!("  Mean:            {:.2} μs", self.latency.mean_us);
        if self.latency.max_us > 0.0 {
            println!("  Max:             {:.2} μs", self.latency.max_us);
        }
        if self.latency.p50_us > 0.0 {
            println!("  p50:             {:.2} μs", self.latency.p50_us);
            println!("  p95:             {:.2} μs", self.latency.p95_us);
            println!("  p99:             {:.2} μs", self.latency.p99_us);
            println!("  p999:            {:.2} μs", self.latency.p999_us);
        }
        if self.latency.std_dev_us > 0.0 {
            println!("  Std Dev:         {:.2} μs", self.latency.std_dev_us);
        }
        if self.latency.confidence_interval_us > 0.0 {
            println!("  95% CI:          ±{:.2} μs", self.latency.confidence_interval_us);
        }
        println!();
        println!("{}", "=".repeat(60));
    }

    /// Write results to JSON file
    pub fn write_json(&self, path: &str) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        let mut file = File::create(path)?;
        file.write_all(json.as_bytes())?;
        println!("Results written to: {path}");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_experiment_results_creation() {
        let stats = BasicStats {
            count: 100,
            min: Duration::from_micros(50),
            max: Duration::from_micros(500),
            mean: Duration::from_micros(100),
        };

        let results = ExperimentResults::from_stats(
            "redis".to_string(),
            "127.0.0.1:6379".to_string(),
            Duration::from_secs(10),
            1000,
            50000,
            100000,
            stats,
        );

        assert_eq!(results.protocol, "redis");
        assert_eq!(results.total_requests, 1000);
        assert_eq!(results.duration_secs, 10.0);
        assert_eq!(results.throughput_rps, 100.0);
        assert_eq!(results.latency.sample_count, 100);
        assert!((results.latency.min_us - 50.0).abs() < 0.01);
        assert!((results.latency.mean_us - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_json_serialization() {
        let stats = BasicStats {
            count: 10,
            min: Duration::from_micros(10),
            max: Duration::from_micros(100),
            mean: Duration::from_micros(50),
        };

        let results = ExperimentResults::from_stats(
            "echo".to_string(),
            "127.0.0.1:9999".to_string(),
            Duration::from_secs(1),
            100,
            1000,
            1000,
            stats,
        );

        let json = serde_json::to_string(&results).unwrap();
        assert!(json.contains("\"protocol\":\"echo\""));
        assert!(json.contains("\"total_requests\":100"));
    }
}
