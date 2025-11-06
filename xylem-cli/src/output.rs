//! Results output formatting

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::time::Duration;
use xylem_core::stats::BasicStats;

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
    pub sample_count: usize,
}

impl ExperimentResults {
    /// Create results from stats collector
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
        println!("  TX Bytes:        {}", self.total_tx_bytes);
        println!("  RX Bytes:        {}", self.total_rx_bytes);
        println!();
        println!("Latency (microseconds):");
        println!("  Samples:         {}", self.latency.sample_count);
        println!("  Min:             {:.2} μs", self.latency.min_us);
        println!("  Mean:            {:.2} μs", self.latency.mean_us);
        println!("  Max:             {:.2} μs", self.latency.max_us);
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
