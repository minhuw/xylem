//! Statistical analysis (percentiles, CI)

use std::time::Duration;

use statrs::distribution::{ContinuousCDF, StudentsT};

use super::{AggregatedStats, GroupStatsCollector, SamplingMode, StatsCollector};
use std::collections::HashMap;

/// Calculate percentile from sorted samples
///
/// Uses linear interpolation between samples for more accurate results.
/// percentile should be between 0.0 and 1.0 (e.g., 0.95 for p95)
fn calculate_percentile(sorted_samples: &[Duration], percentile: f64) -> Duration {
    if sorted_samples.is_empty() {
        return Duration::ZERO;
    }

    if percentile <= 0.0 {
        return sorted_samples[0];
    }

    if percentile >= 1.0 {
        return *sorted_samples.last().unwrap();
    }

    let n = sorted_samples.len();
    let rank = percentile * (n - 1) as f64;
    let lower_idx = rank.floor() as usize;
    let upper_idx = rank.ceil() as usize;

    if lower_idx == upper_idx {
        return sorted_samples[lower_idx];
    }

    // Linear interpolation
    let lower = sorted_samples[lower_idx];
    let upper = sorted_samples[upper_idx];
    let fraction = rank - lower_idx as f64;

    let lower_nanos = lower.as_nanos() as f64;
    let upper_nanos = upper.as_nanos() as f64;
    let interpolated = lower_nanos + (upper_nanos - lower_nanos) * fraction;

    Duration::from_nanos(interpolated as u64)
}

/// Calculate standard deviation from samples
fn calculate_std_dev(samples: &[Duration], mean: Duration) -> Duration {
    if samples.len() <= 1 {
        return Duration::ZERO;
    }

    let mean_nanos = mean.as_nanos() as f64;
    let variance: f64 = samples
        .iter()
        .map(|&sample| {
            let diff = sample.as_nanos() as f64 - mean_nanos;
            diff * diff
        })
        .sum::<f64>()
        / (samples.len() - 1) as f64; // Use sample variance (n-1)

    Duration::from_nanos(variance.sqrt() as u64)
}

/// Calculate confidence interval using Student's t-distribution
///
/// Returns the half-width of the confidence interval for the given confidence level.
/// For example, if mean = 100ms and ci = 5ms at 95% confidence, the true mean
/// is estimated to be in [95ms, 105ms] with 95% confidence.
fn calculate_confidence_interval(
    samples: &[Duration],
    _mean: Duration,
    std_dev: Duration,
    confidence_level: f64,
) -> Duration {
    if samples.len() <= 1 {
        return Duration::ZERO;
    }

    let n = samples.len() as f64;
    let degrees_of_freedom = n - 1.0;

    // Create Student's t-distribution
    let t_dist = match StudentsT::new(0.0, 1.0, degrees_of_freedom) {
        Ok(dist) => dist,
        Err(_) => return Duration::ZERO, // Invalid degrees of freedom
    };

    // Calculate t-value for the given confidence level
    // For 95% confidence, we want the 97.5th percentile (two-tailed test)
    let alpha = 1.0 - confidence_level;
    let t_critical = match t_dist.inverse_cdf(1.0 - alpha / 2.0) {
        val if val.is_finite() => val,
        _ => return Duration::ZERO,
    };

    // Calculate standard error
    let std_dev_nanos = std_dev.as_nanos() as f64;
    let standard_error = std_dev_nanos / n.sqrt();

    // Calculate margin of error
    let margin_of_error = t_critical * standard_error;

    Duration::from_nanos(margin_of_error.abs() as u64)
}

/// Aggregate statistics from a collector
///
/// Calculates all statistics including percentiles, mean, std dev, and confidence intervals.
pub fn aggregate_stats(
    collector: &StatsCollector,
    duration: Duration,
    confidence_level: f64,
) -> AggregatedStats {
    // If latency sampling is disabled, return zeros for latency fields but keep throughput
    if matches!(collector.sampling_mode(), SamplingMode::None) {
        let duration_secs = duration.as_secs_f64();
        let total_requests = collector.tx_requests();
        let throughput_rps = if duration_secs > 0.0 {
            total_requests as f64 / duration_secs
        } else {
            0.0
        };

        let total_bytes = (collector.tx_bytes() + collector.rx_bytes()) as f64;
        let throughput_mbps = if duration_secs > 0.0 {
            (total_bytes * 8.0) / (duration_secs * 1_000_000.0)
        } else {
            0.0
        };

        return AggregatedStats {
            latency_p50: Duration::ZERO,
            latency_p95: Duration::ZERO,
            latency_p99: Duration::ZERO,
            latency_p999: Duration::ZERO,
            latency_p9999: Duration::ZERO,
            latency_p99999: Duration::ZERO,
            mean_latency: Duration::ZERO,
            std_dev: Duration::ZERO,
            confidence_interval: Duration::ZERO,
            throughput_rps,
            throughput_mbps,
            total_requests,
        };
    }
    // Check if we're using DDSketch mode
    if let Some(sketch) = collector.ddsketch() {
        // Use DDSketch for percentile calculations
        // quantile returns Result<Option<f64>, DDSketchError>
        let p50 = sketch
            .quantile(0.50)
            .ok()
            .flatten()
            .map(|q| Duration::from_nanos(q as u64))
            .unwrap_or(Duration::ZERO);
        let p95 = sketch
            .quantile(0.95)
            .ok()
            .flatten()
            .map(|q| Duration::from_nanos(q as u64))
            .unwrap_or(Duration::ZERO);
        let p99 = sketch
            .quantile(0.99)
            .ok()
            .flatten()
            .map(|q| Duration::from_nanos(q as u64))
            .unwrap_or(Duration::ZERO);
        let p999 = sketch
            .quantile(0.999)
            .ok()
            .flatten()
            .map(|q| Duration::from_nanos(q as u64))
            .unwrap_or(Duration::ZERO);
        let p9999 = sketch
            .quantile(0.9999)
            .ok()
            .flatten()
            .map(|q| Duration::from_nanos(q as u64))
            .unwrap_or(Duration::ZERO);
        let p99999 = sketch
            .quantile(0.99999)
            .ok()
            .flatten()
            .map(|q| Duration::from_nanos(q as u64))
            .unwrap_or(Duration::ZERO);

        // For DDSketch, we don't have exact mean/stddev/CI
        // We approximate using percentiles
        let mean = p50; // Approximate mean with median
        let std_dev = Duration::ZERO; // Not available in sketch mode
        let ci = Duration::ZERO; // Not available in sketch mode

        // Calculate throughput
        let duration_secs = duration.as_secs_f64();
        let total_requests = collector.tx_requests();
        let throughput_rps = if duration_secs > 0.0 {
            total_requests as f64 / duration_secs
        } else {
            0.0
        };

        let total_bytes = (collector.tx_bytes() + collector.rx_bytes()) as f64;
        let throughput_mbps = if duration_secs > 0.0 {
            (total_bytes * 8.0) / (duration_secs * 1_000_000.0)
        } else {
            0.0
        };

        return AggregatedStats {
            latency_p50: p50,
            latency_p95: p95,
            latency_p99: p99,
            latency_p999: p999,
            latency_p9999: p9999,
            latency_p99999: p99999,
            mean_latency: mean,
            std_dev,
            confidence_interval: ci,
            throughput_rps,
            throughput_mbps,
            total_requests,
        };
    }

    // Check if we're using T-Digest mode
    if let Some(tdigest_mutex) = collector.tdigest() {
        let tdigest = tdigest_mutex.lock().unwrap();

        // Use T-Digest for percentile calculations
        let p50 = tdigest.estimate_quantile(0.50);
        let p95 = tdigest.estimate_quantile(0.95);
        let p99 = tdigest.estimate_quantile(0.99);
        let p999 = tdigest.estimate_quantile(0.999);
        let p9999 = tdigest.estimate_quantile(0.9999);
        let p99999 = tdigest.estimate_quantile(0.99999);

        let p50_dur = Duration::from_nanos(p50 as u64);
        let p95_dur = Duration::from_nanos(p95 as u64);
        let p99_dur = Duration::from_nanos(p99 as u64);
        let p999_dur = Duration::from_nanos(p999 as u64);
        let p9999_dur = Duration::from_nanos(p9999 as u64);
        let p99999_dur = Duration::from_nanos(p99999 as u64);

        // For T-Digest, we don't have exact mean/stddev/CI
        let mean = p50_dur; // Approximate mean with median
        let std_dev = Duration::ZERO;
        let ci = Duration::ZERO;

        // Calculate throughput
        let duration_secs = duration.as_secs_f64();
        let total_requests = collector.tx_requests();
        let throughput_rps = if duration_secs > 0.0 {
            total_requests as f64 / duration_secs
        } else {
            0.0
        };

        let total_bytes = (collector.tx_bytes() + collector.rx_bytes()) as f64;
        let throughput_mbps = if duration_secs > 0.0 {
            (total_bytes * 8.0) / (duration_secs * 1_000_000.0)
        } else {
            0.0
        };

        return AggregatedStats {
            latency_p50: p50_dur,
            latency_p95: p95_dur,
            latency_p99: p99_dur,
            latency_p999: p999_dur,
            latency_p9999: p9999_dur,
            latency_p99999: p99999_dur,
            mean_latency: mean,
            std_dev,
            confidence_interval: ci,
            throughput_rps,
            throughput_mbps,
            total_requests,
        };
    }

    // Check if we're using HDR Histogram mode
    if let Some(hdr_mutex) = collector.hdrhistogram() {
        let hdr = hdr_mutex.lock().unwrap();

        // Use HDR Histogram for percentile calculations
        let p50 = hdr.value_at_quantile(0.50);
        let p95 = hdr.value_at_quantile(0.95);
        let p99 = hdr.value_at_quantile(0.99);
        let p999 = hdr.value_at_quantile(0.999);
        let p9999 = hdr.value_at_quantile(0.9999);
        let p99999 = hdr.value_at_quantile(0.99999);

        let p50_dur = Duration::from_nanos(p50);
        let p95_dur = Duration::from_nanos(p95);
        let p99_dur = Duration::from_nanos(p99);
        let p999_dur = Duration::from_nanos(p999);
        let p9999_dur = Duration::from_nanos(p9999);
        let p99999_dur = Duration::from_nanos(p99999);

        // For HDR Histogram, we can calculate mean
        let mean = Duration::from_nanos(hdr.mean() as u64);
        let std_dev = Duration::from_nanos(hdr.stdev() as u64);
        let ci = Duration::ZERO; // CI not directly available

        // Calculate throughput
        let duration_secs = duration.as_secs_f64();
        let total_requests = collector.tx_requests();
        let throughput_rps = if duration_secs > 0.0 {
            total_requests as f64 / duration_secs
        } else {
            0.0
        };

        let total_bytes = (collector.tx_bytes() + collector.rx_bytes()) as f64;
        let throughput_mbps = if duration_secs > 0.0 {
            (total_bytes * 8.0) / (duration_secs * 1_000_000.0)
        } else {
            0.0
        };

        return AggregatedStats {
            latency_p50: p50_dur,
            latency_p95: p95_dur,
            latency_p99: p99_dur,
            latency_p999: p999_dur,
            latency_p9999: p9999_dur,
            latency_p99999: p99999_dur,
            mean_latency: mean,
            std_dev,
            confidence_interval: ci,
            throughput_rps,
            throughput_mbps,
            total_requests,
        };
    }

    // Sample-based mode
    let samples = collector.samples();

    if samples.is_empty() {
        return AggregatedStats {
            latency_p50: Duration::ZERO,
            latency_p95: Duration::ZERO,
            latency_p99: Duration::ZERO,
            latency_p999: Duration::ZERO,
            latency_p9999: Duration::ZERO,
            latency_p99999: Duration::ZERO,
            mean_latency: Duration::ZERO,
            std_dev: Duration::ZERO,
            confidence_interval: Duration::ZERO,
            throughput_rps: 0.0,
            throughput_mbps: 0.0,
            total_requests: 0,
        };
    }

    // Sort samples for percentile calculation
    let mut sorted = samples.to_vec();
    sorted.sort();

    // Calculate percentiles
    let p50 = calculate_percentile(&sorted, 0.50);
    let p95 = calculate_percentile(&sorted, 0.95);
    let p99 = calculate_percentile(&sorted, 0.99);
    let p999 = calculate_percentile(&sorted, 0.999);
    let p9999 = calculate_percentile(&sorted, 0.9999);
    let p99999 = calculate_percentile(&sorted, 0.99999);

    // Calculate mean
    let sum: Duration = sorted.iter().sum();
    let mean = sum / sorted.len() as u32;

    // Calculate standard deviation
    let std_dev = calculate_std_dev(&sorted, mean);

    // Calculate confidence interval (default 95% confidence)
    let ci = calculate_confidence_interval(&sorted, mean, std_dev, confidence_level);

    // Calculate throughput
    let duration_secs = duration.as_secs_f64();
    let total_requests = collector.tx_requests();
    let throughput_rps = if duration_secs > 0.0 {
        total_requests as f64 / duration_secs
    } else {
        0.0
    };

    let total_bytes = (collector.tx_bytes() + collector.rx_bytes()) as f64;
    let throughput_mbps = if duration_secs > 0.0 {
        (total_bytes * 8.0) / (duration_secs * 1_000_000.0)
    } else {
        0.0
    };

    AggregatedStats {
        latency_p50: p50,
        latency_p95: p95,
        latency_p99: p99,
        latency_p999: p999,
        latency_p9999: p9999,
        latency_p99999: p99999,
        mean_latency: mean,
        std_dev,
        confidence_interval: ci,
        throughput_rps,
        throughput_mbps,
        total_requests,
    }
}

/// Aggregate statistics for all groups individually
///
/// Returns a map of group_id -> AggregatedStats
pub fn aggregate_stats_per_group(
    group_collector: &GroupStatsCollector,
    duration: Duration,
    confidence_level: f64,
) -> HashMap<usize, AggregatedStats> {
    let mut per_group_stats = HashMap::new();

    for group_id in group_collector.group_ids() {
        if let Some(collector) = group_collector.get_group(group_id) {
            let stats = aggregate_stats(collector, duration, confidence_level);
            per_group_stats.insert(group_id, stats);
        }
    }

    per_group_stats
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_percentile_empty() {
        let samples: Vec<Duration> = vec![];
        assert_eq!(calculate_percentile(&samples, 0.5), Duration::ZERO);
    }

    #[test]
    fn test_calculate_percentile_single() {
        let samples = vec![Duration::from_millis(10)];
        assert_eq!(calculate_percentile(&samples, 0.5), Duration::from_millis(10));
        assert_eq!(calculate_percentile(&samples, 0.95), Duration::from_millis(10));
    }

    #[test]
    fn test_calculate_percentile_sorted() {
        let samples: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();

        // p50 should be around 50ms
        let p50 = calculate_percentile(&samples, 0.5);
        assert!((p50.as_millis() as i64 - 50).abs() <= 1);

        // p95 should be around 95ms
        let p95 = calculate_percentile(&samples, 0.95);
        assert!((p95.as_millis() as i64 - 95).abs() <= 1);

        // p99 should be around 99ms
        let p99 = calculate_percentile(&samples, 0.99);
        assert!((p99.as_millis() as i64 - 99).abs() <= 1);
    }

    #[test]
    fn test_calculate_percentile_boundaries() {
        let samples: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();

        // p0 should be minimum
        let p0 = calculate_percentile(&samples, 0.0);
        assert_eq!(p0, Duration::from_millis(1));

        // p100 should be maximum
        let p100 = calculate_percentile(&samples, 1.0);
        assert_eq!(p100, Duration::from_millis(100));
    }

    #[test]
    fn test_calculate_std_dev_empty() {
        let samples: Vec<Duration> = vec![];
        let mean = Duration::ZERO;
        assert_eq!(calculate_std_dev(&samples, mean), Duration::ZERO);
    }

    #[test]
    fn test_calculate_std_dev_single() {
        let samples = vec![Duration::from_millis(10)];
        let mean = Duration::from_millis(10);
        assert_eq!(calculate_std_dev(&samples, mean), Duration::ZERO);
    }

    #[test]
    fn test_calculate_std_dev_constant() {
        let samples = vec![Duration::from_millis(10); 100];
        let mean = Duration::from_millis(10);
        let std_dev = calculate_std_dev(&samples, mean);
        // Standard deviation of constant values should be ~0
        assert!(std_dev.as_nanos() < 1000);
    }

    #[test]
    fn test_calculate_std_dev_known() {
        // Sample with known std dev: [0, 10, 20, 30, 40]
        // Mean = 20, Std Dev ≈ 15.81
        let samples: Vec<Duration> =
            vec![0, 10, 20, 30, 40].into_iter().map(Duration::from_millis).collect();
        let mean = Duration::from_millis(20);
        let std_dev = calculate_std_dev(&samples, mean);

        // Check if std_dev is approximately 15.81ms
        let std_dev_ms = std_dev.as_millis();
        assert!((std_dev_ms as i64 - 16).abs() <= 1); // Allow ±1ms tolerance
    }

    #[test]
    fn test_calculate_confidence_interval_empty() {
        let samples: Vec<Duration> = vec![];
        let mean = Duration::ZERO;
        let std_dev = Duration::ZERO;
        assert_eq!(calculate_confidence_interval(&samples, mean, std_dev, 0.95), Duration::ZERO);
    }

    #[test]
    fn test_calculate_confidence_interval_single() {
        let samples = vec![Duration::from_millis(10)];
        let mean = Duration::from_millis(10);
        let std_dev = Duration::ZERO;
        assert_eq!(calculate_confidence_interval(&samples, mean, std_dev, 0.95), Duration::ZERO);
    }

    #[test]
    fn test_calculate_confidence_interval_larger_for_higher_stddev() {
        let samples_low: Vec<Duration> =
            vec![10, 11, 12, 13, 14].into_iter().map(Duration::from_millis).collect();
        let mean_low = Duration::from_millis(12);
        let std_dev_low = calculate_std_dev(&samples_low, mean_low);
        let ci_low = calculate_confidence_interval(&samples_low, mean_low, std_dev_low, 0.95);

        let samples_high: Vec<Duration> =
            vec![0, 10, 20, 30, 40].into_iter().map(Duration::from_millis).collect();
        let mean_high = Duration::from_millis(20);
        let std_dev_high = calculate_std_dev(&samples_high, mean_high);
        let ci_high = calculate_confidence_interval(&samples_high, mean_high, std_dev_high, 0.95);

        // CI should be larger for higher standard deviation
        assert!(ci_high > ci_low);
    }

    #[test]
    fn test_aggregate_stats_empty() {
        let collector = StatsCollector::new(100, 1.0);
        let stats = aggregate_stats(&collector, Duration::from_secs(1), 0.95);

        assert_eq!(stats.latency_p50, Duration::ZERO);
        assert_eq!(stats.latency_p95, Duration::ZERO);
        assert_eq!(stats.latency_p99, Duration::ZERO);
        assert_eq!(stats.latency_p999, Duration::ZERO);
        assert_eq!(stats.mean_latency, Duration::ZERO);
        assert_eq!(stats.std_dev, Duration::ZERO);
        assert_eq!(stats.confidence_interval, Duration::ZERO);
        assert_eq!(stats.throughput_rps, 0.0);
        assert_eq!(stats.throughput_mbps, 0.0);
        assert_eq!(stats.total_requests, 0);
    }

    #[test]
    fn test_aggregate_stats_with_samples() {
        let mut collector = StatsCollector::new(100, 1.0);

        // Add 100 samples: 1ms to 100ms
        for i in 1..=100 {
            collector.record_latency(Duration::from_millis(i));
            collector.record_tx_bytes(100);
            collector.record_rx_bytes(100);
        }

        let stats = aggregate_stats(&collector, Duration::from_secs(1), 0.95);

        // p50 should be around 50ms
        assert!((stats.latency_p50.as_millis() as i64 - 50).abs() <= 2);

        // p95 should be around 95ms
        assert!((stats.latency_p95.as_millis() as i64 - 95).abs() <= 2);

        // p99 should be around 99ms
        assert!((stats.latency_p99.as_millis() as i64 - 99).abs() <= 2);

        // Mean should be around 50.5ms
        assert!((stats.mean_latency.as_millis() as i64 - 50).abs() <= 2);

        // Standard deviation should be around 29ms (for uniform 1-100)
        assert!((stats.std_dev.as_millis() as i64 - 29).abs() <= 3);

        // CI should be non-zero
        assert!(stats.confidence_interval > Duration::ZERO);

        // Throughput should be 100 requests/sec
        assert!((stats.throughput_rps - 100.0).abs() < 0.1);

        // Total requests
        assert_eq!(stats.total_requests, 100);
    }

    #[test]
    fn test_aggregate_stats_throughput() {
        let mut collector = StatsCollector::new(100, 1.0);

        // 1000 requests in 10 seconds = 100 req/s
        for _ in 0..1000 {
            collector.record_latency(Duration::from_millis(10));
            collector.record_tx_bytes(100); // 100 bytes per request
            collector.record_rx_bytes(100);
        }

        let stats = aggregate_stats(&collector, Duration::from_secs(10), 0.95);

        // Throughput should be 100 requests/sec
        assert!((stats.throughput_rps - 100.0).abs() < 0.1);

        // Total bytes: 1000 * (100 + 100) = 200,000 bytes
        // Mbps = (200,000 * 8) / (10 * 1,000,000) = 0.16 Mbps
        assert!((stats.throughput_mbps - 0.16).abs() < 0.01);
    }
}
