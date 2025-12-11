//! Tuple-keyed statistics collection with on-demand aggregation
//!
//! Stores statistics keyed by `(time_bucket, group_id, connection_id)` tuple,
//! enabling flexible post-hoc aggregation by any dimension.

use super::collector::SamplingPolicy;
use super::AggregatedStats;
use hdrhistogram::Histogram;
use sketches_ddsketch::{Config as DDSketchConfig, DDSketch};
use std::collections::HashMap;
use std::time::Duration;
use tdigest::TDigest;

/// Composite key for statistics entries
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct StatsKey {
    /// Time bucket (seconds since experiment start)
    pub time_bucket: u64,
    /// Traffic group ID
    pub group_id: usize,
    /// Connection ID within the group
    pub connection_id: usize,
}

/// Latency storage variants based on sampling policy
pub enum LatencyStorage {
    /// Latency sampling disabled
    None,
    /// Sample-based storage (Limited/Unlimited modes)
    Samples {
        samples: Vec<Duration>,
        max_samples: usize,
        sampling_rate: f64,
    },
    /// DDSketch for memory-efficient quantile estimation
    DdSketch(DDSketch),
    /// T-Digest for accurate tail percentiles
    TDigest(TDigest),
    /// HDR Histogram for high accuracy
    HdrHistogram(Histogram<u64>),
}

impl LatencyStorage {
    /// Create new storage based on sampling policy
    pub fn from_policy(policy: &SamplingPolicy) -> Self {
        match policy {
            SamplingPolicy::None => LatencyStorage::None,
            SamplingPolicy::Limited { max_samples, rate } => LatencyStorage::Samples {
                samples: Vec::with_capacity((*max_samples).min(1024)),
                max_samples: *max_samples,
                sampling_rate: *rate,
            },
            SamplingPolicy::Unlimited => LatencyStorage::Samples {
                samples: Vec::new(),
                max_samples: usize::MAX,
                sampling_rate: 1.0,
            },
            SamplingPolicy::Adaptive { max_samples, initial_rate, .. } => LatencyStorage::Samples {
                samples: Vec::with_capacity((*max_samples).min(1024)),
                max_samples: *max_samples,
                sampling_rate: *initial_rate,
            },
            SamplingPolicy::DdSketch { alpha, max_bins } => {
                let config = DDSketchConfig::new(*alpha, *max_bins, 1.0);
                LatencyStorage::DdSketch(DDSketch::new(config))
            }
            SamplingPolicy::TDigest { compression } => {
                LatencyStorage::TDigest(TDigest::new_with_size(*compression))
            }
            SamplingPolicy::HdrHistogram { sigfigs, max_value_us } => {
                let max_value_ns = max_value_us * 1000;
                let histogram = Histogram::<u64>::new_with_max(max_value_ns, *sigfigs)
                    .expect("Failed to create HDR histogram");
                LatencyStorage::HdrHistogram(histogram)
            }
        }
    }

    /// Record a latency sample
    pub fn record(&mut self, latency: Duration) {
        match self {
            LatencyStorage::None => {
                // Skip latency storage
            }
            LatencyStorage::Samples { samples, max_samples, sampling_rate } => {
                if samples.len() < *max_samples
                    && (*sampling_rate >= 1.0 || rand::random::<f64>() < *sampling_rate)
                {
                    samples.push(latency);
                }
            }
            LatencyStorage::DdSketch(sketch) => {
                sketch.add(latency.as_nanos() as f64);
            }
            LatencyStorage::TDigest(digest) => {
                *digest = digest.merge_unsorted(vec![latency.as_nanos() as f64]);
            }
            LatencyStorage::HdrHistogram(hist) => {
                let _ = hist.record(latency.as_nanos() as u64);
            }
        }
    }

    /// Merge another storage into this one (consumes other)
    pub fn merge(&mut self, other: Self) {
        match (self, other) {
            (LatencyStorage::None, _) => {
                // Remain none; ignore other
            }
            (_, LatencyStorage::None) => {
                // Nothing to merge
            }
            (
                LatencyStorage::Samples { samples, max_samples, .. },
                LatencyStorage::Samples { samples: other_samples, .. },
            ) => {
                for sample in other_samples {
                    if samples.len() < *max_samples {
                        samples.push(sample);
                    }
                }
            }
            (LatencyStorage::DdSketch(sketch), LatencyStorage::DdSketch(other)) => {
                let _ = sketch.merge(&other);
            }
            (LatencyStorage::TDigest(digest), LatencyStorage::TDigest(other)) => {
                *digest = TDigest::merge_digests(vec![digest.clone(), other]);
            }
            (LatencyStorage::HdrHistogram(hist), LatencyStorage::HdrHistogram(other)) => {
                let _ = hist.add(&other);
            }
            _ => {
                // Mismatched types - this shouldn't happen in practice
                tracing::warn!("Attempted to merge incompatible latency storage types");
            }
        }
    }

    /// Merge from a reference (for aggregation without consuming)
    pub fn merge_ref(&mut self, other: &Self) {
        match (self, other) {
            (LatencyStorage::None, _) => {
                // Remain none
            }
            (_, LatencyStorage::None) => {
                // Nothing to merge
            }
            (
                LatencyStorage::Samples { samples, max_samples, .. },
                LatencyStorage::Samples { samples: other_samples, .. },
            ) => {
                for sample in other_samples {
                    if samples.len() < *max_samples {
                        samples.push(*sample);
                    }
                }
            }
            (LatencyStorage::DdSketch(sketch), LatencyStorage::DdSketch(other)) => {
                let _ = sketch.merge(other);
            }
            (LatencyStorage::TDigest(digest), LatencyStorage::TDigest(other)) => {
                *digest = TDigest::merge_digests(vec![digest.clone(), other.clone()]);
            }
            (LatencyStorage::HdrHistogram(hist), LatencyStorage::HdrHistogram(other)) => {
                let _ = hist.add(other);
            }
            _ => {
                tracing::warn!("Attempted to merge incompatible latency storage types");
            }
        }
    }

    /// Get sample count
    pub fn count(&self) -> u64 {
        match self {
            LatencyStorage::None => 0,
            LatencyStorage::Samples { samples, .. } => samples.len() as u64,
            LatencyStorage::DdSketch(sketch) => sketch.count() as u64,
            LatencyStorage::TDigest(digest) => digest.count() as u64,
            LatencyStorage::HdrHistogram(hist) => hist.len(),
        }
    }

    /// Calculate percentile (returns nanoseconds)
    pub fn percentile(&self, p: f64) -> u64 {
        match self {
            LatencyStorage::None => 0,
            LatencyStorage::Samples { samples, .. } => {
                if samples.is_empty() {
                    return 0;
                }
                let mut sorted = samples.clone();
                sorted.sort();
                let idx = ((p * (sorted.len() - 1) as f64).round() as usize).min(sorted.len() - 1);
                sorted[idx].as_nanos() as u64
            }
            LatencyStorage::DdSketch(sketch) => {
                sketch.quantile(p).ok().flatten().map(|v| v as u64).unwrap_or(0)
            }
            LatencyStorage::TDigest(digest) => digest.estimate_quantile(p) as u64,
            LatencyStorage::HdrHistogram(hist) => hist.value_at_quantile(p),
        }
    }

    /// Calculate mean (returns nanoseconds)
    pub fn mean(&self) -> u64 {
        match self {
            LatencyStorage::None => 0,
            LatencyStorage::Samples { samples, .. } => {
                if samples.is_empty() {
                    return 0;
                }
                let sum: u128 = samples.iter().map(|d| d.as_nanos()).sum();
                (sum / samples.len() as u128) as u64
            }
            LatencyStorage::DdSketch(sketch) => {
                // DDSketch doesn't have mean, approximate with median
                sketch.quantile(0.5).ok().flatten().map(|v| v as u64).unwrap_or(0)
            }
            LatencyStorage::TDigest(digest) => digest.mean() as u64,
            LatencyStorage::HdrHistogram(hist) => hist.mean() as u64,
        }
    }

    /// Get min value (returns nanoseconds)
    pub fn min(&self) -> u64 {
        match self {
            LatencyStorage::None => 0,
            LatencyStorage::Samples { samples, .. } => {
                samples.iter().min().map(|d| d.as_nanos() as u64).unwrap_or(0)
            }
            LatencyStorage::DdSketch(sketch) => sketch.min().map(|v| v as u64).unwrap_or(0),
            LatencyStorage::TDigest(digest) => digest.min() as u64,
            LatencyStorage::HdrHistogram(hist) => hist.min(),
        }
    }

    /// Get max value (returns nanoseconds)
    pub fn max(&self) -> u64 {
        match self {
            LatencyStorage::None => 0,
            LatencyStorage::Samples { samples, .. } => {
                samples.iter().max().map(|d| d.as_nanos() as u64).unwrap_or(0)
            }
            LatencyStorage::DdSketch(sketch) => sketch.max().map(|v| v as u64).unwrap_or(0),
            LatencyStorage::TDigest(digest) => digest.max() as u64,
            LatencyStorage::HdrHistogram(hist) => hist.max(),
        }
    }
}

/// Statistics entry for a single (time, group, connection) tuple
pub struct StatsEntry {
    /// Total request count in this bucket
    pub request_count: u64,
    /// Total transmitted bytes
    pub tx_bytes: u64,
    /// Total received bytes
    pub rx_bytes: u64,
    /// Latency storage
    pub latency: LatencyStorage,
}

impl StatsEntry {
    /// Create a new stats entry with the given sampling policy
    pub fn new(policy: &SamplingPolicy) -> Self {
        Self {
            request_count: 0,
            tx_bytes: 0,
            rx_bytes: 0,
            latency: LatencyStorage::from_policy(policy),
        }
    }

    /// Record a latency sample
    pub fn record_latency(&mut self, latency: Duration) {
        self.request_count += 1;
        self.latency.record(latency);
    }

    /// Record transmitted bytes
    pub fn record_tx_bytes(&mut self, bytes: usize) {
        self.tx_bytes += bytes as u64;
    }

    /// Record received bytes
    pub fn record_rx_bytes(&mut self, bytes: usize) {
        self.rx_bytes += bytes as u64;
    }

    /// Merge another entry into this one
    pub fn merge(&mut self, other: Self) {
        self.request_count += other.request_count;
        self.tx_bytes += other.tx_bytes;
        self.rx_bytes += other.rx_bytes;
        self.latency.merge(other.latency);
    }
}

/// Time series data point
#[derive(Debug, Clone)]
pub struct TimeSeriesPoint {
    /// Time bucket (seconds since experiment start)
    pub time_bucket: u64,
    /// Request count in this bucket
    pub request_count: u64,
    /// Throughput in requests per second
    pub throughput_rps: f64,
    /// Throughput in Mbps
    pub throughput_mbps: f64,
    /// p50 latency in nanoseconds
    pub latency_p50_ns: u64,
    /// p99 latency in nanoseconds
    pub latency_p99_ns: u64,
    /// p999 latency in nanoseconds
    pub latency_p999_ns: u64,
    /// Mean latency in nanoseconds
    pub latency_mean_ns: u64,
}

/// Per-connection statistics summary
#[derive(Debug, Clone)]
pub struct ConnectionStats {
    /// Connection ID
    pub connection_id: usize,
    /// Group ID this connection belongs to
    pub group_id: usize,
    /// Total request count
    pub request_count: u64,
    /// Total transmitted bytes
    pub tx_bytes: u64,
    /// Total received bytes
    pub rx_bytes: u64,
    /// Aggregated latency statistics
    pub latency: AggregatedStats,
}

/// Tuple-keyed statistics collector
///
/// Stores statistics keyed by (time_bucket, group_id, connection_id) for flexible
/// on-demand aggregation by any dimension.
///
/// Supports two modes:
/// - **In-memory**: All entries kept in memory (default)
/// - **Streaming**: Old buckets flushed to Parquet files for bounded memory
pub struct TupleStatsCollector {
    /// Statistics entries keyed by (time_bucket, group_id, connection_id)
    entries: HashMap<StatsKey, StatsEntry>,
    /// Global sampling policy for latency storage (used for global aggregation)
    sampling_policy: SamplingPolicy,
    /// Per-group sampling policies (used for per-group aggregation)
    /// If a group is not in this map, falls back to global sampling_policy
    group_sampling_policies: HashMap<usize, SamplingPolicy>,
    /// Time bucket duration in nanoseconds
    bucket_duration_ns: u64,
    /// Experiment start time in nanoseconds
    experiment_start_ns: u64,
    /// Current time bucket (for optimization)
    current_bucket: u64,
    /// Optional streaming dumper for bounded-memory operation
    dumper: Option<super::dumper::StatsDumper>,
    /// Number of buckets to retain in memory when streaming
    retention_buckets: u64,
}

impl TupleStatsCollector {
    /// Create a new tuple stats collector (in-memory mode)
    pub fn new(sampling_policy: SamplingPolicy, bucket_duration: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            sampling_policy,
            group_sampling_policies: HashMap::new(),
            bucket_duration_ns: bucket_duration.as_nanos() as u64,
            experiment_start_ns: crate::timing::time_ns(),
            current_bucket: 0,
            dumper: None,
            retention_buckets: u64::MAX, // Keep all buckets in memory
        }
    }

    /// Create with a specific start time (for testing or synchronized starts)
    pub fn with_start_time(
        sampling_policy: SamplingPolicy,
        bucket_duration: Duration,
        start_time_ns: u64,
    ) -> Self {
        Self {
            entries: HashMap::new(),
            sampling_policy,
            group_sampling_policies: HashMap::new(),
            bucket_duration_ns: bucket_duration.as_nanos() as u64,
            experiment_start_ns: start_time_ns,
            current_bucket: 0,
            dumper: None,
            retention_buckets: u64::MAX,
        }
    }

    /// Create with streaming to Parquet file (bounded memory mode)
    ///
    /// # Arguments
    /// * `sampling_policy` - Policy for latency sampling
    /// * `bucket_duration` - Duration of each time bucket
    /// * `start_time_ns` - Experiment start time in nanoseconds
    /// * `dumper` - Parquet writer for streaming old buckets
    /// * `retention_buckets` - Number of buckets to keep in memory (default: 2)
    pub fn with_streaming(
        sampling_policy: SamplingPolicy,
        bucket_duration: Duration,
        start_time_ns: u64,
        dumper: super::dumper::StatsDumper,
        retention_buckets: usize,
    ) -> Self {
        Self {
            entries: HashMap::new(),
            sampling_policy,
            group_sampling_policies: HashMap::new(),
            bucket_duration_ns: bucket_duration.as_nanos() as u64,
            experiment_start_ns: start_time_ns,
            current_bucket: 0,
            dumper: Some(dumper),
            retention_buckets: retention_buckets as u64,
        }
    }

    /// Register a per-group sampling policy
    ///
    /// This allows different traffic groups to have independent sampling policies.
    /// When aggregating per-group stats, the registered policy for each group is used.
    pub fn register_group_policy(&mut self, group_id: usize, policy: SamplingPolicy) {
        self.group_sampling_policies.insert(group_id, policy);
    }

    /// Get the sampling policy for a specific group
    ///
    /// Panics if the group was not registered (this is a programmer error)
    fn get_group_policy(&self, group_id: usize) -> &SamplingPolicy {
        self.group_sampling_policies
            .get(&group_id)
            .unwrap_or_else(|| panic!("Group {} sampling policy not registered", group_id))
    }

    /// Compute time bucket for the current time
    fn compute_bucket(&self, now_ns: u64) -> u64 {
        now_ns.saturating_sub(self.experiment_start_ns) / self.bucket_duration_ns
    }

    /// Record a latency sample
    pub fn record_latency(&mut self, group_id: usize, connection_id: usize, latency: Duration) {
        let now_ns = crate::timing::time_ns();
        let time_bucket = self.compute_bucket(now_ns);

        // Flush old buckets if streaming and bucket changed
        if time_bucket > self.current_bucket && self.dumper.is_some() {
            self.flush_old_buckets(time_bucket);
        }
        self.current_bucket = time_bucket;

        let key = StatsKey { time_bucket, group_id, connection_id };
        let entry = self
            .entries
            .entry(key)
            .or_insert_with(|| StatsEntry::new(&self.sampling_policy));
        entry.record_latency(latency);
    }

    /// Record transmitted bytes
    pub fn record_tx_bytes(&mut self, group_id: usize, connection_id: usize, bytes: usize) {
        let now_ns = crate::timing::time_ns();
        let time_bucket = self.compute_bucket(now_ns);

        // Flush old buckets if streaming and bucket changed
        if time_bucket > self.current_bucket && self.dumper.is_some() {
            self.flush_old_buckets(time_bucket);
        }
        self.current_bucket = time_bucket;

        let key = StatsKey { time_bucket, group_id, connection_id };
        let entry = self
            .entries
            .entry(key)
            .or_insert_with(|| StatsEntry::new(&self.sampling_policy));
        entry.record_tx_bytes(bytes);
    }

    /// Record received bytes
    pub fn record_rx_bytes(&mut self, group_id: usize, connection_id: usize, bytes: usize) {
        let now_ns = crate::timing::time_ns();
        let time_bucket = self.compute_bucket(now_ns);

        // Flush old buckets if streaming and bucket changed
        if time_bucket > self.current_bucket && self.dumper.is_some() {
            self.flush_old_buckets(time_bucket);
        }
        self.current_bucket = time_bucket;

        let key = StatsKey { time_bucket, group_id, connection_id };
        let entry = self
            .entries
            .entry(key)
            .or_insert_with(|| StatsEntry::new(&self.sampling_policy));
        entry.record_rx_bytes(bytes);
    }

    /// Flush old buckets to disk (streaming mode only)
    fn flush_old_buckets(&mut self, current_bucket: u64) {
        let Some(ref mut dumper) = self.dumper else {
            return;
        };

        let cutoff = current_bucket.saturating_sub(self.retention_buckets);

        // Collect keys to flush
        let old_keys: Vec<StatsKey> =
            self.entries.keys().filter(|k| k.time_bucket < cutoff).cloned().collect();

        // Flush and remove old entries
        for key in old_keys {
            let Some(entry) = self.entries.remove(&key) else {
                continue;
            };
            if let Err(e) = dumper.write_row(&key, &entry) {
                tracing::warn!("Failed to write stats row to Parquet: {}", e);
            }
        }
    }

    /// Finalize the collector, flushing all remaining entries to disk
    ///
    /// Must be called at the end of an experiment when using streaming mode.
    /// Returns the path to the output file if streaming was enabled.
    pub fn finalize(mut self) -> Option<std::path::PathBuf> {
        if let Some(mut dumper) = self.dumper.take() {
            // Flush all remaining entries
            for (key, entry) in self.entries.drain() {
                if let Err(e) = dumper.write_row(&key, &entry) {
                    tracing::warn!("Failed to write stats row to Parquet: {}", e);
                }
            }

            let path = dumper.path().clone();
            if let Err(e) = dumper.close() {
                tracing::error!("Failed to close stats dumper: {}", e);
            }
            Some(path)
        } else {
            None
        }
    }

    /// Check if streaming mode is enabled
    pub fn is_streaming(&self) -> bool {
        self.dumper.is_some()
    }

    /// Get all unique group IDs
    pub fn group_ids(&self) -> Vec<usize> {
        let mut ids: Vec<usize> = self.entries.keys().map(|k| k.group_id).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Get all unique connection IDs for a group
    pub fn connection_ids(&self, group_id: usize) -> Vec<usize> {
        let mut ids: Vec<usize> = self
            .entries
            .keys()
            .filter(|k| k.group_id == group_id)
            .map(|k| k.connection_id)
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Get all unique time buckets
    pub fn time_buckets(&self) -> Vec<u64> {
        let mut buckets: Vec<u64> = self.entries.keys().map(|k| k.time_bucket).collect();
        buckets.sort_unstable();
        buckets.dedup();
        buckets
    }

    /// Aggregate all entries into global statistics
    pub fn aggregate_global(&self, duration: Duration) -> AggregatedStats {
        let mut total_requests = 0u64;
        let mut total_tx_bytes = 0u64;
        let mut total_rx_bytes = 0u64;

        // Create latency storage based on global sampling policy
        // If policy is None, we skip latency aggregation but still count requests/bytes
        let mut merged_latency = LatencyStorage::from_policy(&self.sampling_policy);

        for entry in self.entries.values() {
            total_requests += entry.request_count;
            total_tx_bytes += entry.tx_bytes;
            total_rx_bytes += entry.rx_bytes;

            // Only merge latency if global policy is not None
            if !matches!(self.sampling_policy, SamplingPolicy::None) {
                merged_latency.merge_ref(&entry.latency);
            }
        }

        self.compute_aggregated_stats(
            &merged_latency,
            total_requests,
            total_tx_bytes,
            total_rx_bytes,
            duration,
        )
    }

    /// Aggregate entries by group (collapse time and connection dimensions)
    pub fn aggregate_by_group(&self, duration: Duration) -> HashMap<usize, AggregatedStats> {
        let mut result = HashMap::new();

        for group_id in self.group_ids() {
            let mut total_requests = 0u64;
            let mut total_tx_bytes = 0u64;
            let mut total_rx_bytes = 0u64;

            // Use this group's specific sampling policy
            let group_policy = self.get_group_policy(group_id);
            let mut merged_latency = LatencyStorage::from_policy(group_policy);
            let should_merge_latency = !matches!(group_policy, SamplingPolicy::None);

            for (key, entry) in &self.entries {
                if key.group_id != group_id {
                    continue;
                }

                total_requests += entry.request_count;
                total_tx_bytes += entry.tx_bytes;
                total_rx_bytes += entry.rx_bytes;

                // Only merge latency if this group's policy is not None
                if should_merge_latency {
                    merged_latency.merge_ref(&entry.latency);
                }
            }

            let stats = self.compute_aggregated_stats(
                &merged_latency,
                total_requests,
                total_tx_bytes,
                total_rx_bytes,
                duration,
            );
            result.insert(group_id, stats);
        }

        result
    }

    /// Aggregate entries by connection within a group
    pub fn aggregate_by_connection(
        &self,
        group_id: usize,
        duration: Duration,
    ) -> HashMap<usize, ConnectionStats> {
        let mut result = HashMap::new();

        for conn_id in self.connection_ids(group_id) {
            let mut total_requests = 0u64;
            let mut total_tx_bytes = 0u64;
            let mut total_rx_bytes = 0u64;
            let mut merged_latency = LatencyStorage::from_policy(&self.sampling_policy);

            for (key, entry) in &self.entries {
                if key.group_id == group_id && key.connection_id == conn_id {
                    total_requests += entry.request_count;
                    total_tx_bytes += entry.tx_bytes;
                    total_rx_bytes += entry.rx_bytes;
                    merged_latency.merge_ref(&entry.latency);
                }
            }

            let latency_stats = self.compute_aggregated_stats(
                &merged_latency,
                total_requests,
                total_tx_bytes,
                total_rx_bytes,
                duration,
            );

            result.insert(
                conn_id,
                ConnectionStats {
                    connection_id: conn_id,
                    group_id,
                    request_count: total_requests,
                    tx_bytes: total_tx_bytes,
                    rx_bytes: total_rx_bytes,
                    latency: latency_stats,
                },
            );
        }

        result
    }

    /// Get time series for global statistics (collapse group and connection)
    pub fn time_series_global(&self) -> Vec<TimeSeriesPoint> {
        let bucket_duration_secs = self.bucket_duration_ns as f64 / 1_000_000_000.0;
        let mut bucket_data: HashMap<u64, (u64, u64, u64, LatencyStorage)> = HashMap::new();

        for (key, entry) in &self.entries {
            let data = bucket_data
                .entry(key.time_bucket)
                .or_insert_with(|| (0, 0, 0, LatencyStorage::from_policy(&self.sampling_policy)));
            data.0 += entry.request_count;
            data.1 += entry.tx_bytes;
            data.2 += entry.rx_bytes;
            data.3.merge_ref(&entry.latency);
        }

        let mut points: Vec<TimeSeriesPoint> = bucket_data
            .into_iter()
            .map(|(bucket, (requests, tx, rx, latency))| {
                let total_bytes = (tx + rx) as f64;
                TimeSeriesPoint {
                    time_bucket: bucket,
                    request_count: requests,
                    throughput_rps: requests as f64 / bucket_duration_secs,
                    throughput_mbps: (total_bytes * 8.0) / (bucket_duration_secs * 1_000_000.0),
                    latency_p50_ns: latency.percentile(0.5),
                    latency_p99_ns: latency.percentile(0.99),
                    latency_p999_ns: latency.percentile(0.999),
                    latency_mean_ns: latency.mean(),
                }
            })
            .collect();

        points.sort_by_key(|p| p.time_bucket);
        points
    }

    /// Get time series for a specific group
    pub fn time_series_by_group(&self, group_id: usize) -> Vec<TimeSeriesPoint> {
        let bucket_duration_secs = self.bucket_duration_ns as f64 / 1_000_000_000.0;
        let mut bucket_data: HashMap<u64, (u64, u64, u64, LatencyStorage)> = HashMap::new();

        for (key, entry) in &self.entries {
            if key.group_id == group_id {
                let data = bucket_data.entry(key.time_bucket).or_insert_with(|| {
                    (0, 0, 0, LatencyStorage::from_policy(&self.sampling_policy))
                });
                data.0 += entry.request_count;
                data.1 += entry.tx_bytes;
                data.2 += entry.rx_bytes;
                data.3.merge_ref(&entry.latency);
            }
        }

        let mut points: Vec<TimeSeriesPoint> = bucket_data
            .into_iter()
            .map(|(bucket, (requests, tx, rx, latency))| {
                let total_bytes = (tx + rx) as f64;
                TimeSeriesPoint {
                    time_bucket: bucket,
                    request_count: requests,
                    throughput_rps: requests as f64 / bucket_duration_secs,
                    throughput_mbps: (total_bytes * 8.0) / (bucket_duration_secs * 1_000_000.0),
                    latency_p50_ns: latency.percentile(0.5),
                    latency_p99_ns: latency.percentile(0.99),
                    latency_p999_ns: latency.percentile(0.999),
                    latency_mean_ns: latency.mean(),
                }
            })
            .collect();

        points.sort_by_key(|p| p.time_bucket);
        points
    }

    /// Get time series for a specific connection
    pub fn time_series_by_connection(
        &self,
        group_id: usize,
        connection_id: usize,
    ) -> Vec<TimeSeriesPoint> {
        let bucket_duration_secs = self.bucket_duration_ns as f64 / 1_000_000_000.0;

        let mut points: Vec<TimeSeriesPoint> = self
            .entries
            .iter()
            .filter(|(k, _)| k.group_id == group_id && k.connection_id == connection_id)
            .map(|(key, entry)| {
                let total_bytes = (entry.tx_bytes + entry.rx_bytes) as f64;
                TimeSeriesPoint {
                    time_bucket: key.time_bucket,
                    request_count: entry.request_count,
                    throughput_rps: entry.request_count as f64 / bucket_duration_secs,
                    throughput_mbps: (total_bytes * 8.0) / (bucket_duration_secs * 1_000_000.0),
                    latency_p50_ns: entry.latency.percentile(0.5),
                    latency_p99_ns: entry.latency.percentile(0.99),
                    latency_p999_ns: entry.latency.percentile(0.999),
                    latency_mean_ns: entry.latency.mean(),
                }
            })
            .collect();

        points.sort_by_key(|p| p.time_bucket);
        points
    }

    /// Merge multiple collectors (from different threads)
    ///
    /// Note: Streaming dumpers are not merged - each thread should finalize
    /// its own dumper before merging in-memory entries.
    pub fn merge(collectors: Vec<Self>) -> Self {
        if collectors.is_empty() {
            return Self::new(SamplingPolicy::default(), Duration::from_secs(1));
        }

        let first = &collectors[0];
        let mut merged = Self {
            entries: HashMap::new(),
            sampling_policy: first.sampling_policy.clone(),
            group_sampling_policies: HashMap::new(),
            bucket_duration_ns: first.bucket_duration_ns,
            experiment_start_ns: first.experiment_start_ns,
            current_bucket: 0,
            dumper: None, // Merged collector is always in-memory
            retention_buckets: u64::MAX,
        };

        // Merge entries from all collectors
        for collector in collectors {
            // Merge per-group sampling policies (first collector's policy wins for each group)
            for (group_id, policy) in collector.group_sampling_policies {
                merged.group_sampling_policies.entry(group_id).or_insert(policy);
            }

            // Merge stats entries
            for (key, entry) in collector.entries {
                merged.entries.entry(key).and_modify(|e| e.merge_from(&entry)).or_insert(entry);
            }
        }

        merged
    }

    /// Get raw entries (for dumping)
    pub fn entries(&self) -> &HashMap<StatsKey, StatsEntry> {
        &self.entries
    }

    /// Take ownership of entries (for dumping)
    pub fn into_entries(self) -> HashMap<StatsKey, StatsEntry> {
        self.entries
    }

    /// Get bucket duration
    pub fn bucket_duration(&self) -> Duration {
        Duration::from_nanos(self.bucket_duration_ns)
    }

    /// Get experiment start time
    pub fn experiment_start_ns(&self) -> u64 {
        self.experiment_start_ns
    }

    /// Helper to compute aggregated stats
    fn compute_aggregated_stats(
        &self,
        latency: &LatencyStorage,
        total_requests: u64,
        total_tx_bytes: u64,
        total_rx_bytes: u64,
        duration: Duration,
    ) -> AggregatedStats {
        let duration_secs = duration.as_secs_f64();
        let total_bytes = (total_tx_bytes + total_rx_bytes) as f64;

        AggregatedStats {
            latency_p50: Duration::from_nanos(latency.percentile(0.5)),
            latency_p95: Duration::from_nanos(latency.percentile(0.95)),
            latency_p99: Duration::from_nanos(latency.percentile(0.99)),
            latency_p999: Duration::from_nanos(latency.percentile(0.999)),
            latency_p9999: Duration::from_nanos(latency.percentile(0.9999)),
            latency_p99999: Duration::from_nanos(latency.percentile(0.99999)),
            mean_latency: Duration::from_nanos(latency.mean()),
            std_dev: Duration::ZERO, // TODO: compute from samples if available
            confidence_interval: Duration::ZERO,
            throughput_rps: if duration_secs > 0.0 {
                total_requests as f64 / duration_secs
            } else {
                0.0
            },
            throughput_mbps: if duration_secs > 0.0 {
                (total_bytes * 8.0) / (duration_secs * 1_000_000.0)
            } else {
                0.0
            },
            total_requests,
        }
    }
}

impl StatsEntry {
    /// Merge from another entry (borrow version for HashMap updates)
    fn merge_from(&mut self, other: &Self) {
        self.request_count += other.request_count;
        self.tx_bytes += other.tx_bytes;
        self.rx_bytes += other.rx_bytes;
        self.latency.merge_ref(&other.latency);
    }
}

impl Default for TupleStatsCollector {
    fn default() -> Self {
        Self::new(SamplingPolicy::default(), Duration::from_secs(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_key_equality() {
        let k1 = StatsKey {
            time_bucket: 0,
            group_id: 1,
            connection_id: 2,
        };
        let k2 = StatsKey {
            time_bucket: 0,
            group_id: 1,
            connection_id: 2,
        };
        let k3 = StatsKey {
            time_bucket: 1,
            group_id: 1,
            connection_id: 2,
        };

        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
    }

    #[test]
    fn test_latency_storage_samples() {
        let mut storage =
            LatencyStorage::from_policy(&SamplingPolicy::Limited { max_samples: 100, rate: 1.0 });

        for i in 1..=100 {
            storage.record(Duration::from_micros(i));
        }

        assert_eq!(storage.count(), 100);
        assert!(storage.percentile(0.5) > 0);
        assert!(storage.mean() > 0);
    }

    #[test]
    fn test_latency_storage_ddsketch() {
        let mut storage =
            LatencyStorage::from_policy(&SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 1024 });

        for i in 1..=1000 {
            storage.record(Duration::from_micros(i));
        }

        assert_eq!(storage.count(), 1000);
        let p50 = storage.percentile(0.5);
        assert!((400_000..=600_000).contains(&p50)); // ~500us in ns
    }

    #[test]
    fn test_tuple_collector_basic() {
        let start_ns = crate::timing::time_ns();
        let mut collector = TupleStatsCollector::with_start_time(
            SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 },
            Duration::from_secs(1),
            start_ns,
        );

        // Record some samples
        collector.record_latency(0, 0, Duration::from_micros(100));
        collector.record_latency(0, 0, Duration::from_micros(200));
        collector.record_latency(0, 1, Duration::from_micros(150));
        collector.record_latency(1, 0, Duration::from_micros(300));

        collector.record_tx_bytes(0, 0, 100);
        collector.record_rx_bytes(0, 0, 50);

        assert_eq!(collector.group_ids(), vec![0, 1]);
        assert_eq!(collector.connection_ids(0), vec![0, 1]);
        assert_eq!(collector.connection_ids(1), vec![0]);
    }

    #[test]
    fn test_tuple_collector_merge() {
        let start_ns = crate::timing::time_ns();

        let mut c1 = TupleStatsCollector::with_start_time(
            SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 },
            Duration::from_secs(1),
            start_ns,
        );
        let mut c2 = TupleStatsCollector::with_start_time(
            SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 },
            Duration::from_secs(1),
            start_ns,
        );

        c1.record_latency(0, 0, Duration::from_micros(100));
        c1.record_tx_bytes(0, 0, 100);

        c2.record_latency(0, 1, Duration::from_micros(200));
        c2.record_tx_bytes(0, 1, 200);

        let merged = TupleStatsCollector::merge(vec![c1, c2]);

        assert_eq!(merged.connection_ids(0), vec![0, 1]);
    }

    #[test]
    fn test_time_series() {
        let start_ns = crate::timing::time_ns();
        let mut collector = TupleStatsCollector::with_start_time(
            SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 },
            Duration::from_secs(1),
            start_ns,
        );

        // All samples in bucket 0 (current time)
        for _ in 0..100 {
            collector.record_latency(0, 0, Duration::from_micros(100));
        }

        let ts = collector.time_series_global();
        assert!(!ts.is_empty());
        assert_eq!(ts[0].request_count, 100);
    }
}
