//! Tuple-keyed statistics collection with on-demand aggregation
//!
//! Stores statistics keyed by `(time_bucket, group_id, connection_id)` tuple,
//! enabling flexible post-hoc aggregation by any dimension.

use super::policy::SamplingPolicy;
use super::AggregatedStats;
use hdrhistogram::Histogram;
use sketches_ddsketch::{Config as DDSketchConfig, DDSketch};
use std::collections::HashMap;
use std::time::Duration;
use tdigest::TDigest;

/// Number of t-digest samples buffered before they are merged into the digest.
/// Each merge rebuilds the digest, so batching keeps that cost off the
/// per-request path.
const TDIGEST_BATCH_SIZE: usize = 1024;

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
    /// T-Digest for accurate tail percentiles.
    ///
    /// Recorded values wait in `pending` until [`TDIGEST_BATCH_SIZE`] of them
    /// are merged into `digest` at once. Reads and merges include pending values.
    TDigest { digest: TDigest, pending: Vec<f64> },
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
            SamplingPolicy::TDigest { compression } => LatencyStorage::TDigest {
                digest: TDigest::new_with_size(*compression),
                pending: Vec::new(),
            },
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
            LatencyStorage::TDigest { digest, pending } => {
                pending.push(latency.as_nanos() as f64);
                if pending.len() >= TDIGEST_BATCH_SIZE {
                    // Hand the full buffer to the digest and start a new one.
                    let batch = std::mem::replace(pending, Vec::with_capacity(TDIGEST_BATCH_SIZE));
                    *digest = digest.merge_unsorted(batch);
                }
            }
            LatencyStorage::HdrHistogram(hist) => {
                let _ = hist.record(latency.as_nanos() as u64);
            }
        }
    }

    /// Merge another storage into this one (consumes other).
    ///
    /// # Panics
    /// Panics if storage configurations are incompatible. See [`Self::try_merge_ref`].
    pub fn merge(&mut self, other: Self) {
        self.merge_ref(&other);
    }

    /// Merge from a reference, retaining this storage's sample capacity.
    ///
    /// # Panics
    /// Panics if storage configurations are incompatible or a sketch merge fails.
    pub fn merge_ref(&mut self, other: &Self) {
        self.try_merge_ref(other).expect("Cannot merge incompatible latency storage");
    }

    /// Merge compatible storage, returning an error instead of dropping samples
    /// when policies differ. Limited samples are retained in input order up to
    /// the configured capacity. None can only merge with None.
    pub fn try_merge_ref(&mut self, other: &Self) -> Result<(), &'static str> {
        match (self, other) {
            (LatencyStorage::None, LatencyStorage::None) => {}
            (
                LatencyStorage::Samples { samples, max_samples, sampling_rate },
                LatencyStorage::Samples {
                    samples: other_samples,
                    max_samples: other_max,
                    sampling_rate: other_rate,
                },
            ) if max_samples == other_max && sampling_rate == other_rate => {
                samples
                    .extend(other_samples.iter().take(max_samples.saturating_sub(samples.len())));
            }
            (LatencyStorage::DdSketch(sketch), LatencyStorage::DdSketch(other)) => {
                sketch.merge(other).map_err(|_| "DDSketch configurations differ")?;
            }
            (
                LatencyStorage::TDigest { digest, pending },
                LatencyStorage::TDigest { digest: other, pending: other_pending },
            ) if digest.max_size() == other.max_size() => {
                // Fold pending values into both digests first, so the merged
                // digest holds every value and this side has none pending.
                let mine = digest.merge_unsorted(std::mem::take(pending));
                let theirs = other.merge_unsorted(other_pending.clone());
                *digest = TDigest::merge_digests(vec![mine, theirs]);
            }
            (LatencyStorage::HdrHistogram(hist), LatencyStorage::HdrHistogram(other))
                if (hist.low(), hist.high(), hist.sigfig())
                    == (other.low(), other.high(), other.sigfig()) =>
            {
                hist.add(other).map_err(|_| "HDR histogram merge failed")?;
            }
            _ => return Err("latency storage configurations differ"),
        }
        Ok(())
    }

    /// Get sample count
    pub fn count(&self) -> u64 {
        match self {
            LatencyStorage::None => 0,
            LatencyStorage::Samples { samples, .. } => samples.len() as u64,
            LatencyStorage::DdSketch(sketch) => sketch.count() as u64,
            LatencyStorage::TDigest { digest, pending } => {
                digest.count() as u64 + pending.len() as u64
            }
            LatencyStorage::HdrHistogram(hist) => hist.len(),
        }
    }

    /// Calculate percentile (returns nanoseconds).
    ///
    /// Uses the same rule as [`Self::percentiles`].
    pub fn percentile(&self, p: f64) -> u64 {
        self.percentiles(&[p])[0]
    }

    /// Calculate several percentiles (returns nanoseconds, in the order of `ps`).
    ///
    /// Sample storage is sorted once, and each percentile takes the nearest-rank
    /// value at index `round(p * (n - 1))` of the ascending samples. Sketch
    /// storage answers each percentile with its own quantile estimate.
    pub fn percentiles(&self, ps: &[f64]) -> Vec<u64> {
        match self {
            LatencyStorage::None => vec![0; ps.len()],
            LatencyStorage::Samples { samples, .. } => {
                if samples.is_empty() {
                    return vec![0; ps.len()];
                }
                let mut sorted = samples.to_vec();
                sorted.sort();
                let last = sorted.len() - 1;
                ps.iter()
                    .map(|&p| {
                        let idx = ((p * last as f64).round() as usize).min(last);
                        sorted[idx].as_nanos() as u64
                    })
                    .collect()
            }
            LatencyStorage::DdSketch(sketch) => ps
                .iter()
                .map(|&p| sketch.quantile(p).ok().flatten().map(|v| v as u64).unwrap_or(0))
                .collect(),
            LatencyStorage::TDigest { digest, pending } => {
                let digest = tdigest_with_pending(digest, pending);
                ps.iter().map(|&p| digest.estimate_quantile(p) as u64).collect()
            }
            LatencyStorage::HdrHistogram(hist) => {
                ps.iter().map(|&p| hist.value_at_quantile(p)).collect()
            }
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
            LatencyStorage::TDigest { digest, pending } => {
                tdigest_with_pending(digest, pending).mean() as u64
            }
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
            LatencyStorage::TDigest { digest, pending } => {
                tdigest_with_pending(digest, pending).min() as u64
            }
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
            LatencyStorage::TDigest { digest, pending } => {
                tdigest_with_pending(digest, pending).max() as u64
            }
            LatencyStorage::HdrHistogram(hist) => hist.max(),
        }
    }
}

/// Returns a copy of `digest` that also holds the `pending` values.
///
/// Reads take `&self`, so they fold pending values into this temporary digest
/// instead of flushing them into the stored one.
fn tdigest_with_pending(digest: &TDigest, pending: &[f64]) -> TDigest {
    digest.merge_unsorted(pending.to_vec())
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
/// on-demand aggregation by any dimension. Global latency storage is independent
/// of group storage: each recorded latency is sampled once under each policy.
/// This requires additional storage per retained bucket and allows heterogeneous
/// groups without converting or discarding their sketches.
///
/// Supports two modes:
/// - **In-memory**: All entries kept in memory (default)
/// - **Streaming**: Old buckets flushed to Parquet files for bounded memory
pub struct TupleStatsCollector {
    /// Statistics entries keyed by (time_bucket, group_id, connection_id)
    entries: HashMap<StatsKey, StatsEntry>,
    /// Independent global storage per bucket, recorded with the global policy.
    /// This avoids trying to convert incompatible group sketches during aggregation.
    global_entries: HashMap<u64, StatsEntry>,
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
            global_entries: HashMap::new(),
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
            global_entries: HashMap::new(),
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
            global_entries: HashMap::new(),
            sampling_policy,
            group_sampling_policies: HashMap::new(),
            bucket_duration_ns: bucket_duration.as_nanos() as u64,
            experiment_start_ns: start_time_ns,
            current_bucket: 0,
            dumper: Some(dumper),
            retention_buckets: retention_buckets as u64,
        }
    }

    /// Register a per-group sampling policy before recording that group's data.
    /// Unregistered groups use the global policy. Replacing a policy while that
    /// group has data in memory is a programmer error and panics.
    pub fn register_group_policy(&mut self, group_id: usize, policy: SamplingPolicy) {
        assert!(
            !self.entries.keys().any(|key| key.group_id == group_id)
                || self.get_group_policy(group_id) == &policy,
            "Cannot change a group's sampling policy after recording data"
        );
        self.group_sampling_policies.insert(group_id, policy);
    }

    fn get_group_policy(&self, group_id: usize) -> &SamplingPolicy {
        self.group_sampling_policies.get(&group_id).unwrap_or(&self.sampling_policy)
    }

    /// Compute time bucket for the current time
    fn compute_bucket(&self, now_ns: u64) -> u64 {
        now_ns.saturating_sub(self.experiment_start_ns) / self.bucket_duration_ns
    }

    /// Create both entries through one path, including when bytes arrive before latency.
    fn recording_entries(
        &mut self,
        group_id: usize,
        connection_id: usize,
    ) -> (&mut StatsEntry, &mut StatsEntry) {
        let time_bucket = self.compute_bucket(crate::timing::time_ns());
        if time_bucket > self.current_bucket && self.dumper.is_some() {
            self.flush_old_buckets(time_bucket);
        }
        self.current_bucket = time_bucket;
        let policy = self.group_sampling_policies.get(&group_id).unwrap_or(&self.sampling_policy);
        let key = StatsKey { time_bucket, group_id, connection_id };
        let entry = self.entries.entry(key).or_insert_with(|| StatsEntry::new(policy));
        let global = self
            .global_entries
            .entry(time_bucket)
            .or_insert_with(|| StatsEntry::new(&self.sampling_policy));
        (entry, global)
    }

    /// Record latency independently under the group and global sampling policies.
    pub fn record_latency(&mut self, group_id: usize, connection_id: usize, latency: Duration) {
        let (entry, global) = self.recording_entries(group_id, connection_id);
        entry.record_latency(latency);
        global.record_latency(latency);
    }

    /// Record transmitted bytes
    pub fn record_tx_bytes(&mut self, group_id: usize, connection_id: usize, bytes: usize) {
        let (entry, global) = self.recording_entries(group_id, connection_id);
        entry.record_tx_bytes(bytes);
        global.record_tx_bytes(bytes);
    }

    /// Record received bytes
    pub fn record_rx_bytes(&mut self, group_id: usize, connection_id: usize, bytes: usize) {
        let (entry, global) = self.recording_entries(group_id, connection_id);
        entry.record_rx_bytes(bytes);
        global.record_rx_bytes(bytes);
    }

    /// Flush old buckets to disk (streaming mode only)
    fn flush_old_buckets(&mut self, current_bucket: u64) {
        let Some(ref mut dumper) = self.dumper else {
            return;
        };

        let cutoff = current_bucket.saturating_sub(self.retention_buckets);
        self.global_entries.retain(|bucket, _| *bucket >= cutoff);

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

    /// Aggregate retained buckets using the independently recorded global policy.
    /// Global sampling is independent of group sampling, including when groups
    /// disable sampling or use different sketch types. Streaming evicts global
    /// buckets alongside group entries; Parquet output contains group entries only.
    pub fn aggregate_global(&self, duration: Duration) -> AggregatedStats {
        let mut total_requests = 0u64;
        let mut total_tx_bytes = 0u64;
        let mut total_rx_bytes = 0u64;

        // Create latency storage based on global sampling policy
        // If policy is None, we skip latency aggregation but still count requests/bytes
        let mut merged_latency = LatencyStorage::from_policy(&self.sampling_policy);

        for entry in self.global_entries.values() {
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
            let mut merged_latency = LatencyStorage::from_policy(self.get_group_policy(group_id));

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
        let mut points: Vec<TimeSeriesPoint> = self
            .global_entries
            .iter()
            .map(|(&bucket, entry)| {
                let total_bytes = (entry.tx_bytes + entry.rx_bytes) as f64;
                let latency_percentiles = entry.latency.percentiles(&[0.5, 0.99, 0.999]);
                TimeSeriesPoint {
                    time_bucket: bucket,
                    request_count: entry.request_count,
                    throughput_rps: entry.request_count as f64 / bucket_duration_secs,
                    throughput_mbps: (total_bytes * 8.0) / (bucket_duration_secs * 1_000_000.0),
                    latency_p50_ns: latency_percentiles[0],
                    latency_p99_ns: latency_percentiles[1],
                    latency_p999_ns: latency_percentiles[2],
                    latency_mean_ns: entry.latency.mean(),
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
                    (0, 0, 0, LatencyStorage::from_policy(self.get_group_policy(group_id)))
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
                let latency_percentiles = latency.percentiles(&[0.5, 0.99, 0.999]);
                TimeSeriesPoint {
                    time_bucket: bucket,
                    request_count: requests,
                    throughput_rps: requests as f64 / bucket_duration_secs,
                    throughput_mbps: (total_bytes * 8.0) / (bucket_duration_secs * 1_000_000.0),
                    latency_p50_ns: latency_percentiles[0],
                    latency_p99_ns: latency_percentiles[1],
                    latency_p999_ns: latency_percentiles[2],
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
                let latency_percentiles = entry.latency.percentiles(&[0.5, 0.99, 0.999]);
                TimeSeriesPoint {
                    time_bucket: key.time_bucket,
                    request_count: entry.request_count,
                    throughput_rps: entry.request_count as f64 / bucket_duration_secs,
                    throughput_mbps: (total_bytes * 8.0) / (bucket_duration_secs * 1_000_000.0),
                    latency_p50_ns: latency_percentiles[0],
                    latency_p99_ns: latency_percentiles[1],
                    latency_p999_ns: latency_percentiles[2],
                    latency_mean_ns: entry.latency.mean(),
                }
            })
            .collect();

        points.sort_by_key(|p| p.time_bucket);
        points
    }

    /// Merge multiple collectors (from different threads).
    ///
    /// # Panics
    /// Panics if global/group policies differ. As before, bucket coordinates are
    /// interpreted relative to the first collector's start time and bucket duration.
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
            global_entries: HashMap::new(),
            sampling_policy: first.sampling_policy.clone(),
            group_sampling_policies: HashMap::new(),
            bucket_duration_ns: first.bucket_duration_ns,
            experiment_start_ns: first.experiment_start_ns,
            current_bucket: 0,
            dumper: None, // Merged collector is always in-memory
            retention_buckets: u64::MAX,
        };

        // Validate policies before merging any entries.
        for collector in &collectors {
            assert_eq!(
                merged.sampling_policy, collector.sampling_policy,
                "Global sampling policies differ"
            );
            for (&group_id, policy) in &collector.group_sampling_policies {
                if let Some(existing) = merged.group_sampling_policies.get(&group_id) {
                    assert_eq!(existing, policy, "Group sampling policies differ");
                } else {
                    merged.group_sampling_policies.insert(group_id, policy.clone());
                }
            }
        }
        for collector in &collectors {
            for key in collector.entries.keys() {
                assert_eq!(
                    merged.get_group_policy(key.group_id),
                    collector.get_group_policy(key.group_id),
                    "Group sampling policies differ"
                );
            }
        }
        for collector in collectors {
            for (key, entry) in collector.entries {
                merged.entries.entry(key).and_modify(|e| e.merge_from(&entry)).or_insert(entry);
            }
            for (bucket, entry) in collector.global_entries {
                merged
                    .global_entries
                    .entry(bucket)
                    .and_modify(|e| e.merge_from(&entry))
                    .or_insert(entry);
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
        let latency_percentiles = latency.percentiles(&[0.5, 0.95, 0.99, 0.999, 0.9999, 0.99999]);

        AggregatedStats {
            latency_p50: Duration::from_nanos(latency_percentiles[0]),
            latency_p95: Duration::from_nanos(latency_percentiles[1]),
            latency_p99: Duration::from_nanos(latency_percentiles[2]),
            latency_p999: Duration::from_nanos(latency_percentiles[3]),
            latency_p9999: Duration::from_nanos(latency_percentiles[4]),
            latency_p99999: Duration::from_nanos(latency_percentiles[5]),
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

    fn mixed_collector(global: SamplingPolicy, start: u64) -> TupleStatsCollector {
        let mut collector =
            TupleStatsCollector::with_start_time(global, Duration::from_secs(3600), start);
        collector
            .register_group_policy(0, SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 1024 });
        collector.register_group_policy(1, SamplingPolicy::TDigest { compression: 100 });
        collector.register_group_policy(
            2,
            SamplingPolicy::HdrHistogram { sigfigs: 3, max_value_us: 10_000 },
        );
        for group in 0..3 {
            // Both byte APIs can create an entry before the first latency arrives.
            if group % 2 == 0 {
                collector.record_tx_bytes(group, 0, 100);
                collector.record_rx_bytes(group, 0, 200);
            } else {
                collector.record_rx_bytes(group, 0, 200);
                collector.record_tx_bytes(group, 0, 100);
            }
            for _ in 0..100 {
                collector.record_latency(group, 0, Duration::from_micros(10 + group as u64 * 10));
            }
        }
        collector
    }

    #[test]
    fn heterogeneous_groups_preserve_all_output_dimensions() {
        for global in [
            SamplingPolicy::None,
            SamplingPolicy::Unlimited,
            SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 },
            SamplingPolicy::DdSketch { alpha: 0.02, max_bins: 2048 },
        ] {
            let collector = mixed_collector(global.clone(), crate::timing::time_ns());
            let duration = Duration::from_secs(1);
            let stats = collector.aggregate_global(duration);
            assert_eq!(stats.total_requests, 300);
            assert_eq!(stats.throughput_rps, 300.0);
            assert_eq!(stats.throughput_mbps, 0.0072);
            if global == SamplingPolicy::None {
                assert_eq!(stats.latency_p99, Duration::ZERO);
                assert_eq!(collector.time_series_global()[0].latency_p99_ns, 0);
            } else {
                assert!((29_000..=31_000).contains(&(stats.latency_p99.as_nanos() as u64)));
                assert!(
                    (29_000..=31_000).contains(&collector.time_series_global()[0].latency_p99_ns)
                );
            }
            let groups = collector.aggregate_by_group(duration);
            for group in 0..3 {
                let expected = 10_000 + group as u64 * 10_000;
                let close =
                    |value: u64| assert!(value.abs_diff(expected) <= 500, "group {group}: {value}");
                let stats = &groups[&group];
                assert_eq!(stats.total_requests, 100);
                assert_eq!(stats.throughput_mbps, 0.0024);
                close(stats.latency_p99.as_nanos() as u64);
                let connection = &collector.aggregate_by_connection(group, duration)[&0];
                assert_eq!((connection.tx_bytes, connection.rx_bytes), (100, 200));
                close(connection.latency.latency_p99.as_nanos() as u64);
                close(collector.time_series_by_group(group)[0].latency_p99_ns);
                close(collector.time_series_by_connection(group, 0)[0].latency_p99_ns);
            }
            assert!(collector.entries.values().all(|entry| entry.latency.count() == 100));
        }
    }

    #[test]
    fn unregistered_group_uses_global_policy() {
        let mut collector =
            TupleStatsCollector::new(SamplingPolicy::Unlimited, Duration::from_secs(3600));
        collector.record_tx_bytes(9, 1, 100);
        collector.record_latency(9, 1, Duration::from_micros(42));
        assert_eq!(
            collector.aggregate_by_group(Duration::from_secs(1))[&9].latency_p99,
            Duration::from_micros(42)
        );
    }

    #[test]
    fn heterogeneous_collectors_merge_global_and_group_storage() {
        let start = crate::timing::time_ns();
        let merged = TupleStatsCollector::merge(vec![
            mixed_collector(SamplingPolicy::Unlimited, start),
            mixed_collector(SamplingPolicy::Unlimited, start),
        ]);
        let duration = Duration::from_secs(1);
        let global = merged.aggregate_global(duration);
        assert_eq!(global.total_requests, 600);
        assert_eq!(global.latency_p99, Duration::from_micros(30));
        assert_eq!(merged.time_series_global()[0].request_count, 600);
        for stats in merged.aggregate_by_group(duration).values() {
            assert_eq!(stats.total_requests, 200);
            assert!(stats.latency_p99 > Duration::ZERO);
        }
    }

    #[test]
    fn streaming_evicts_global_storage_alongside_group_entries() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stats.parquet");
        let dumper = super::super::StatsDumper::new(path.clone(), None).unwrap();
        let mut collector = TupleStatsCollector::with_streaming(
            SamplingPolicy::Unlimited,
            Duration::from_secs(3600),
            crate::timing::time_ns(),
            dumper,
            1,
        );
        collector
            .register_group_policy(0, SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 1024 });
        collector.record_tx_bytes(0, 0, 100);
        collector.record_latency(0, 0, Duration::from_micros(10));
        assert_eq!(collector.global_entries.len(), 1);
        collector.flush_old_buckets(2);
        assert!(collector.entries.is_empty());
        assert!(collector.global_entries.is_empty());
        assert_eq!(collector.aggregate_global(Duration::from_secs(1)).total_requests, 0);
        assert_eq!(collector.finalize(), Some(path.clone()));
        let reader = SerializedFileReader::new(std::fs::File::open(path).unwrap()).unwrap();
        // Only group entries are written; no synthetic global rows enter Parquet.
        assert_eq!(reader.metadata().file_metadata().num_rows(), 1);
    }

    #[test]
    #[should_panic(expected = "Global sampling policies differ")]
    fn tuple_merge_rejects_global_policy_mismatch() {
        TupleStatsCollector::merge(vec![
            TupleStatsCollector::new(SamplingPolicy::None, Duration::from_secs(1)),
            TupleStatsCollector::new(SamplingPolicy::Unlimited, Duration::from_secs(1)),
        ]);
    }

    #[test]
    #[should_panic(expected = "Group sampling policies differ")]
    fn tuple_merge_rejects_group_policy_mismatch() {
        let mut a = TupleStatsCollector::new(SamplingPolicy::Unlimited, Duration::from_secs(1));
        let mut b = TupleStatsCollector::new(SamplingPolicy::Unlimited, Duration::from_secs(1));
        a.record_latency(0, 0, Duration::from_micros(1));
        b.register_group_policy(0, SamplingPolicy::None);
        b.record_latency(0, 1, Duration::from_micros(1));
        TupleStatsCollector::merge(vec![a, b]);
    }

    #[test]
    fn latency_merge_rejects_incompatible_storage() {
        let mut samples = LatencyStorage::from_policy(&SamplingPolicy::Unlimited);
        let sketch =
            LatencyStorage::from_policy(&SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 1024 });
        assert!(samples.try_merge_ref(&sketch).is_err());
        let mut other_sketch =
            LatencyStorage::from_policy(&SamplingPolicy::DdSketch { alpha: 0.02, max_bins: 1024 });
        assert!(other_sketch.try_merge_ref(&sketch).is_err());
        assert!(LatencyStorage::None.try_merge_ref(&samples).is_err());
    }

    #[test]
    #[should_panic(expected = "Cannot change a group's sampling policy")]
    fn policy_change_after_recording_is_rejected() {
        let mut collector = TupleStatsCollector::default();
        collector.record_tx_bytes(0, 0, 10);
        collector.register_group_policy(0, SamplingPolicy::None);
    }

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
    fn percentiles_match_nearest_rank_values() {
        // Expected values follow the nearest-rank rule: index round(p * (n - 1))
        // into the samples sorted in ascending order.
        const QUANTILES: [f64; 6] = [0.5, 0.95, 0.99, 0.999, 0.9999, 0.99999];
        let storage_with = |samples_ns: &[u64]| {
            let mut storage = LatencyStorage::from_policy(&SamplingPolicy::Unlimited);
            for &ns in samples_ns {
                storage.record(Duration::from_nanos(ns));
            }
            storage
        };
        let percentiles = |storage: &LatencyStorage| QUANTILES.map(|p| storage.percentile(p));
        let percentiles_at_once = |storage: &LatencyStorage| storage.percentiles(&QUANTILES);

        let empty = LatencyStorage::from_policy(&SamplingPolicy::Unlimited);
        assert_eq!(empty.count(), 0);
        assert_eq!(percentiles(&empty), [0, 0, 0, 0, 0, 0]);
        assert_eq!(percentiles_at_once(&empty), [0, 0, 0, 0, 0, 0]);

        // Odd count (n = 13). Sorted: 600, 1300, 1300, 2700, 2700, 2700, 3900,
        // 4200, 5500, 7100, 9800, 15000, 31000. p * 12 rounds to indices
        // 6, 11, 12, 12, 12, 12.
        let odd = storage_with(&[
            4_200, 1_300, 9_800, 1_300, 2_700, 15_000, 2_700, 600, 5_500, 2_700, 31_000, 7_100,
            3_900,
        ]);
        assert_eq!(odd.count(), 13);
        assert_eq!(percentiles(&odd), [3_900, 15_000, 31_000, 31_000, 31_000, 31_000]);
        assert_eq!(percentiles_at_once(&odd), [3_900, 15_000, 31_000, 31_000, 31_000, 31_000]);

        // Even count (n = 12). Sorted: 250, 250, 250, 800, 800, 800, 1900, 2100,
        // 4400, 6300, 12000, 40000. p * 11 rounds to indices 6 (from 5.5),
        // 10, 11, 11, 11, 11.
        let even = storage_with(&[
            800, 250, 250, 1_900, 12_000, 800, 4_400, 250, 6_300, 800, 2_100, 40_000,
        ]);
        assert_eq!(even.count(), 12);
        assert_eq!(percentiles(&even), [1_900, 12_000, 40_000, 40_000, 40_000, 40_000]);
        assert_eq!(percentiles_at_once(&even), [1_900, 12_000, 40_000, 40_000, 40_000, 40_000]);

        // Even count (n = 100_002) recorded in descending order, so sorted index
        // k holds k + 1 ns. p * 100_001 is 50_000.5, 95_000.95, 99_000.99,
        // 99_900.999, 99_990.9999, and 99_999.99999; the half rounds away from
        // zero, giving indices 50_001, 95_001, 99_001, 99_901, 99_991, 100_000.
        let descending: Vec<u64> = (1..=100_002).rev().collect();
        let large = storage_with(&descending);
        assert_eq!(large.count(), 100_002);
        assert_eq!(percentiles(&large), [50_002, 95_002, 99_002, 99_902, 99_992, 100_001]);
        assert_eq!(percentiles_at_once(&large), [50_002, 95_002, 99_002, 99_902, 99_992, 100_001]);
    }

    #[test]
    fn adaptive_sampling_policy_samples_at_initial_rate() {
        let policy: SamplingPolicy = serde_json::from_str(
            r#"{"type": "adaptive", "max_samples": 3, "initial_rate": 1.0, "target_ci_width_us": 10}"#,
        )
        .unwrap();
        let mut storage = LatencyStorage::from_policy(&policy);
        match &storage {
            LatencyStorage::Samples { max_samples, sampling_rate, .. } => {
                assert_eq!(*max_samples, 3);
                assert_eq!(*sampling_rate, 1.0);
            }
            _ => panic!("adaptive policy must store samples"),
        }
        for i in 1..=5 {
            storage.record(Duration::from_micros(i));
        }
        // Every sample is kept until max_samples is reached.
        assert_eq!(storage.count(), 3);
        assert_eq!(storage.max(), 3_000);

        // The rate stays at initial_rate: min_rate and target_ci_width_us do not adjust it.
        let policy: SamplingPolicy = serde_json::from_str(
            r#"{"type": "adaptive", "initial_rate": 0.0, "target_ci_width_us": 10, "min_rate": 0.5}"#,
        )
        .unwrap();
        let mut storage = LatencyStorage::from_policy(&policy);
        match &storage {
            LatencyStorage::Samples { max_samples, sampling_rate, .. } => {
                assert_eq!(*max_samples, 128 * 1024);
                assert_eq!(*sampling_rate, 0.0);
            }
            _ => panic!("adaptive policy must store samples"),
        }
        for _ in 0..1000 {
            storage.record(Duration::from_micros(1));
        }
        assert_eq!(storage.count(), 0);
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

    /// T-digest policy as parsed from a config that omits `compression`.
    fn default_tdigest_policy() -> SamplingPolicy {
        let policy: SamplingPolicy = serde_json::from_str(r#"{"type": "t-digest"}"#).unwrap();
        assert_eq!(policy, SamplingPolicy::TDigest { compression: 1000 });
        policy
    }

    /// 100,000 latencies with a long exponential tail above a 20 us floor. The
    /// value of rank k (0-based, ascending) is 20 us + 30 us * -ln(1 - (k + 0.5) / n),
    /// and the values arrive in a fixed scrambled order: position i holds rank
    /// i * 7_919 mod n, which visits every rank once because 7_919 is coprime to n.
    fn tdigest_batch_latencies_ns() -> Vec<u64> {
        const N: u64 = 100_000;
        (0..N)
            .map(|i| {
                let rank = (i * 7_919) % N;
                let u = (rank as f64 + 0.5) / N as f64;
                20_000 + (-30_000.0 * (1.0 - u).ln()).round() as u64
            })
            .collect()
    }

    fn record_tdigest(latencies_ns: &[u64]) -> LatencyStorage {
        let mut storage = LatencyStorage::from_policy(&default_tdigest_policy());
        for &ns in latencies_ns {
            storage.record(Duration::from_nanos(ns));
        }
        storage
    }

    fn assert_within_one_percent(what: &str, estimate: u64, exact: u64) {
        assert!(
            estimate.abs_diff(exact) * 100 <= exact,
            "{what}: estimate {estimate} ns is not within 1% of exact {exact} ns"
        );
    }

    /// Compares p50, p99, and p999 with the exact nearest-rank values of
    /// `latencies_ns`: index round(p * (n - 1)) of the ascending values.
    fn assert_tdigest_batch_quantiles(storage: &LatencyStorage, latencies_ns: &[u64]) {
        let mut sorted = latencies_ns.to_vec();
        sorted.sort_unstable();
        let last = sorted.len() - 1;
        let quantiles = [("p50", 0.5), ("p99", 0.99), ("p999", 0.999)];
        let estimates = storage.percentiles(&quantiles.map(|(_, p)| p));
        for ((name, p), estimate) in quantiles.into_iter().zip(estimates) {
            let exact = sorted[(p * last as f64).round() as usize];
            assert_within_one_percent(name, estimate, exact);
        }
    }

    #[test]
    fn tdigest_batch_quantiles_match_nearest_rank() {
        let latencies_ns = tdigest_batch_latencies_ns();
        let storage = record_tdigest(&latencies_ns);
        assert_eq!(storage.count(), 100_000);
        assert_tdigest_batch_quantiles(&storage, &latencies_ns);
    }

    #[test]
    fn tdigest_batch_merged_storages_keep_quantiles_and_count() {
        let latencies_ns = tdigest_batch_latencies_ns();
        // Uneven, odd-sized parts, as two connections might record them.
        let (first, second) = latencies_ns.split_at(33_333);
        let mut first = record_tdigest(first);
        let second = record_tdigest(second);
        assert_eq!((first.count(), second.count()), (33_333, 66_667));

        // Aggregation merges each entry into fresh storage.
        let mut aggregated = LatencyStorage::from_policy(&default_tdigest_policy());
        aggregated.merge_ref(&first);
        aggregated.merge_ref(&second);
        assert_eq!(aggregated.count(), 100_000);
        assert_tdigest_batch_quantiles(&aggregated, &latencies_ns);

        // Collector merges fold one entry's storage into another's.
        first.merge_ref(&second);
        assert_eq!(first.count(), 100_000);
        assert_tdigest_batch_quantiles(&first, &latencies_ns);
    }

    #[test]
    fn tdigest_batch_mean_min_max_match_exact() {
        let latencies_ns = tdigest_batch_latencies_ns();
        let storage = record_tdigest(&latencies_ns);
        let exact_mean = latencies_ns.iter().sum::<u64>() / latencies_ns.len() as u64;
        assert_within_one_percent("mean", storage.mean(), exact_mean);
        assert_within_one_percent("min", storage.min(), *latencies_ns.iter().min().unwrap());
        assert_within_one_percent("max", storage.max(), *latencies_ns.iter().max().unwrap());
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
