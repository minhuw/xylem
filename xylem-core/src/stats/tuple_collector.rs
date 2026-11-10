//! Tuple-keyed statistics collection with on-demand aggregation
//!
//! Stores statistics keyed by `(time_bucket, group_id, thread_id, connection_id)`,
//! enabling flexible post-hoc aggregation by any dimension.

use super::breakdown::{BreakdownStats, BreakdownSummary, LatencyBreakdown};
use super::policy::SamplingPolicy;
use super::requests::{RequestCounts, SendLagStats, SendLagSummary, LATE_SEND_THRESHOLD};
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
    /// Worker thread that owns the connection. Each thread numbers its
    /// connections from 0, so this tells apart equal connection IDs.
    pub thread_id: usize,
    /// Connection ID within the thread's connection pool
    pub connection_id: usize,
}

/// Exact summary of every latency a storage recorded, in nanoseconds.
///
/// Integer sums make merges exact, so the summary does not depend on the order
/// in which entries are merged. Sums saturate rather than wrap.
#[derive(Debug, Clone, Default, PartialEq)]
struct Moments {
    count: u64,
    sum: u128,
    sum_sq: u128,
    min: u64,
    max: u64,
}

impl Moments {
    fn record(&mut self, ns: u64) {
        self.min = if self.count == 0 {
            ns
        } else {
            self.min.min(ns)
        };
        self.max = self.max.max(ns);
        self.count += 1;
        self.sum = self.sum.saturating_add(ns as u128);
        self.sum_sq = self.sum_sq.saturating_add((ns as u128) * (ns as u128));
    }

    fn merge(&mut self, other: &Self) {
        if other.count == 0 {
            return;
        }
        self.min = if self.count == 0 {
            other.min
        } else {
            self.min.min(other.min)
        };
        self.max = self.max.max(other.max);
        self.count += other.count;
        self.sum = self.sum.saturating_add(other.sum);
        self.sum_sq = self.sum_sq.saturating_add(other.sum_sq);
    }

    fn mean(&self) -> Option<f64> {
        (self.count > 0).then(|| self.sum as f64 / self.count as f64)
    }

    /// Sample standard deviation (divides by n - 1); needs two values.
    fn std_dev(&self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }
        let n = self.count as u128;
        // n * sum_sq - sum^2 is exact while it fits in u128.
        let numerator = match (n.checked_mul(self.sum_sq), self.sum.checked_mul(self.sum)) {
            (Some(a), Some(b)) => a.saturating_sub(b) as f64,
            _ => {
                let mean = self.sum as f64 / self.count as f64;
                (self.sum_sq as f64 - self.sum as f64 * mean).max(0.0) * self.count as f64
            }
        };
        let variance = numerator / (self.count as f64 * (self.count - 1) as f64);
        Some(variance.sqrt())
    }
}

/// Uniform random sample of at most `capacity` latencies (nanoseconds).
///
/// `seen` counts every latency offered after the rate filter; `samples` is a
/// uniform sample of those, so it holds `min(seen, capacity)` values.
struct Reservoir {
    samples: Vec<u64>,
    capacity: usize,
    rate: f64,
    seen: u64,
}

impl Reservoir {
    fn new(capacity: usize, rate: f64) -> Self {
        Self {
            samples: Vec::with_capacity(capacity.min(1024)),
            capacity,
            rate,
            seen: 0,
        }
    }

    /// Offer one latency; returns whether it passed the rate filter.
    fn offer(&mut self, ns: u64) -> bool {
        if self.rate < 1.0 && rand::random::<f64>() >= self.rate {
            return false;
        }
        self.seen += 1;
        if self.samples.len() < self.capacity {
            self.samples.push(ns);
        } else {
            // Algorithm R: the new value replaces a stored one with
            // probability capacity / seen.
            let slot = rand::random_range(0..self.seen);
            if slot < self.capacity as u64 {
                self.samples[slot as usize] = ns;
            }
        }
        true
    }

    /// Merge so the result is a uniform sample of both sides' seen values.
    ///
    /// The number of samples taken from this side follows the hypergeometric
    /// distribution of drawing `keep` values from `self.seen + other.seen`
    /// without replacement. A uniform subset of a uniform sample is uniform.
    fn merge(&mut self, other: &Self) {
        use rand::seq::SliceRandom;
        use rand_distr::{Distribution, Hypergeometric};

        let total = self.seen + other.seen;
        let keep = (self.capacity as u64).min(total);
        if (self.samples.len() + other.samples.len()) as u64 <= keep {
            self.samples.extend_from_slice(&other.samples);
            self.seen = total;
            return;
        }
        let mut rng = rand::rng();
        let from_self = Hypergeometric::new(total, self.seen, keep)
            .expect("valid hypergeometric parameters")
            .sample(&mut rng) as usize;
        let from_other = keep as usize - from_self;
        let (mine, _) = self.samples.partial_shuffle(&mut rng, from_self);
        let mut merged = mine.to_vec();
        let mut theirs = other.samples.clone();
        let (theirs, _) = theirs.partial_shuffle(&mut rng, from_other);
        merged.extend_from_slice(theirs);
        self.samples = merged;
        self.seen = total;
    }
}

/// How a storage keeps latencies for percentiles.
enum Quantiles {
    /// Latency sampling disabled
    None,
    /// Sample-based storage (Limited/Unlimited modes)
    Samples(Reservoir),
    /// DDSketch for memory-efficient quantile estimation
    DdSketch(DDSketch),
    /// T-Digest for accurate tail percentiles.
    ///
    /// Recorded values wait in `pending` until [`TDIGEST_BATCH_SIZE`] of them
    /// are merged into `digest` at once. Reads and merges include pending values.
    TDigest { digest: TDigest, pending: Vec<f64> },
    /// HDR histogram. It auto-resizes unless the policy sets a maximum; then
    /// larger values are recorded as the maximum and counted in `clamped`.
    HdrHistogram { hist: Histogram<u64>, clamped: u64 },
}

/// Latency storage for one entry, built from a sampling policy.
///
/// Percentiles come from the policy's structure (samples or a sketch). Mean,
/// min, max and standard deviation are computed exactly from every recorded
/// latency, for every policy except `None`.
pub struct LatencyStorage {
    quantiles: Quantiles,
    moments: Moments,
}

impl LatencyStorage {
    /// Storage that records no latencies.
    pub fn none() -> Self {
        Self {
            quantiles: Quantiles::None,
            moments: Moments::default(),
        }
    }

    /// Create new storage based on sampling policy
    pub fn from_policy(policy: &SamplingPolicy) -> Self {
        let quantiles = match policy {
            SamplingPolicy::None => Quantiles::None,
            SamplingPolicy::Limited { max_samples, rate } => {
                Quantiles::Samples(Reservoir::new(*max_samples, *rate))
            }
            SamplingPolicy::Unlimited => Quantiles::Samples(Reservoir::new(usize::MAX, 1.0)),
            SamplingPolicy::DdSketch { alpha, max_bins } => {
                let config = DDSketchConfig::new(*alpha, *max_bins, 1.0);
                Quantiles::DdSketch(DDSketch::new(config))
            }
            SamplingPolicy::TDigest { compression } => Quantiles::TDigest {
                digest: TDigest::new_with_size(*compression),
                pending: Vec::new(),
            },
            SamplingPolicy::HdrHistogram { sigfigs, max_value_us } => {
                let hist = match max_value_us {
                    None => Histogram::<u64>::new(*sigfigs),
                    Some(max_us) => Histogram::new_with_max(max_us.saturating_mul(1000), *sigfigs),
                }
                .expect("Failed to create HDR histogram");
                Quantiles::HdrHistogram { hist, clamped: 0 }
            }
        };
        Self { quantiles, moments: Moments::default() }
    }

    /// Record a latency sample
    pub fn record(&mut self, latency: Duration) {
        let ns = latency.as_nanos().min(u64::MAX as u128) as u64;
        match &mut self.quantiles {
            Quantiles::None => return,
            Quantiles::Samples(reservoir) => {
                if !reservoir.offer(ns) {
                    return;
                }
            }
            Quantiles::DdSketch(sketch) => sketch.add(ns as f64),
            Quantiles::TDigest { digest, pending } => {
                pending.push(ns as f64);
                if pending.len() >= TDIGEST_BATCH_SIZE {
                    // Hand the full buffer to the digest and start a new one.
                    let batch = std::mem::replace(pending, Vec::with_capacity(TDIGEST_BATCH_SIZE));
                    *digest = digest.merge_unsorted(batch);
                }
            }
            Quantiles::HdrHistogram { hist, clamped } => {
                // Only a bounded histogram (or a value beyond any resize)
                // rejects a value; record it at the bound and count it.
                if hist.record(ns).is_err() {
                    hist.saturating_record(ns);
                    *clamped += 1;
                }
            }
        }
        self.moments.record(ns);
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
    /// when policies differ. Limited samples are merged so the result is a
    /// uniform sample of both sides, weighted by how many latencies each side
    /// saw. None can only merge with None.
    pub fn try_merge_ref(&mut self, other: &Self) -> Result<(), &'static str> {
        match (&mut self.quantiles, &other.quantiles) {
            (Quantiles::None, Quantiles::None) => {}
            (Quantiles::Samples(mine), Quantiles::Samples(theirs))
                if mine.capacity == theirs.capacity && mine.rate == theirs.rate =>
            {
                mine.merge(theirs);
            }
            (Quantiles::DdSketch(sketch), Quantiles::DdSketch(other)) => {
                sketch.merge(other).map_err(|_| "DDSketch configurations differ")?;
            }
            (
                Quantiles::TDigest { digest, pending },
                Quantiles::TDigest { digest: other, pending: other_pending },
            ) if digest.max_size() == other.max_size() => {
                // Fold pending values into both digests first, so the merged
                // digest holds every value and this side has none pending.
                let mine = digest.merge_unsorted(std::mem::take(pending));
                let theirs = other.merge_unsorted(other_pending.clone());
                *digest = TDigest::merge_digests(vec![mine, theirs]);
            }
            (
                Quantiles::HdrHistogram { hist, clamped },
                Quantiles::HdrHistogram { hist: other, clamped: other_clamped },
            ) if hist.sigfig() == other.sigfig()
                && hist.is_auto_resize() == other.is_auto_resize()
                && (hist.is_auto_resize() || hist.high() == other.high()) =>
            {
                hist.add(other).map_err(|_| "HDR histogram merge failed")?;
                *clamped += other_clamped;
            }
            _ => return Err("latency storage configurations differ"),
        }
        self.moments.merge(&other.moments);
        Ok(())
    }

    /// Number of latencies recorded (after any sampling rate). For `limited`
    /// storage this can exceed the number of samples kept for percentiles.
    pub fn count(&self) -> u64 {
        self.moments.count
    }

    /// Number of latencies a bounded HDR histogram recorded at its maximum
    /// because they exceeded it. `None` for storage that never clamps.
    pub fn clamped_count(&self) -> Option<u64> {
        match &self.quantiles {
            Quantiles::HdrHistogram { hist, clamped } if !hist.is_auto_resize() || *clamped > 0 => {
                Some(*clamped)
            }
            _ => None,
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
        match &self.quantiles {
            Quantiles::None => vec![0; ps.len()],
            Quantiles::Samples(reservoir) => {
                if reservoir.samples.is_empty() {
                    return vec![0; ps.len()];
                }
                let mut sorted = reservoir.samples.clone();
                sorted.sort_unstable();
                let last = sorted.len() - 1;
                ps.iter()
                    .map(|&p| {
                        let idx = ((p * last as f64).round() as usize).min(last);
                        sorted[idx]
                    })
                    .collect()
            }
            Quantiles::DdSketch(sketch) => ps
                .iter()
                .map(|&p| sketch.quantile(p).ok().flatten().map(|v| v as u64).unwrap_or(0))
                .collect(),
            Quantiles::TDigest { digest, pending } => {
                let digest = tdigest_with_pending(digest, pending);
                ps.iter().map(|&p| digest.estimate_quantile(p) as u64).collect()
            }
            Quantiles::HdrHistogram { hist, .. } => {
                ps.iter().map(|&p| hist.value_at_quantile(p)).collect()
            }
        }
    }

    /// Exact mean of the recorded latencies (nanoseconds, rounded); 0 when empty.
    pub fn mean(&self) -> u64 {
        self.moments.mean().map_or(0, |mean| mean.round() as u64)
    }

    /// Smallest recorded latency (nanoseconds), if any.
    pub fn min(&self) -> Option<u64> {
        (self.moments.count > 0).then_some(self.moments.min)
    }

    /// Largest recorded latency (nanoseconds), if any.
    pub fn max(&self) -> Option<u64> {
        (self.moments.count > 0).then_some(self.moments.max)
    }

    /// Sample standard deviation of the recorded latencies (nanoseconds);
    /// `None` with fewer than two latencies.
    pub fn std_dev(&self) -> Option<f64> {
        self.moments.std_dev()
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

    /// Record the latency of a request completed after the measurement
    /// window, which throughput does not count
    pub fn record_drain_latency(&mut self, latency: Duration) {
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
    /// Worker thread that owns the connection
    pub thread_id: usize,
    /// Connection ID within the thread
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

// Empty collectors are neutral; an unusable worker interval poisons any aggregate.
#[derive(Clone, Copy, Default)]
enum ActualLoadLoop {
    #[default]
    Empty,
    Valid(u64, u64),
    Invalid,
}

impl ActualLoadLoop {
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Invalid, _) | (_, Self::Invalid) => Self::Invalid,
            (Self::Empty, value) | (value, Self::Empty) => value,
            (Self::Valid(a, b), Self::Valid(c, d)) => Self::Valid(a.min(c), b.max(d)),
        }
    }
}

/// Tuple-keyed statistics collector
///
/// Stores statistics keyed by (time_bucket, group_id, thread_id, connection_id) for flexible
/// on-demand aggregation by any dimension. Global latency storage is independent
/// of group storage: each recorded latency is sampled once under each policy.
/// This requires additional storage per bucket and allows heterogeneous
/// groups without converting or discarding their sketches.
pub struct TupleStatsCollector {
    /// Statistics entries keyed by (time_bucket, group_id, thread_id, connection_id)
    entries: HashMap<StatsKey, StatsEntry>,
    /// Thread recorded in the keys of new entries; see [`Self::set_thread_id`].
    thread_id: usize,
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
    /// Time of the first recorded event: the first measured request's send.
    /// Warmup requests are not recorded, so they fall before this.
    first_record_ns: Option<u64>,
    /// End of the measurement loop, set by [`StatsRecorder::end_measurement`].
    ///
    /// [`StatsRecorder::end_measurement`]: super::StatsRecorder::end_measurement
    measurement_end_ns: Option<u64>,
    /// Aggregate of worker loop intervals in Unix microseconds. Unusable
    /// samples invalidate the aggregate; empty collectors remain neutral.
    actual_load_loop: ActualLoadLoop,
    /// Request counts per group
    request_counts: HashMap<usize, RequestCounts>,
    /// Send lags of open-loop requests per group
    send_lags: HashMap<usize, SendLagStats>,
    /// Latencies of error replies per group, under the group's policy. They
    /// are not part of the latency distribution of the entries.
    error_latencies: HashMap<usize, LatencyStorage>,
    /// Latencies of every group's error replies, under the global policy
    global_error_latency: LatencyStorage,
    /// Latency breakdowns of measurement requests completed by successful
    /// responses per group; error replies are left out as from the entries
    breakdowns: HashMap<usize, BreakdownStats>,
}

impl TupleStatsCollector {
    /// Create a new tuple stats collector
    pub fn new(sampling_policy: SamplingPolicy, bucket_duration: Duration) -> Self {
        let global_error_latency = LatencyStorage::from_policy(&sampling_policy);
        Self {
            entries: HashMap::new(),
            thread_id: 0,
            global_entries: HashMap::new(),
            sampling_policy,
            group_sampling_policies: HashMap::new(),
            bucket_duration_ns: bucket_duration.as_nanos() as u64,
            experiment_start_ns: crate::timing::time_ns(),
            first_record_ns: None,
            measurement_end_ns: None,
            actual_load_loop: ActualLoadLoop::Empty,
            request_counts: HashMap::new(),
            send_lags: HashMap::new(),
            error_latencies: HashMap::new(),
            global_error_latency,
            breakdowns: HashMap::new(),
        }
    }

    /// Create with a specific start time (for testing or synchronized starts)
    pub fn with_start_time(
        sampling_policy: SamplingPolicy,
        bucket_duration: Duration,
        start_time_ns: u64,
    ) -> Self {
        let global_error_latency = LatencyStorage::from_policy(&sampling_policy);
        Self {
            entries: HashMap::new(),
            thread_id: 0,
            global_entries: HashMap::new(),
            sampling_policy,
            group_sampling_policies: HashMap::new(),
            bucket_duration_ns: bucket_duration.as_nanos() as u64,
            experiment_start_ns: start_time_ns,
            first_record_ns: None,
            measurement_end_ns: None,
            actual_load_loop: ActualLoadLoop::Empty,
            request_counts: HashMap::new(),
            send_lags: HashMap::new(),
            error_latencies: HashMap::new(),
            global_error_latency,
            breakdowns: HashMap::new(),
        }
    }

    /// Set the worker thread recorded with each connection's statistics.
    ///
    /// Each worker thread numbers its connections from 0, so collectors from
    /// different threads must have different thread IDs; otherwise merging
    /// them combines unrelated connections that share an ID. Defaults to 0.
    pub fn set_thread_id(&mut self, thread_id: usize) {
        self.thread_id = thread_id;
    }

    /// The worker thread recorded with new entries.
    pub fn thread_id(&self) -> usize {
        self.thread_id
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
        let mut now_ns = crate::timing::time_ns();
        if let Some(end_ns) = self.measurement_end_ns {
            // Responses drained after the window belong to its last bucket.
            now_ns = now_ns.min(end_ns.saturating_sub(1));
        }
        self.first_record_ns.get_or_insert(now_ns);
        let time_bucket = self.compute_bucket(now_ns);
        let policy = self.group_sampling_policies.get(&group_id).unwrap_or(&self.sampling_policy);
        let key = StatsKey {
            time_bucket,
            group_id,
            thread_id: self.thread_id,
            connection_id,
        };
        let entry = self.entries.entry(key).or_insert_with(|| StatsEntry::new(policy));
        let global = self
            .global_entries
            .entry(time_bucket)
            .or_insert_with(|| StatsEntry::new(&self.sampling_policy));
        (entry, global)
    }

    /// Record latency independently under the group and global sampling policies.
    ///
    /// After [`Self::end_measurement`], the request completed during the
    /// drain: its latency joins the distribution of the window's last bucket,
    /// but it does not count toward that bucket's requests or throughput.
    pub fn record_latency(&mut self, group_id: usize, connection_id: usize, latency: Duration) {
        self.request_counts_mut(group_id).completed += 1;
        let draining = self.measurement_end_ns.is_some();
        let (entry, global) = self.recording_entries(group_id, connection_id);
        if draining {
            entry.record_drain_latency(latency);
            global.record_drain_latency(latency);
        } else {
            entry.record_latency(latency);
            global.record_latency(latency);
        }
    }

    /// Record a measurement request completed by an error reply of `kind`
    ///
    /// It counts as completed, in the errors by kind, and toward its bucket's
    /// requests and throughput unless it completed during the drain. Its
    /// latency is recorded under the group and global policies apart from
    /// the entries, so the latency distribution holds successful responses
    /// only.
    pub fn record_error_reply(
        &mut self,
        group_id: usize,
        connection_id: usize,
        kind: &str,
        latency: Duration,
    ) {
        let counts = self.request_counts_mut(group_id);
        counts.completed += 1;
        counts.errors += 1;
        counts.errors_by_kind.add(kind, 1);
        let policy = self.group_sampling_policies.get(&group_id).unwrap_or(&self.sampling_policy);
        self.error_latencies
            .entry(group_id)
            .or_insert_with(|| LatencyStorage::from_policy(policy))
            .record(latency);
        self.global_error_latency.record(latency);
        let draining = self.measurement_end_ns.is_some();
        let (entry, global) = self.recording_entries(group_id, connection_id);
        if !draining {
            entry.request_count += 1;
            global.request_count += 1;
        }
    }

    /// Latency summary of a group's error replies
    pub fn error_latency(&self, group_id: usize) -> AggregatedStats {
        let empty;
        let latency = match self.error_latencies.get(&group_id) {
            Some(latency) => latency,
            None => {
                empty = LatencyStorage::from_policy(self.get_group_policy(group_id));
                &empty
            }
        };
        self.compute_aggregated_stats(latency, 0, 0, 0, Duration::ZERO)
    }

    /// Latency summary of every group's error replies together
    pub fn error_latency_global(&self) -> AggregatedStats {
        self.compute_aggregated_stats(&self.global_error_latency, 0, 0, 0, Duration::ZERO)
    }

    /// Record a sent measurement request with its send lag (`None` without
    /// an intended send time); lags above [`LATE_SEND_THRESHOLD`] are late.
    pub fn record_sent(&mut self, group_id: usize, lag: Option<Duration>) {
        let counts = self.request_counts.entry(group_id).or_default();
        counts.sent += 1;
        if let Some(lag) = lag {
            if lag > LATE_SEND_THRESHOLD {
                counts.late_sends += 1;
            }
            self.send_lags.entry(group_id).or_default().record(lag);
        }
    }

    /// Record where a completed measurement request's latency went. Like
    /// its latency, a request completed during the drain is included, and an
    /// error reply is not: its latency goes to the error latency instead.
    #[inline]
    pub fn record_breakdown(&mut self, group_id: usize, breakdown: &LatencyBreakdown) {
        self.breakdowns.entry(group_id).or_default().record(breakdown);
    }

    /// Request counts of a group, to add to
    pub fn request_counts_mut(&mut self, group_id: usize) -> &mut RequestCounts {
        self.request_counts.entry(group_id).or_default()
    }

    /// Request counts of a group; zero for a group that recorded none
    pub fn request_counts(&self, group_id: usize) -> RequestCounts {
        self.request_counts.get(&group_id).cloned().unwrap_or_default()
    }

    /// Request counts of every group together
    pub fn request_counts_global(&self) -> RequestCounts {
        let mut total = RequestCounts::default();
        for counts in self.request_counts.values() {
            total.merge(counts);
        }
        total
    }

    /// Send lags of a group's open-loop requests
    pub fn send_lag(&self, group_id: usize) -> SendLagSummary {
        self.send_lags.get(&group_id).map(SendLagStats::summary).unwrap_or_default()
    }

    /// Send lags of every group's open-loop requests together
    pub fn send_lag_global(&self) -> SendLagSummary {
        let mut total = SendLagStats::default();
        for lags in self.send_lags.values() {
            total.merge(lags);
        }
        total.summary()
    }

    /// Latency breakdown of a group's completed measurement requests
    pub fn latency_breakdown(&self, group_id: usize) -> BreakdownSummary {
        self.breakdowns.get(&group_id).map(BreakdownStats::summary).unwrap_or_default()
    }

    /// Latency breakdown of every group's completed measurement requests
    pub fn latency_breakdown_global(&self) -> BreakdownSummary {
        let mut total = BreakdownStats::default();
        for breakdowns in self.breakdowns.values() {
            total.merge(breakdowns);
        }
        total.summary()
    }

    /// Record transmitted bytes
    pub fn record_tx_bytes(&mut self, group_id: usize, connection_id: usize, bytes: usize) {
        let (entry, global) = self.recording_entries(group_id, connection_id);
        entry.record_tx_bytes(bytes);
        global.record_tx_bytes(bytes);
    }

    /// Record received bytes. Bytes of responses drained after the
    /// measurement window are not recorded, as they count toward no rate.
    pub fn record_rx_bytes(&mut self, group_id: usize, connection_id: usize, bytes: usize) {
        if self.measurement_end_ns.is_some() {
            return;
        }
        let (entry, global) = self.recording_entries(group_id, connection_id);
        entry.record_rx_bytes(bytes);
        global.record_rx_bytes(bytes);
    }

    /// Get all unique group IDs
    pub fn group_ids(&self) -> Vec<usize> {
        let mut ids: Vec<usize> = self.entries.keys().map(|k| k.group_id).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Get the `(thread_id, connection_id)` of every connection in a group, sorted
    pub fn connection_ids(&self, group_id: usize) -> Vec<(usize, usize)> {
        let mut ids: Vec<(usize, usize)> = self
            .entries
            .keys()
            .filter(|k| k.group_id == group_id)
            .map(|k| (k.thread_id, k.connection_id))
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

    /// Aggregate all buckets using the independently recorded global policy.
    /// Global sampling is independent of group sampling, including when groups
    /// disable sampling or use different sketch types.
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
    ) -> HashMap<(usize, usize), ConnectionStats> {
        let mut result = HashMap::new();

        for (thread_id, conn_id) in self.connection_ids(group_id) {
            let mut total_requests = 0u64;
            let mut total_tx_bytes = 0u64;
            let mut total_rx_bytes = 0u64;
            let mut merged_latency = LatencyStorage::from_policy(self.get_group_policy(group_id));

            for (key, entry) in &self.entries {
                if key.group_id == group_id
                    && key.thread_id == thread_id
                    && key.connection_id == conn_id
                {
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
                (thread_id, conn_id),
                ConnectionStats {
                    thread_id,
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
        let mut points: Vec<TimeSeriesPoint> = self
            .global_entries
            .iter()
            .map(|(&bucket, entry)| {
                let bucket_duration_secs = self.bucket_width(bucket).as_secs_f64();
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
                let bucket_duration_secs = self.bucket_width(bucket).as_secs_f64();
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
        thread_id: usize,
        connection_id: usize,
    ) -> Vec<TimeSeriesPoint> {
        let mut points: Vec<TimeSeriesPoint> = self
            .entries
            .iter()
            .filter(|(k, _)| {
                k.group_id == group_id
                    && k.thread_id == thread_id
                    && k.connection_id == connection_id
            })
            .map(|(key, entry)| {
                let bucket_duration_secs = self.bucket_width(key.time_bucket).as_secs_f64();
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
    pub fn merge(collectors: Vec<Self>) -> Self {
        if collectors.is_empty() {
            return Self::new(SamplingPolicy::default(), Duration::from_secs(1));
        }

        let first = &collectors[0];
        let mut merged = Self {
            entries: HashMap::new(),
            thread_id: 0,
            global_entries: HashMap::new(),
            sampling_policy: first.sampling_policy.clone(),
            group_sampling_policies: HashMap::new(),
            bucket_duration_ns: first.bucket_duration_ns,
            experiment_start_ns: first.experiment_start_ns,
            first_record_ns: collectors.iter().filter_map(|c| c.first_record_ns).min(),
            measurement_end_ns: collectors.iter().filter_map(|c| c.measurement_end_ns).max(),
            actual_load_loop: collectors
                .iter()
                .fold(ActualLoadLoop::Empty, |combined, c| combined.merge(c.actual_load_loop)),
            request_counts: HashMap::new(),
            send_lags: HashMap::new(),
            error_latencies: HashMap::new(),
            global_error_latency: LatencyStorage::from_policy(&first.sampling_policy),
            breakdowns: HashMap::new(),
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
            for (group_id, counts) in &collector.request_counts {
                merged.request_counts.entry(*group_id).or_default().merge(counts);
            }
            for (group_id, lags) in &collector.send_lags {
                merged.send_lags.entry(*group_id).or_default().merge(lags);
            }
            for (group_id, latency) in &collector.error_latencies {
                let policy =
                    merged.group_sampling_policies.get(group_id).unwrap_or(&merged.sampling_policy);
                merged
                    .error_latencies
                    .entry(*group_id)
                    .or_insert_with(|| LatencyStorage::from_policy(policy))
                    .merge_ref(latency);
            }
            merged.global_error_latency.merge_ref(&collector.global_error_latency);
            for (group_id, breakdowns) in &collector.breakdowns {
                merged.breakdowns.entry(*group_id).or_default().merge(breakdowns);
            }
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

    /// Get raw entries
    pub fn entries(&self) -> &HashMap<StatsKey, StatsEntry> {
        &self.entries
    }

    /// Take ownership of entries
    pub fn into_entries(self) -> HashMap<StatsKey, StatsEntry> {
        self.entries
    }

    /// Take the start of the measurement loop, shared by every thread, as the
    /// epoch of the time buckets. Ignored once something was recorded, whose
    /// bucket was computed from the previous epoch.
    pub fn start_measurement(&mut self, start_ns: u64) {
        if self.first_record_ns.is_none() {
            self.experiment_start_ns = start_ns;
        }
    }

    /// Record the end of the measurement loop; a merge keeps the latest end.
    pub fn end_measurement(&mut self, end_ns: u64) {
        self.measurement_end_ns = Some(self.measurement_end_ns.map_or(end_ns, |e| e.max(end_ns)));
    }

    /// Record an actual load-loop interval in Unix microseconds.
    /// Reversed values or values above `2^53 - 1` invalidate this interval and
    /// any merged interval containing it.
    pub fn record_actual_load_loop(&mut self, start_us: u64, end_us: u64) {
        self.record_actual_load_loop_samples(Some(start_us), Some(end_us));
    }

    /// Record a worker's start and end wall-clock samples in Unix microseconds.
    /// A missing sample, a start after the end, or a sample above `2^53 - 1`
    /// invalidates this interval and every merged interval containing it.
    /// Collectors with no recorded interval remain neutral when merged.
    pub fn record_actual_load_loop_samples(&mut self, start_us: Option<u64>, end_us: Option<u64>) {
        const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
        let sample = match (start_us, end_us) {
            (Some(start), Some(end)) if start <= end && end <= MAX_SAFE_INTEGER => {
                ActualLoadLoop::Valid(start, end)
            }
            _ => ActualLoadLoop::Invalid,
        };
        self.actual_load_loop = self.actual_load_loop.merge(sample);
    }

    /// Earliest worker loop start and latest worker loop end, in Unix microseconds.
    /// The merged interval includes gaps between worker loops. Returns `None`
    /// if no interval was recorded or any worker's samples are unusable. These
    /// readings do not imply alignment with any other host's clock.
    pub fn actual_load_loop_unix_us(&self) -> Option<(u64, u64)> {
        match self.actual_load_loop {
            ActualLoadLoop::Valid(start, end) => Some((start, end)),
            ActualLoadLoop::Empty | ActualLoadLoop::Invalid => None,
        }
    }

    /// The measurement window in nanoseconds: from the first recorded event
    /// (the first measured send, after any warmup) to the end of the
    /// measurement loop. `None` until both are known.
    pub fn measurement_window_ns(&self) -> Option<(u64, u64)> {
        let (start, end) = (self.first_record_ns?, self.measurement_end_ns?);
        Some((start, end.max(start)))
    }

    /// Length of [`Self::measurement_window_ns`]. Divide by this, not the
    /// configured duration, so warmup and setup time do not lower throughput.
    pub fn measured_duration(&self) -> Option<Duration> {
        self.measurement_window_ns()
            .map(|(start, end)| Duration::from_nanos(end - start))
    }

    /// Time a bucket overlaps the measurement window, so the partial first
    /// and last buckets report rates over the time they covered. Responses
    /// drained after the window are recorded in its last bucket, so no bucket
    /// lies outside it; one that did, or any bucket without a known window,
    /// would use the full width.
    pub fn bucket_width(&self, time_bucket: u64) -> Duration {
        let full = self.bucket_duration_ns;
        let Some((start, end)) = self.measurement_window_ns() else {
            return Duration::from_nanos(full);
        };
        let bucket_start =
            self.experiment_start_ns.saturating_add(time_bucket.saturating_mul(full));
        let bucket_end = bucket_start.saturating_add(full);
        let overlap = bucket_end.min(end).saturating_sub(bucket_start.max(start));
        Duration::from_nanos(if overlap > 0 { overlap } else { full })
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
        // Seconds, for Duration::from_secs_f64.
        let std_dev = latency.std_dev().map(|ns| ns / 1e9);

        AggregatedStats {
            latency_p50: Duration::from_nanos(latency_percentiles[0]),
            latency_p95: Duration::from_nanos(latency_percentiles[1]),
            latency_p99: Duration::from_nanos(latency_percentiles[2]),
            latency_p999: Duration::from_nanos(latency_percentiles[3]),
            latency_p9999: Duration::from_nanos(latency_percentiles[4]),
            latency_p99999: Duration::from_nanos(latency_percentiles[5]),
            mean_latency: Duration::from_nanos(latency.mean()),
            min_latency: latency.min().map(Duration::from_nanos),
            max_latency: latency.max().map(Duration::from_nanos),
            std_dev: std_dev.map(Duration::from_secs_f64),
            confidence_interval: std_dev
                .map(|sd| Duration::from_secs_f64(1.96 * sd / (latency.count() as f64).sqrt())),
            latency_sample_count: latency.count(),
            clamped_samples: latency.clamped_count(),
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
            SamplingPolicy::HdrHistogram { sigfigs: 3, max_value_us: Some(10_000) },
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
                let connection = &collector.aggregate_by_connection(group, duration)[&(0, 0)];
                assert_eq!((connection.tx_bytes, connection.rx_bytes), (100, 200));
                close(connection.latency.latency_p99.as_nanos() as u64);
                close(collector.time_series_by_group(group)[0].latency_p99_ns);
                close(collector.time_series_by_connection(group, 0, 0)[0].latency_p99_ns);
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
        assert!(LatencyStorage::none().try_merge_ref(&samples).is_err());
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
            thread_id: 0,
            connection_id: 2,
        };
        let k2 = StatsKey {
            time_bucket: 0,
            group_id: 1,
            thread_id: 0,
            connection_id: 2,
        };
        let k3 = StatsKey {
            time_bucket: 1,
            group_id: 1,
            thread_id: 0,
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
    fn adaptive_sampling_policy_is_rejected() {
        let error = serde_json::from_str::<SamplingPolicy>(
            r#"{"type": "adaptive", "initial_rate": 1.0, "target_ci_width_us": 10}"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("unknown variant `adaptive`"), "{error}");
    }

    fn storage_with(
        policy: &SamplingPolicy,
        latencies_ns: impl IntoIterator<Item = u64>,
    ) -> LatencyStorage {
        let mut storage = LatencyStorage::from_policy(policy);
        for ns in latencies_ns {
            storage.record(Duration::from_nanos(ns));
        }
        storage
    }

    /// Ten buckets of 20,000 latencies each; bucket b holds (b + 1) ms, so
    /// only an aggregate over every bucket has a median near 5.5 ms.
    fn collector_with_ten_buckets(policy: SamplingPolicy) -> TupleStatsCollector {
        let mut collector =
            TupleStatsCollector::with_start_time(policy.clone(), Duration::from_secs(1), 0);
        for bucket in 0..10u64 {
            let mut entry = StatsEntry::new(&policy);
            for _ in 0..20_000 {
                entry.record_latency(Duration::from_millis(bucket + 1));
            }
            collector.global_entries.insert(bucket, entry);
        }
        collector
    }

    #[test]
    fn percentiles_beyond_max_samples_reflect_every_bucket() {
        for policy in [
            SamplingPolicy::default(),
            SamplingPolicy::Limited { max_samples: 128 * 1024, rate: 1.0 },
            SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 },
        ] {
            let stats = collector_with_ten_buckets(policy.clone())
                .aggregate_global(Duration::from_secs(10));
            assert_eq!(stats.total_requests, 200_000, "{policy:?}");
            assert_eq!(stats.latency_sample_count, 200_000, "{policy:?}");
            // Buckets 5 and 6 straddle the median; the old first-come merge
            // reported whichever buckets the HashMap visited first.
            let p50 = stats.latency_p50.as_micros();
            assert!((5_000..=6_010).contains(&p50), "{policy:?}: p50 {p50} us");
            let p99 = stats.latency_p99.as_micros();
            assert!((9_990..=10_010).contains(&p99), "{policy:?}: p99 {p99} us");
            assert_eq!(stats.mean_latency, Duration::from_micros(5_500), "{policy:?}");
            assert_eq!(stats.min_latency, Some(Duration::from_millis(1)), "{policy:?}");
            assert_eq!(stats.max_latency, Some(Duration::from_millis(10)), "{policy:?}");
        }
    }

    #[test]
    fn limited_reservoir_is_uniform_and_counts_every_latency() {
        let policy = SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 };
        let storage = storage_with(&policy, 0..100_000);
        assert_eq!(storage.count(), 100_000);
        let Quantiles::Samples(reservoir) = &storage.quantiles else {
            unreachable!()
        };
        assert_eq!(reservoir.samples.len(), 1000);
        // Keeping the first 1,000 values would put the median at about 500.
        let p50 = storage.percentile(0.5);
        assert!((40_000..=60_000).contains(&p50), "p50 {p50}");
        assert_eq!(storage.mean(), 49_999 + 1); // 49,999.5 rounded
    }

    #[test]
    fn limited_merge_weights_each_side_by_observed_count() {
        let policy = SamplingPolicy::Limited { max_samples: 1000, rate: 1.0 };
        let many = storage_with(&policy, std::iter::repeat_n(1, 90_000));
        let few = storage_with(&policy, std::iter::repeat_n(2, 10_000));
        for (first, second) in [(&many, &few), (&few, &many)] {
            let mut merged = LatencyStorage::from_policy(&policy);
            merged.merge_ref(first);
            merged.merge_ref(second);
            assert_eq!(merged.count(), 100_000);
            let Quantiles::Samples(reservoir) = &merged.quantiles else {
                unreachable!()
            };
            assert_eq!(reservoir.samples.len(), 1000);
            assert_eq!(reservoir.seen, 100_000);
            // Expect 100 twos (10%); the standard deviation is under 10.
            let twos = reservoir.samples.iter().filter(|&&ns| ns == 2).count();
            assert!((40..=160).contains(&twos), "{twos} of 1000 samples from the 10% side");
            assert_eq!(merged.mean(), 1); // 1.1 rounded
            assert_eq!((merged.min(), merged.max()), (Some(1), Some(2)));
        }
    }

    #[test]
    fn hdr_merge_is_independent_of_order() {
        let policy = SamplingPolicy::default();
        let parts: Vec<LatencyStorage> = (0..5u64)
            .map(|part| storage_with(&policy, (0..10_000).map(|i| 1_000 + part * 7_919 + i * 13)))
            .collect();
        let merge = |order: &[usize]| {
            let mut merged = LatencyStorage::from_policy(&policy);
            for &i in order {
                merged.merge_ref(&parts[i]);
            }
            merged
        };
        let forward = merge(&[0, 1, 2, 3, 4]);
        let backward = merge(&[4, 3, 2, 1, 0]);
        let shuffled = merge(&[2, 0, 4, 1, 3]);
        let quantiles = [0.0, 0.5, 0.9, 0.99, 0.999, 1.0];
        for other in [&backward, &shuffled] {
            assert_eq!(forward.percentiles(&quantiles), other.percentiles(&quantiles));
            assert_eq!(forward.moments, other.moments);
        }
        assert_eq!(forward.count(), 50_000);
    }

    #[test]
    fn hdr_never_drops_out_of_range_values() {
        // The default histogram resizes: two hours exceeds the former 1 h bound.
        let two_hours = Duration::from_secs(7_200).as_nanos() as u64;
        let storage = storage_with(&SamplingPolicy::default(), [1_000, two_hours]);
        assert_eq!(storage.count(), 2);
        assert_eq!(storage.max(), Some(two_hours));
        assert!(storage.percentile(1.0).abs_diff(two_hours) * 1000 <= two_hours);
        assert_eq!(storage.clamped_count(), None);

        // A configured bound clamps and counts instead of discarding.
        let bounded = SamplingPolicy::HdrHistogram { sigfigs: 3, max_value_us: Some(1_000) };
        let storage = storage_with(&bounded, [500_000, 5_000_000, 7_000_000]);
        assert_eq!(storage.count(), 3);
        assert_eq!(storage.clamped_count(), Some(2));
        let p100 = storage.percentile(1.0);
        assert!(p100.abs_diff(1_000_000) <= 1_000, "clamped to the bound: {p100}");
        // Exact moments still see the true values.
        assert_eq!(storage.max(), Some(7_000_000));
        let mut merged = LatencyStorage::from_policy(&bounded);
        merged.merge_ref(&storage);
        merged.merge_ref(&storage);
        assert_eq!(merged.clamped_count(), Some(4));
        assert_eq!(LatencyStorage::from_policy(&bounded).clamped_count(), Some(0));
    }

    #[test]
    fn mean_is_exact_for_a_skewed_distribution() {
        // 99 x 1 us and 1 x 10 ms: the median is 1 us, the mean 100.99 us.
        let latencies = std::iter::repeat_n(1_000, 99).chain([10_000_000]);
        let latencies: Vec<u64> = latencies.collect();
        for policy in [
            SamplingPolicy::default(),
            SamplingPolicy::Unlimited,
            SamplingPolicy::Limited { max_samples: 10, rate: 1.0 },
            SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 1024 },
            SamplingPolicy::TDigest { compression: 100 },
        ] {
            let storage = storage_with(&policy, latencies.iter().copied());
            assert_eq!(storage.mean(), 100_990, "{policy:?}");
        }
    }

    #[test]
    fn aggregated_stats_report_std_dev_and_confidence_interval() {
        let mut collector =
            TupleStatsCollector::new(SamplingPolicy::default(), Duration::from_secs(3600));
        for us in [2, 4, 4, 4, 5, 5, 7, 9] {
            collector.record_latency(0, 0, Duration::from_micros(us));
        }
        let stats = collector.aggregate_global(Duration::from_secs(1));
        // Sample variance: sum of squared deviations 32 over n - 1 = 7.
        let sd = (32.0f64 / 7.0).sqrt();
        let sd_us = stats.std_dev.unwrap().as_secs_f64() * 1e6;
        // Durations hold whole nanoseconds.
        assert!((sd_us - sd).abs() <= 1e-3, "{sd_us}");
        let ci_us = stats.confidence_interval.unwrap().as_secs_f64() * 1e6;
        assert!((ci_us - 1.96 * sd / 8f64.sqrt()).abs() <= 1e-3, "{ci_us}");
        assert_eq!(stats.latency_sample_count, 8);
        assert_eq!(stats.min_latency, Some(Duration::from_micros(2)));
        assert_eq!(stats.max_latency, Some(Duration::from_micros(9)));
        assert_eq!(stats.clamped_samples, None);

        // Unavailable values are None rather than zero.
        let mut single =
            TupleStatsCollector::new(SamplingPolicy::default(), Duration::from_secs(3600));
        single.record_latency(0, 0, Duration::from_micros(3));
        let stats = single.aggregate_global(Duration::from_secs(1));
        assert_eq!((stats.std_dev, stats.confidence_interval), (None, None));
        let mut none = TupleStatsCollector::new(SamplingPolicy::None, Duration::from_secs(3600));
        none.record_latency(0, 0, Duration::from_micros(3));
        let stats = none.aggregate_global(Duration::from_secs(1));
        assert_eq!(stats.total_requests, 1);
        assert_eq!((stats.min_latency, stats.max_latency, stats.std_dev), (None, None, None));
        assert_eq!(stats.latency_sample_count, 0);
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
        // Min and max are tracked exactly beside the digest.
        assert_eq!(storage.min(), latencies_ns.iter().min().copied());
        assert_eq!(storage.max(), latencies_ns.iter().max().copied());
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
        assert_eq!(collector.connection_ids(0), vec![(0, 0), (0, 1)]);
        assert_eq!(collector.connection_ids(1), vec![(0, 0)]);
    }

    #[test]
    fn actual_load_loop_merge_uses_valid_intervals_and_empty_collectors_are_neutral() {
        use crate::stats::StatsRecorder;

        // An existing recorder need not implement the new hook, even if it has
        // an unrelated method named `record_actual_load_loop`.
        struct DefaultRecorder;
        impl DefaultRecorder {
            fn record_actual_load_loop(&mut self, _: &str) {}
        }
        impl StatsRecorder for DefaultRecorder {
            fn record_latency(&mut self, _: usize, _: usize, _: Duration) {}
            fn record_tx_bytes(&mut self, _: usize, _: usize, _: usize) {}
            fn record_rx_bytes(&mut self, _: usize, _: usize, _: usize) {}
        }
        let mut recorder = DefaultRecorder;
        recorder.record_actual_load_loop("unrelated");
        StatsRecorder::record_actual_load_loop_samples(&mut recorder, None, Some(1));

        let mut a = TupleStatsCollector::default();
        a.record_actual_load_loop(200, 400);
        let mut b = TupleStatsCollector::default();
        StatsRecorder::record_actual_load_loop_samples(&mut b, Some(100), Some(150));
        let merged = TupleStatsCollector::merge(vec![a, TupleStatsCollector::default(), b]);
        assert_eq!(merged.actual_load_loop_unix_us(), Some((100, 400)), "gaps are included");
        assert_eq!(TupleStatsCollector::merge(vec![]).actual_load_loop_unix_us(), None);

        let mut boundary = TupleStatsCollector::default();
        boundary.record_actual_load_loop((1 << 53) - 1, (1 << 53) - 1);
        assert_eq!(boundary.actual_load_loop_unix_us(), Some(((1 << 53) - 1, (1 << 53) - 1)));
    }

    #[test]
    fn actual_load_loop_invalid_samples_invalidate_both_boundaries_across_merges() {
        for (start, end) in [
            (Some(100), Some(90)),
            (None, Some(120)),
            (Some(100), None),
            (None, None),
            (Some(1), Some(1 << 53)),
            (Some(1 << 53), Some(1 << 53)),
        ] {
            for invalid_first in [false, true] {
                let mut valid = TupleStatsCollector::default();
                valid.record_actual_load_loop(110, 120);
                let mut invalid = TupleStatsCollector::default();
                invalid.record_actual_load_loop_samples(start, end);
                // Once invalid, another sample in the same collector cannot restore it.
                invalid.record_actual_load_loop(100, 200);
                let workers = if invalid_first {
                    vec![invalid, valid]
                } else {
                    vec![valid, invalid]
                };
                let merged = TupleStatsCollector::merge(workers);
                assert_eq!(merged.actual_load_loop_unix_us(), None, "{start:?} {end:?}");
                let mut later = TupleStatsCollector::default();
                later.record_actual_load_loop(130, 140);
                assert_eq!(
                    TupleStatsCollector::merge(vec![merged, later]).actual_load_loop_unix_us(),
                    None
                );
            }
        }
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

        assert_eq!(merged.connection_ids(0), vec![(0, 0), (0, 1)]);
    }

    #[test]
    fn connections_of_different_threads_stay_distinct_after_merge() {
        let start = crate::timing::time_ns();
        let collector_for = |thread_id: usize, latency_us: u64, requests: usize| {
            let mut collector = TupleStatsCollector::with_start_time(
                SamplingPolicy::default(),
                Duration::from_secs(3600),
                start,
            );
            collector.set_thread_id(thread_id);
            for _ in 0..requests {
                collector.record_tx_bytes(0, 0, 10);
                collector.record_latency(0, 0, Duration::from_micros(latency_us));
            }
            collector
        };
        // Both threads' first connection has ID 0.
        let merged =
            TupleStatsCollector::merge(vec![collector_for(2, 10, 3), collector_for(5, 90, 7)]);
        assert_eq!(merged.connection_ids(0), vec![(2, 0), (5, 0)]);
        let connections = merged.aggregate_by_connection(0, Duration::from_secs(1));
        let a = &connections[&(2, 0)];
        let b = &connections[&(5, 0)];
        assert_eq!((a.thread_id, a.request_count, a.tx_bytes), (2, 3, 30));
        assert_eq!((b.thread_id, b.request_count, b.tx_bytes), (5, 7, 70));
        assert_eq!(a.latency.max_latency, Some(Duration::from_micros(10)));
        assert_eq!(b.latency.min_latency, Some(Duration::from_micros(90)));
        assert_eq!(merged.time_series_by_connection(0, 5, 0)[0].request_count, 7);
        // Group and global totals still include both threads.
        assert_eq!(merged.aggregate_global(Duration::from_secs(1)).total_requests, 10);
        assert_eq!(merged.aggregate_by_group(Duration::from_secs(1))[&0].total_requests, 10);
    }

    #[test]
    fn measurement_window_merges_and_sets_edge_bucket_widths() {
        const SEC: u64 = 1_000_000_000;
        let epoch = 1_000 * SEC;
        let collector = |first_s: f64, end_s: f64| {
            let mut c = TupleStatsCollector::with_start_time(
                SamplingPolicy::default(),
                Duration::from_secs(1),
                epoch,
            );
            c.first_record_ns = Some(epoch + (first_s * SEC as f64) as u64);
            c.end_measurement(epoch + (end_s * SEC as f64) as u64);
            c
        };
        // No window without a recorded event and an end.
        let empty = TupleStatsCollector::with_start_time(
            SamplingPolicy::default(),
            Duration::from_secs(1),
            epoch,
        );
        assert_eq!(empty.measured_duration(), None);
        assert_eq!(empty.bucket_width(0), Duration::from_secs(1));

        // Thread A measures from 0.5 s to 3.25 s, thread B from 0.25 s to 3.0 s.
        let merged = TupleStatsCollector::merge(vec![collector(0.5, 3.25), collector(0.25, 3.0)]);
        assert_eq!(merged.measurement_window_ns(), Some((epoch + SEC / 4, epoch + 13 * SEC / 4)));
        assert_eq!(merged.measured_duration(), Some(Duration::from_secs(3)));
        assert_eq!(merged.bucket_width(0), Duration::from_millis(750));
        assert_eq!(merged.bucket_width(1), Duration::from_secs(1));
        assert_eq!(merged.bucket_width(3), Duration::from_millis(250));
        // Only drained responses: outside the window, full width.
        assert_eq!(merged.bucket_width(4), Duration::from_secs(1));

        // A bucket's rate divides by the time it covered.
        let mut merged = merged;
        let mut entry = StatsEntry::new(&SamplingPolicy::default());
        for _ in 0..500 {
            entry.record_latency(Duration::from_micros(10));
        }
        merged.global_entries.insert(3, entry);
        let point = &merged.time_series_global()[0];
        assert_eq!((point.time_bucket, point.throughput_rps), (3, 2_000.0));
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

    #[test]
    fn late_sends_and_send_lags_are_counted_per_group_and_merged() {
        let mut first = TupleStatsCollector::default();
        first.record_sent(0, None); // closed loop: no lag
        first.record_sent(0, Some(Duration::from_micros(5)));
        first.record_sent(0, Some(LATE_SEND_THRESHOLD)); // not above the threshold
        first.record_sent(0, Some(Duration::from_millis(3)));
        let mut second = TupleStatsCollector::default();
        second.set_thread_id(1);
        second.record_sent(1, Some(Duration::from_millis(7)));
        second.request_counts_mut(1).unsent = 4;
        second.request_counts_mut(1).unsent_capped = true;

        let merged = TupleStatsCollector::merge(vec![first, second]);
        let group0 = merged.request_counts(0);
        assert_eq!((group0.sent, group0.late_sends, group0.unsent_capped), (4, 1, false));
        assert_eq!(merged.send_lag(0).sample_count, 3);
        assert_eq!(merged.send_lag(0).max, Some(Duration::from_millis(3)));
        let global = merged.request_counts_global();
        assert_eq!((global.sent, global.late_sends, global.unsent), (5, 2, 4));
        assert!(global.unsent_capped);
        let lag = merged.send_lag_global();
        assert_eq!((lag.sample_count, lag.max), (4, Some(Duration::from_millis(7))));
        assert_eq!(merged.send_lag(9), SendLagSummary::default());
    }

    #[test]
    fn latency_breakdowns_are_kept_per_group_and_merged_across_threads() {
        use crate::stats::{BreakdownClock, KernelSplit};
        use xylem_transport::KernelClock;
        let split = |host_us, network_us| LatencyBreakdown {
            service: Duration::from_micros(network_us),
            kernel: Some(KernelSplit {
                clock: KernelClock::Realtime,
                network_server: Duration::from_micros(network_us),
                host_stack: Duration::from_micros(host_us),
            }),
        };
        let user = |us| LatencyBreakdown {
            service: Duration::from_micros(us),
            kernel: None,
        };
        let mut first = TupleStatsCollector::default();
        first.record_breakdown(0, &split(5, 20));
        first.record_breakdown(1, &user(40));
        let mut second = TupleStatsCollector::default();
        second.set_thread_id(1);
        second.record_breakdown(0, &split(7, 30));
        second.end_measurement(crate::timing::time_ns());
        // Requests completed during the drain count, like their latencies
        second.record_breakdown(0, &split(9, 50));

        let merged = TupleStatsCollector::merge(vec![first, second]);
        let group0 = merged.latency_breakdown(0);
        assert_eq!(group0.clock, Some(BreakdownClock::KernelSoftware));
        assert_eq!((group0.kernel_split_count, group0.service.sample_count), (3, 3));
        assert_eq!(group0.service.mean, Some(Duration::from_micros(100) / 3));
        assert_eq!(group0.host_stack.unwrap().max, Some(Duration::from_micros(9)));
        assert_eq!(group0.network_server.unwrap().max, Some(Duration::from_micros(50)));
        let group1 = merged.latency_breakdown(1);
        assert_eq!(group1.clock, Some(BreakdownClock::UserSpace));
        assert_eq!((group1.kernel_split_count, group1.host_stack), (0, None));

        let global = merged.latency_breakdown_global();
        assert_eq!(global.clock, Some(BreakdownClock::Mixed));
        assert_eq!((global.kernel_split_count, global.service.sample_count), (3, 4));
        assert_eq!(global.service.max, Some(Duration::from_micros(50)));
        assert_eq!(merged.latency_breakdown(9), BreakdownSummary::default());
    }

    #[test]
    fn drained_latency_joins_the_last_bucket_without_counting_toward_throughput() {
        let start = crate::timing::time_ns();
        let mut collector = TupleStatsCollector::with_start_time(
            SamplingPolicy::Unlimited,
            Duration::from_secs(3600),
            start,
        );
        collector.record_tx_bytes(0, 0, 10);
        collector.record_latency(0, 0, Duration::from_micros(10));
        collector.record_rx_bytes(0, 0, 10);
        collector.end_measurement(crate::timing::time_ns());
        collector.record_latency(0, 0, Duration::from_micros(30));
        collector.record_rx_bytes(0, 0, 10);
        let global = collector.aggregate_global(Duration::from_secs(1));
        assert_eq!((global.total_requests, global.latency_sample_count), (1, 2));
        assert_eq!(global.max_latency, Some(Duration::from_micros(30)));
        let group = &collector.aggregate_by_group(Duration::from_secs(1))[&0];
        assert_eq!((group.total_requests, group.latency_sample_count), (1, 2));
        assert_eq!(collector.time_buckets(), vec![0]);
        assert_eq!(collector.entries().values().map(|e| e.rx_bytes).sum::<u64>(), 10);
        assert_eq!(collector.request_counts(0).completed, 2);
    }

    #[test]
    fn error_replies_are_completed_requests_with_a_separate_latency() {
        let group_policy = SamplingPolicy::Limited { max_samples: 10, rate: 1.0 };
        let collector = |thread_id| {
            let mut collector = TupleStatsCollector::with_start_time(
                SamplingPolicy::Unlimited,
                Duration::from_secs(1),
                crate::timing::time_ns(),
            );
            collector.register_group_policy(1, group_policy.clone());
            collector.set_thread_id(thread_id);
            collector
        };
        let mut first = collector(0);
        first.record_latency(1, 0, Duration::from_micros(100));
        first.record_error_reply(1, 0, "WRONGTYPE", Duration::from_micros(5));
        let mut second = collector(1);
        second.record_error_reply(1, 0, "OOM", Duration::from_micros(7));
        second.end_measurement(crate::timing::time_ns());
        // Drained after the window: completed, but not throughput
        second.record_error_reply(1, 0, "OOM", Duration::from_micros(9));
        let merged = TupleStatsCollector::merge(vec![first, second]);

        let counts = merged.request_counts(1);
        assert_eq!((counts.completed, counts.errors), (4, 3));
        assert_eq!(counts.errors_by_kind.get("OOM"), 2);
        assert_eq!(counts.errors_by_kind.get("WRONGTYPE"), 1);
        assert_eq!(merged.request_counts_global(), counts);
        for stats in [
            merged.aggregate_by_group(Duration::from_secs(1))[&1].clone(),
            merged.aggregate_global(Duration::from_secs(1)),
        ] {
            assert_eq!(stats.total_requests, 3);
            assert_eq!(stats.latency_sample_count, 1);
            assert_eq!(stats.max_latency, Some(Duration::from_micros(100)));
        }
        for errors in [merged.error_latency(1), merged.error_latency_global()] {
            assert_eq!(errors.latency_sample_count, 3);
            assert_eq!(errors.min_latency, Some(Duration::from_micros(5)));
            assert_eq!(errors.max_latency, Some(Duration::from_micros(9)));
            assert_eq!(errors.mean_latency, Duration::from_micros(7));
        }
        // A group without error replies has an empty summary
        assert_eq!(merged.error_latency(7).latency_sample_count, 0);
    }
}
