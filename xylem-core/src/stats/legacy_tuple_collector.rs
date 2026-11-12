//! The tuple collector as of 112813b, before per-bucket storage: the
//! reference the equivalence tests compare the collector against
#![allow(dead_code)]

use super::breakdown::{BreakdownStats, BreakdownSummary, LatencyBreakdown};
use super::policy::SamplingPolicy;
use super::requests::{RequestCounts, SendLagStats, SendLagSummary, LATE_SEND_THRESHOLD};
use super::AggregatedStats;
use hdrhistogram::Histogram;
use sketches_ddsketch::{Config as DDSketchConfig, DDSketch};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::time::Duration;
use tdigest::TDigest;
use xylem_common::FastHashMap;

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

    /// Empty storage configured like this one; `None` for the policies
    /// whose new storage costs little. An auto-resizing HDR histogram gets
    /// counts up to twice the largest latency this one holds, so latencies
    /// up to that do not make it grow; its range sets only how many counts it
    /// holds, not where values go. The range follows the latencies, not this
    /// one's range, which could hold more counts than they need.
    fn empty_like(&self) -> Option<Self> {
        let Quantiles::HdrHistogram { hist, .. } = &self.quantiles else {
            return None;
        };
        let hist = if hist.is_auto_resize() {
            let high = hist.max().max(hist.low()).saturating_mul(2);
            Histogram::new_with_bounds(hist.low(), high, hist.sigfig())
                .map(|mut wider| {
                    wider.auto(true);
                    wider
                })
                .unwrap_or_else(|_| Histogram::new_from(hist))
        } else {
            Histogram::new_from(hist)
        };
        Some(Self {
            quantiles: Quantiles::HdrHistogram { hist, clamped: 0 },
            moments: Moments::default(),
        })
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
                // An all-empty dependency merge returns its default size, not
                // the configured size. Keep this digest after checking compatibility.
                if !digest.is_empty()
                    || !pending.is_empty()
                    || !other.is_empty()
                    || !other_pending.is_empty()
                {
                    // Fold both buffers into the merged digest.
                    let mine = digest.merge_unsorted(std::mem::take(pending));
                    let theirs = other.merge_unsorted(other_pending.clone());
                    *digest = TDigest::merge_digests(vec![mine, theirs]);
                }
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

    /// Observations represented by the percentile estimator, after merges.
    /// Sketches retain observation counts even when values are compressed;
    /// the t-digest count includes values still waiting in its buffer.
    pub fn percentile_sample_count(&self) -> u64 {
        match &self.quantiles {
            Quantiles::None => 0,
            Quantiles::Samples(reservoir) => reservoir.samples.len() as u64,
            // Every observation reaches the sketch, including collapsed bins.
            // Its count() returns usize and saturates on 32-bit targets.
            Quantiles::DdSketch(_) => self.moments.count,
            // Moments count every value inserted into the digest or its pending
            // buffer, without converting the digest's floating-point count.
            Quantiles::TDigest { .. } => self.moments.count,
            Quantiles::HdrHistogram { hist, .. } => hist.len(),
        }
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

    /// An empty entry whose latency storage is like this one's; see
    /// [`LatencyStorage::empty_like`]
    fn empty_like(&self) -> Option<Self> {
        Some(Self {
            request_count: 0,
            tx_bytes: 0,
            rx_bytes: 0,
            latency: self.latency.empty_like()?,
        })
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

/// Entries made ahead of the time bucket that will need them; see
/// [`TupleStatsCollector::prepare_next_bucket`]
#[derive(Default)]
struct NextBucket {
    /// An entry per (group, connection), with the bucket it was made for
    entries: FastHashMap<(usize, usize), (u64, StatsEntry)>,
    /// The global entry, with the bucket it was made for
    global: Option<(u64, StatsEntry)>,
    /// Keys of the entries created that no entry was made after yet
    pending: Vec<StatsKey>,
    /// The bucket whose entries the entries map has room for
    reserved: u64,
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
    entries: FastHashMap<StatsKey, StatsEntry>,
    /// Thread recorded in the keys of new entries; see [`Self::set_thread_id`].
    thread_id: usize,
    /// Independent global storage per bucket, recorded with the global policy.
    /// This avoids trying to convert incompatible group sketches during aggregation.
    global_entries: FastHashMap<u64, StatsEntry>,
    /// Empty entries made for the next bucket, which no aggregation sees
    next_bucket: NextBucket,
    /// Global sampling policy for latency storage (used for global aggregation)
    sampling_policy: SamplingPolicy,
    /// Per-group sampling policies (used for per-group aggregation)
    /// If a group is not in this map, falls back to global sampling_policy
    group_sampling_policies: FastHashMap<usize, SamplingPolicy>,
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
    /// Aggregate of worker loop intervals in Unix microseconds, including loops
    /// that exited with an error. Unusable clock samples invalidate the aggregate;
    /// collectors whose workers never ran remain neutral when merged.
    actual_load_loop: ActualLoadLoop,
    /// Request counts per group
    request_counts: FastHashMap<usize, RequestCounts>,
    /// Send lags of open-loop requests per group
    send_lags: FastHashMap<usize, SendLagStats>,
    /// Latencies of error replies per group, under the group's policy. They
    /// are not part of the latency distribution of the entries.
    error_latencies: FastHashMap<usize, LatencyStorage>,
    /// Latencies of every group's error replies, under the global policy
    global_error_latency: LatencyStorage,
    /// Latency breakdowns of measurement requests completed by successful
    /// responses per group; error replies are left out as from the entries
    breakdowns: FastHashMap<usize, BreakdownStats>,
}

impl TupleStatsCollector {
    /// Create a new tuple stats collector
    pub fn new(sampling_policy: SamplingPolicy, bucket_duration: Duration) -> Self {
        let global_error_latency = LatencyStorage::from_policy(&sampling_policy);
        Self {
            entries: FastHashMap::default(),
            thread_id: 0,
            global_entries: FastHashMap::default(),
            next_bucket: NextBucket::default(),
            sampling_policy,
            group_sampling_policies: FastHashMap::default(),
            bucket_duration_ns: bucket_duration.as_nanos() as u64,
            experiment_start_ns: crate::timing::time_ns(),
            first_record_ns: None,
            measurement_end_ns: None,
            actual_load_loop: ActualLoadLoop::Empty,
            request_counts: FastHashMap::default(),
            send_lags: FastHashMap::default(),
            error_latencies: FastHashMap::default(),
            global_error_latency,
            breakdowns: FastHashMap::default(),
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
            entries: FastHashMap::default(),
            thread_id: 0,
            global_entries: FastHashMap::default(),
            next_bucket: NextBucket::default(),
            sampling_policy,
            group_sampling_policies: FastHashMap::default(),
            bucket_duration_ns: bucket_duration.as_nanos() as u64,
            experiment_start_ns: start_time_ns,
            first_record_ns: None,
            measurement_end_ns: None,
            actual_load_loop: ActualLoadLoop::Empty,
            request_counts: FastHashMap::default(),
            send_lags: FastHashMap::default(),
            error_latencies: FastHashMap::default(),
            global_error_latency,
            breakdowns: FastHashMap::default(),
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
        let next_bucket = &mut self.next_bucket;
        let entry = match self.entries.entry(key) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(slot) => {
                next_bucket.pending.push(slot.key().clone());
                let entry = match next_bucket.entries.remove(&(group_id, connection_id)) {
                    Some((bucket, entry)) if bucket == time_bucket => entry,
                    _ => StatsEntry::new(policy),
                };
                slot.insert(entry)
            }
        };
        let global = self.global_entries.entry(time_bucket).or_insert_with(|| {
            match next_bucket.global.take() {
                Some((bucket, entry)) if bucket == time_bucket => entry,
                _ => StatsEntry::new(&self.sampling_policy),
            }
        });
        (entry, global)
    }

    /// Make one of the entries the next time bucket will need, while the
    /// worker has time to spare; returns whether it made one.
    ///
    /// A connection's first event in a bucket creates its entry there. An
    /// HDR histogram zeroes its whole range of counts when it is made and
    /// when it grows, about 90 KiB of fresh memory for latencies of 1 ms,
    /// which delays the sends due then; at a bucket boundary every
    /// connection does it within milliseconds, and the map of entries grows
    /// by rehashing every entry of every bucket. In the second half of a
    /// bucket this makes room in the map for as many entries as this bucket
    /// has, then makes the next bucket's global entry, then an entry for
    /// each connection with one in this bucket, sized like it. Recording
    /// takes an entry made ahead only in the bucket it was made for, and it
    /// is empty, so what is recorded does not change. Only HDR histograms
    /// are made ahead; the other storage costs little to create.
    pub fn prepare_next_bucket(&mut self, now_ns: u64) -> bool {
        let since_start = now_ns.saturating_sub(self.experiment_start_ns);
        if self.measurement_end_ns.is_some()
            || since_start % self.bucket_duration_ns < self.bucket_duration_ns / 2
        {
            return false;
        }
        let bucket = since_start / self.bucket_duration_ns;
        let next_bucket = &mut self.next_bucket;
        if next_bucket.reserved <= bucket {
            self.entries.reserve(next_bucket.pending.len());
            next_bucket.reserved = bucket + 1;
            return true;
        }
        if !matches!(next_bucket.global, Some((next, _)) if next > bucket) {
            if let Some(entry) = self.global_entries.get(&bucket).and_then(StatsEntry::empty_like) {
                next_bucket.global = Some((bucket + 1, entry));
                return true;
            }
        }
        while let Some(key) = next_bucket.pending.pop() {
            // An entry of a past bucket is no guide to the next one
            if key.time_bucket != bucket {
                continue;
            }
            if let Some(entry) = self.entries.get(&key).and_then(StatsEntry::empty_like) {
                next_bucket
                    .entries
                    .insert((key.group_id, key.connection_id), (bucket + 1, entry));
                return true;
            }
        }
        false
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
            entries: FastHashMap::default(),
            thread_id: 0,
            global_entries: FastHashMap::default(),
            next_bucket: NextBucket::default(),
            sampling_policy: first.sampling_policy.clone(),
            group_sampling_policies: FastHashMap::default(),
            bucket_duration_ns: first.bucket_duration_ns,
            experiment_start_ns: first.experiment_start_ns,
            first_record_ns: collectors.iter().filter_map(|c| c.first_record_ns).min(),
            measurement_end_ns: collectors.iter().filter_map(|c| c.measurement_end_ns).max(),
            actual_load_loop: collectors
                .iter()
                .fold(ActualLoadLoop::Empty, |combined, c| combined.merge(c.actual_load_loop)),
            request_counts: FastHashMap::default(),
            send_lags: FastHashMap::default(),
            error_latencies: FastHashMap::default(),
            global_error_latency: LatencyStorage::from_policy(&first.sampling_policy),
            breakdowns: FastHashMap::default(),
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
    pub fn entries(&self) -> &FastHashMap<StatsKey, StatsEntry> {
        &self.entries
    }

    /// Take ownership of entries
    pub fn into_entries(self) -> FastHashMap<StatsKey, StatsEntry> {
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
    /// The merged interval includes any gaps between worker loops. Returns `None`
    /// if no interval was recorded or if any worker's samples were missing, out
    /// of order, or above `2^53 - 1`. Empty collectors remain neutral when merged.
    /// These wall-clock readings do not imply alignment with any other host's clock.
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
            percentile_sample_count: Some(latency.percentile_sample_count()),
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

impl TupleStatsCollector {
    /// Move the clock `ns` on, as the equivalence tests do to the collector
    pub(crate) fn advance_ns(&mut self, ns: u64) {
        self.experiment_start_ns -= ns;
    }
}
