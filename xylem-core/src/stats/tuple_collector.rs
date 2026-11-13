//! Tuple-keyed statistics collection with on-demand aggregation
//!
//! Stores statistics keyed by `(time_bucket, group_id, thread_id, connection_id)`,
//! enabling flexible post-hoc aggregation by any dimension. Without records
//! (see [`TupleStatsCollector::set_keep_records`]), groups whose policy
//! merges exactly keep one total each instead.

use super::breakdown::{BreakdownStats, BreakdownSummary, LatencyBreakdown};
use super::policy::SamplingPolicy;
use super::requests::{RequestCounts, SendLagStats, SendLagSummary, LATE_SEND_THRESHOLD};
use super::AggregatedStats;
use hdrhistogram::Histogram;
use sketches_ddsketch::{Config as DDSketchConfig, DDSketch};
use std::collections::{BTreeMap, HashMap};
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
            samples: Vec::new(),
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

/// Where an HDR histogram of the `hdrhistogram` crate whose lowest
/// discernible value is 1, as every policy makes them, counts a value: the
/// index of its count in the crate's counts array, called its bin here.
///
/// This mirrors the crate's private index arithmetic, so a list of (bin,
/// count) pairs holds what the crate's counts array holds, without the
/// zero counts of every bin up to the largest value.
#[derive(Debug, Clone, Copy, PartialEq)]
struct HdrLayout {
    sigfig: u8,
    /// log2 of the number of bins each bucket past the first adds
    half_magnitude: u8,
    /// Highest trackable value and number of bins of a histogram that does
    /// not resize; `None` for one that grows to fit any value
    bound: Option<(u64, usize)>,
}

impl HdrLayout {
    /// Layout of `Histogram::new(sigfig)`, or of `Histogram::new_with_max(high, sigfig)`
    ///
    /// # Panics
    /// Panics for arguments the crate rejects, as the crate's histogram would.
    fn new(sigfig: u8, high: Option<u64>) -> Self {
        assert!(
            sigfig <= 5 && high.is_none_or(|high| high >= 2),
            "Failed to create HDR histogram"
        );
        let largest = 2 * 10_u32.pow(u32::from(sigfig));
        let count_magnitude = f64::from(largest).log2().ceil() as u8;
        let mut layout = Self {
            sigfig,
            half_magnitude: count_magnitude - 1,
            bound: None,
        };
        layout.bound = high.map(|high| (high, layout.bins_to_cover(high)));
        layout
    }

    /// Bins of the first bucket, which holds the values below this
    fn sub_bucket_count(&self) -> u64 {
        1 << (self.half_magnitude + 1)
    }

    fn bucket(&self, value: u64) -> u32 {
        let leading_zero_count_base = 64 - (u32::from(self.half_magnitude) + 1);
        leading_zero_count_base - (value | (self.sub_bucket_count() - 1)).leading_zeros()
    }

    /// The bin that counts `value`
    fn bin(&self, value: u64) -> usize {
        let bucket = self.bucket(value);
        let sub_bucket = (value >> bucket) as usize;
        ((bucket as usize + 1) << self.half_magnitude) + sub_bucket - (1 << self.half_magnitude)
    }

    /// The lowest value that `bin` counts
    fn lowest(&self, bin: usize) -> u64 {
        let half = 1 << self.half_magnitude;
        let bucket = (bin >> self.half_magnitude) as isize - 1;
        let sub_bucket = (bin & (half - 1)) + half;
        if bucket < 0 {
            (sub_bucket - half) as u64
        } else {
            (sub_bucket as u64) << bucket
        }
    }

    /// The highest value that `bin` counts
    fn highest(&self, bin: usize) -> u64 {
        let lowest = self.lowest(bin);
        lowest.saturating_add(1 << self.bucket(lowest)) - 1
    }

    /// Number of bins of a histogram whose highest trackable value is `high`
    fn bins_to_cover(&self, high: u64) -> usize {
        let mut smallest_untrackable = self.sub_bucket_count();
        let mut buckets = 1;
        while smallest_untrackable <= high {
            if smallest_untrackable > u64::MAX / 2 {
                buckets += 1;
                break;
            }
            smallest_untrackable <<= 1;
            buckets += 1;
        }
        (buckets + 1) << self.half_magnitude
    }

    /// Whether a histogram of this layout and one of `hist`'s can merge
    fn merges_with(&self, hist: &Histogram<u64>) -> bool {
        self.sigfig == hist.sigfig()
            && hist.low() == 1
            && match self.bound {
                None => hist.is_auto_resize(),
                Some((high, _)) => !hist.is_auto_resize() && hist.high() == high,
            }
    }
}

/// An HDR histogram kept as its bins with a count: memory grows with the
/// number of distinct bins recorded, not with the largest value. Its bins,
/// counts, clamping and percentiles are those of the crate's histogram of
/// the same layout given the same values.
#[derive(Debug, Clone, PartialEq)]
struct HdrBins {
    layout: HdrLayout,
    /// (bin, count) of every bin with a count, in ascending bin order
    counts: Vec<(u32, u64)>,
    total: u64,
}

impl HdrBins {
    fn new(layout: HdrLayout) -> Self {
        Self { layout, counts: Vec::new(), total: 0 }
    }

    /// Record `value`; returns whether it was clamped to the highest
    /// trackable value of a bounded histogram, as `saturating_record` does
    /// to a value the crate's histogram cannot hold
    fn record(&mut self, value: u64) -> bool {
        let bin = self.layout.bin(value);
        match self.layout.bound {
            Some((high, bins)) if bin >= bins => {
                self.add(self.layout.bin(high), 1);
                true
            }
            _ => {
                self.add(bin, 1);
                false
            }
        }
    }

    fn add(&mut self, bin: usize, count: u64) {
        let bin = u32::try_from(bin).expect("HDR bin fits in u32");
        match self.counts.binary_search_by_key(&bin, |&(b, _)| b) {
            Ok(i) => self.counts[i].1 = self.counts[i].1.saturating_add(count),
            Err(i) => self.counts.insert(i, (bin, count)),
        }
        self.total = self.total.saturating_add(count);
    }

    fn merge(&mut self, other: &Self) {
        if self.counts.is_empty() {
            self.counts.clone_from(&other.counts);
            self.total = other.total;
            return;
        }
        let mut merged = Vec::with_capacity(self.counts.len() + other.counts.len());
        let (mut mine, mut theirs) =
            (self.counts.iter().peekable(), other.counts.iter().peekable());
        loop {
            let next = match (mine.peek(), theirs.peek()) {
                (Some(&&(a, x)), Some(&&(b, y))) if a == b => {
                    mine.next();
                    theirs.next();
                    (a, x.saturating_add(y))
                }
                (Some(&&(a, _)), Some(&&(b, _))) if b < a => *theirs.next().unwrap(),
                (Some(_), _) => *mine.next().unwrap(),
                (None, Some(_)) => *theirs.next().unwrap(),
                (None, None) => break,
            };
            merged.push(next);
        }
        self.counts = merged;
        self.total = self.total.saturating_add(other.total);
    }

    /// Add every count of the crate's histogram `hist`
    fn add_histogram(&mut self, hist: &Histogram<u64>) {
        for value in hist.iter_recorded() {
            self.add(self.layout.bin(value.value_iterated_to()), value.count_at_value());
        }
    }

    /// Add every count to the crate's histogram `hist`
    fn add_to(&self, hist: &mut Histogram<u64>) -> Result<(), &'static str> {
        for &(bin, count) in &self.counts {
            hist.record_n(self.layout.lowest(bin as usize), count)
                .map_err(|_| "HDR histogram merge failed")?;
        }
        Ok(())
    }

    /// `Histogram::value_at_quantile` of the same counts
    fn value_at_quantile(&self, quantile: f64) -> u64 {
        let quantile = if quantile > 1.0 { 1.0 } else { quantile };
        let count_at_quantile = ((quantile * self.total as f64).ceil() as u64).max(1);
        let mut total_to_current_bin = 0u64;
        for &(bin, count) in &self.counts {
            total_to_current_bin += count;
            if total_to_current_bin >= count_at_quantile {
                return if quantile == 0.0 {
                    self.layout.lowest(bin as usize)
                } else {
                    self.layout.highest(bin as usize)
                };
            }
        }
        0
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
    /// The same HDR histogram kept as its bins with a count
    HdrBins { bins: HdrBins, clamped: u64 },
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

    /// Storage like [`Self::from_policy`]'s whose memory grows with the
    /// latencies recorded: an HDR histogram keeps only its bins with a
    /// count. Its percentiles, counts and merges are those of the policy's
    /// histogram; recording costs a search of its bins.
    pub fn compact_from_policy(policy: &SamplingPolicy) -> Self {
        let SamplingPolicy::HdrHistogram { sigfigs, max_value_us } = policy else {
            return Self::from_policy(policy);
        };
        let layout = HdrLayout::new(*sigfigs, max_value_us.map(|us| us.saturating_mul(1000)));
        Self {
            quantiles: Quantiles::HdrBins { bins: HdrBins::new(layout), clamped: 0 },
            moments: Moments::default(),
        }
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
            Quantiles::HdrBins { bins, clamped } => {
                if bins.record(ns) {
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
            (
                Quantiles::HdrHistogram { hist, clamped },
                Quantiles::HdrBins { bins, clamped: other_clamped },
            ) if bins.layout.merges_with(hist) => {
                bins.add_to(hist)?;
                *clamped += other_clamped;
            }
            (
                Quantiles::HdrBins { bins, clamped },
                Quantiles::HdrBins { bins: other, clamped: other_clamped },
            ) if bins.layout == other.layout => {
                bins.merge(other);
                *clamped += other_clamped;
            }
            (
                Quantiles::HdrBins { bins, clamped },
                Quantiles::HdrHistogram { hist, clamped: other_clamped },
            ) if bins.layout.merges_with(hist) => {
                bins.add_histogram(hist);
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
            Quantiles::HdrBins { bins, .. } => bins.total,
        }
    }

    /// Number of latencies a bounded HDR histogram recorded at its maximum
    /// because they exceeded it. `None` for storage that never clamps.
    pub fn clamped_count(&self) -> Option<u64> {
        match &self.quantiles {
            Quantiles::HdrHistogram { hist, clamped } if !hist.is_auto_resize() || *clamped > 0 => {
                Some(*clamped)
            }
            Quantiles::HdrBins { bins, clamped } if bins.layout.bound.is_some() || *clamped > 0 => {
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
            Quantiles::HdrBins { bins, .. } => {
                ps.iter().map(|&p| bins.value_at_quantile(p)).collect()
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

    /// A new entry whose latency storage is
    /// [`LatencyStorage::compact_from_policy`]'s
    pub fn compact(policy: &SamplingPolicy) -> Self {
        Self {
            request_count: 0,
            tx_bytes: 0,
            rx_bytes: 0,
            latency: LatencyStorage::compact_from_policy(policy),
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

/// Where a connection's entry sits in a time bucket: (group, thread, connection)
type ConnectionKey = (usize, usize, usize);

/// Statistics of one time bucket
#[derive(Default)]
struct Bucket {
    /// An entry per connection that recorded in the bucket
    entries: FastHashMap<ConnectionKey, StatsEntry>,
    /// Global storage of the bucket, under the global policy
    global: Option<StatsEntry>,
}

/// Whether merging a policy's storage gives what recording every latency
/// into one storage gives, whatever the order and split: then a total can
/// stand in for entries per time bucket and connection
fn merges_exactly(policy: &SamplingPolicy) -> bool {
    matches!(
        policy,
        SamplingPolicy::None | SamplingPolicy::Unlimited | SamplingPolicy::HdrHistogram { .. }
    )
}

/// Tuple-keyed statistics collector
///
/// Stores statistics keyed by (time_bucket, group_id, thread_id, connection_id) for flexible
/// on-demand aggregation by any dimension. Global latency storage is independent
/// of group storage: each recorded latency is sampled once under each policy.
/// This requires additional storage per bucket and allows heterogeneous
/// groups without converting or discarding their sketches.
///
/// Each time bucket has its own map of entries, so making a bucket's
/// entries never moves another bucket's. Entries keep HDR histograms as
/// their bins with a count ([`LatencyStorage::compact_from_policy`]).
/// Without records ([`Self::set_keep_records`]), a group or the global
/// storage whose policy merges exactly (none, unlimited, HDR histogram)
/// records into one total instead, so its memory does not grow with the
/// length of the run or the number of connections. A sketch or a limited
/// sample keeps its entries either way: its merged estimate depends on how
/// its latencies were split and merged.
pub struct TupleStatsCollector {
    /// Entries of each time bucket
    buckets: BTreeMap<u64, Bucket>,
    /// Whether every group and the global storage keep entries per time
    /// bucket and connection; see [`Self::set_keep_records`]
    keep_records: bool,
    /// Without records, the total of each group whose policy merges exactly
    group_totals: FastHashMap<usize, StatsEntry>,
    /// Without records and with a global policy that merges exactly, the
    /// global storage of every bucket
    global_total: Option<StatsEntry>,
    /// Thread recorded in the keys of new entries; see [`Self::set_thread_id`].
    thread_id: usize,
    /// Room for the entries of the bucket it was made for, which no
    /// aggregation sees; see [`Self::prepare_next_bucket`]
    next_bucket: Option<(u64, FastHashMap<ConnectionKey, StatsEntry>)>,
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
        Self::with_start_time(sampling_policy, bucket_duration, crate::timing::time_ns())
    }

    /// Create with a specific start time (for testing or synchronized starts)
    pub fn with_start_time(
        sampling_policy: SamplingPolicy,
        bucket_duration: Duration,
        start_time_ns: u64,
    ) -> Self {
        let global_error_latency = LatencyStorage::from_policy(&sampling_policy);
        Self {
            buckets: BTreeMap::new(),
            keep_records: true,
            group_totals: FastHashMap::default(),
            global_total: None,
            thread_id: 0,
            next_bucket: None,
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

    /// Set whether to keep statistics per time bucket and connection, which
    /// records ([`Self::entries`]), time series and per-connection
    /// aggregates need; the default keeps them. Without them, a group or the
    /// global storage whose policy merges exactly records into one total,
    /// and those queries panic. Group and global aggregates are the same
    /// either way.
    ///
    /// # Panics
    /// Panics once something was recorded.
    pub fn set_keep_records(&mut self, keep_records: bool) {
        assert!(
            self.buckets.is_empty() && self.group_totals.is_empty() && self.global_total.is_none(),
            "Cannot change whether records are kept after recording data"
        );
        self.keep_records = keep_records;
    }

    /// Whether statistics are kept per time bucket and connection; see
    /// [`Self::set_keep_records`]
    pub fn keeps_records(&self) -> bool {
        self.keep_records
    }

    fn expect_records(&self, query: &str) {
        assert!(
            self.keep_records,
            "{query} needs statistics per time bucket and connection, which this collector \
             does not keep (set_keep_records)"
        );
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
            !self.group_ids().contains(&group_id) || self.get_group_policy(group_id) == &policy,
            "Cannot change a group's sampling policy after recording data"
        );
        self.group_sampling_policies.insert(group_id, policy);
    }

    fn get_group_policy(&self, group_id: usize) -> &SamplingPolicy {
        self.group_sampling_policies.get(&group_id).unwrap_or(&self.sampling_policy)
    }

    /// Create both entries through one path, including when bytes arrive before latency.
    fn recording_entries(
        &mut self,
        group_id: usize,
        connection_id: usize,
    ) -> (&mut StatsEntry, &mut StatsEntry) {
        let Self {
            buckets,
            keep_records,
            group_totals,
            global_total,
            thread_id,
            next_bucket,
            sampling_policy,
            group_sampling_policies,
            first_record_ns,
            measurement_end_ns,
            experiment_start_ns,
            bucket_duration_ns,
            ..
        } = self;
        let policy = group_sampling_policies.get(&group_id).unwrap_or(sampling_policy);
        let group_in_total = !*keep_records && merges_exactly(policy);
        let global_in_total = !*keep_records && merges_exactly(sampling_policy);
        let in_totals = group_in_total && global_in_total;
        // The clock is read only for the first record and for a bucket
        let now_ns = (!in_totals || first_record_ns.is_none()).then(|| {
            let now_ns = crate::timing::time_ns();
            // Responses drained after the window belong to its last bucket.
            let now_ns = measurement_end_ns.map_or(now_ns, |end| now_ns.min(end.saturating_sub(1)));
            first_record_ns.get_or_insert(now_ns);
            now_ns
        });
        let (bucket_entries, bucket_global) = if in_totals {
            (None, None)
        } else {
            let now_ns = now_ns.expect("read for the bucket");
            let time_bucket = now_ns.saturating_sub(*experiment_start_ns) / *bucket_duration_ns;
            let bucket = buckets.entry(time_bucket).or_insert_with(|| Bucket {
                entries: match next_bucket.take() {
                    Some((bucket, entries)) if bucket == time_bucket => entries,
                    other => {
                        *next_bucket = other;
                        FastHashMap::default()
                    }
                },
                global: None,
            });
            (Some(&mut bucket.entries), Some(&mut bucket.global))
        };
        let entry = if group_in_total {
            group_totals.entry(group_id).or_insert_with(|| StatsEntry::new(policy))
        } else {
            bucket_entries
                .expect("the bucket holds the group's entries")
                .entry((group_id, *thread_id, connection_id))
                .or_insert_with(|| StatsEntry::compact(policy))
        };
        let global = if global_in_total {
            global_total.get_or_insert_with(|| StatsEntry::new(sampling_policy))
        } else {
            bucket_global
                .expect("the bucket holds the global storage")
                .get_or_insert_with(|| StatsEntry::compact(sampling_policy))
        };
        (entry, global)
    }

    /// Make room for the next time bucket's entries while the worker has
    /// time to spare; returns whether it did.
    ///
    /// A connection's first event in a bucket creates its entry there, so
    /// at a bucket boundary every connection adds one to the new bucket's
    /// map within milliseconds, and the map rehashes as it grows. In the
    /// second half of a bucket this makes a map for the next bucket with
    /// room for as many entries as this bucket has. A bucket takes the map
    /// only if it was made for it; it is empty, so what is recorded does not
    /// change. Entries are cheap to make: an HDR histogram keeps only its
    /// bins with a count, and the other storage grows as it records.
    pub fn prepare_next_bucket(&mut self, now_ns: u64) -> bool {
        let since_start = now_ns.saturating_sub(self.experiment_start_ns);
        if self.measurement_end_ns.is_some()
            || since_start % self.bucket_duration_ns < self.bucket_duration_ns / 2
        {
            return false;
        }
        let bucket = since_start / self.bucket_duration_ns;
        if matches!(self.next_bucket, Some((next, _)) if next > bucket) {
            return false;
        }
        let Some(current) = self.buckets.get(&bucket).filter(|b| !b.entries.is_empty()) else {
            return false;
        };
        let entries =
            FastHashMap::with_capacity_and_hasher(current.entries.len(), Default::default());
        self.next_bucket = Some((bucket + 1, entries));
        true
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

    /// Record a request completed by a successful response of `bytes`
    /// bytes, as [`Self::record_rx_bytes`], [`Self::record_latency`] and
    /// [`Self::record_breakdown`] would, with one lookup of its entries
    pub fn record_response(
        &mut self,
        group_id: usize,
        connection_id: usize,
        bytes: usize,
        latency: Duration,
        breakdown: &LatencyBreakdown,
    ) {
        self.request_counts_mut(group_id).completed += 1;
        let draining = self.measurement_end_ns.is_some();
        let (entry, global) = self.recording_entries(group_id, connection_id);
        if draining {
            entry.record_drain_latency(latency);
            global.record_drain_latency(latency);
        } else {
            entry.record_rx_bytes(bytes);
            global.record_rx_bytes(bytes);
            entry.record_latency(latency);
            global.record_latency(latency);
        }
        self.record_breakdown(group_id, breakdown);
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

    /// Every entry kept per time bucket and connection, in time, then key
    /// order, so that merges whose result depends on their order (sketches,
    /// limited samples) give the same result every time
    fn sorted_entries(&self) -> impl Iterator<Item = (StatsKey, &StatsEntry)> + '_ {
        self.buckets.iter().flat_map(|(&time_bucket, bucket)| {
            let mut entries: Vec<_> = bucket.entries.iter().collect();
            entries.sort_unstable_by_key(|(key, _)| **key);
            entries.into_iter().map(move |(&(group_id, thread_id, connection_id), entry)| {
                let key = StatsKey {
                    time_bucket,
                    group_id,
                    thread_id,
                    connection_id,
                };
                (key, entry)
            })
        })
    }

    /// Get all unique group IDs
    pub fn group_ids(&self) -> Vec<usize> {
        let mut ids: Vec<usize> = self
            .buckets
            .values()
            .flat_map(|bucket| bucket.entries.keys().map(|&(group_id, _, _)| group_id))
            .chain(self.group_totals.keys().copied())
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Get the `(thread_id, connection_id)` of every connection in a group, sorted
    ///
    /// # Panics
    /// Panics without records; see [`Self::set_keep_records`].
    pub fn connection_ids(&self, group_id: usize) -> Vec<(usize, usize)> {
        self.expect_records("connection_ids");
        let mut ids: Vec<(usize, usize)> = self
            .buckets
            .values()
            .flat_map(|bucket| bucket.entries.keys())
            .filter(|&&(group, _, _)| group == group_id)
            .map(|&(_, thread_id, connection_id)| (thread_id, connection_id))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Get all unique time buckets
    ///
    /// # Panics
    /// Panics without records; see [`Self::set_keep_records`].
    pub fn time_buckets(&self) -> Vec<u64> {
        self.expect_records("time_buckets");
        self.buckets
            .iter()
            .filter(|(_, bucket)| !bucket.entries.is_empty())
            .map(|(&time_bucket, _)| time_bucket)
            .collect()
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

        let bucket_globals = self.buckets.values().filter_map(|bucket| bucket.global.as_ref());
        for entry in self.global_total.iter().chain(bucket_globals) {
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
        let mut totals: HashMap<usize, (u64, u64, u64, LatencyStorage)> = HashMap::new();
        let entries = self
            .group_totals
            .iter()
            .map(|(&group_id, entry)| (group_id, entry))
            .chain(self.sorted_entries().map(|(key, entry)| (key.group_id, entry)));
        for (group_id, entry) in entries {
            // Use this group's specific sampling policy
            let group_policy = self.get_group_policy(group_id);
            let total = totals
                .entry(group_id)
                .or_insert_with(|| (0, 0, 0, LatencyStorage::from_policy(group_policy)));
            total.0 += entry.request_count;
            total.1 += entry.tx_bytes;
            total.2 += entry.rx_bytes;
            // Only merge latency if this group's policy is not None
            if !matches!(group_policy, SamplingPolicy::None) {
                total.3.merge_ref(&entry.latency);
            }
        }

        totals
            .into_iter()
            .map(|(group_id, (requests, tx_bytes, rx_bytes, latency))| {
                let stats =
                    self.compute_aggregated_stats(&latency, requests, tx_bytes, rx_bytes, duration);
                (group_id, stats)
            })
            .collect()
    }

    /// Aggregate entries by connection within a group
    ///
    /// # Panics
    /// Panics without records; see [`Self::set_keep_records`].
    pub fn aggregate_by_connection(
        &self,
        group_id: usize,
        duration: Duration,
    ) -> HashMap<(usize, usize), ConnectionStats> {
        self.expect_records("aggregate_by_connection");
        let mut totals: HashMap<(usize, usize), (u64, u64, u64, LatencyStorage)> = HashMap::new();
        for (key, entry) in self.sorted_entries().filter(|(key, _)| key.group_id == group_id) {
            let total = totals.entry((key.thread_id, key.connection_id)).or_insert_with(|| {
                (0, 0, 0, LatencyStorage::from_policy(self.get_group_policy(group_id)))
            });
            total.0 += entry.request_count;
            total.1 += entry.tx_bytes;
            total.2 += entry.rx_bytes;
            total.3.merge_ref(&entry.latency);
        }

        totals
            .into_iter()
            .map(|((thread_id, connection_id), (requests, tx_bytes, rx_bytes, latency))| {
                let latency_stats =
                    self.compute_aggregated_stats(&latency, requests, tx_bytes, rx_bytes, duration);
                let stats = ConnectionStats {
                    thread_id,
                    connection_id,
                    group_id,
                    request_count: requests,
                    tx_bytes,
                    rx_bytes,
                    latency: latency_stats,
                };
                ((thread_id, connection_id), stats)
            })
            .collect()
    }

    /// A time series point of a bucket's counts and latencies
    fn time_series_point(
        &self,
        time_bucket: u64,
        requests: u64,
        bytes: u64,
        latency: &LatencyStorage,
    ) -> TimeSeriesPoint {
        let bucket_duration_secs = self.bucket_width(time_bucket).as_secs_f64();
        let latency_percentiles = latency.percentiles(&[0.5, 0.99, 0.999]);
        TimeSeriesPoint {
            time_bucket,
            request_count: requests,
            throughput_rps: requests as f64 / bucket_duration_secs,
            throughput_mbps: (bytes as f64 * 8.0) / (bucket_duration_secs * 1_000_000.0),
            latency_p50_ns: latency_percentiles[0],
            latency_p99_ns: latency_percentiles[1],
            latency_p999_ns: latency_percentiles[2],
            latency_mean_ns: latency.mean(),
        }
    }

    /// Get time series for global statistics (collapse group and connection)
    ///
    /// # Panics
    /// Panics without records; see [`Self::set_keep_records`].
    pub fn time_series_global(&self) -> Vec<TimeSeriesPoint> {
        self.expect_records("time_series_global");
        self.buckets
            .iter()
            .filter_map(|(&time_bucket, bucket)| {
                let entry = bucket.global.as_ref()?;
                let bytes = entry.tx_bytes + entry.rx_bytes;
                Some(self.time_series_point(
                    time_bucket,
                    entry.request_count,
                    bytes,
                    &entry.latency,
                ))
            })
            .collect()
    }

    /// Get time series for a specific group
    ///
    /// # Panics
    /// Panics without records; see [`Self::set_keep_records`].
    pub fn time_series_by_group(&self, group_id: usize) -> Vec<TimeSeriesPoint> {
        self.expect_records("time_series_by_group");
        let mut bucket_data: BTreeMap<u64, (u64, u64, LatencyStorage)> = BTreeMap::new();
        for (key, entry) in self.sorted_entries().filter(|(key, _)| key.group_id == group_id) {
            let data = bucket_data.entry(key.time_bucket).or_insert_with(|| {
                (0, 0, LatencyStorage::from_policy(self.get_group_policy(group_id)))
            });
            data.0 += entry.request_count;
            data.1 += entry.tx_bytes + entry.rx_bytes;
            data.2.merge_ref(&entry.latency);
        }

        bucket_data
            .into_iter()
            .map(|(time_bucket, (requests, bytes, latency))| {
                self.time_series_point(time_bucket, requests, bytes, &latency)
            })
            .collect()
    }

    /// Get time series for a specific connection
    ///
    /// # Panics
    /// Panics without records; see [`Self::set_keep_records`].
    pub fn time_series_by_connection(
        &self,
        group_id: usize,
        thread_id: usize,
        connection_id: usize,
    ) -> Vec<TimeSeriesPoint> {
        self.expect_records("time_series_by_connection");
        self.buckets
            .iter()
            .filter_map(|(&time_bucket, bucket)| {
                let entry = bucket.entries.get(&(group_id, thread_id, connection_id))?;
                let bytes = entry.tx_bytes + entry.rx_bytes;
                Some(self.time_series_point(
                    time_bucket,
                    entry.request_count,
                    bytes,
                    &entry.latency,
                ))
            })
            .collect()
    }

    /// Merge multiple collectors (from different threads).
    ///
    /// # Panics
    /// Panics if global/group policies differ, or if some collectors keep
    /// records and others do not. As before, bucket coordinates are
    /// interpreted relative to the first collector's start time and bucket duration.
    pub fn merge(collectors: Vec<Self>) -> Self {
        if collectors.is_empty() {
            return Self::new(SamplingPolicy::default(), Duration::from_secs(1));
        }

        let first = &collectors[0];
        let mut merged = Self {
            buckets: BTreeMap::new(),
            keep_records: first.keep_records,
            group_totals: FastHashMap::default(),
            global_total: None,
            thread_id: 0,
            next_bucket: None,
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
            assert_eq!(
                merged.keep_records, collector.keep_records,
                "Collectors differ in whether they keep records"
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
            for group_id in collector.group_ids() {
                assert_eq!(
                    merged.get_group_policy(group_id),
                    collector.get_group_policy(group_id),
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
            for (time_bucket, bucket) in collector.buckets {
                let target = merged.buckets.entry(time_bucket).or_default();
                for (key, entry) in bucket.entries {
                    target.entries.entry(key).and_modify(|e| e.merge_from(&entry)).or_insert(entry);
                }
                merge_into(&mut target.global, bucket.global);
            }
            for (group_id, entry) in collector.group_totals {
                merged
                    .group_totals
                    .entry(group_id)
                    .and_modify(|e| e.merge_from(&entry))
                    .or_insert(entry);
            }
            merge_into(&mut merged.global_total, collector.global_total);
        }

        merged
    }

    /// Every entry kept per time bucket and connection, in time, then group,
    /// thread and connection order
    ///
    /// # Panics
    /// Panics without records; see [`Self::set_keep_records`].
    pub fn entries(&self) -> impl Iterator<Item = (StatsKey, &StatsEntry)> + '_ {
        self.expect_records("entries");
        self.sorted_entries()
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

/// Merge `entry` into `target`, or make it `target` if there is none
fn merge_into(target: &mut Option<StatsEntry>, entry: Option<StatsEntry>) {
    match (target.as_mut(), entry) {
        (Some(target), Some(entry)) => target.merge_from(&entry),
        (None, entry) => *target = entry,
        (Some(_), None) => {}
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
            assert!(collector.entries().all(|(_, entry)| entry.latency.count() == 100));
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
    fn tdigest_no_error_multiworker_collector_merge() {
        let policy = SamplingPolicy::TDigest { compression: 1000 };
        let start = crate::timing::time_ns();
        let collectors = (0..3)
            .map(|thread_id| {
                let mut collector = TupleStatsCollector::with_start_time(
                    policy.clone(),
                    Duration::from_secs(3600),
                    start,
                );
                collector.set_thread_id(thread_id);
                collector.record_sent(7, None);
                collector.record_latency(7, 0, Duration::from_micros(10 * (thread_id as u64 + 1)));
                collector
            })
            .collect();

        let merged = TupleStatsCollector::merge(collectors);
        let counts = merged.request_counts_global();
        assert_eq!((counts.sent, counts.completed, counts.errors), (3, 3, 0));
        let global = merged.aggregate_global(Duration::from_secs(1));
        assert_eq!(global.total_requests, 3);
        assert_eq!(global.latency_sample_count, 3);
        assert_eq!(global.percentile_sample_count, Some(3));
        assert_eq!(global.mean_latency, Duration::from_micros(20));
        assert_eq!(global.min_latency, Some(Duration::from_micros(10)));
        assert_eq!(global.max_latency, Some(Duration::from_micros(30)));
        assert_eq!(global.std_dev, Some(Duration::from_micros(10)));
        assert_eq!(global.latency_p50, Duration::from_micros(20));
        assert_eq!(global.latency_p99, Duration::from_micros(30));
        let errors = merged.error_latency_global();
        assert_eq!(errors.latency_sample_count, 0);
        assert_eq!(errors.percentile_sample_count, Some(0));
        assert_eq!(errors.min_latency, None);
        assert_eq!(errors.max_latency, None);
        assert_eq!(errors.std_dev, None);
    }

    #[test]
    fn tdigest_error_only_workers_merge_empty_success_bucket() {
        let policy = SamplingPolicy::TDigest { compression: 1000 };
        let start = crate::timing::time_ns();
        let collectors = (0..2)
            .map(|thread_id| {
                let mut collector = TupleStatsCollector::with_start_time(
                    policy.clone(),
                    Duration::from_secs(3600),
                    start,
                );
                collector.set_thread_id(thread_id);
                collector.record_sent(7, None);
                collector.record_error_reply(
                    7,
                    0,
                    "error",
                    Duration::from_micros(10 + 20 * thread_id as u64),
                );
                collector
            })
            .collect();

        let merged = TupleStatsCollector::merge(collectors);
        let counts = merged.request_counts_global();
        assert_eq!((counts.sent, counts.completed, counts.errors), (2, 2, 2));
        assert_eq!(counts.errors_by_kind.get("error"), 2);
        let errors = merged.error_latency_global();
        assert_eq!(errors.latency_sample_count, 2);
        assert_eq!(errors.percentile_sample_count, Some(2));
        assert_eq!(errors.mean_latency, Duration::from_micros(20));
        assert_eq!(errors.min_latency, Some(Duration::from_micros(10)));
        assert_eq!(errors.max_latency, Some(Duration::from_micros(30)));

        // The shared bucket counted error replies but has no successful latency.
        let global = merged.aggregate_global(Duration::from_secs(1));
        assert_eq!(global.total_requests, 2);
        assert_eq!(global.latency_sample_count, 0);
        assert_eq!(global.percentile_sample_count, Some(0));
        assert_eq!(global.mean_latency, Duration::ZERO);
        assert_eq!(global.latency_p50, Duration::ZERO);
        assert_eq!(global.min_latency, None);
        assert_eq!(global.max_latency, None);
        assert_eq!(global.std_dev, None);
        let group = &merged.aggregate_by_group(Duration::from_secs(1))[&7];
        assert_eq!(group.total_requests, 2);
        assert_eq!(group.latency_sample_count, 0);
        assert_eq!(group.percentile_sample_count, Some(0));
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
            collector.buckets.entry(bucket).or_default().global = Some(entry);
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

    // Compare every represented rank, not just the separate exact moments count.
    // A collapsed store can have the right count metadata but wrong bin weights.
    fn assert_ddsketch_merge_ranks(max_bins: u32, low: u64, high: u64, collapsed: bool) {
        let policy = SamplingPolicy::DdSketch { alpha: 0.01, max_bins };
        let lows = || std::iter::repeat_n(low, 100);
        let highs = || std::iter::repeat_n(high, 10);
        let direct = storage_with(&policy, highs().chain(lows()));
        let mut high_first = storage_with(&policy, highs());
        high_first.merge_ref(&storage_with(&policy, lows()));
        let mut low_first = storage_with(&policy, lows());
        low_first.merge_ref(&storage_with(&policy, highs()));

        for storage in [&direct, &high_first, &low_first] {
            assert_eq!(storage.count(), 110);
            let Quantiles::DdSketch(sketch) = &storage.quantiles else {
                unreachable!()
            };
            assert_eq!(sketch.count(), 110);
        }
        // Sample inside each rank interval; the last rank is only reached at 1.0.
        let ps: Vec<f64> = (0..109).map(|rank| (rank as f64 + 0.5) / 109.0).chain([1.0]).collect();
        let expected = direct.percentiles(&ps);
        assert_eq!(high_first.percentiles(&ps), expected, "high destination, {max_bins} bins");
        assert_eq!(low_first.percentiles(&ps), expected, "low destination, {max_bins} bins");
        if collapsed {
            // The low bin was collapsed upward; its estimate need not be near low.
            assert!(direct.percentile(0.5) > low * 2);
        } else {
            for (estimate, value) in
                [(direct.percentile(0.5), low), (direct.percentile(0.95), high)]
            {
                assert!(estimate.abs_diff(value) * 100 <= value, "{estimate} vs {value}");
            }
        }
    }

    #[test]
    fn ddsketch_merge_preserves_collapsed_bin_weights_in_both_orders() {
        assert_ddsketch_merge_ranks(128, 100, 1_000_000, true);
        // The default 1024 bins also collapse over a sufficiently wide range.
        assert_ddsketch_merge_ranks(1024, 100, 1_000_000_000_000, true);
    }

    #[test]
    fn ddsketch_merge_preserves_noncollapsing_bin_weights_in_both_orders() {
        assert_ddsketch_merge_ranks(128, 100, 200, false);
    }

    #[test]
    fn ddsketch_merge_empty_sketches() {
        let policy = SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 128 };
        let empty = LatencyStorage::from_policy(&policy);
        let mut empty_merged = LatencyStorage::from_policy(&policy);
        empty_merged.merge_ref(&empty);
        assert_eq!(empty_merged.count(), 0);
        assert_eq!(empty_merged.percentile(0.5), 0);

        let data = storage_with(&policy, [100, 100, 1_000_000]);
        let ps = [0.0, 0.5, 0.9, 1.0];
        let expected = data.percentiles(&ps);
        empty_merged.merge_ref(&data);
        assert_eq!(empty_merged.count(), 3);
        assert_eq!(empty_merged.percentiles(&ps), expected);
        empty_merged.merge_ref(&empty);
        assert_eq!(empty_merged.count(), 3);
        assert_eq!(empty_merged.percentiles(&ps), expected);
    }

    #[test]
    fn ddsketch_merge_zero_only_and_positive_preserves_endpoints() {
        let policy = SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 128 };
        let mut zero_first = storage_with(&policy, [0, 0]);
        zero_first.merge_ref(&storage_with(&policy, [100, 1_000]));
        let mut positive_first = storage_with(&policy, [100, 1_000]);
        positive_first.merge_ref(&storage_with(&policy, [0, 0]));

        for storage in [&zero_first, &positive_first] {
            assert_eq!(storage.count(), 4);
            let Quantiles::DdSketch(sketch) = &storage.quantiles else {
                unreachable!()
            };
            assert_eq!(sketch.count(), 4);
            assert_eq!(storage.percentiles(&[0.0, 1.0]), [0, 1_000]);
        }
    }

    #[test]
    fn ddsketch_merge_collapsed_with_noncollapsed_in_both_orders() {
        const LOW: u64 = 100;
        const HIGH: u64 = 100_000_000_000;
        const MIDDLE: u64 = HIGH / 10;
        for max_bins in [128, 1024] {
            let policy = SamplingPolicy::DdSketch { alpha: 0.01, max_bins };
            let collapsed = storage_with(
                &policy,
                std::iter::repeat_n(LOW, 100).chain(std::iter::repeat_n(HIGH, 10)),
            );
            let middle = storage_with(&policy, std::iter::repeat_n(MIDDLE, 30));
            let direct = storage_with(
                &policy,
                std::iter::repeat_n(LOW, 100)
                    .chain(std::iter::repeat_n(MIDDLE, 30))
                    .chain(std::iter::repeat_n(HIGH, 10)),
            );
            let quantiles = [0.0, 0.5, 0.75, 0.95, 1.0];
            let expected = direct.percentiles(&quantiles);
            assert_eq!((expected[0], expected[4]), (LOW, HIGH));
            assert!(expected[1] > LOW && expected[1] < MIDDLE);
            assert!(expected[2].abs_diff(MIDDLE) <= MIDDLE / 100 + 1);
            assert!(expected[3].abs_diff(HIGH) <= HIGH / 100 + 1);
            for (first, second) in [(&collapsed, &middle), (&middle, &collapsed)] {
                let mut merged = LatencyStorage::from_policy(&policy);
                merged.merge_ref(first);
                merged.merge_ref(second);
                assert_eq!(merged.count(), 140);
                assert_eq!(merged.moments, direct.moments);
                assert_eq!(merged.percentiles(&quantiles), expected, "{max_bins} bins");
            }
        }
    }

    #[test]
    fn ddsketch_zero_only_merges_preserve_interior_quantiles() {
        let policy = SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 1024 };
        let empty = LatencyStorage::from_policy(&policy);
        let zero = storage_with(&policy, std::iter::repeat_n(0, 37));
        let positive = storage_with(&policy, [10_000, 10_000]);
        let quantiles = [0.0, 0.5, 0.99, 1.0];
        for (first, second, expected_count) in
            [(&empty, &zero, 37), (&zero, &empty, 37), (&zero, &zero, 74)]
        {
            let mut merged = LatencyStorage::from_policy(&policy);
            merged.merge_ref(first);
            merged.merge_ref(second);
            assert_eq!(merged.count(), expected_count);
            assert_eq!((merged.min(), merged.max()), (Some(0), Some(0)));
            assert_eq!(merged.percentiles(&quantiles), [0, 0, 0, 0]);
        }
        for (first, second) in [(&zero, &positive), (&positive, &zero)] {
            let mut merged = LatencyStorage::from_policy(&policy);
            merged.merge_ref(first);
            merged.merge_ref(second);
            let values = merged.percentiles(&quantiles);
            assert_eq!(merged.count(), 39);
            assert_eq!((merged.min(), merged.max()), (Some(0), Some(10_000)));
            assert_eq!((values[0], values[1], values[3]), (0, 0, 10_000));
            assert!(values[2].abs_diff(10_000) <= 101, "p99: {}", values[2]);
        }
    }

    #[test]
    fn ddsketch_one_nanosecond_uses_zero_bucket_for_interior_quantiles() {
        let policy = SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 1024 };
        let storage = storage_with(&policy, [1, 1]);
        // Endpoints retain exact extrema; the interior rank uses the zero bucket.
        assert_eq!(storage.percentiles(&[0.0, 0.5, 1.0]), [1, 0, 1]);
        let above_cutoff = storage_with(&policy, [2, 2]);
        assert!(above_cutoff.percentile(0.5) > 0, "2 ns must be outside the zero bucket");
    }

    #[test]
    fn ddsketch_quantiles_follow_uncollapsed_distribution() {
        let policy = SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 1024 };
        let samples = std::iter::repeat_n(1_000_000, 100)
            .chain(std::iter::repeat_n(1_000, 100))
            .chain(std::iter::repeat_n(20_000, 100));
        let storage = storage_with(&policy, samples);
        let quantiles = [0.0, 0.1, 0.33, 0.5, 0.66, 0.9, 0.99, 1.0];
        // DDSketch uses floor(p * (n - 1)); 100 observations have each value.
        let expected: [u64; 8] =
            [1_000, 1_000, 1_000, 20_000, 20_000, 1_000_000, 1_000_000, 1_000_000];
        assert_eq!(storage.count(), 300);
        for ((p, estimate), exact) in
            quantiles.into_iter().zip(storage.percentiles(&quantiles)).zip(expected)
        {
            // The extra nanosecond accounts for conversion from f64 to u64.
            assert!(
                estimate.abs_diff(exact) <= exact / 100 + 1,
                "p{p}: {estimate}, expected {exact}"
            );
        }
        assert_eq!((storage.percentile(0.0), storage.percentile(1.0)), (1_000, 1_000_000));
    }

    #[test]
    fn ddsketch_bin_boundary_estimates_meet_accuracy() {
        // With alpha = 1/3, values near 1024 ns exercise a bin boundary.
        // Check accuracy instead of relying on floating-point boundary placement.
        let policy = SamplingPolicy::DdSketch { alpha: 1.0 / 3.0, max_bins: 128 };
        for ns in [1023, 1024, 1025, 2047, 2048] {
            let storage = storage_with(&policy, [ns, ns]);
            let estimates = storage.percentiles(&[0.0, 0.5, 1.0]);
            assert_eq!((estimates[0], estimates[2]), (ns, ns));
            assert!(estimates[1].abs_diff(ns) * 3 <= ns + 3, "{ns}: {}", estimates[1]);
        }
    }

    // Build a weighted population through Xylem's record/merge boundary in
    // O(log count) merges, without allocating one value per observation.
    fn ddsketch_repeated(policy: &SamplingPolicy, ns: u64, mut count: u64) -> LatencyStorage {
        let mut result = LatencyStorage::from_policy(policy);
        let mut power = storage_with(policy, [ns]);
        while count != 0 {
            if count & 1 != 0 {
                result.merge_ref(&power);
            }
            count >>= 1;
            if count != 0 {
                let mut doubled = LatencyStorage::from_policy(policy);
                doubled.merge_ref(&power);
                doubled.merge_ref(&power);
                power = doubled;
            }
        }
        result
    }

    #[test]
    fn ddsketch_quantiles_and_counts_across_u32_boundary() {
        const HALF: u64 = 1 << 31;
        const LOW: u64 = 1_000_000;
        const HIGH: u64 = 1_000_000_000;
        let policy = SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 1024 };
        let below_half = ddsketch_repeated(&policy, LOW, HALF - 1);
        let low_half = ddsketch_repeated(&policy, LOW, HALF);
        let high_half = ddsketch_repeated(&policy, HIGH, HALF);
        let one_high = storage_with(&policy, [HIGH]);
        let quantiles = [0.0, 0.25, 0.5, 0.75, 1.0];

        for (low, extra, total, expected) in [
            (&below_half, false, (1u64 << 32) - 1, [LOW, LOW, HIGH, HIGH, HIGH]),
            (&low_half, false, 1u64 << 32, [LOW, LOW, LOW, HIGH, HIGH]),
            (&low_half, true, (1u64 << 32) + 1, [LOW, LOW, HIGH, HIGH, HIGH]),
        ] {
            let mut merged = LatencyStorage::from_policy(&policy);
            merged.merge_ref(low);
            merged.merge_ref(&high_half);
            if extra {
                merged.merge_ref(&one_high);
            }
            assert_eq!(merged.count(), total);
            assert_eq!(merged.percentile_sample_count(), total);
            let Quantiles::DdSketch(sketch) = &merged.quantiles else {
                unreachable!()
            };
            assert_eq!(sketch.count(), usize::try_from(total).unwrap_or(usize::MAX));
            assert_eq!((merged.min(), merged.max()), (Some(LOW), Some(HIGH)));
            for ((p, estimate), exact) in
                quantiles.into_iter().zip(merged.percentiles(&quantiles)).zip(expected)
            {
                assert!(
                    estimate.abs_diff(exact) <= exact / 100 + 1,
                    "count {total}, p{p}: {estimate}, expected {exact}"
                );
            }

            // Model 0.3.2 on a 32-bit target from Xylem's independent u64
            // total; this is not native execution of the old dependency.
            let narrowed = merged.count() as u32;
            let old_p75 = if narrowed == 0 {
                None
            } else {
                let rank = (0.75 * (f64::from(narrowed) - 1.0)) as u64;
                Some(if rank < low.count() { LOW } else { HIGH })
            };
            let expected_old_p75 = match total {
                n if n < (1u64 << 32) => Some(HIGH),
                n if n == (1u64 << 32) => None,
                _ => Some(LOW),
            };
            assert_eq!(old_p75, expected_old_p75);
        }
    }

    /// T-digest policy as parsed from a config that omits `compression`.
    fn default_tdigest_policy() -> SamplingPolicy {
        let policy: SamplingPolicy = serde_json::from_str(r#"{"type": "t-digest"}"#).unwrap();
        assert_eq!(policy, SamplingPolicy::TDigest { compression: 1000 });
        policy
    }

    #[test]
    fn tdigest_empty_merges_preserve_configuration_for_later_observations() {
        let policy = SamplingPolicy::TDigest { compression: 1000 };
        let mut merged = LatencyStorage::from_policy(&policy);
        let empty = LatencyStorage::from_policy(&policy);
        for _ in 0..3 {
            assert_eq!(merged.try_merge_ref(&empty), Ok(()));
            let Quantiles::TDigest { digest, pending } = &merged.quantiles else {
                panic!("expected a t-digest");
            };
            assert_eq!(digest.max_size(), 1000);
            assert!(digest.is_empty());
            assert!(pending.is_empty());
            assert_eq!(merged.count(), 0);
            assert_eq!(merged.percentile_sample_count(), 0);
            assert_eq!(merged.percentiles(&[0.0, 0.5, 1.0]), [0, 0, 0]);
            assert_eq!(merged.mean(), 0);
            assert_eq!((merged.min(), merged.max(), merged.std_dev()), (None, None, None));
        }

        let observed = record_tdigest(&[10_000, 20_000, 30_000]);
        assert_eq!(merged.try_merge_ref(&observed), Ok(()));
        assert_eq!(merged.count(), 3);
        assert_eq!(merged.percentile_sample_count(), 3);
        assert_eq!(merged.mean(), 20_000);
        assert_eq!(merged.percentiles(&[0.0, 0.5, 1.0]), [10_000, 20_000, 30_000]);
    }

    #[test]
    fn tdigest_empty_and_nonempty_merge_in_both_orders() {
        let policy = SamplingPolicy::TDigest { compression: 1000 };
        let flushed_only = vec![10_000; TDIGEST_BATCH_SIZE];
        let mut flushed_and_pending = flushed_only.clone();
        flushed_and_pending.extend([20_000, 30_000]);
        for (values, expected_p50, flushed, has_pending) in [
            (vec![10_000, 20_000, 30_000], 20_000, false, true),
            (flushed_only, 10_000, true, false),
            (flushed_and_pending, 10_000, true, true),
        ] {
            for empty_first in [true, false] {
                let nonempty = record_tdigest(&values);
                let Quantiles::TDigest { digest, pending } = &nonempty.quantiles else {
                    panic!("expected a t-digest");
                };
                assert_eq!(!digest.is_empty(), flushed);
                assert_eq!(!pending.is_empty(), has_pending);
                let expected_moments = nonempty.moments.clone();
                let empty = LatencyStorage::from_policy(&policy);
                let (mut merged, other) = if empty_first {
                    (empty, nonempty)
                } else {
                    (nonempty, empty)
                };
                let other_moments = other.moments.clone();
                let other_percentiles = other.percentiles(&[0.0, 0.5, 1.0]);
                assert_eq!(merged.try_merge_ref(&other), Ok(()));
                assert_eq!(merged.moments, expected_moments);
                assert_eq!(merged.count(), values.len() as u64);
                assert_eq!(merged.percentile_sample_count(), values.len() as u64);
                assert_eq!(
                    merged.percentiles(&[0.0, 0.5, 1.0]),
                    [10_000, expected_p50, *values.iter().max().unwrap()]
                );
                assert_eq!(other.moments, other_moments);
                assert_eq!(other.percentiles(&[0.0, 0.5, 1.0]), other_percentiles);
                let Quantiles::TDigest { digest, pending } = &merged.quantiles else {
                    panic!("expected a t-digest");
                };
                assert_eq!(digest.max_size(), 1000);
                assert!(pending.is_empty());
            }
        }
    }

    #[test]
    fn tdigest_merge_includes_both_pending_buffers_in_both_orders() {
        for reverse in [false, true] {
            let mut first = record_tdigest(&vec![10_000; TDIGEST_BATCH_SIZE]);
            first.record(Duration::from_nanos(20_000));
            let second = record_tdigest(&[30_000, 40_000]);
            let (mut merged, other) = if reverse {
                (second, first)
            } else {
                (first, second)
            };
            assert_eq!(merged.try_merge_ref(&other), Ok(()));
            assert_eq!(merged.count(), 1027);
            assert_eq!(merged.percentile_sample_count(), 1027);
            assert_eq!(merged.moments.sum, 10_330_000);
            assert_eq!(merged.moments.sum_sq, 105_300_000_000);
            assert_eq!((merged.min(), merged.max()), (Some(10_000), Some(40_000)));
            assert_eq!(merged.mean(), (10_330_000.0_f64 / 1027.0).round() as u64);
            let expected_std = ((1027.0_f64 * 105_300_000_000.0 - 10_330_000.0_f64.powi(2))
                / (1027.0 * 1026.0))
                .sqrt();
            assert!((merged.std_dev().unwrap() - expected_std).abs() < 1e-9);
            assert_eq!(merged.percentiles(&[0.0, 0.5, 1.0]), [10_000, 10_000, 40_000]);
            // This rank lies near the buffered 20 us sample; without it, the
            // estimate falls back near the 10 us population.
            let p9976 = merged.percentile(0.9976);
            assert!((15_000..=25_000).contains(&p9976), "reverse={reverse}, p99.76={p9976}");
            let Quantiles::TDigest { digest, pending } = &merged.quantiles else {
                panic!("expected a t-digest");
            };
            assert_eq!(digest.max_size(), 1000);
            assert!(pending.is_empty());
            assert_eq!(digest.count(), 1027.0);
        }
    }

    #[test]
    fn tdigest_merge_two_pending_only_buffers_in_both_orders() {
        for reverse in [false, true] {
            let first = record_tdigest(&[10_000; 4]);
            let second = record_tdigest(&[1_000_000; 4]);
            for storage in [&first, &second] {
                let Quantiles::TDigest { digest, pending } = &storage.quantiles else {
                    panic!("expected a t-digest");
                };
                assert!(digest.is_empty());
                assert_eq!(digest.count(), 0.0);
                assert!(!pending.is_empty());
            }
            let (mut merged, other) = if reverse {
                (second, first)
            } else {
                (first, second)
            };
            assert_eq!(merged.try_merge_ref(&other), Ok(()));
            let percentiles = merged.percentiles(&[0.25, 0.75]);
            assert!(percentiles[0] < 100_000, "reverse={reverse}, p25={}", percentiles[0]);
            assert!(percentiles[1] > 900_000, "reverse={reverse}, p75={}", percentiles[1]);
            let Quantiles::TDigest { digest, pending } = &merged.quantiles else {
                panic!("expected a t-digest");
            };
            assert_eq!(digest.max_size(), 1000);
            assert!(pending.is_empty());
            assert_eq!(digest.count(), 8.0);
            assert_eq!(merged.count(), 8);
            assert_eq!(merged.percentile_sample_count(), 8);
            assert_eq!(
                merged.moments,
                Moments {
                    count: 8,
                    sum: 4_040_000,
                    sum_sq: 4_000_400_000_000,
                    min: 10_000,
                    max: 1_000_000,
                }
            );
        }
    }

    fn tdigest_snapshot(storage: &LatencyStorage) -> (TDigest, Vec<f64>, Moments) {
        let Quantiles::TDigest { digest, pending } = &storage.quantiles else {
            panic!("expected a t-digest");
        };
        (digest.clone(), pending.clone(), storage.moments.clone())
    }

    #[test]
    fn tdigest_incompatible_compressions_are_rejected_even_when_empty() {
        for (mine, theirs) in [(1000, 100), (100, 1000)] {
            for (mine_value, their_value) in
                [(None, None), (Some(10_000), None), (None, Some(20_000))]
            {
                let mut left =
                    LatencyStorage::from_policy(&SamplingPolicy::TDigest { compression: mine });
                let mut right =
                    LatencyStorage::from_policy(&SamplingPolicy::TDigest { compression: theirs });
                if let Some(ns) = mine_value {
                    left.record(Duration::from_nanos(ns));
                }
                if let Some(ns) = their_value {
                    right.record(Duration::from_nanos(ns));
                }
                let left_before = tdigest_snapshot(&left);
                let right_before = tdigest_snapshot(&right);
                assert_eq!(
                    left.try_merge_ref(&right),
                    Err("latency storage configurations differ")
                );
                assert_eq!(tdigest_snapshot(&left), left_before);
                assert_eq!(tdigest_snapshot(&right), right_before);
            }
        }
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
    fn actual_load_loop_merge_uses_complete_valid_intervals() {
        use crate::stats::StatsRecorder;

        fn record_samples<S: StatsRecorder>(
            recorder: &mut S,
            start: Option<u64>,
            end: Option<u64>,
        ) {
            recorder.record_actual_load_loop_samples(start, end);
        }

        // A pre-existing recorder need not implement the new timing hook, even
        // if it has an unrelated method with the interval-recording name.
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
        record_samples(&mut recorder, None, Some(1));

        let mut a = TupleStatsCollector::default();
        a.record_actual_load_loop(200, 400);
        let mut b = TupleStatsCollector::default();
        b.record_actual_load_loop(100, 300);
        let merged = TupleStatsCollector::merge(vec![a, TupleStatsCollector::default(), b]);
        assert_eq!(merged.actual_load_loop_unix_us(), Some((100, 400)));
        assert_eq!(TupleStatsCollector::merge(vec![]).actual_load_loop_unix_us(), None);

        let mut first = TupleStatsCollector::default();
        first.record_actual_load_loop_samples(Some(100), Some(200));
        let mut second = TupleStatsCollector::default();
        record_samples(&mut second, Some(300), Some(400));
        let merged = TupleStatsCollector::merge(vec![first, second]);
        assert_eq!(merged.actual_load_loop_unix_us(), Some((100, 400)), "gaps are included");

        let max_safe_integer = (1 << 53) - 1;
        let mut boundary = TupleStatsCollector::default();
        boundary.record_actual_load_loop(max_safe_integer, max_safe_integer);
        assert_eq!(boundary.actual_load_loop_unix_us(), Some((max_safe_integer, max_safe_integer)));
    }

    #[test]
    fn actual_load_loop_invalid_worker_poisons_merge_in_both_orders() {
        for (start, end) in [
            (Some(100), Some(90)),
            (None, Some(120)),
            (Some(100), None),
            (None, None),
            (Some(1), Some(1 << 53)),
            (Some(1 << 53), Some(1 << 53)),
        ] {
            for (invalid_first, another_valid) in
                [(false, false), (true, false), (false, true), (true, true)]
            {
                let mut valid = TupleStatsCollector::default();
                valid.record_actual_load_loop(110, 120);
                let mut invalid = TupleStatsCollector::default();
                invalid.record_actual_load_loop_samples(start, end);
                assert_eq!(invalid.actual_load_loop_unix_us(), None, "{start:?} {end:?}");
                if another_valid {
                    // A later valid interval in the same collector cannot restore it.
                    invalid.record_actual_load_loop(100, 200);
                    assert_eq!(invalid.actual_load_loop_unix_us(), None, "{start:?} {end:?}");
                }
                let workers = if invalid_first {
                    vec![invalid, valid]
                } else {
                    vec![valid, invalid]
                };
                let merged = TupleStatsCollector::merge(workers);
                assert_eq!(
                    merged.actual_load_loop_unix_us(),
                    None,
                    "{start:?} {end:?} {invalid_first} {another_valid}"
                );
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

    const BUCKET: Duration = Duration::from_millis(200);

    /// A collector 120 ms into its first 200 ms bucket: past its middle.
    /// The clock starts with the process, so this waits until its start can
    /// move back four buckets ([`advance`]).
    fn collector_past_middle(policy: SamplingPolicy) -> TupleStatsCollector {
        while crate::timing::time_ns() < 1_000_000_000 {
            std::thread::sleep(Duration::from_millis(10));
        }
        let start = crate::timing::time_ns() - 120_000_000;
        TupleStatsCollector::with_start_time(policy, BUCKET, start)
    }

    /// Move the collector's clock `buckets` buckets on
    fn advance(collector: &mut TupleStatsCollector, buckets: u64) {
        collector.experiment_start_ns -= buckets * BUCKET.as_nanos() as u64;
    }

    /// Record on connections 0 and 1 of group 0: bytes, latencies around
    /// `base_us` and an error reply
    fn record_traffic(collector: &mut TupleStatsCollector, base_us: u64) {
        for connection in 0..2 {
            collector.record_tx_bytes(0, connection, 64);
            for i in 0..100 {
                let latency = base_us + i * 37 + connection as u64 * 500;
                collector.record_latency(0, connection, Duration::from_micros(latency));
            }
            collector.record_error_reply(0, connection, "ERR", Duration::from_micros(base_us));
            collector.record_rx_bytes(0, connection, 128);
        }
    }

    /// Every aggregation and time series of group 0, for comparison
    fn outputs(collector: &TupleStatsCollector) -> String {
        let duration = Duration::from_secs(1);
        let mut connections: Vec<_> =
            collector.aggregate_by_connection(0, duration).into_iter().collect();
        connections.sort_by_key(|(id, _)| *id);
        let series: Vec<_> = (0..2)
            .map(|connection| collector.time_series_by_connection(0, 0, connection))
            .collect();
        format!(
            "{:?}\n{:?}\n{connections:?}\n{:?}\n{:?}\n{series:?}\n{:?}",
            collector.aggregate_global(duration),
            collector.aggregate_by_group(duration)[&0],
            collector.time_series_global(),
            collector.time_series_by_group(0),
            collector.time_buckets(),
        )
    }

    #[test]
    fn a_map_made_ahead_changes_nothing_recorded() {
        let mut ahead = collector_past_middle(SamplingPolicy::default());
        let mut plain = collector_past_middle(SamplingPolicy::default());
        plain.experiment_start_ns = ahead.experiment_start_ns;
        for collector in [&mut ahead, &mut plain] {
            record_traffic(collector, 1_000);
        }
        // One map, with room for this bucket's two entries
        let now = crate::timing::time_ns();
        assert!(ahead.prepare_next_bucket(now));
        assert!(!ahead.prepare_next_bucket(now));
        let (bucket, entries) = ahead.next_bucket.as_ref().unwrap();
        assert_eq!(*bucket, 1);
        assert!(entries.is_empty() && entries.capacity() >= 2);
        // It is no bucket until that bucket records
        plain.first_record_ns = ahead.first_record_ns;
        assert_eq!(outputs(&ahead), outputs(&plain));

        // The next bucket takes it
        for collector in [&mut ahead, &mut plain] {
            advance(collector, 1);
            record_traffic(collector, 2_000);
            advance(collector, 1);
            record_traffic(collector, 30_000);
        }
        assert!(ahead.next_bucket.is_none());
        assert_eq!(outputs(&ahead), outputs(&plain));
    }

    #[test]
    fn maps_are_made_ahead_only_late_in_a_bucket_with_entries() {
        // The first half of a bucket
        let start = crate::timing::time_ns();
        let mut early =
            TupleStatsCollector::with_start_time(SamplingPolicy::default(), BUCKET, start);
        record_traffic(&mut early, 1_000);
        assert!(!early.prepare_next_bucket(start));

        // The drain after the window
        let mut drained = collector_past_middle(SamplingPolicy::default());
        record_traffic(&mut drained, 1_000);
        drained.end_measurement(crate::timing::time_ns());
        assert!(!drained.prepare_next_bucket(crate::timing::time_ns()));

        // Without records, totals need no bucket
        let mut totals = collector_past_middle(SamplingPolicy::default());
        totals.set_keep_records(false);
        record_traffic(&mut totals, 1_000);
        assert!(totals.buckets.is_empty());
        assert!(!totals.prepare_next_bucket(crate::timing::time_ns()));
    }

    #[test]
    fn a_map_made_ahead_for_a_bucket_that_passed_is_not_taken() {
        let mut ahead = collector_past_middle(SamplingPolicy::default());
        let mut plain = collector_past_middle(SamplingPolicy::default());
        plain.experiment_start_ns = ahead.experiment_start_ns;
        for collector in [&mut ahead, &mut plain] {
            record_traffic(collector, 1_000);
        }
        assert!(ahead.prepare_next_bucket(crate::timing::time_ns()));
        // Nothing recorded in bucket 1; bucket 2 makes its own map
        for collector in [&mut ahead, &mut plain] {
            advance(collector, 2);
            record_traffic(collector, 3_000);
        }
        assert!(matches!(ahead.next_bucket, Some((1, _))));
        assert!(ahead.prepare_next_bucket(crate::timing::time_ns()));
        assert!(matches!(ahead.next_bucket, Some((3, _))));
        plain.first_record_ns = ahead.first_record_ns;
        assert_eq!(outputs(&ahead), outputs(&plain));
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
        merged.buckets.entry(3).or_default().global = Some(entry);
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
        assert_eq!(collector.entries().map(|(_, e)| e.rx_bytes).sum::<u64>(), 10);
        assert_eq!(collector.request_counts(0).completed, 2);
    }

    #[test]
    fn percentile_sample_count_tracks_merged_reservoirs_and_error_summaries() {
        let policy = SamplingPolicy::Limited { max_samples: 3, rate: 1.0 };
        let collector = |thread_id| {
            let mut collector = TupleStatsCollector::new(policy.clone(), Duration::from_secs(3600));
            collector.set_thread_id(thread_id);
            collector
        };
        let mut first = collector(0);
        let mut second = collector(1);
        for collector in [&mut first, &mut second] {
            for i in 0..5 {
                collector.record_latency(1, i % 2, Duration::from_micros(10 + i as u64));
                collector.record_error_reply(1, i % 2, "error", Duration::from_micros(20));
            }
        }
        let merged = TupleStatsCollector::merge(vec![first, second]);
        let duration = Duration::from_secs(1);
        for summary in [
            merged.aggregate_global(duration),
            merged.aggregate_by_group(duration)[&1].clone(),
            merged.error_latency_global(),
            merged.error_latency(1),
        ] {
            assert_eq!(summary.latency_sample_count, 10);
            assert_eq!(summary.percentile_sample_count, Some(3));
        }
        assert_eq!(merged.error_latency(2).percentile_sample_count, Some(0));
    }

    #[test]
    fn percentile_sample_count_respects_distinct_global_and_group_policies() {
        let collector = |thread_id| {
            let mut collector = TupleStatsCollector::new(
                SamplingPolicy::HdrHistogram { sigfigs: 3, max_value_us: None },
                Duration::from_secs(3600),
            );
            collector
                .register_group_policy(1, SamplingPolicy::Limited { max_samples: 3, rate: 1.0 });
            collector.set_thread_id(thread_id);
            collector
        };
        let mut first = collector(0);
        let mut second = collector(1);
        for collector in [&mut first, &mut second] {
            for _ in 0..5 {
                collector.record_latency(1, 0, Duration::from_micros(10));
                collector.record_error_reply(1, 0, "error", Duration::from_micros(20));
            }
        }
        let merged = TupleStatsCollector::merge(vec![first, second]);
        let duration = Duration::from_secs(1);
        assert_eq!(merged.aggregate_global(duration).percentile_sample_count, Some(10));
        assert_eq!(merged.aggregate_by_group(duration)[&1].percentile_sample_count, Some(3));
        assert_eq!(merged.error_latency_global().percentile_sample_count, Some(10));
        assert_eq!(merged.error_latency(1).percentile_sample_count, Some(3));
    }

    #[test]
    fn ddsketch_percentile_sample_count_crosses_u32_boundary() {
        let policy = SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 128 };
        // Include zero-bucket values and a wide enough positive range to collapse bins.
        let mut storage = storage_with(&policy, [0, 1, 100, 1_000_000_000_000]);
        assert!(storage.percentile(0.75) > 200);

        // Doubling a real sketch builds 2^32 represented observations in 30 merges.
        for _ in 0..30 {
            let Quantiles::DdSketch(sketch) = &storage.quantiles else {
                unreachable!()
            };
            // Every count in this loop still fits in a 32-bit usize.
            assert_eq!(sketch.count() as u64, storage.count());
            let copy = LatencyStorage {
                quantiles: Quantiles::DdSketch(sketch.clone()),
                moments: storage.moments.clone(),
            };
            storage.merge_ref(&copy);
        }
        let boundary = 1_u64 << 32;
        assert_eq!(storage.count(), boundary);
        assert_eq!(storage.percentile_sample_count(), boundary);
        let Quantiles::DdSketch(sketch) = &storage.quantiles else {
            unreachable!()
        };
        // The public count saturates when usize cannot hold the u64 total.
        assert_eq!(sketch.count(), usize::try_from(boundary).unwrap_or(usize::MAX));

        storage.record(Duration::from_nanos(5));
        storage.merge_ref(&storage_with(&policy, [0, 1_000_000_000_000]));
        assert_eq!(storage.count(), boundary + 3);
        assert_eq!(storage.percentile_sample_count(), boundary + 3);
        let Quantiles::DdSketch(sketch) = &storage.quantiles else {
            unreachable!()
        };
        assert_eq!(sketch.count(), usize::try_from(boundary + 3).unwrap_or(usize::MAX));
    }

    #[test]
    fn percentile_sample_count_tracks_sketches_and_pending_values() {
        let policies = [
            SamplingPolicy::None,
            SamplingPolicy::Unlimited,
            SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 1024 },
            SamplingPolicy::TDigest { compression: 100 },
            SamplingPolicy::HdrHistogram { sigfigs: 3, max_value_us: Some(10) },
        ];
        for policy in policies {
            let mut first = LatencyStorage::from_policy(&policy);
            let mut second = LatencyStorage::from_policy(&policy);
            for _ in 0..1025 {
                first.record(Duration::from_micros(20));
            }
            second.record(Duration::from_micros(1));
            let expected = if matches!(policy, SamplingPolicy::None) {
                0
            } else {
                1025
            };
            assert_eq!(first.percentile_sample_count(), expected, "{policy:?}");
            first.merge(second);
            assert_eq!(
                first.percentile_sample_count(),
                if expected == 0 { 0 } else { 1026 },
                "{policy:?}"
            );
        }
    }

    #[test]
    fn aggregated_percentile_count_serialization_defaults_for_old_data() {
        let mut collector =
            TupleStatsCollector::new(SamplingPolicy::default(), Duration::from_secs(1));
        collector.record_latency(0, 0, Duration::from_micros(10));
        let stats = collector.aggregate_global(Duration::from_secs(1));
        let mut json = serde_json::to_value(stats).unwrap();
        assert_eq!(json["percentile_sample_count"], 1);
        let decoded: AggregatedStats = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(decoded.percentile_sample_count, Some(1));
        json.as_object_mut().unwrap().remove("percentile_sample_count");
        let old: AggregatedStats = serde_json::from_value(json).unwrap();
        assert_eq!(old.percentile_sample_count, None);
        assert!(serde_json::to_value(&old).unwrap().get("percentile_sample_count").is_none());
        assert_eq!(old.latency_sample_count, 1);
    }

    #[test]
    fn empty_aggregated_estimator_serializes_an_explicit_zero() {
        let collector = TupleStatsCollector::new(SamplingPolicy::None, Duration::from_secs(1));
        let stats = collector.aggregate_global(Duration::from_secs(1));
        assert_eq!(stats.percentile_sample_count, Some(0));
        let json = serde_json::to_value(stats).unwrap();
        assert_eq!(json.get("percentile_sample_count"), Some(&serde_json::json!(0)));
        let decoded: AggregatedStats = serde_json::from_value(json).unwrap();
        assert_eq!(decoded.percentile_sample_count, Some(0));
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

    #[test]
    fn hdr_bins_are_the_crate_histogram_bins() {
        for sigfig in 0..=5 {
            let layout = HdrLayout::new(sigfig, None);
            let hist = Histogram::<u64>::new(sigfig).unwrap();
            let mut values = vec![0, 1, 2, 3, u64::MAX / 4, u64::MAX - 1, u64::MAX];
            for shift in 1..64 {
                let v = 1u64 << shift;
                values.extend([v - 1, v, v + 1, v + v / 3]);
            }
            for v in values {
                let bin = layout.bin(v);
                assert_eq!(layout.lowest(bin), hist.lowest_equivalent(v), "{sigfig} {v}");
                // Percentiles take the highest value of a bin from its lowest
                let highest = hist.highest_equivalent(hist.lowest_equivalent(v));
                assert_eq!(layout.highest(bin), highest, "{sigfig} {v}");
            }
            // Bins count every value once, in ascending order from 0
            let bins = layout.bins_to_cover(1 << 40);
            assert_eq!(layout.lowest(0), 0);
            for bin in 0..bins {
                assert_eq!(layout.bin(layout.lowest(bin)), bin, "{sigfig} {bin}");
                assert_eq!(layout.bin(layout.highest(bin)), bin, "{sigfig} {bin}");
                assert_eq!(layout.lowest(bin + 1), layout.highest(bin) + 1, "{sigfig} {bin}");
            }
            for high in [2, 3, 1_000, 2_047, 2_048, 5_000_000, 10_000_000_000, 1 << 40] {
                let bounded = Histogram::<u64>::new_with_max(high, sigfig).unwrap();
                assert_eq!(
                    layout.bins_to_cover(high),
                    bounded.distinct_values(),
                    "{sigfig} {high}"
                );
                assert_eq!(
                    HdrLayout::new(sigfig, Some(high)).bound,
                    Some((high, bounded.distinct_values()))
                );
            }
        }
    }

    /// A fixed sequence of latencies: mostly around 1 ms, some in the first
    /// bucket of an HDR histogram, and outliers past 5 ms
    fn next_latency(state: &mut u64) -> Duration {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let r = *state >> 33;
        Duration::from_nanos(match r % 16 {
            0 => r % 3_000,
            1 => 5_000_000 + r % 60_000_000,
            _ => 900_000 + r % 400_000,
        })
    }

    /// Everything a storage reports
    fn storage_outputs(storage: &LatencyStorage) -> String {
        let ps = [0.0, 1e-9, 0.25, 0.5, 0.9, 0.99, 0.999, 0.99999, 1.0, 1.5];
        format!(
            "{:?} {} {} {:?} {:?} {:?} {:?} {:?}",
            storage.percentiles(&ps),
            storage.count(),
            storage.percentile_sample_count(),
            storage.clamped_count(),
            storage.min(),
            storage.max(),
            storage.mean(),
            storage.std_dev(),
        )
    }

    /// A histogram and bins of `policy` that recorded the same latencies
    fn dense_and_compact(policy: &SamplingPolicy, state: &mut u64) -> [LatencyStorage; 2] {
        let mut dense = LatencyStorage::from_policy(policy);
        let mut compact = LatencyStorage::compact_from_policy(policy);
        assert!(matches!(compact.quantiles, Quantiles::HdrBins { .. }));
        for _ in 0..3_000 {
            let latency = next_latency(state);
            dense.record(latency);
            compact.record(latency);
        }
        assert_eq!(storage_outputs(&dense), storage_outputs(&compact), "{policy:?}");
        [dense, compact]
    }

    #[test]
    fn compact_hdr_storage_reports_and_merges_like_the_histogram() {
        let policies = [1, 2, 3, 5].into_iter().flat_map(|sigfigs| {
            [None, Some(5_000), Some(1)]
                .map(|max_value_us| SamplingPolicy::HdrHistogram { sigfigs, max_value_us })
        });
        for policy in policies {
            let mut state = 7;
            let [dense_a, mut compact_a] = dense_and_compact(&policy, &mut state);
            let [dense_b, compact_b] = dense_and_compact(&policy, &mut state);
            let mut expected = LatencyStorage::from_policy(&policy);
            expected.merge_ref(&dense_a);
            expected.merge_ref(&dense_b);
            let expected = storage_outputs(&expected);
            // Into a histogram, into bins, and a histogram into bins
            let mut into_dense = LatencyStorage::from_policy(&policy);
            into_dense.merge_ref(&compact_a);
            into_dense.merge_ref(&compact_b);
            assert_eq!(storage_outputs(&into_dense), expected, "{policy:?}");
            let mut into_compact = LatencyStorage::compact_from_policy(&policy);
            into_compact.merge_ref(&compact_a);
            into_compact.merge_ref(&compact_b);
            assert_eq!(storage_outputs(&into_compact), expected, "{policy:?}");
            compact_a.merge_ref(&dense_b);
            assert_eq!(storage_outputs(&compact_a), expected, "{policy:?}");
        }
        // Bins of different layouts do not merge
        let mut a = LatencyStorage::compact_from_policy(&SamplingPolicy::default());
        let other = SamplingPolicy::HdrHistogram { sigfigs: 3, max_value_us: Some(1_000) };
        assert!(a.try_merge_ref(&LatencyStorage::compact_from_policy(&other)).is_err());
        assert!(a.try_merge_ref(&LatencyStorage::from_policy(&other)).is_err());
        assert!(LatencyStorage::from_policy(&other).try_merge_ref(&a).is_err());
    }

    use super::super::legacy_tuple_collector as legacy;

    const TDIGEST: SamplingPolicy = SamplingPolicy::TDigest { compression: 100 };

    /// Group policies of the equivalence tests: every policy, and HDR
    /// histograms bounded below some latencies or with one figure. The
    /// t-digest is last: merging digests gives an estimate that depends on
    /// their order.
    fn equivalence_policies() -> Vec<SamplingPolicy> {
        vec![
            SamplingPolicy::default(),
            SamplingPolicy::HdrHistogram { sigfigs: 2, max_value_us: Some(5_000) },
            SamplingPolicy::HdrHistogram { sigfigs: 1, max_value_us: None },
            SamplingPolicy::Unlimited,
            SamplingPolicy::None,
            SamplingPolicy::Limited { max_samples: 1 << 20, rate: 1.0 },
            SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 1024 },
            TDIGEST,
        ]
    }

    /// Wait out a bucket boundary that is near, so that an event recorded
    /// in both collectors lands in the same bucket of each
    fn off_boundary(start_ns: u64) {
        let bucket = BUCKET.as_nanos() as u64;
        loop {
            let position = (crate::timing::time_ns() - start_ns) % bucket;
            if position + 20_000_000 < bucket {
                return;
            }
            std::thread::sleep(Duration::from_nanos(bucket - position + 1_000_000));
        }
    }

    /// Record the same traffic of one thread in a collector and in the
    /// collector as of 112813b: three buckets of every group, connections
    /// with different counts, one that skips a bucket, error replies,
    /// bytes, and latencies drained after the window
    fn equivalence_pair(
        global: &SamplingPolicy,
        keep_records: bool,
        thread_id: usize,
        start: u64,
    ) -> (TupleStatsCollector, legacy::TupleStatsCollector) {
        let mut new = TupleStatsCollector::with_start_time(global.clone(), BUCKET, start);
        let mut old = legacy::TupleStatsCollector::with_start_time(global.clone(), BUCKET, start);
        new.set_keep_records(keep_records);
        new.set_thread_id(thread_id);
        old.set_thread_id(thread_id);
        for (group, policy) in equivalence_policies().into_iter().enumerate() {
            new.register_group_policy(group, policy.clone());
            old.register_group_policy(group, policy);
        }
        macro_rules! both {
            ($method:ident($($arg:expr),*)) => {{
                off_boundary(new.experiment_start_ns);
                new.$method($($arg),*);
                old.$method($($arg),*);
            }};
        }
        let mut state = thread_id as u64 + 1;
        let groups = equivalence_policies().len();
        for bucket in 0..3 {
            if bucket > 0 {
                advance(&mut new, 1);
                old.advance_ns(BUCKET.as_nanos() as u64);
            }
            let connections = (0..groups).flat_map(|group| (0..3).map(move |c| (group, c)));
            // One connection of every other group skips the second bucket
            for (group, connection) in
                connections.filter(|&(group, c)| !(bucket == 1 && c == 2 && group % 2 == 1))
            {
                both!(record_tx_bytes(group, connection, 64));
                for latency in (0..20 + 15 * connection).map(|_| next_latency(&mut state)) {
                    both!(record_latency(group, connection, latency));
                }
                let latency = next_latency(&mut state);
                both!(record_error_reply(group, connection, "ERR", latency));
                both!(record_rx_bytes(group, connection, 128 + connection));
            }
        }
        let end = crate::timing::time_ns();
        both!(end_measurement(end));
        for group in 0..equivalence_policies().len() {
            let latency = next_latency(&mut state);
            both!(record_latency(group, 1, latency));
            both!(record_rx_bytes(group, 1, 32));
        }
        // The first event's time differs by the time to record it once
        new.first_record_ns = old.measurement_window_ns().map(|(first, _)| first);
        (new, old)
    }

    /// Every output of a collector of either kind: aggregates, error
    /// latencies and counts, and with records, each record's fields, time
    /// series and per-connection aggregates
    macro_rules! equivalence_outputs {
        ($collector:expr, $groups:expr, $records:expr, $entries:expr) => {{
            let c = &$collector;
            let d = Duration::from_secs(1);
            let groups = $groups;
            let mut by_group: Vec<_> = c
                .aggregate_by_group(d)
                .into_iter()
                .filter(|(group, _)| *group < groups)
                .collect();
            by_group.sort_by_key(|(group, _)| *group);
            let mut out = format!(
                "{:?}\n{by_group:?}\n{:?}\n{:?}\n{:?}\n",
                c.aggregate_global(d),
                c.error_latency_global(),
                c.request_counts_global(),
                c.group_ids(),
            );
            for group in 0..groups {
                out += &format!("{:?} {:?}\n", c.error_latency(group), c.request_counts(group));
            }
            if $records {
                let mut rows: Vec<_> = $entries
                    .map(|(k, e)| {
                        (
                            (k.time_bucket, k.group_id, k.thread_id, k.connection_id),
                            (e.request_count, e.tx_bytes, e.rx_bytes),
                            (e.latency.count(), e.latency.percentile_sample_count()),
                            e.latency.percentiles(&[0.5, 0.99, 0.999]),
                            (e.latency.mean(), e.latency.min(), e.latency.max()),
                            e.latency.clamped_count(),
                        )
                    })
                    .collect();
                rows.sort_by_key(|row| row.0);
                out += &format!("{rows:?}\n{:?}\n{:?}\n", c.time_series_global(), c.time_buckets());
                for group in 0..groups {
                    let mut connections: Vec<_> =
                        c.aggregate_by_connection(group, d).into_iter().collect();
                    connections.sort_by_key(|(id, _)| *id);
                    out += &format!(
                        "{:?}\n{:?}\n{connections:?}\n",
                        c.time_series_by_group(group),
                        c.connection_ids(group),
                    );
                    for (thread, connection) in c.connection_ids(group) {
                        let series = c.time_series_by_connection(group, thread, connection);
                        out += &format!("{series:?}\n");
                    }
                }
            }
            out
        }};
    }

    /// The merged collectors of two threads that recorded the same traffic,
    /// and the merged collectors as of 112813b
    fn equivalence_collectors(
        global: &SamplingPolicy,
        keep_records: bool,
    ) -> (TupleStatsCollector, legacy::TupleStatsCollector) {
        while crate::timing::time_ns() < 1_000_000_000 {
            std::thread::sleep(Duration::from_millis(10));
        }
        let start = crate::timing::time_ns() - 10_000_000;
        let (new_a, old_a) = equivalence_pair(global, keep_records, 0, start);
        let (new_b, old_b) = equivalence_pair(global, keep_records, 3, start);
        (
            TupleStatsCollector::merge(vec![new_a, new_b]),
            legacy::TupleStatsCollector::merge(vec![old_a, old_b]),
        )
    }

    #[test]
    fn every_output_matches_the_collector_before_per_bucket_storage() {
        // Every group but the t-digest's, under every global policy but it
        let groups = equivalence_policies().len() - 1;
        for keep_records in [true, false] {
            for global in &equivalence_policies()[..groups] {
                let (new, old) = equivalence_collectors(global, keep_records);
                assert_eq!(
                    equivalence_outputs!(new, groups, keep_records, new.entries()),
                    equivalence_outputs!(old, groups, keep_records, old.entries().iter()),
                    "global {global:?}, records {keep_records}"
                );
            }
        }
    }

    /// The outputs of two collectors that recorded the same responses under
    /// `global` with every group policy, one with `record_response`, the
    /// other with the bytes, latency and breakdown of each apart
    fn response_outputs(global: &SamplingPolicy, keep_records: bool) -> (String, String) {
        let groups = equivalence_policies().len();
        let start = crate::timing::time_ns() - 10_000_000;
        let mut one = TupleStatsCollector::with_start_time(global.clone(), BUCKET, start);
        let mut three = TupleStatsCollector::with_start_time(global.clone(), BUCKET, start);
        for collector in [&mut one, &mut three] {
            collector.set_keep_records(keep_records);
            for (group, policy) in equivalence_policies().into_iter().enumerate() {
                collector.register_group_policy(group, policy);
            }
        }
        let mut state = 1;
        let mut record = |one: &mut TupleStatsCollector, three: &mut TupleStatsCollector, group| {
            for connection in 0..3 {
                let latency = next_latency(&mut state);
                let breakdown = LatencyBreakdown { service: latency, kernel: None };
                let bytes = 100 + connection;
                off_boundary(one.experiment_start_ns);
                one.record_response(group, connection, bytes, latency, &breakdown);
                three.record_rx_bytes(group, connection, bytes);
                three.record_latency(group, connection, latency);
                three.record_breakdown(group, &breakdown);
            }
        };
        for bucket in 0..2 {
            if bucket > 0 {
                advance(&mut one, 1);
                advance(&mut three, 1);
            }
            for _ in 0..10 {
                (0..groups).for_each(|group| record(&mut one, &mut three, group));
            }
        }
        // Requests completed during the drain count toward latency only
        let end = crate::timing::time_ns();
        one.end_measurement(end);
        three.end_measurement(end);
        (0..groups).for_each(|group| record(&mut one, &mut three, group));
        three.first_record_ns = one.first_record_ns;
        let outputs = |c: &TupleStatsCollector| {
            let breakdowns: Vec<_> = (0..groups).map(|group| c.latency_breakdown(group)).collect();
            let out = equivalence_outputs!(c, groups, keep_records, c.entries());
            format!("{out}{breakdowns:?}\n{:?}", c.latency_breakdown_global())
        };
        (outputs(&one), outputs(&three))
    }

    #[test]
    fn a_response_records_as_its_bytes_latency_and_breakdown_do() {
        // Room to move the start back by a bucket
        while crate::timing::time_ns() < 1_000_000_000 {
            std::thread::sleep(Duration::from_millis(10));
        }
        for keep_records in [true, false] {
            for global in &equivalence_policies() {
                let (one, three) = response_outputs(global, keep_records);
                assert_eq!(one, three, "global {global:?}, records {keep_records}");
            }
        }
    }

    #[test]
    fn tdigest_outputs_match_the_collector_before_but_for_merge_order() {
        let group = equivalence_policies().len() - 1;
        let d = Duration::from_secs(1);
        let (new, old) = equivalence_collectors(&TDIGEST, true);
        // Each record is one digest, merged from the threads in order
        let groups = equivalence_policies().len();
        let records = |out: String| out.lines().nth(groups + 5).unwrap().to_string();
        assert_eq!(
            records(equivalence_outputs!(new, groups, true, new.entries())),
            records(equivalence_outputs!(old, groups, true, old.entries().iter())),
        );
        // Counts and moments of aggregates are exact
        let without_percentiles = |stats: AggregatedStats| AggregatedStats {
            latency_p50: Duration::ZERO,
            latency_p95: Duration::ZERO,
            latency_p99: Duration::ZERO,
            latency_p999: Duration::ZERO,
            latency_p9999: Duration::ZERO,
            latency_p99999: Duration::ZERO,
            ..stats
        };
        assert_eq!(
            format!("{:?}", without_percentiles(new.aggregate_global(d))),
            format!("{:?}", without_percentiles(old.aggregate_global(d))),
        );
        // The group's aggregate merges the same digests in time, then key
        // order, where the collector before took the order of its map
        let mut keys: Vec<_> = old.entries().keys().filter(|k| k.group_id == group).collect();
        keys.sort_by_key(|k| (k.time_bucket, k.group_id, k.thread_id, k.connection_id));
        let mut merged = legacy::LatencyStorage::from_policy(&TDIGEST);
        for key in keys {
            merged.merge_ref(&old.entries()[key].latency);
        }
        let stats = &new.aggregate_by_group(d)[&group];
        let ps = [0.5, 0.95, 0.99, 0.999, 0.9999, 0.99999];
        let percentiles = [
            stats.latency_p50,
            stats.latency_p95,
            stats.latency_p99,
            stats.latency_p999,
            stats.latency_p9999,
            stats.latency_p99999,
        ]
        .map(|p| p.as_nanos() as u64);
        assert_eq!(percentiles.to_vec(), merged.percentiles(&ps));
        assert_eq!(
            format!("{:?}", without_percentiles(stats.clone())),
            format!("{:?}", without_percentiles(old.aggregate_by_group(d)[&group].clone())),
        );
    }
}
