//! Request accounting: what happened to every measured request, and how late
//! open-loop requests went out compared with their intended send times

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

/// Send lag above which a send counts as late
///
/// The worker wakes a millisecond before a scheduled send and spins, so an
/// on-time send lags by the cost of a loop iteration and a send call, a few
/// microseconds. 100 µs is well above that jitter, yet below the service
/// latency of most servers, so a late send means the generator could not keep
/// its schedule (full pipeline, slow loop, preemption) rather than noise.
pub const LATE_SEND_THRESHOLD: Duration = Duration::from_micros(100);

/// Most intended sends counted as unsent per connection, so counting the end
/// of a schedule always finishes
pub const UNSENT_WALK_CAP: u64 = 10_000_000;

/// Most error reply kinds counted by name, per traffic group and globally;
/// error replies of later kinds count under [`OTHER_ERROR_KIND`]
pub const MAX_ERROR_KINDS: usize = 32;

/// Longest error reply kind kept, in bytes; a longer kind is cut
pub const MAX_ERROR_KIND_LEN: usize = 64;

/// Kind under which error replies past the first [`MAX_ERROR_KINDS`] kinds count
pub const OTHER_ERROR_KIND: &str = "other";

/// Error replies by kind, for at most [`MAX_ERROR_KINDS`] kinds plus
/// [`OTHER_ERROR_KIND`]
///
/// Kinds are named in the order they are first counted. Merging counts of
/// several threads keeps that bound, so with more kinds than the bound a kind
/// may be split between its own entry and `other`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ErrorKindCounts(BTreeMap<String, u64>);

impl ErrorKindCounts {
    /// Count `count` error replies of `kind`; only a new kind allocates
    pub fn add(&mut self, kind: &str, count: u64) {
        if let Some(total) = self.0.get_mut(kind) {
            *total += count;
            return;
        }
        let named = self.0.len() - usize::from(self.0.contains_key(OTHER_ERROR_KIND));
        if named < MAX_ERROR_KINDS {
            self.0.insert(kind.to_owned(), count);
        } else if let Some(total) = self.0.get_mut(OTHER_ERROR_KIND) {
            *total += count;
        } else {
            self.0.insert(OTHER_ERROR_KIND.to_owned(), count);
        }
    }

    /// Add other counts to these
    pub fn merge(&mut self, other: &Self) {
        for (kind, &count) in &other.0 {
            self.add(kind, count);
        }
    }

    /// Error replies of `kind`
    pub fn get(&self, kind: &str) -> u64 {
        self.0.get(kind).copied().unwrap_or(0)
    }

    /// Every kind with its count, sorted by kind
    pub fn iter(&self) -> impl Iterator<Item = (&str, u64)> {
        self.0.iter().map(|(kind, &count)| (kind.as_str(), count))
    }

    /// Whether no error reply was counted
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Counts of measurement requests; warmup requests are excluded
///
/// Once a run ends, `sent = completed + timed_out + failed`, where
/// `completed` includes the `errors`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestCounts {
    /// Requests sent
    pub sent: u64,
    /// Requests whose response arrived, in the measurement window or the
    /// drain, successful or an error reply
    pub completed: u64,
    /// Completed requests whose response was an error reply (a Redis error,
    /// an HTTP 4xx or 5xx, ...). Their latencies are kept apart, in the
    /// error latency summary.
    #[serde(default)]
    pub errors: u64,
    /// `errors` by kind (the Redis error code, the HTTP status, ...)
    #[serde(default)]
    pub errors_by_kind: ErrorKindCounts,
    /// Requests that got no response within their traffic group's
    /// `request_timeout`, or by the drain deadline. They have no latency.
    pub timed_out: u64,
    /// Responses that arrived after their request had timed out. They were
    /// dropped; their requests count in `timed_out`.
    #[serde(default)]
    pub late_responses: u64,
    /// Requests pending on a connection that closed or failed
    pub failed: u64,
    /// Sends whose lag behind their intended send time exceeded
    /// [`LATE_SEND_THRESHOLD`]
    pub late_sends: u64,
    /// Intended sends scheduled before the end of the measurement window
    /// that were never sent
    pub unsent: u64,
    /// Whether counting `unsent` stopped at [`UNSENT_WALK_CAP`] on a connection,
    /// so `unsent` is a lower bound
    pub unsent_capped: bool,
}

impl RequestCounts {
    /// Add another set of counts to these
    pub fn merge(&mut self, other: &Self) {
        self.sent += other.sent;
        self.completed += other.completed;
        self.errors += other.errors;
        self.errors_by_kind.merge(&other.errors_by_kind);
        self.timed_out += other.timed_out;
        self.late_responses += other.late_responses;
        self.failed += other.failed;
        self.late_sends += other.late_sends;
        self.unsent += other.unsent;
        self.unsent_capped |= other.unsent_capped;
    }
}

/// Send lags of open-loop requests, in nanoseconds
///
/// Every lag is recorded exactly in the mean and maximum; the 99th percentile
/// comes from an HDR histogram with three significant digits.
#[derive(Debug, Clone)]
pub struct SendLagStats {
    count: u64,
    sum_ns: u128,
    max_ns: u64,
    histogram: Histogram<u64>,
}

impl Default for SendLagStats {
    fn default() -> Self {
        Self {
            count: 0,
            sum_ns: 0,
            max_ns: 0,
            histogram: Histogram::new(3).expect("valid HDR histogram precision"),
        }
    }
}

impl SendLagStats {
    /// Record one send lag
    pub fn record(&mut self, lag: Duration) {
        let ns = lag.as_nanos().min(u64::MAX as u128) as u64;
        self.count += 1;
        self.sum_ns = self.sum_ns.saturating_add(ns as u128);
        self.max_ns = self.max_ns.max(ns);
        if self.histogram.record(ns).is_err() {
            // Only a value beyond any resize fails; keep it at the top.
            self.histogram.saturating_record(ns);
        }
    }

    /// Add another set of lags to these
    pub fn merge(&mut self, other: &Self) {
        self.count += other.count;
        self.sum_ns = self.sum_ns.saturating_add(other.sum_ns);
        self.max_ns = self.max_ns.max(other.max_ns);
        self.histogram.add(&other.histogram).expect("auto-resizing histograms merge");
    }

    /// Summarize the recorded lags
    pub fn summary(&self) -> SendLagSummary {
        let recorded = self.count > 0;
        SendLagSummary {
            sample_count: self.count,
            mean: recorded.then(|| Duration::from_nanos((self.sum_ns / self.count as u128) as u64)),
            p99: recorded.then(|| Duration::from_nanos(self.histogram.value_at_quantile(0.99))),
            max: recorded.then(|| Duration::from_nanos(self.max_ns)),
        }
    }
}

/// Summary of send lags; the values are `None` when no open-loop request was sent
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SendLagSummary {
    /// Number of open-loop measurement requests sent
    pub sample_count: u64,
    pub mean: Option<Duration>,
    pub p99: Option<Duration>,
    pub max: Option<Duration>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_kinds_beyond_the_bound_count_as_other() {
        let mut counts = ErrorKindCounts::default();
        for kind in 0..MAX_ERROR_KINDS + 3 {
            counts.add(&format!("E{kind}"), 2);
        }
        counts.add("E0", 1);
        counts.add("E40", 1);
        assert_eq!(counts.iter().count(), MAX_ERROR_KINDS + 1);
        assert_eq!(counts.get("E0"), 3);
        assert_eq!(counts.get(&format!("E{}", MAX_ERROR_KINDS - 1)), 2);
        assert_eq!(counts.get(&format!("E{MAX_ERROR_KINDS}")), 0);
        assert_eq!(counts.get(OTHER_ERROR_KIND), 3 * 2 + 1);

        // Merging keeps the bound; counts of known kinds and of other add up
        let mut other = ErrorKindCounts::default();
        other.add("E1", 5);
        other.add("NEW", 4);
        other.add(OTHER_ERROR_KIND, 1);
        counts.merge(&other);
        assert_eq!(counts.iter().count(), MAX_ERROR_KINDS + 1);
        assert_eq!(counts.get("E1"), 7);
        assert_eq!(counts.get("NEW"), 0);
        assert_eq!(counts.get(OTHER_ERROR_KIND), 7 + 4 + 1);
        let total: u64 = counts.iter().map(|(_, count)| count).sum();
        assert_eq!(total, (MAX_ERROR_KINDS as u64 + 3) * 2 + 2 + 10);

        // Below the bound, other is an ordinary kind name
        let mut few = ErrorKindCounts::default();
        few.add("WRONGTYPE", 1);
        assert_eq!(serde_json::to_string(&few).unwrap(), r#"{"WRONGTYPE":1}"#);
    }

    #[test]
    fn send_lag_summary_is_exact_for_mean_and_max() {
        assert_eq!(SendLagStats::default().summary(), SendLagSummary::default());
        let mut lags = SendLagStats::default();
        for us in 1..=100 {
            lags.record(Duration::from_micros(us));
        }
        let mut other = SendLagStats::default();
        other.record(Duration::from_millis(50));
        lags.merge(&other);
        let summary = lags.summary();
        assert_eq!(summary.sample_count, 101);
        assert_eq!(summary.max, Some(Duration::from_millis(50)));
        assert_eq!(summary.mean, Some(Duration::from_nanos((5050 + 50_000) * 1000 / 101)));
        let p99 = summary.p99.unwrap();
        assert!(p99 >= Duration::from_micros(99) && p99 <= Duration::from_micros(101), "{p99:?}");
    }
}
