//! Request accounting: what happened to every measured request, and how late
//! open-loop requests went out compared with their intended send times

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
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

/// Counts of measurement requests; warmup requests are excluded
///
/// Once a run ends, `sent = completed + timed_out + failed`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestCounts {
    /// Requests sent
    pub sent: u64,
    /// Requests whose response arrived, in the measurement window or the drain
    pub completed: u64,
    /// Requests still pending when the drain deadline passed. They have no latency.
    pub timed_out: u64,
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
        self.timed_out += other.timed_out;
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
