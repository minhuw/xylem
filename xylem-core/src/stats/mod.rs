//! Statistics collection and analysis

use serde::{Deserialize, Serialize};
use std::time::Duration;

pub mod breakdown;
pub mod policy;
pub mod requests;
pub mod tuple_collector;

// Re-export main types
pub use breakdown::{
    BreakdownClock, BreakdownStats, BreakdownSummary, KernelSplit, LatencyBreakdown, PartSummary,
};
pub use policy::SamplingPolicy;
pub use requests::{
    ErrorKindCounts, RequestCounts, SendLagStats, SendLagSummary, LATE_SEND_THRESHOLD,
    MAX_ERROR_KINDS, MAX_ERROR_KIND_LEN, OTHER_ERROR_KIND, UNSENT_WALK_CAP,
};
pub use tuple_collector::{
    ConnectionStats, LatencyStorage, StatsEntry, StatsKey, TimeSeriesPoint, TupleStatsCollector,
};

/// Trait for statistics collectors used by Worker
///
/// `TupleStatsCollector` implements this trait and groups stats by
/// (time_bucket, group_id, connection_id).
pub trait StatsRecorder: Send {
    /// Record a latency sample of a request completed by a successful response
    ///
    /// A latency recorded after [`Self::end_measurement`] belongs to a request
    /// that completed during the drain: it counts toward the latency
    /// distribution and the completed requests, but not toward throughput.
    ///
    /// # Arguments
    /// * `group_id` - Traffic group ID
    /// * `connection_id` - Connection ID within the group
    /// * `latency` - Measured latency
    fn record_latency(&mut self, group_id: usize, connection_id: usize, latency: Duration);

    /// Record a measurement request completed by an error reply of `kind`
    ///
    /// It counts as completed and toward throughput like a successful
    /// response, and in the errors by kind. Its latency is kept apart from
    /// the latency distribution of successful responses.
    fn record_error_reply(
        &mut self,
        _group_id: usize,
        _connection_id: usize,
        _kind: &str,
        _latency: Duration,
    ) {
    }

    /// Record where the latency of a completed request went (see
    /// [`LatencyBreakdown`]); called with each [`Self::record_latency`], so
    /// never for an error reply (see [`Self::record_error_reply`]), whose
    /// latency stays out of the breakdown as it stays out of the latency
    fn record_breakdown(
        &mut self,
        _group_id: usize,
        _connection_id: usize,
        _breakdown: &LatencyBreakdown,
    ) {
    }

    /// Record transmitted bytes
    fn record_tx_bytes(&mut self, group_id: usize, connection_id: usize, bytes: usize);

    /// Record received bytes
    fn record_rx_bytes(&mut self, group_id: usize, connection_id: usize, bytes: usize);

    /// Mark the start of the measurement loop (a `timing::time_ns` value),
    /// which every thread shares, before anything is recorded. Collectors
    /// take it as the epoch of their time buckets.
    fn start_measurement(&mut self, _start_ns: u64) {}

    /// Mark the end of the measurement loop (a `timing::time_ns` value).
    /// Responses drained after it still count toward latency, not throughput.
    fn end_measurement(&mut self, _end_ns: u64) {}

    /// Record a measurement request that was sent, with its lag behind its
    /// intended send time (`None` for a closed-loop request)
    fn record_sent(&mut self, _group_id: usize, _connection_id: usize, _lag: Option<Duration>) {}

    /// Record measurement requests that timed out: their request timeout
    /// expired, or they were still pending when the drain deadline passed
    fn record_timed_out(&mut self, _group_id: usize, _connection_id: usize, _count: u64) {}

    /// Record responses that arrived after their measurement request timed out
    fn record_late_responses(&mut self, _group_id: usize, _connection_id: usize, _count: u64) {}

    /// Record measurement requests pending on a connection that closed or failed
    fn record_failed(&mut self, _group_id: usize, _connection_id: usize, _count: u64) {}

    /// Record intended sends before the end of the window that were never
    /// sent; `capped` when counting stopped at its cap
    fn record_unsent(
        &mut self,
        _group_id: usize,
        _connection_id: usize,
        _count: u64,
        _capped: bool,
    ) {
    }
}

// Implement StatsRecorder for TupleStatsCollector (uses all dimensions)
impl StatsRecorder for TupleStatsCollector {
    fn record_latency(&mut self, group_id: usize, connection_id: usize, latency: Duration) {
        TupleStatsCollector::record_latency(self, group_id, connection_id, latency);
    }

    fn record_error_reply(
        &mut self,
        group_id: usize,
        connection_id: usize,
        kind: &str,
        latency: Duration,
    ) {
        TupleStatsCollector::record_error_reply(self, group_id, connection_id, kind, latency);
    }

    fn record_breakdown(
        &mut self,
        group_id: usize,
        _connection_id: usize,
        breakdown: &LatencyBreakdown,
    ) {
        TupleStatsCollector::record_breakdown(self, group_id, breakdown);
    }

    fn record_tx_bytes(&mut self, group_id: usize, connection_id: usize, bytes: usize) {
        TupleStatsCollector::record_tx_bytes(self, group_id, connection_id, bytes);
    }

    fn record_rx_bytes(&mut self, group_id: usize, connection_id: usize, bytes: usize) {
        TupleStatsCollector::record_rx_bytes(self, group_id, connection_id, bytes);
    }

    fn start_measurement(&mut self, start_ns: u64) {
        TupleStatsCollector::start_measurement(self, start_ns);
    }

    fn end_measurement(&mut self, end_ns: u64) {
        TupleStatsCollector::end_measurement(self, end_ns);
    }

    fn record_sent(&mut self, group_id: usize, _connection_id: usize, lag: Option<Duration>) {
        TupleStatsCollector::record_sent(self, group_id, lag);
    }

    fn record_timed_out(&mut self, group_id: usize, _connection_id: usize, count: u64) {
        self.request_counts_mut(group_id).timed_out += count;
    }

    fn record_late_responses(&mut self, group_id: usize, _connection_id: usize, count: u64) {
        self.request_counts_mut(group_id).late_responses += count;
    }

    fn record_failed(&mut self, group_id: usize, _connection_id: usize, count: u64) {
        self.request_counts_mut(group_id).failed += count;
    }

    fn record_unsent(&mut self, group_id: usize, _connection_id: usize, count: u64, capped: bool) {
        let counts = self.request_counts_mut(group_id);
        counts.unsent += count;
        counts.unsent_capped |= capped;
    }
}

/// Aggregated statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedStats {
    pub latency_p50: Duration,
    pub latency_p95: Duration,
    pub latency_p99: Duration,
    pub latency_p999: Duration,
    pub latency_p9999: Duration,
    pub latency_p99999: Duration,
    /// Exact mean of every recorded latency; zero when none were recorded.
    pub mean_latency: Duration,
    /// Smallest recorded latency; `None` when none were recorded.
    pub min_latency: Option<Duration>,
    /// Largest recorded latency; `None` when none were recorded.
    pub max_latency: Option<Duration>,
    /// Sample standard deviation; `None` with fewer than two latencies.
    pub std_dev: Option<Duration>,
    /// Half-width of the 95% confidence interval of the mean
    /// (1.96 * std_dev / sqrt(n)); `None` with fewer than two latencies.
    pub confidence_interval: Option<Duration>,
    /// Number of latencies recorded, after any sampling rate.
    pub latency_sample_count: u64,
    /// Latencies a bounded HDR histogram recorded at its `max_value_us`;
    /// `None` unless the policy sets that bound.
    pub clamped_samples: Option<u64>,
    pub throughput_rps: f64,
    pub throughput_mbps: f64,
    /// Requests completed inside the measurement window, which throughput
    /// counts. Requests completed during the drain are excluded.
    pub total_requests: u64,
}
