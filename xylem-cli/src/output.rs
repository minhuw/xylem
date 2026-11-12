//! Results output formatting

pub mod html;

use crate::config::OutputFormat;
use crate::thresholds::{ThresholdResult, ValidityResult, Verdict};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use xylem_core::stats::{
    AggregatedStats, BreakdownClock, BreakdownSummary, PartSummary, RequestCounts, SendLagSummary,
    TupleStatsCollector, LATE_SEND_THRESHOLD,
};
use xylem_core::traffic_group::Dispatch;

/// Latency summary in microseconds. Optional min, max, standard deviation, and
/// confidence interval fields use `null` in JSON when absent. Numeric mean and
/// percentile values can be zero placeholders. The mean requires a nonzero
/// `sample_count`; percentiles also require a nonzero `percentile_sample_count`.
/// Text and HTML display unavailable values as `n/a`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencyStats {
    /// Smallest recorded latency; null when no latency was recorded.
    pub min_us: Option<f64>,
    /// Largest recorded latency; null when no latency was recorded.
    pub max_us: Option<f64>,
    pub mean_us: f64,
    pub p50_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub p9999_us: f64,
    pub p99999_us: f64,
    /// Sample standard deviation; null with fewer than two latencies.
    pub std_dev_us: Option<f64>,
    /// Half-width of the 95% confidence interval of the mean; null with
    /// fewer than two latencies.
    pub confidence_interval_us: Option<f64>,
    /// Number of latencies recorded, after any sampling rate.
    pub sample_count: u64,
    /// Retained percentile samples for reservoir policies, or total represented
    /// observation weight for sketches and histograms, after aggregation.
    /// Unknown for older serialized summaries that did not include this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percentile_sample_count: Option<u64>,
    /// Latencies above a configured `max_value_us` of an HDR histogram, which
    /// were recorded at that bound. Omitted unless the bound is configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clamped_samples: Option<u64>,
}

impl LatencyStats {
    /// The mean is meaningful only when a latency was recorded.
    fn mean_for_display(&self) -> Option<f64> {
        (self.sample_count > 0).then_some(self.mean_us)
    }

    /// Percentiles also require retained or represented percentile samples.
    fn percentile_for_display(&self, value: f64) -> Option<f64> {
        (self.sample_count > 0 && self.percentile_sample_count.unwrap_or(0) > 0).then_some(value)
    }

    /// Min and max require at least one recorded latency.
    fn range_for_display(&self, value: Option<f64>) -> Option<f64> {
        value.filter(|_| self.sample_count > 0)
    }

    /// Sample standard deviation and mean confidence interval require two.
    fn dispersion_for_display(&self, value: Option<f64>) -> Option<f64> {
        value.filter(|_| self.sample_count >= 2)
    }
}

/// Lag of open-loop requests behind their intended send times, in
/// microseconds. The values are null when no open-loop request was sent
/// (closed loop).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SendLagStats {
    /// Number of open-loop measurement requests sent
    pub sample_count: u64,
    pub mean_us: Option<f64>,
    pub p99_us: Option<f64>,
    pub max_us: Option<f64>,
    /// Lag above which a send counts in `requests.late_sends`
    pub late_threshold_us: f64,
}

impl SendLagStats {
    fn new(summary: &SendLagSummary) -> Self {
        Self {
            sample_count: summary.sample_count,
            mean_us: summary.mean.map(micros),
            p99_us: summary.p99.map(micros),
            max_us: summary.max.map(micros),
            late_threshold_us: micros(LATE_SEND_THRESHOLD),
        }
    }
}

/// Where the latency of completed measurement requests went, apart from the
/// send lag (see [`SendLagStats`]).
///
/// Each part is summarized over its own distribution: the percentiles of the
/// parts do not add up to the percentiles of the total latency.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencyBreakdownStats {
    /// Clock of the service latencies: `user_space`, `kernel_software`,
    /// `nic_hardware`, `nic_calibrated`, or `mixed` when requests differ.
    /// Null when no request completed.
    pub clock: Option<BreakdownClock>,
    /// Requests whose send and receive had kernel timestamps from the same
    /// clock, which split their round trip into `host_stack_us` and
    /// `network_server_us`
    pub kernel_split_count: u64,
    /// Actual receive minus actual send, in the clock each latency uses: a
    /// request's latency minus its send lag. Always present.
    pub service_us: BreakdownPartStats,
    /// User-space round trip minus kernel round trip, zero if negative; null
    /// when no request had a kernel split
    pub host_stack_us: Option<BreakdownPartStats>,
    /// Kernel round trip, kernel RX minus kernel TX timestamp; null when no
    /// request had a kernel split
    pub network_server_us: Option<BreakdownPartStats>,
}

impl LatencyBreakdownStats {
    fn new(summary: &BreakdownSummary) -> Self {
        Self {
            clock: summary.clock,
            kernel_split_count: summary.kernel_split_count,
            service_us: BreakdownPartStats::new(&summary.service),
            host_stack_us: summary.host_stack.as_ref().map(BreakdownPartStats::new),
            network_server_us: summary.network_server.as_ref().map(BreakdownPartStats::new),
        }
    }

    /// Each part with its label, in output order
    pub(crate) fn parts(&self) -> [(&'static str, Option<&BreakdownPartStats>); 3] {
        [
            ("Service", Some(&self.service_us)),
            ("Host stack", self.host_stack_us.as_ref()),
            ("Net+server", self.network_server_us.as_ref()),
        ]
    }

    /// The clock as written in JSON, or `n/a`
    pub(crate) fn clock_name(&self) -> &'static str {
        match self.clock {
            None => "n/a",
            Some(BreakdownClock::UserSpace) => "user_space",
            Some(BreakdownClock::KernelSoftware) => "kernel_software",
            Some(BreakdownClock::NicHardware) => "nic_hardware",
            Some(BreakdownClock::NicCalibrated) => "nic_calibrated",
            Some(BreakdownClock::Mixed) => "mixed",
        }
    }
}

/// One part of the latency breakdown in microseconds; the values are null
/// when the part has no samples
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BreakdownPartStats {
    pub sample_count: u64,
    /// Exact mean
    pub mean_us: Option<f64>,
    pub p50_us: Option<f64>,
    pub p99_us: Option<f64>,
    pub p999_us: Option<f64>,
    /// Exact maximum
    pub max_us: Option<f64>,
}

impl BreakdownPartStats {
    fn new(summary: &PartSummary) -> Self {
        Self {
            sample_count: summary.sample_count,
            mean_us: summary.mean.map(micros),
            p50_us: summary.p50.map(micros),
            p99_us: summary.p99.map(micros),
            p999_us: summary.p999.map(micros),
            max_us: summary.max.map(micros),
        }
    }

    /// The values in output order: mean, p50, p99, p99.9, max
    pub(crate) fn values(&self) -> [Option<f64>; 5] {
        [self.mean_us, self.p50_us, self.p99_us, self.p999_us, self.max_us]
    }
}

/// Note shown with every latency breakdown
pub(crate) const BREAKDOWN_NOTE: &str =
    "Each part's percentiles are of that part alone; they do not add up to the total's.";

/// Microseconds in a duration.
fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

/// Format an optional value with two decimals, or `n/a`.
pub(crate) fn format_optional(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_string(), |value| format!("{value:.2}"))
}

/// Format an optional sample count, preserving zero and large integers.
fn format_optional_count(value: Option<u64>) -> String {
    value.map_or_else(|| "n/a".to_string(), |value| value.to_string())
}

/// The error replies by kind, as ` (KIND: count, ...)`; empty without any.
/// Kinds come from the server, so control characters are escaped.
fn error_kinds(requests: &RequestCounts) -> String {
    if requests.errors_by_kind.is_empty() {
        return String::new();
    }
    let kinds: Vec<_> = requests
        .errors_by_kind
        .iter()
        .map(|(kind, count)| format!("{}: {count}", kind.escape_debug()))
        .collect();
    format!(" ({})", kinds.join(", "))
}

/// Statistics for one (time bucket, group, thread, connection) tuple.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsRecord {
    pub time_secs: f64,
    pub group_id: usize,
    /// Worker thread that owns the connection; connection IDs restart at 0
    /// on every thread.
    pub thread_id: usize,
    pub connection_id: usize,
    pub request_count: u64,
    /// Recorded successful latencies for this tuple, after any sampling rate.
    /// Unknown for records serialized before this field was added.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_sample_count: Option<u64>,
    /// Samples retained by this tuple's reservoir, or observations represented
    /// by its sketch or histogram. Aggregate reservoirs have their own capacity,
    /// so their retained counts need not equal summed record counts. Unknown for
    /// older serialized records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percentile_sample_count: Option<u64>,
    pub throughput_rps: f64,
    pub throughput_mbps: f64,
    pub latency_p50_us: f64,
    pub latency_p99_us: f64,
    pub latency_p999_us: f64,
    pub latency_mean_us: f64,
}

impl StatsRecord {
    /// Format the console preview using only this record's sample counts.
    fn format_preview(&self) -> String {
        let p99 = if self.latency_sample_count.unwrap_or(0) > 0
            && self.percentile_sample_count.unwrap_or(0) > 0
        {
            format!("{:.2}μs", self.latency_p99_us)
        } else {
            "n/a".to_string()
        };
        format!(
            "  t={:.1}s g={} t{} c={}: {:.0} rps, p99={p99}",
            self.time_secs, self.group_id, self.thread_id, self.connection_id, self.throughput_rps
        )
    }
}

/// Write `results` to `path` as pretty-printed JSON, then print `message`
/// followed by the path.
fn write_pretty_json<T: Serialize>(results: &T, path: &str, message: &str) -> Result<()> {
    let json = serde_json::to_string_pretty(results)?;
    let mut file = File::create(path)?;
    file.write_all(json.as_bytes())?;
    println!("{message}: {path}");
    Ok(())
}

/// Detailed experiment results with per-group breakdown
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailedExperimentResults {
    pub experiment: ExperimentMetadata,
    pub target: TargetMetadata,
    /// "invalid_run" when a validity check failed, else "thresholds_failed"
    /// when a threshold failed, else "pass"
    #[serde(default)]
    pub verdict: Verdict,
    /// Every threshold expression, in key order
    #[serde(default)]
    pub thresholds: Vec<ThresholdResult>,
    /// Every validity check of every rate-driven traffic group
    #[serde(default)]
    pub validity: Vec<ValidityResult>,
    pub global: GlobalStats,
    pub traffic_groups: Vec<TrafficGroupResults>,
    /// Raw statistics records per time bucket, group, thread and connection.
    /// Users can aggregate by time, group, or connection as needed.
    /// Only included when `stats.include_records = true` in config.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub records: Vec<StatsRecord>,
    /// How each worker thread with `timestamps = "nic"` converted its NIC's
    /// timestamps to the user-space clock, by thread id; absent without such
    /// threads
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub nic_clock_calibration: Vec<NicClockCalibration>,
}

/// How one worker thread converted its NIC's timestamps to the user-space
/// clock (`nic_calibrated` latency breakdowns)
///
/// The thread fits the NIC clock to the user-space clock from paired
/// readings taken about every 100 ms over a sliding window of 32. The fit
/// fields are null if no fit existed at the end of the run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NicClockCalibration {
    pub thread_id: usize,
    /// Nanoseconds of the user-space clock per NIC clock tick, by the last fit
    pub ns_per_tick: Option<f64>,
    /// RMS residual of the last fit, in nanoseconds: the typical error of a
    /// converted RX timestamp
    pub residual_rms_ns: Option<f64>,
    /// Largest residual of any fit during the run, in nanoseconds
    pub max_residual_ns: Option<f64>,
    /// Readings of the last fit's window that it used and dropped as outliers
    pub inliers: usize,
    pub outliers: usize,
    /// Readings refused because a clock did not move forward
    pub rejected_samples: u64,
    /// Receives whose RX hardware timestamp was converted
    pub converted_rx_stamps: u64,
    /// Receives without a convertible RX hardware timestamp; their requests
    /// have no kernel split
    pub unconverted_rx_stamps: u64,
}

impl NicClockCalibration {
    /// Worker thread `thread_id`'s report
    pub fn new(thread_id: usize, report: &xylem_transport::ClockCalibrationReport) -> Self {
        Self {
            thread_id,
            ns_per_tick: report.ns_per_tick,
            residual_rms_ns: report.residual_rms_ns,
            max_residual_ns: report.max_residual_ns,
            inliers: report.inliers,
            outliers: report.outliers,
            rejected_samples: report.rejected_samples,
            converted_rx_stamps: report.converted,
            unconverted_rx_stamps: report.unconverted,
        }
    }
}

/// Experiment metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentMetadata {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub duration_secs: f64,
    /// Seconds from the first measured request to the end of the run, which
    /// throughput is computed over. Excludes connection setup and warmup.
    #[serde(default)]
    pub measured_duration_secs: f64,
    /// Earliest actual worker load-loop start, as Unix microseconds on this host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_load_loop_start_unix_us: Option<u64>,
    /// Latest actual worker load-loop end, before drain, as Unix microseconds on this host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_load_loop_end_unix_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
    /// The worker threads' busy-poll budget (`experiment.busy_poll_us`);
    /// null when they did not busy-poll
    #[serde(default)]
    pub busy_poll_us: Option<u64>,
}

/// Target metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetMetadata {
    pub address: String,
    pub protocol: String,
    pub transport: String,
}

/// Global aggregated statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalStats {
    /// Requests completed inside the measurement window, which throughput
    /// counts, error replies included
    pub total_requests: u64,
    pub throughput_rps: f64,
    pub throughput_mbps: f64,
    /// Latencies of every request completed by a successful response,
    /// including those completed during the drain after the window
    pub latency: LatencyStats,
    /// Latencies of the requests completed by an error reply
    /// (`requests.errors`), kept out of `latency`
    #[serde(default)]
    pub error_latency: LatencyStats,
    /// What happened to every measurement request
    #[serde(default)]
    pub requests: RequestCounts,
    #[serde(default)]
    pub send_lag: SendLagStats,
    /// Where the latency went, apart from the send lag
    #[serde(default)]
    pub latency_breakdown: LatencyBreakdownStats,
}

/// Per-traffic-group results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrafficGroupResults {
    pub id: usize,
    pub name: String,
    pub target: String,
    pub protocol: String,
    pub threads: Vec<usize>,
    pub connections: usize,
    pub policy: String,
    /// How the policy's arrivals reach the connections: "connection" or "group"
    #[serde(default)]
    pub dispatch: Dispatch,
    pub stats: GroupStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_metadata: Option<JsonValue>,
    /// Every connection the group opened, by thread and connection ID (the
    /// key of `records`), with its local socket address
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub connection_addrs: Vec<ConnectionAddr>,
}

/// The local socket address of one connection
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionAddr {
    pub thread_id: usize,
    pub connection_id: usize,
    /// `IP:port` the connection had once open; null without one (Unix
    /// sockets). An unconnected UDP socket reports its unspecified bound IP.
    pub local_addr: Option<SocketAddr>,
}

/// Per-group statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupStats {
    /// Requests completed inside the measurement window, which throughput
    /// counts, error replies included
    pub total_requests: u64,
    pub throughput_rps: f64,
    pub throughput_mbps: f64,
    /// Latencies of every request completed by a successful response,
    /// including those completed during the drain after the window
    pub latency: LatencyStats,
    /// Latencies of the requests completed by an error reply
    /// (`requests.errors`), kept out of `latency`
    #[serde(default)]
    pub error_latency: LatencyStats,
    /// What happened to every measurement request
    #[serde(default)]
    pub requests: RequestCounts,
    #[serde(default)]
    pub send_lag: SendLagStats,
    /// Where the latency went, apart from the send lag
    #[serde(default)]
    pub latency_breakdown: LatencyBreakdownStats,
}

impl DetailedExperimentResults {
    /// Put each thread's `(group_id, connection_id, local_addr)` of its
    /// connections in its group's `connection_addrs`, in thread and
    /// connection order
    ///
    /// A group that recorded no requests is not in the results (see
    /// [`Self::from_tuple_stats`]), and neither are its connections.
    pub fn set_connection_addrs(
        &mut self,
        threads: impl IntoIterator<Item = (usize, Vec<(usize, usize, Option<SocketAddr>)>)>,
    ) {
        for (thread_id, connections) in threads {
            for (group_id, connection_id, local_addr) in connections {
                let Some(group) = self.traffic_groups.iter_mut().find(|group| group.id == group_id)
                else {
                    continue;
                };
                group.connection_addrs.push(ConnectionAddr {
                    thread_id,
                    connection_id,
                    local_addr,
                });
            }
        }
        for group in &mut self.traffic_groups {
            group.connection_addrs.sort_by_key(|addr| (addr.thread_id, addr.connection_id));
        }
    }

    #[allow(clippy::too_many_arguments)]
    /// Create detailed results from tuple stats collector; a traffic group
    /// that recorded no requests is left out
    ///
    /// # Arguments
    /// * `include_records` - Whether to include per-bucket, per-connection records
    pub fn from_tuple_stats(
        experiment_name: String,
        experiment_description: Option<String>,
        seed: Option<u64>,
        target_address: String,
        target_protocol: String,
        target_transport: String,
        duration: Duration,
        tuple_stats: &TupleStatsCollector,
        traffic_group_configs: &[xylem_core::traffic_group::TrafficGroupConfig],
        include_records: bool,
    ) -> Self {
        let duration_secs = duration.as_secs_f64();
        let bucket_duration_secs = tuple_stats.bucket_duration().as_secs_f64();
        // Rates cover the measurement window, which excludes setup and
        // warmup; collectors that never ran a worker fall back to `duration`.
        let measured = tuple_stats.measured_duration().unwrap_or(duration);

        // Aggregate global stats
        let global_aggregated = tuple_stats.aggregate_global(measured);

        // Aggregate per-group stats
        let per_group_aggregated = tuple_stats.aggregate_by_group(measured);

        // Build raw records from entries (only if requested)
        let mut records: Vec<StatsRecord> = if include_records {
            tuple_stats
                .entries()
                .iter()
                .map(|(key, entry)| {
                    let width_secs = tuple_stats.bucket_width(key.time_bucket).as_secs_f64();
                    let total_bytes = (entry.tx_bytes + entry.rx_bytes) as f64;
                    let latency_percentiles = entry.latency.percentiles(&[0.5, 0.99, 0.999]);
                    StatsRecord {
                        time_secs: key.time_bucket as f64 * bucket_duration_secs,
                        group_id: key.group_id,
                        thread_id: key.thread_id,
                        connection_id: key.connection_id,
                        request_count: entry.request_count,
                        latency_sample_count: Some(entry.latency.count()),
                        percentile_sample_count: Some(entry.latency.percentile_sample_count()),
                        throughput_rps: entry.request_count as f64 / width_secs,
                        throughput_mbps: (total_bytes * 8.0) / (width_secs * 1_000_000.0),
                        latency_p50_us: latency_percentiles[0] as f64 / 1000.0,
                        latency_p99_us: latency_percentiles[1] as f64 / 1000.0,
                        latency_p999_us: latency_percentiles[2] as f64 / 1000.0,
                        latency_mean_us: entry.latency.mean() as f64 / 1000.0,
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        // Sort by time, then group, then connection for consistent output
        records.sort_by(|a, b| {
            a.time_secs
                .partial_cmp(&b.time_secs)
                .unwrap()
                .then_with(|| a.group_id.cmp(&b.group_id))
                .then_with(|| a.thread_id.cmp(&b.thread_id))
                .then_with(|| a.connection_id.cmp(&b.connection_id))
        });

        // Build global stats
        let global = GlobalStats {
            total_requests: global_aggregated.total_requests,
            throughput_rps: global_aggregated.throughput_rps,
            throughput_mbps: global_aggregated.throughput_mbps,
            latency: Self::latency_stats(&global_aggregated),
            error_latency: Self::latency_stats(&tuple_stats.error_latency_global()),
            requests: tuple_stats.request_counts_global(),
            send_lag: SendLagStats::new(&tuple_stats.send_lag_global()),
            latency_breakdown: LatencyBreakdownStats::new(&tuple_stats.latency_breakdown_global()),
        };

        // Build per-group results
        let mut traffic_groups = Vec::new();

        for (group_id, config) in traffic_group_configs.iter().enumerate() {
            if let Some(aggregated) = per_group_aggregated.get(&group_id) {
                let protocol_name = config.protocol.clone();
                let policy_str = Self::format_policy_config(&config.traffic_policy);

                let group_result = TrafficGroupResults {
                    id: group_id,
                    name: config.name.clone(),
                    target: config.target.clone(),
                    protocol: protocol_name,
                    threads: config.threads.clone(),
                    connections: config.connections_per_thread * config.threads.len(),
                    policy: policy_str,
                    dispatch: config.traffic_policy.dispatch(),
                    stats: GroupStats {
                        total_requests: aggregated.total_requests,
                        throughput_rps: aggregated.throughput_rps,
                        throughput_mbps: aggregated.throughput_mbps,
                        latency: Self::latency_stats(aggregated),
                        error_latency: Self::latency_stats(&tuple_stats.error_latency(group_id)),
                        requests: tuple_stats.request_counts(group_id),
                        send_lag: SendLagStats::new(&tuple_stats.send_lag(group_id)),
                        latency_breakdown: LatencyBreakdownStats::new(
                            &tuple_stats.latency_breakdown(group_id),
                        ),
                    },
                    protocol_metadata: None,
                    connection_addrs: Vec::new(),
                };

                traffic_groups.push(group_result);
            }
        }

        DetailedExperimentResults {
            experiment: ExperimentMetadata {
                name: experiment_name,
                description: experiment_description,
                duration_secs,
                measured_duration_secs: measured.as_secs_f64(),
                actual_load_loop_start_unix_us: tuple_stats.actual_load_loop_unix_us().map(|v| v.0),
                actual_load_loop_end_unix_us: tuple_stats.actual_load_loop_unix_us().map(|v| v.1),
                seed,
                // `run_experiment` records the profile's
                busy_poll_us: None,
            },
            target: TargetMetadata {
                address: target_address,
                protocol: target_protocol,
                transport: target_transport,
            },
            // `thresholds::evaluate` records the checks
            verdict: Verdict::Pass,
            thresholds: Vec::new(),
            validity: Vec::new(),
            global,
            traffic_groups,
            records,
            // `run_experiment` adds the worker threads' reports
            nic_clock_calibration: Vec::new(),
        }
    }

    /// Convert aggregated latencies to microseconds.
    fn latency_stats(aggregated: &AggregatedStats) -> LatencyStats {
        LatencyStats {
            min_us: aggregated.min_latency.map(micros),
            max_us: aggregated.max_latency.map(micros),
            mean_us: micros(aggregated.mean_latency),
            p50_us: micros(aggregated.latency_p50),
            p95_us: micros(aggregated.latency_p95),
            p99_us: micros(aggregated.latency_p99),
            p999_us: micros(aggregated.latency_p999),
            p9999_us: micros(aggregated.latency_p9999),
            p99999_us: micros(aggregated.latency_p99999),
            std_dev_us: aggregated.std_dev.map(micros),
            confidence_interval_us: aggregated.confidence_interval.map(micros),
            sample_count: aggregated.latency_sample_count,
            percentile_sample_count: aggregated.percentile_sample_count,
            clamped_samples: aggregated.clamped_samples,
        }
    }

    /// Format the latency summary lines with `indent` before each label.
    fn format_latency(latency: &LatencyStats, indent: &str) -> String {
        // Align values in column 17 when the label fits.
        let width = 17 - indent.len();
        let mut lines = vec![
            ("Mean:", format_optional(latency.mean_for_display())),
            ("Min:", format_optional(latency.range_for_display(latency.min_us))),
            ("Max:", format_optional(latency.range_for_display(latency.max_us))),
            ("p50:", format_optional(latency.percentile_for_display(latency.p50_us))),
            ("p95:", format_optional(latency.percentile_for_display(latency.p95_us))),
            ("p99:", format_optional(latency.percentile_for_display(latency.p99_us))),
            ("p999:", format_optional(latency.percentile_for_display(latency.p999_us))),
            ("Std Dev:", format_optional(latency.dispersion_for_display(latency.std_dev_us))),
            (
                "95% CI:",
                latency
                    .dispersion_for_display(latency.confidence_interval_us)
                    .map_or("n/a".into(), |ci| format!("±{ci:.2}")),
            ),
            ("Samples:", latency.sample_count.to_string()),
            ("Percentile samples:", format_optional_count(latency.percentile_sample_count)),
        ];
        if let Some(clamped) = latency.clamped_samples {
            lines.push(("Clamped:", clamped.to_string()));
        }
        lines
            .into_iter()
            .map(|(label, value)| {
                let width = width.max(label.len() + 1);
                format!("{indent}{label:<width$}{value}\n")
            })
            .collect()
    }

    /// Format the error latency summary with request-count-style indentation.
    fn format_error_latency(latency: &LatencyStats, indent: &str) -> String {
        let width = 17 - indent.len();
        let line = |label: &str, value: String| {
            let width = width.max(label.len() + 1);
            format!("{indent}{label:<width$}{value}\n")
        };
        format!(
            "{indent}Error latency (μs):\n{}{}{}{}{}{}",
            line("  Mean:", format_optional(latency.mean_for_display())),
            line("  p50:", format_optional(latency.percentile_for_display(latency.p50_us))),
            line("  p99:", format_optional(latency.percentile_for_display(latency.p99_us))),
            line("  Max:", format_optional(latency.range_for_display(latency.max_us))),
            line("  Samples:", latency.sample_count.to_string()),
            line("  Percentile samples:", format_optional_count(latency.percentile_sample_count)),
        )
    }

    /// Print the request counts, error replies and send lags with `indent`
    /// before each label.
    fn print_requests(
        requests: &RequestCounts,
        error_latency: &LatencyStats,
        send_lag: &SendLagStats,
        indent: &str,
    ) {
        let width = 17 - indent.len();
        let line = |label: &str, value: String| println!("{indent}{label:<width$}{value}");
        println!("{indent}Requests:");
        line("  Sent:", requests.sent.to_string());
        line("  Completed:", requests.completed.to_string());
        line("  Errors:", format!("{}{}", requests.errors, error_kinds(requests)));
        line("  Timed out:", requests.timed_out.to_string());
        line("  Late resp.:", requests.late_responses.to_string());
        line("  Failed:", requests.failed.to_string());
        line("  Late sends:", requests.late_sends.to_string());
        let capped = if requests.unsent_capped {
            " (lower bound)"
        } else {
            ""
        };
        line("  Unsent:", format!("{}{capped}", requests.unsent));
        if requests.timed_out > 0 {
            println!(
                "{indent}  WARNING: {} requests got no response within the request or \
                 drain timeout; they have no latency",
                requests.timed_out
            );
        }
        if requests.errors > 0 {
            println!(
                "{indent}  WARNING: {} requests got an error reply; their latency is in \
                 the error latency, not the latency above",
                requests.errors
            );
            println!();
            print!("{}", Self::format_error_latency(error_latency, indent));
        }
        println!();
        println!("{indent}Send lag (μs, late above {:.0}):", send_lag.late_threshold_us);
        line("  Mean:", format_optional(send_lag.mean_us));
        line("  p99:", format_optional(send_lag.p99_us));
        line("  Max:", format_optional(send_lag.max_us));
    }

    /// Print the latency breakdown with `indent` before each label: one row
    /// per part, with the mean, p50, p99, p99.9 and max in columns.
    fn print_breakdown(breakdown: &LatencyBreakdownStats, indent: &str) {
        let width = 17 - indent.len();
        let line = |label: &str, value: String| println!("{indent}{label:<width$}{value}");
        println!("{indent}Latency breakdown (μs):");
        line("  Clock:", breakdown.clock_name().to_string());
        line("  Split reqs:", breakdown.kernel_split_count.to_string());
        line("", "Mean      p50       p99       p99.9     Max".to_string());
        for (label, part) in breakdown.parts() {
            let row = match part {
                Some(part) => part
                    .values()
                    .map(|value| format!("{:<10}", format_optional(value)))
                    .concat()
                    .trim_end()
                    .to_string(),
                None => "n/a (no kernel split)".to_string(),
            };
            line(&format!("  {label}:"), row);
        }
        println!("{indent}  {BREAKDOWN_NOTE}");
    }

    /// Print one line per worker thread's NIC clock calibration
    fn print_nic_clock_calibration(calibrations: &[NicClockCalibration]) {
        println!("NIC clock calibration:");
        let ns =
            |value: Option<f64>| value.map_or_else(|| "n/a".to_string(), |v| format!("{v:.0}"));
        for c in calibrations {
            let rate = c.ns_per_tick.map_or_else(|| "no fit".to_string(), |r| format!("{r:.6}"));
            println!(
                "  Thread {}: {} ns/tick, residual rms {} ns, max {} ns; RX stamps \
                 converted {}, unconverted {}",
                c.thread_id,
                rate,
                ns(c.residual_rms_ns),
                ns(c.max_residual_ns),
                c.converted_rx_stamps,
                c.unconverted_rx_stamps
            );
        }
    }

    /// Format a policy config as a human-readable string
    fn format_policy_config(policy: &xylem_core::traffic_group::PolicyConfig) -> String {
        match policy {
            xylem_core::traffic_group::PolicyConfig::ClosedLoop => "closed-loop".to_string(),
            xylem_core::traffic_group::PolicyConfig::FixedRate { rate, .. } => {
                format!("fixed-rate({})", rate)
            }
            xylem_core::traffic_group::PolicyConfig::Poisson { rate, .. } => {
                format!("poisson({})", rate)
            }
            xylem_core::traffic_group::PolicyConfig::Adaptive { .. } => "adaptive".to_string(),
            xylem_core::traffic_group::PolicyConfig::Sinusoidal {
                base_rate,
                amplitude,
                period,
                ..
            } => {
                format!(
                    "sinusoidal(base={}, amp={}, period={}s)",
                    base_rate,
                    amplitude,
                    period.as_secs_f64()
                )
            }
            xylem_core::traffic_group::PolicyConfig::Ramp {
                start_rate,
                end_rate,
                duration,
                ..
            } => {
                format!("ramp({} -> {} over {}s)", start_rate, end_rate, duration.as_secs_f64())
            }
            xylem_core::traffic_group::PolicyConfig::Spike {
                normal_rate,
                spike_rate,
                spike_duration,
                ..
            } => {
                format!(
                    "spike(normal={}, spike={}, dur={}s)",
                    normal_rate,
                    spike_rate,
                    spike_duration.as_secs_f64()
                )
            }
            xylem_core::traffic_group::PolicyConfig::Sawtooth {
                min_rate,
                max_rate,
                period,
                ..
            } => {
                format!("sawtooth({}-{}, period={}s)", min_rate, max_rate, period.as_secs_f64())
            }
        }
    }

    /// Print results to stdout in human-readable format
    pub fn print_human(&self) {
        println!("\n{}", "=".repeat(70));
        println!("Xylem Detailed Experiment Results");
        println!("{}", "=".repeat(70));
        println!();
        println!("Experiment: {}", self.experiment.name);
        if let Some(ref desc) = self.experiment.description {
            println!("Description: {}", desc);
        }
        println!("Duration: {:.2}s", self.experiment.duration_secs);
        println!("Measured: {:.2}s", self.experiment.measured_duration_secs);
        if let Some(seed) = self.experiment.seed {
            println!("Seed: {}", seed);
        }
        println!();
        println!("Transport: {}", self.target.transport);
        println!("Traffic Groups: {}", self.traffic_groups.len());
        println!();

        println!("{}", "-".repeat(70));
        println!("GLOBAL STATISTICS");
        println!("{}", "-".repeat(70));
        println!("Total Requests:  {}", self.global.total_requests);
        println!("Throughput:      {:.2} req/s", self.global.throughput_rps);
        println!("Bandwidth:       {:.2} Mbps", self.global.throughput_mbps);
        println!();
        println!("Latency (μs):");
        print!("{}", Self::format_latency(&self.global.latency, "  "));
        println!();
        Self::print_requests(
            &self.global.requests,
            &self.global.error_latency,
            &self.global.send_lag,
            "",
        );
        println!();
        Self::print_breakdown(&self.global.latency_breakdown, "");
        println!();
        if !self.nic_clock_calibration.is_empty() {
            Self::print_nic_clock_calibration(&self.nic_clock_calibration);
            println!();
        }

        for (i, group) in self.traffic_groups.iter().enumerate() {
            println!("{}", "-".repeat(70));
            println!("TRAFFIC GROUP {}: {}", i, group.name);
            println!("{}", "-".repeat(70));
            println!("Target:          {} ({})", group.target, group.protocol);
            println!("Policy:          {}", group.policy);
            if !group.dispatch.is_connection() {
                println!("Dispatch:        {}", group.dispatch);
            }
            println!("Threads:         {:?}", group.threads);
            println!("Connections:     {}", group.connections);
            println!();
            println!("Statistics:");
            println!("  Requests:      {}", group.stats.total_requests);
            println!("  Throughput:    {:.2} req/s", group.stats.throughput_rps);
            println!("  Bandwidth:     {:.2} Mbps", group.stats.throughput_mbps);
            println!();
            println!("  Latency (μs):");
            print!("{}", Self::format_latency(&group.stats.latency, "    "));
            println!();
            Self::print_requests(
                &group.stats.requests,
                &group.stats.error_latency,
                &group.stats.send_lag,
                "  ",
            );
            println!();
            Self::print_breakdown(&group.stats.latency_breakdown, "  ");

            if let Some(ref metadata) = group.protocol_metadata {
                println!();
                println!("  Protocol Metadata:");
                println!("{}", serde_json::to_string_pretty(metadata).unwrap_or_default());
            }
            println!();
        }

        // Records summary (per-connection data)
        if !self.records.is_empty() {
            // Count unique time buckets and connections
            let unique_times: std::collections::HashSet<_> =
                self.records.iter().map(|r| r.time_secs as u64).collect();
            let unique_conns: std::collections::HashSet<_> = self
                .records
                .iter()
                .map(|r| (r.group_id, r.thread_id, r.connection_id))
                .collect();

            println!("{}", "-".repeat(70));
            println!(
                "RECORDS ({} entries: {} time buckets x {} connections)",
                self.records.len(),
                unique_times.len(),
                unique_conns.len()
            );
            println!("{}", "-".repeat(70));

            // Show first few entries
            println!("Sample records (time, group, thread, conn, rps, p99):");
            for record in self.records.iter().take(10) {
                println!("{}", record.format_preview());
            }
            if self.records.len() > 10 {
                println!("  ... and {} more records", self.records.len() - 10);
            }
            println!();
        }

        self.print_checks();
        println!("{}", "=".repeat(70));
    }

    /// Print the verdict with every validity check and threshold
    fn print_checks(&self) {
        let status = |ok: bool| if ok { "ok" } else { "FAIL" };
        println!("{}", "-".repeat(70));
        println!("VERDICT: {}", self.verdict.label());
        println!("{}", "-".repeat(70));
        if self.validity.is_empty() {
            println!("Validity:        none applied");
        } else {
            println!("Validity:");
        }
        for check in &self.validity {
            println!(
                "  {:<6}{}: {} (limit {})",
                status(check.ok),
                check.key(),
                check.observed_text(),
                check.limit_text()
            );
            if let Some(reason) = &check.reason {
                println!("        {reason}");
            }
        }
        if self.thresholds.is_empty() {
            println!("Thresholds:      none configured");
        } else {
            println!("Thresholds:");
        }
        for threshold in &self.thresholds {
            println!(
                "  {:<6}{}: {} (observed {})",
                status(threshold.ok),
                threshold.key(),
                threshold.expression,
                threshold.observed_text()
            );
        }
        println!();
    }

    /// Write results to JSON file
    pub fn write_json(&self, path: &str) -> Result<()> {
        write_pretty_json(self, path, "Detailed results written to")
    }
}

/// Print the human-readable summary, then write the result files for `format`.
///
/// `json` and `html` replace the extension of `file`. `both` removes the
/// extension of `file` and then sets one, so a dotted stem loses its last
/// part: `results.v1.txt` becomes `results.json` and `results.html`. `human`
/// writes no file.
pub fn write_results(
    results: &DetailedExperimentResults,
    format: &OutputFormat,
    file: &Path,
) -> Result<()> {
    results.print_human();

    match format {
        OutputFormat::Human => {
            // Human format - console output only, no file
            tracing::debug!("Human format selected - output printed to console");
        }
        OutputFormat::Json => {
            let json_path = file.with_extension("json");
            tracing::debug!("Writing JSON results to {}", json_path.display());
            write_json_file(results, &json_path)?;
            tracing::info!("Results written to: {}", json_path.display());
        }
        OutputFormat::Html => {
            let html_path = file.with_extension("html");
            tracing::debug!("Writing HTML report to {}", html_path.display());
            write_html_file(results, &html_path)?;
            tracing::info!("HTML report written to: {}", html_path.display());
        }
        OutputFormat::Both => {
            let base_path = file.with_extension("");
            let json_path = base_path.with_extension("json");
            let html_path = base_path.with_extension("html");
            tracing::debug!(
                "Writing JSON and HTML results to {} and {}",
                json_path.display(),
                html_path.display()
            );
            write_json_file(results, &json_path)?;
            write_html_file(results, &html_path)?;
            tracing::info!(
                "Results written to: {} and {}",
                json_path.display(),
                html_path.display()
            );
        }
    }

    Ok(())
}

/// Write the JSON results file and log a failure.
fn write_json_file(results: &DetailedExperimentResults, path: &Path) -> Result<()> {
    let path_str = path.to_str().ok_or_else(|| anyhow::anyhow!("Invalid output file path"))?;
    if let Err(e) = results.write_json(path_str) {
        tracing::error!("Failed to write JSON results to {}: {}", path.display(), e);
        return Err(e);
    }
    Ok(())
}

/// Write the HTML report and log a failure.
fn write_html_file(results: &DetailedExperimentResults, path: &Path) -> Result<()> {
    let path_str = path.to_str().ok_or_else(|| anyhow::anyhow!("Invalid output file path"))?;
    if let Err(e) = html::generate_html_report(results, path_str) {
        tracing::error!("Failed to write HTML report to {}: {}", path.display(), e);
        return Err(e);
    }
    Ok(())
}

#[cfg(test)]
mod percentile_count_tests {
    use super::*;

    #[test]
    fn latency_sample_count_preserves_u64_in_output_and_thresholds() {
        let mut config: crate::config::ProfileConfig = toml::from_str(
            r#"
[experiment]
name = "latency-count"
duration = "1s"

[[traffic_groups]]
name = "api"
target = "unused"
protocol = "xylem-echo"
threads = [0]
connections_per_thread = 1
traffic_policy = { type = "closed-loop" }

[output]
file = "unused"

[thresholds]
latency = ["mean == 5us", "p99 == 5us"]
"#,
        )
        .unwrap();
        config.validate().unwrap();

        let duration = Duration::from_secs(1);
        let mut collector =
            TupleStatsCollector::new(xylem_core::stats::SamplingPolicy::Unlimited, duration);
        collector.record_latency(0, 0, Duration::from_micros(5));
        let mut summary = collector.aggregate_global(duration);
        let mut results = DetailedExperimentResults::from_tuple_stats(
            "latency-count".into(),
            None,
            None,
            "unused".into(),
            "xylem-echo".into(),
            "tcp".into(),
            duration,
            &collector,
            &config.traffic_groups,
            false,
        );

        for count in [0, 1, u32::MAX as u64, u32::MAX as u64 + 1, u32::MAX as u64 + 2, u64::MAX] {
            // Change only the aggregate count; no large number of samples is needed.
            summary.latency_sample_count = count;
            let stats = DetailedExperimentResults::latency_stats(&summary);
            let sample_count: u64 = stats.sample_count;
            assert_eq!(sample_count, count);

            let json = serde_json::to_string(&stats).unwrap();
            let value: JsonValue = serde_json::from_str(&json).unwrap();
            assert_eq!(value["sample_count"].as_u64(), Some(count));
            let decoded: LatencyStats = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.sample_count, count);
            results.global.latency = decoded;
            let output = serde_json::to_value(&results).unwrap();
            assert_eq!(output["global"]["latency"]["sample_count"].as_u64(), Some(count));

            let verdict = crate::thresholds::evaluate(&mut results, &config);
            assert_eq!(
                verdict,
                if count == 0 {
                    Verdict::ThresholdsFailed
                } else {
                    Verdict::Pass
                },
                "count {count}"
            );
            assert_eq!(results.thresholds.len(), 2);
            for threshold in &results.thresholds {
                assert_eq!(threshold.observed, (count > 0).then_some(5.0), "count {count}");
            }
        }
    }

    #[test]
    fn latency_json_includes_percentile_count_and_accepts_old_output() {
        let policy = xylem_core::stats::SamplingPolicy::Limited { max_samples: 3, rate: 1.0 };
        let mut collector = TupleStatsCollector::new(policy, Duration::from_secs(1));
        for _ in 0..10 {
            collector.record_latency(0, 0, Duration::from_micros(5));
        }
        let summary = collector.aggregate_global(Duration::from_secs(1));
        let stats = DetailedExperimentResults::latency_stats(&summary);
        assert_eq!((stats.sample_count, stats.percentile_sample_count), (10, Some(3)));
        let mut json = serde_json::to_value(stats).unwrap();
        assert_eq!(json["percentile_sample_count"], 3);
        let decoded: LatencyStats = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(decoded.percentile_sample_count, Some(3));
        json.as_object_mut().unwrap().remove("percentile_sample_count");
        let old: LatencyStats = serde_json::from_value(json).unwrap();
        assert_eq!(old.percentile_sample_count, None);
        assert!(serde_json::to_value(&old).unwrap().get("percentile_sample_count").is_none());
        assert_eq!(old.sample_count, 10);
    }

    #[test]
    fn freshly_computed_empty_summaries_serialize_zero() {
        let mut collector = TupleStatsCollector::new(
            xylem_core::stats::SamplingPolicy::None,
            Duration::from_secs(1),
        );
        collector.record_latency(1, 0, Duration::from_micros(5));
        collector.record_error_reply(1, 0, "error", Duration::from_micros(5));
        let duration = Duration::from_secs(1);
        for summary in [
            collector.aggregate_global(duration),
            collector.aggregate_by_group(duration)[&1].clone(),
            collector.error_latency_global(),
            collector.error_latency(1),
        ] {
            let stats = DetailedExperimentResults::latency_stats(&summary);
            assert_eq!(stats.percentile_sample_count, Some(0));
            let json = serde_json::to_value(stats).unwrap();
            assert_eq!(json.get("percentile_sample_count"), Some(&serde_json::json!(0)));
            let decoded: LatencyStats = serde_json::from_value(json).unwrap();
            assert_eq!(decoded.percentile_sample_count, Some(0));
        }
    }
}

#[cfg(test)]
mod latency_display_tests {
    use super::*;
    use xylem_core::stats::SamplingPolicy;
    use xylem_core::traffic_group::{OnError, PolicyConfig, TrafficGroupConfig};

    #[derive(Clone, Copy, Debug)]
    enum Case {
        NoSamples,
        ZeroRetained,
        LegacyUnknown,
        LegacyNull,
        ObservedZero,
        ObservedNonzero,
        EmptyWithStaleValues,
    }

    const CASES: [Case; 7] = [
        Case::NoSamples,
        Case::ZeroRetained,
        Case::LegacyUnknown,
        Case::LegacyNull,
        Case::ObservedZero,
        Case::ObservedNonzero,
        Case::EmptyWithStaleValues,
    ];

    impl Case {
        fn expected(self) -> (&'static str, &'static str, Option<f64>) {
            match self {
                Self::NoSamples | Self::EmptyWithStaleValues => ("n/a", "n/a", None),
                Self::ZeroRetained | Self::LegacyUnknown | Self::LegacyNull => {
                    ("7.00", "n/a", None)
                }
                Self::ObservedZero => ("0.00", "0.00", Some(0.0)),
                Self::ObservedNonzero => ("7.00", "7.00", Some(7.0)),
            }
        }

        fn expected_percentile_count(self) -> &'static str {
            match self {
                Self::NoSamples | Self::ZeroRetained => "0",
                Self::LegacyUnknown | Self::LegacyNull => "n/a",
                Self::ObservedZero | Self::ObservedNonzero | Self::EmptyWithStaleValues => "1",
            }
        }
    }

    fn results(case: Case) -> DetailedExperimentResults {
        let policy = match case {
            Case::NoSamples => SamplingPolicy::None,
            Case::ZeroRetained => SamplingPolicy::Limited { max_samples: 0, rate: 1.0 },
            _ => SamplingPolicy::Unlimited,
        };
        let latency = if matches!(case, Case::ObservedZero) {
            Duration::ZERO
        } else {
            Duration::from_micros(7)
        };
        let duration = Duration::from_secs(1);
        let mut collector = TupleStatsCollector::new(policy.clone(), duration);
        collector.record_latency(0, 0, latency);
        collector.record_error_reply(0, 0, "<error>", latency);
        let group = TrafficGroupConfig {
            name: "api <&>".into(),
            threads: vec![0],
            connections_per_thread: 1,
            max_pending_per_connection: 1,
            request_timeout: None,
            on_error: OnError::Count,
            timestamps: Default::default(),
            protocol: "xylem-echo".into(),
            target: "unused".into(),
            transport: "tcp".into(),
            traffic_policy: PolicyConfig::ClosedLoop,
            sampling_policy: policy,
            protocol_config: None,
        };
        let mut results = DetailedExperimentResults::from_tuple_stats(
            "display <&>".into(),
            None,
            None,
            "unused".into(),
            "xylem-echo".into(),
            "tcp".into(),
            duration,
            &collector,
            &[group],
            false,
        );
        assert_eq!(results.traffic_groups.len(), 1);
        assert_eq!(results.global.requests.errors, 1);
        assert_eq!(results.traffic_groups[0].stats.requests.errors, 1);

        if matches!(case, Case::LegacyUnknown | Case::LegacyNull) {
            let mut json = serde_json::to_value(&results).unwrap();
            for path in [
                "/global/latency",
                "/global/error_latency",
                "/traffic_groups/0/stats/latency",
                "/traffic_groups/0/stats/error_latency",
            ] {
                let summary = json.pointer_mut(path).unwrap().as_object_mut().unwrap();
                if matches!(case, Case::LegacyNull) {
                    summary.insert("percentile_sample_count".into(), serde_json::Value::Null);
                } else {
                    summary.remove("percentile_sample_count");
                }
            }
            results = serde_json::from_value(json).unwrap();
        }
        if matches!(case, Case::EmptyWithStaleValues) {
            let group = &mut results.traffic_groups[0].stats;
            for summary in [
                &mut results.global.latency,
                &mut results.global.error_latency,
                &mut group.latency,
                &mut group.error_latency,
            ] {
                // Older summaries can contain placeholder values even with no samples.
                summary.sample_count = 0;
            }
        }
        results
    }

    fn assert_summary_counts(case: Case, results: &DetailedExperimentResults) {
        let group = &results.traffic_groups[0].stats;
        for summary in [
            &results.global.latency,
            &results.global.error_latency,
            &group.latency,
            &group.error_latency,
        ] {
            let expected = match case {
                Case::NoSamples => (0, Some(0)),
                Case::ZeroRetained => (1, Some(0)),
                Case::LegacyUnknown | Case::LegacyNull => (1, None),
                Case::ObservedZero | Case::ObservedNonzero => (1, Some(1)),
                Case::EmptyWithStaleValues => (0, Some(1)),
            };
            assert_eq!(
                (summary.sample_count, summary.percentile_sample_count),
                expected,
                "{case:?}"
            );
        }
    }

    fn assert_text_line(text: &str, indent: &str, label: &str, value: &str) {
        let width = (17 - indent.len()).max(label.len() + 1);
        let expected = format!("{indent}{label:<width$}{value}\n");
        assert!(text.contains(&expected), "missing {expected:?} in {text:?}");
    }

    #[test]
    fn text_latency_summaries_show_only_available_values() {
        for case in CASES {
            let results = results(case);
            assert_summary_counts(case, &results);
            let (mean, percentile, _) = case.expected();
            let group = &results.traffic_groups[0].stats;
            for (summary, indent) in [(&results.global.latency, "  "), (&group.latency, "    ")] {
                let text = DetailedExperimentResults::format_latency(summary, indent);
                assert_text_line(&text, indent, "Mean:", mean);
                for label in ["p50:", "p95:", "p99:", "p999:"] {
                    assert_text_line(&text, indent, label, percentile);
                }
                assert_text_line(&text, indent, "Samples:", &summary.sample_count.to_string());
                assert_text_line(
                    &text,
                    indent,
                    "Percentile samples:",
                    case.expected_percentile_count(),
                );
            }
            for (summary, indent) in
                [(&results.global.error_latency, ""), (&group.error_latency, "  ")]
            {
                let text = DetailedExperimentResults::format_error_latency(summary, indent);
                assert!(text.starts_with(&format!("{indent}Error latency (μs):\n")));
                assert_text_line(&text, indent, "  Mean:", mean);
                assert_text_line(&text, indent, "  p50:", percentile);
                assert_text_line(&text, indent, "  p99:", percentile);
                assert_text_line(&text, indent, "  Samples:", &summary.sample_count.to_string());
                assert_text_line(
                    &text,
                    indent,
                    "  Percentile samples:",
                    case.expected_percentile_count(),
                );
            }
        }

        // Zero clamped samples remain a known count even without latencies.
        let mut latency = results(Case::NoSamples).global.latency;
        latency.clamped_samples = Some(0);
        let text = DetailedExperimentResults::format_latency(&latency, "  ");
        assert_text_line(&text, "  ", "Clamped:", "0");
    }

    fn html_row<'a>(section: &'a str, label: &str) -> &'a str {
        section
            .split_once(&format!("<td>{label}</td>"))
            .unwrap_or_else(|| panic!("missing HTML row {label}"))
            .1
            .split_once("<td>")
            .unwrap()
            .1
            .split_once("</td>")
            .unwrap()
            .0
    }

    fn html_card<'a>(section: &'a str, label: &str) -> &'a str {
        section
            .split_once(&format!("<div class=\"stat-label\">{label}</div>"))
            .unwrap_or_else(|| panic!("missing HTML card {label}"))
            .1
            .split_once("<div class=\"stat-value\">")
            .unwrap()
            .1
            .split_once("</div>")
            .unwrap()
            .0
    }

    fn chart_data(html: &str, chart_id: &str) -> Vec<Option<f64>> {
        let chart = html.split_once(&format!("document.getElementById('{chart_id}')")).unwrap().1;
        let data = chart.split_once("data: [").unwrap().1;
        let data = data.split_once(']').unwrap().0;
        serde_json::from_str(&format!("[{data}]")).unwrap()
    }

    fn html_value(value: &str) -> String {
        if value == "n/a" {
            value.to_string()
        } else {
            format!("{value} μs")
        }
    }

    #[test]
    fn html_latency_summaries_and_charts_show_only_available_values() {
        for case in CASES {
            let results = results(case);
            assert_summary_counts(case, &results);
            let json_before = serde_json::to_value(&results).unwrap();
            let report = tempfile::NamedTempFile::new().unwrap();
            html::generate_html_report(&results, report.path().to_str().unwrap()).unwrap();
            let report = std::fs::read_to_string(report.path()).unwrap();
            assert_eq!(serde_json::to_value(&results).unwrap(), json_before);

            let (mean, percentile, chart_value) = case.expected();
            let global = report.split_once("<h2>Traffic Groups Comparison</h2>").unwrap().0;
            let group = report.split_once("<div class=\"group-section\">").unwrap().1;
            assert!(global.contains(&format!(
                "<div class=\"stat-label\">Median Latency</div>\n                <div class=\"stat-value\">{}</div>",
                html_value(percentile)
            )), "{case:?}");
            assert_eq!(
                html_card(global, "Latency Samples"),
                results.global.latency.sample_count.to_string(),
                "{case:?}"
            );
            assert_eq!(
                html_card(global, "Latency Percentile Samples"),
                case.expected_percentile_count(),
                "{case:?}"
            );
            for (section, summary) in [
                (global, &results.global.error_latency),
                (group, &results.traffic_groups[0].stats.error_latency),
            ] {
                assert_eq!(
                    html_row(section, "Mean Error Reply Latency"),
                    html_value(mean),
                    "{case:?}"
                );
                assert_eq!(
                    html_row(section, "p99 Error Reply Latency"),
                    html_value(percentile),
                    "{case:?}"
                );
                assert_eq!(
                    html_row(section, "Error Reply Latency Samples"),
                    summary.sample_count.to_string(),
                    "{case:?}"
                );
                assert_eq!(
                    html_row(section, "Error Reply Latency Percentile Samples"),
                    case.expected_percentile_count(),
                    "{case:?}"
                );
                assert!(html_row(section, "Error Replies").contains("&lt;error&gt;"));
            }
            assert_eq!(
                html_row(group, "Latency Samples"),
                results.traffic_groups[0].stats.latency.sample_count.to_string(),
                "{case:?}"
            );
            assert_eq!(
                html_row(group, "Latency Percentile Samples"),
                case.expected_percentile_count(),
                "{case:?}"
            );
            assert_eq!(html_row(group, "Mean Latency"), html_value(mean), "{case:?}");
            for label in ["p50 Latency", "p99 Latency"] {
                assert_eq!(html_row(group, label), html_value(percentile), "{case:?}");
            }
            assert_eq!(chart_data(&report, "globalLatencyChart"), vec![chart_value; 5], "{case:?}");
            assert_eq!(chart_data(&report, "groupLatencyChart0"), vec![chart_value; 4], "{case:?}");
            assert!(report.contains("Xylem Experiment Report: display &lt;&amp;&gt;"));
            assert!(group.contains("Traffic Group 0: api &lt;&amp;&gt;"));

            let paths = [
                "/global/latency",
                "/global/error_latency",
                "/traffic_groups/0/stats/latency",
                "/traffic_groups/0/stats/error_latency",
            ];
            for path in paths {
                let summary = json_before.pointer(path).unwrap();
                let expected_mean = match case {
                    Case::NoSamples | Case::ObservedZero => 0.0,
                    _ => 7.0,
                };
                let expected_percentile = match case {
                    Case::NoSamples | Case::ZeroRetained | Case::ObservedZero => 0.0,
                    _ => 7.0,
                };
                assert_eq!(summary["mean_us"], expected_mean, "{case:?}");
                for field in ["p50_us", "p95_us", "p99_us", "p999_us", "p9999_us", "p99999_us"] {
                    assert_eq!(summary[field], expected_percentile, "{case:?} {field}");
                }
                assert_eq!(
                    summary.get("percentile_sample_count").is_none(),
                    matches!(case, Case::LegacyUnknown | Case::LegacyNull)
                );
            }
        }
    }

    #[test]
    fn global_group_and_error_displays_use_their_own_sample_counts() {
        let mut results = results(Case::ObservedNonzero);
        results.global.latency.sample_count = u64::MAX;
        results.global.latency.percentile_sample_count = Some(u64::MAX);
        results.global.error_latency.sample_count = 0;
        results.global.error_latency.percentile_sample_count = None;
        results.traffic_groups[0].stats.latency.percentile_sample_count = Some(0);
        results.traffic_groups[0].stats.error_latency.sample_count = 8;
        results.traffic_groups[0].stats.error_latency.percentile_sample_count = Some(3);
        let group = &results.traffic_groups[0].stats;

        let global_text = DetailedExperimentResults::format_latency(&results.global.latency, "  ");
        let group_text = DetailedExperimentResults::format_latency(&group.latency, "    ");
        assert_text_line(&global_text, "  ", "p99:", "7.00");
        assert_text_line(&global_text, "  ", "Percentile samples:", "18446744073709551615");
        assert_text_line(&group_text, "    ", "Mean:", "7.00");
        assert_text_line(&group_text, "    ", "p99:", "n/a");
        assert_text_line(&group_text, "    ", "Percentile samples:", "0");
        let global_error =
            DetailedExperimentResults::format_error_latency(&results.global.error_latency, "");
        let group_error =
            DetailedExperimentResults::format_error_latency(&group.error_latency, "  ");
        assert_text_line(&global_error, "", "  Mean:", "n/a");
        assert_text_line(&global_error, "", "  p99:", "n/a");
        assert_text_line(&global_error, "", "  Percentile samples:", "n/a");
        assert_text_line(&group_error, "  ", "  Mean:", "7.00");
        assert_text_line(&group_error, "  ", "  p99:", "7.00");
        assert_text_line(&group_error, "  ", "  Percentile samples:", "3");

        let file = tempfile::NamedTempFile::new().unwrap();
        html::generate_html_report(&results, file.path().to_str().unwrap()).unwrap();
        let report = std::fs::read_to_string(file.path()).unwrap();
        let global = report.split_once("<h2>Traffic Groups Comparison</h2>").unwrap().0;
        let group = report.split_once("<div class=\"group-section\">").unwrap().1;
        assert_eq!(html_card(global, "Latency Percentile Samples"), "18446744073709551615");
        assert_eq!(html_row(global, "Mean Error Reply Latency"), "n/a");
        assert_eq!(html_row(global, "p99 Error Reply Latency"), "n/a");
        assert_eq!(html_row(global, "Error Reply Latency Percentile Samples"), "n/a");
        assert_eq!(html_row(group, "Mean Latency"), "7.00 μs");
        assert_eq!(html_row(group, "p99 Latency"), "n/a");
        assert_eq!(html_row(group, "Latency Percentile Samples"), "0");
        assert_eq!(html_row(group, "Mean Error Reply Latency"), "7.00 μs");
        assert_eq!(html_row(group, "p99 Error Reply Latency"), "7.00 μs");
        assert_eq!(html_row(group, "Error Reply Latency Percentile Samples"), "3");
        assert_eq!(chart_data(&report, "globalLatencyChart"), vec![Some(7.0); 5]);
        assert_eq!(chart_data(&report, "groupLatencyChart0"), vec![None; 4]);
    }
}

#[cfg(test)]
mod raw_record_tests {
    use super::*;
    use xylem_core::stats::SamplingPolicy;
    use xylem_core::traffic_group::{OnError, PolicyConfig, TrafficGroupConfig};

    fn group(id: usize, sampling_policy: SamplingPolicy) -> TrafficGroupConfig {
        TrafficGroupConfig {
            name: format!("group-{id}"),
            threads: vec![0],
            connections_per_thread: 4,
            max_pending_per_connection: 1,
            request_timeout: None,
            on_error: OnError::Count,
            timestamps: Default::default(),
            protocol: "xylem-echo".into(),
            target: "unused".into(),
            transport: "tcp".into(),
            traffic_policy: PolicyConfig::ClosedLoop,
            sampling_policy,
            protocol_config: None,
        }
    }

    fn build_results(
        collector: &TupleStatsCollector,
        groups: &[TrafficGroupConfig],
        include_records: bool,
    ) -> DetailedExperimentResults {
        DetailedExperimentResults::from_tuple_stats(
            "records".into(),
            None,
            None,
            "unused".into(),
            "xylem-echo".into(),
            "tcp".into(),
            Duration::from_secs(1),
            collector,
            groups,
            include_records,
        )
    }

    fn record(results: &DetailedExperimentResults, group: usize, conn: usize) -> &StatsRecord {
        results
            .records
            .iter()
            .find(|record| record.group_id == group && record.connection_id == conn)
            .unwrap()
    }

    #[test]
    fn records_use_their_own_collector_counts_and_preserve_numeric_latencies() {
        let policies = [
            SamplingPolicy::None,
            SamplingPolicy::Limited { max_samples: 0, rate: 1.0 },
            SamplingPolicy::Limited { max_samples: 8, rate: 0.0 },
            SamplingPolicy::Limited { max_samples: 2, rate: 1.0 },
            SamplingPolicy::Unlimited,
            SamplingPolicy::Limited { max_samples: 3, rate: 0.5 },
            SamplingPolicy::DdSketch { alpha: 0.01, max_bins: 1024 },
            SamplingPolicy::TDigest { compression: 100 },
            SamplingPolicy::HdrHistogram { sigfigs: 3, max_value_us: None },
        ];
        let groups: Vec<_> = policies
            .iter()
            .cloned()
            .enumerate()
            .map(|(id, policy)| group(id, policy))
            .collect();
        // Global sampling differs from some group policies, so its counts
        // cannot establish whether an individual entry has samples.
        let mut collector =
            TupleStatsCollector::new(SamplingPolicy::Unlimited, Duration::from_secs(3600));
        for (id, policy) in policies.into_iter().enumerate() {
            collector.register_group_policy(id, policy);
        }
        for (group, count) in [(0, 1), (1, 2), (2, 3), (3, 5), (5, 128), (6, 4), (7, 4), (8, 4)] {
            for _ in 0..count {
                collector.record_latency(group, 0, Duration::from_micros(7));
            }
        }
        collector.record_latency(4, 0, Duration::ZERO);
        collector.record_error_reply(4, 1, "error", Duration::from_micros(5));
        collector.record_latency(4, 2, Duration::from_micros(11));
        collector.end_measurement(xylem_core::timing::time_ns());
        collector.record_latency(4, 3, Duration::from_micros(9));

        let results = build_results(&collector, &groups, true);
        assert_eq!(results.records.len(), collector.entries().len());
        assert!(results.global.latency.sample_count > 0);
        assert!(results.traffic_groups[4].stats.latency.sample_count > 0);
        for (key, entry) in collector.entries() {
            let record = record(&results, key.group_id, key.connection_id);
            let percentiles = entry.latency.percentiles(&[0.5, 0.99, 0.999]);
            assert_eq!(record.thread_id, key.thread_id);
            assert_eq!(record.request_count, entry.request_count);
            assert_eq!(record.latency_sample_count, Some(entry.latency.count()));
            assert_eq!(
                record.percentile_sample_count,
                Some(entry.latency.percentile_sample_count())
            );
            assert_eq!(record.latency_mean_us, entry.latency.mean() as f64 / 1000.0);
            assert_eq!(record.latency_p50_us, percentiles[0] as f64 / 1000.0);
            assert_eq!(record.latency_p99_us, percentiles[1] as f64 / 1000.0);
            assert_eq!(record.latency_p999_us, percentiles[2] as f64 / 1000.0);
        }

        let check = |group, conn, requests, samples, percentiles, mean, p99, preview: &str| {
            let record = record(&results, group, conn);
            assert_eq!(record.request_count, requests);
            assert_eq!(record.latency_sample_count, Some(samples));
            assert_eq!(record.percentile_sample_count, Some(percentiles));
            assert_eq!(record.latency_mean_us, mean);
            assert_eq!(record.latency_p99_us, p99);
            assert!(record.format_preview().ends_with(preview), "{record:?}");
            let json = serde_json::to_value(record).unwrap();
            assert_eq!(json["latency_sample_count"], samples);
            assert_eq!(json["percentile_sample_count"], percentiles);
            assert_eq!(json["latency_mean_us"], mean);
            assert_eq!(json["latency_p99_us"], p99);
        };
        check(0, 0, 1, 0, 0, 0.0, 0.0, "p99=n/a");
        check(1, 0, 2, 2, 0, 7.0, 0.0, "p99=n/a");
        check(2, 0, 3, 0, 0, 0.0, 0.0, "p99=n/a");
        check(3, 0, 5, 5, 2, 7.0, 7.0, "p99=7.00μs");
        check(4, 0, 1, 1, 1, 0.0, 0.0, "p99=0.00μs");
        check(4, 1, 1, 0, 0, 0.0, 0.0, "p99=n/a");
        check(4, 2, 1, 1, 1, 11.0, 11.0, "p99=11.00μs");
        check(4, 3, 0, 1, 1, 9.0, 9.0, "p99=9.00μs");

        // A legacy record stays unknown even in a full result whose group and
        // global summaries have known percentile samples.
        let mut json = serde_json::to_value(&results).unwrap();
        let legacy_record = json["records"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|record| record["group_id"] == 4 && record["connection_id"] == 0)
            .unwrap()
            .as_object_mut()
            .unwrap();
        legacy_record.remove("latency_sample_count");
        legacy_record.remove("percentile_sample_count");
        let decoded: DetailedExperimentResults = serde_json::from_value(json).unwrap();
        assert!(decoded.global.latency.percentile_sample_count.unwrap() > 0);
        assert!(decoded.traffic_groups[4].stats.latency.percentile_sample_count.unwrap() > 0);
        assert!(record(&decoded, 4, 0).format_preview().ends_with("p99=n/a"));
        assert!(record(&decoded, 4, 2).format_preview().ends_with("p99=11.00μs"));
        let reserialized = serde_json::to_value(&decoded).unwrap();
        let legacy_record = reserialized["records"]
            .as_array()
            .unwrap()
            .iter()
            .find(|record| record["group_id"] == 4 && record["connection_id"] == 0)
            .unwrap();
        assert!(legacy_record.get("latency_sample_count").is_none());
        assert!(legacy_record.get("percentile_sample_count").is_none());
        assert_eq!(legacy_record["latency_p99_us"], 0.0);

        for group in 6..=8 {
            let record = record(&results, group, 0);
            assert_eq!(
                (record.latency_sample_count, record.percentile_sample_count),
                (Some(4), Some(4))
            );
        }

        // The 50% filter is random, so check its actual entry counts instead
        // of assuming how many observations passed the filter.
        let sampled = record(&results, 5, 0);
        let sampled_count = sampled.latency_sample_count.unwrap();
        assert_eq!(sampled.request_count, 128);
        assert!(sampled_count <= 128);
        assert_eq!(sampled.percentile_sample_count, Some(sampled_count.min(3)));
        assert_eq!(sampled.latency_mean_us, if sampled_count == 0 { 0.0 } else { 7.0 });
        assert!(sampled.format_preview().ends_with(if sampled_count == 0 {
            "p99=n/a"
        } else {
            "p99=7.00μs"
        }));
        assert!(build_results(&collector, &groups, false).records.is_empty());
    }

    #[test]
    fn legacy_record_counts_are_unknown_omitted_and_used_for_preview() {
        let legacy = serde_json::json!({
            "time_secs": 1.0,
            "group_id": 2,
            "thread_id": 3,
            "connection_id": 4,
            "request_count": 9,
            "throughput_rps": 9.0,
            "throughput_mbps": 1.0,
            "latency_p50_us": 0.0,
            "latency_p99_us": 0.0,
            "latency_p999_us": 0.0,
            "latency_mean_us": 0.0
        });
        let decoded: StatsRecord = serde_json::from_value(legacy.clone()).unwrap();
        assert_eq!((decoded.latency_sample_count, decoded.percentile_sample_count), (None, None));
        assert_eq!(decoded.format_preview(), "  t=1.0s g=2 t3 c=4: 9 rps, p99=n/a");
        assert_eq!(serde_json::to_value(&decoded).unwrap(), legacy);

        // Even a nonzero legacy value is unavailable if either count is absent.
        for missing in ["latency_sample_count", "percentile_sample_count"] {
            let mut json = legacy.clone();
            json["latency_p99_us"] = serde_json::json!(42.0);
            json["latency_sample_count"] = serde_json::json!(1);
            json["percentile_sample_count"] = serde_json::json!(1);
            json.as_object_mut().unwrap().remove(missing);
            let record: StatsRecord = serde_json::from_value(json).unwrap();
            assert!(record.format_preview().ends_with("p99=n/a"));
            let round_trip = serde_json::to_value(record).unwrap();
            assert!(round_trip.get(missing).is_none());
            assert_eq!(round_trip["latency_p99_us"], 42.0);
        }

        for (samples, percentiles, expected) in [
            (Some(1), Some(1), "p99=0.00μs"),
            (Some(0), Some(1), "p99=n/a"),
            (Some(1), Some(0), "p99=n/a"),
            (None, Some(1), "p99=n/a"),
            (Some(1), None, "p99=n/a"),
            (None, None, "p99=n/a"),
        ] {
            let mut json = legacy.clone();
            json["latency_sample_count"] = serde_json::json!(samples);
            json["percentile_sample_count"] = serde_json::json!(percentiles);
            let record: StatsRecord = serde_json::from_value(json).unwrap();
            assert_eq!(
                (record.latency_sample_count, record.percentile_sample_count),
                (samples, percentiles)
            );
            assert!(record.format_preview().ends_with(expected));
            let round_trip = serde_json::to_value(record).unwrap();
            for (field, value) in
                [("latency_sample_count", samples), ("percentile_sample_count", percentiles)]
            {
                assert_eq!(round_trip.get(field).and_then(JsonValue::as_u64), value);
                assert_eq!(round_trip.get(field).is_none(), value.is_none());
            }
            for field in ["latency_p50_us", "latency_p99_us", "latency_p999_us", "latency_mean_us"]
            {
                assert_eq!(round_trip[field], legacy[field]);
            }
        }
    }

    #[test]
    fn connections_of_a_group_that_recorded_nothing_stay_out_like_the_group() {
        let groups = [group(0, SamplingPolicy::Unlimited), group(1, SamplingPolicy::Unlimited)];
        let mut collector =
            TupleStatsCollector::new(SamplingPolicy::Unlimited, Duration::from_secs(3600));
        collector.record_latency(0, 0, Duration::from_micros(7));
        // Group 1 sent nothing in the run, like a low-rate group
        let mut results = build_results(&collector, &groups, false);
        let ids: Vec<_> = results.traffic_groups.iter().map(|group| group.id).collect();
        assert_eq!(ids, [0]);
        results.set_connection_addrs([(0, vec![(0, 0, None), (1, 1, None)])]);
        let json = serde_json::to_value(&results).unwrap();
        assert_eq!(json["traffic_groups"].as_array().unwrap().len(), 1);
        assert_eq!(
            json["traffic_groups"][0]["connection_addrs"],
            serde_json::json!([{ "thread_id": 0, "connection_id": 0, "local_addr": null }])
        );
    }
}
