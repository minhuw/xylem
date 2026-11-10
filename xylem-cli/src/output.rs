//! Results output formatting

pub mod html;

use crate::config::OutputFormat;
use crate::thresholds::{ThresholdResult, ValidityResult, Verdict};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::time::Duration;
use xylem_core::stats::{
    AggregatedStats, BreakdownClock, BreakdownSummary, PartSummary, RequestCounts, SendLagSummary,
    TupleStatsCollector, LATE_SEND_THRESHOLD,
};

/// Latency summary in microseconds. Values the sampling policy cannot provide
/// are `None`, written as `null` in JSON and `n/a` in text and HTML.
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
    pub sample_count: usize,
    /// Latencies above a configured `max_value_us` of an HDR histogram, which
    /// were recorded at that bound. Omitted unless the bound is configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clamped_samples: Option<u64>,
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

/// Per-second per-connection statistics record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsRecord {
    pub time_secs: f64,
    pub group_id: usize,
    /// Worker thread that owns the connection; connection IDs restart at 0
    /// on every thread.
    pub thread_id: usize,
    pub connection_id: usize,
    pub request_count: u64,
    pub throughput_rps: f64,
    pub throughput_mbps: f64,
    pub latency_p50_us: f64,
    pub latency_p99_us: f64,
    pub latency_p999_us: f64,
    pub latency_mean_us: f64,
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
    /// Raw statistics records at per-second per-connection granularity.
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
    pub stats: GroupStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_metadata: Option<JsonValue>,
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
    #[allow(clippy::too_many_arguments)]
    /// Create detailed results from tuple stats collector
    ///
    /// # Arguments
    /// * `include_records` - Whether to include per-second per-connection records
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

        let actual_load_loop = tuple_stats.actual_load_loop_unix_us();

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
                actual_load_loop_start_unix_us: actual_load_loop.map(|(start, _)| start),
                actual_load_loop_end_unix_us: actual_load_loop.map(|(_, end)| end),
                seed,
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
            sample_count: aggregated.latency_sample_count as usize,
            clamped_samples: aggregated.clamped_samples,
        }
    }

    /// Print the latency summary lines with `indent` before each label.
    fn print_latency(latency: &LatencyStats, indent: &str) {
        // Values start in column 17, as in the rest of the summary.
        let width = 17 - indent.len();
        let line = |label: &str, value: String| println!("{indent}{label:<width$}{value}");
        line("Mean:", format!("{:.2}", latency.mean_us));
        line("Min:", format_optional(latency.min_us));
        line("Max:", format_optional(latency.max_us));
        line("p50:", format!("{:.2}", latency.p50_us));
        line("p95:", format!("{:.2}", latency.p95_us));
        line("p99:", format!("{:.2}", latency.p99_us));
        line("p999:", format!("{:.2}", latency.p999_us));
        line("Std Dev:", format_optional(latency.std_dev_us));
        line(
            "95% CI:",
            latency.confidence_interval_us.map_or("n/a".into(), |ci| format!("±{ci:.2}")),
        );
        line("Samples:", latency.sample_count.to_string());
        if let Some(clamped) = latency.clamped_samples {
            line("Clamped:", clamped.to_string());
        }
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
            println!("{indent}Error latency (μs):");
            line("  Mean:", format!("{:.2}", error_latency.mean_us));
            line("  p50:", format!("{:.2}", error_latency.p50_us));
            line("  p99:", format!("{:.2}", error_latency.p99_us));
            line("  Max:", format_optional(error_latency.max_us));
            line("  Samples:", error_latency.sample_count.to_string());
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
            xylem_core::traffic_group::PolicyConfig::FixedRate { rate } => {
                format!("fixed-rate({})", rate)
            }
            xylem_core::traffic_group::PolicyConfig::Poisson { rate } => {
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
            xylem_core::traffic_group::PolicyConfig::Ramp { start_rate, end_rate, duration } => {
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
            xylem_core::traffic_group::PolicyConfig::Sawtooth { min_rate, max_rate, period } => {
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
        Self::print_latency(&self.global.latency, "  ");
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
            println!("Threads:         {:?}", group.threads);
            println!("Connections:     {}", group.connections);
            println!();
            println!("Statistics:");
            println!("  Requests:      {}", group.stats.total_requests);
            println!("  Throughput:    {:.2} req/s", group.stats.throughput_rps);
            println!("  Bandwidth:     {:.2} Mbps", group.stats.throughput_mbps);
            println!();
            println!("  Latency (μs):");
            Self::print_latency(&group.stats.latency, "    ");
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
            for r in self.records.iter().take(10) {
                println!(
                    "  t={:.1}s g={} t{} c={}: {:.0} rps, p99={:.2}μs",
                    r.time_secs,
                    r.group_id,
                    r.thread_id,
                    r.connection_id,
                    r.throughput_rps,
                    r.latency_p99_us
                );
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
