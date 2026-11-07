//! Results output formatting

pub mod html;

use crate::config::OutputFormat;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::time::Duration;
use xylem_core::stats::{AggregatedStats, TupleStatsCollector};

/// Legacy flat experiment results. CLI JSON uses [`DetailedExperimentResults`].
#[deprecated(note = "Use DetailedExperimentResults, the current CLI JSON format.")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentResults {
    pub protocol: String,
    pub target: String,
    pub duration_secs: f64,
    pub total_requests: u64,
    pub total_tx_bytes: u64,
    pub total_rx_bytes: u64,
    pub throughput_rps: f64,
    pub throughput_mbps: f64,
    pub latency: LatencyStats,
}

/// Latency summary in microseconds. Values the sampling policy cannot provide
/// are `None`, written as `null` in JSON and `n/a` in text and HTML.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Microseconds in a duration.
fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

/// Format an optional value with two decimals, or `n/a`.
pub(crate) fn format_optional(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_string(), |value| format!("{value:.2}"))
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

#[allow(deprecated)]
impl ExperimentResults {
    /// Write results to JSON file
    pub fn write_json(&self, path: &str) -> Result<()> {
        write_pretty_json(self, path, "Results written to")
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
    pub global: GlobalStats,
    pub traffic_groups: Vec<TrafficGroupResults>,
    /// Raw statistics records at per-second per-connection granularity.
    /// Users can aggregate by time, group, or connection as needed.
    /// Only included when `stats.include_records = true` in config.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub records: Vec<StatsRecord>,
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
    pub total_requests: u64,
    pub throughput_rps: f64,
    pub throughput_mbps: f64,
    pub latency: LatencyStats,
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
    pub total_requests: u64,
    pub throughput_rps: f64,
    pub throughput_mbps: f64,
    pub latency: LatencyStats,
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
                seed,
            },
            target: TargetMetadata {
                address: target_address,
                protocol: target_protocol,
                transport: target_transport,
            },
            global,
            traffic_groups,
            records,
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

        println!("{}", "=".repeat(70));
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
