//! Results output formatting

pub mod html;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::fs::File;
use std::io::Write;
use std::time::Duration;
use xylem_core::stats::TupleStatsCollector;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyStats {
    pub min_us: f64,
    pub max_us: f64,
    pub mean_us: f64,
    pub p50_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub p9999_us: f64,
    pub p99999_us: f64,
    pub std_dev_us: f64,
    pub confidence_interval_us: f64,
    pub sample_count: usize,
}

/// Per-second per-connection statistics record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsRecord {
    pub time_secs: f64,
    pub group_id: usize,
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
    /// Print results to stdout in human-readable format
    pub fn print_human(&self) {
        println!("\n{}", "=".repeat(60));
        println!("Xylem Latency Measurement Results");
        println!("{}", "=".repeat(60));
        println!();
        println!("Configuration:");
        println!("  Protocol:        {}", self.protocol);
        println!("  Target:          {}", self.target);
        println!("  Duration:        {:.2}s", self.duration_secs);
        println!();
        println!("Throughput:");
        println!("  Requests:        {} total", self.total_requests);
        println!("  Rate:            {:.2} req/s", self.throughput_rps);
        println!("  Bandwidth:       {:.2} MB/s", self.throughput_mbps);
        if self.total_tx_bytes > 0 || self.total_rx_bytes > 0 {
            println!("  TX Bytes:        {}", self.total_tx_bytes);
            println!("  RX Bytes:        {}", self.total_rx_bytes);
        }
        println!();
        println!("Latency (microseconds):");
        if self.latency.sample_count > 0 {
            println!("  Samples:         {}", self.latency.sample_count);
        }
        if self.latency.min_us > 0.0 {
            println!("  Min:             {:.2} μs", self.latency.min_us);
        }
        println!("  Mean:            {:.2} μs", self.latency.mean_us);
        if self.latency.max_us > 0.0 {
            println!("  Max:             {:.2} μs", self.latency.max_us);
        }
        if self.latency.p50_us > 0.0 {
            println!("  p50:             {:.2} μs", self.latency.p50_us);
            println!("  p95:             {:.2} μs", self.latency.p95_us);
            println!("  p99:             {:.2} μs", self.latency.p99_us);
            println!("  p999:            {:.2} μs", self.latency.p999_us);
        }
        if self.latency.std_dev_us > 0.0 {
            println!("  Std Dev:         {:.2} μs", self.latency.std_dev_us);
        }
        if self.latency.confidence_interval_us > 0.0 {
            println!("  95% CI:          ±{:.2} μs", self.latency.confidence_interval_us);
        }
        println!();
        println!("{}", "=".repeat(60));
    }

    /// Write results to JSON file
    pub fn write_json(&self, path: &str) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        let mut file = File::create(path)?;
        file.write_all(json.as_bytes())?;
        println!("Results written to: {path}");
        Ok(())
    }
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

        // Aggregate global stats
        let global_aggregated = tuple_stats.aggregate_global(duration);

        // Aggregate per-group stats
        let per_group_aggregated = tuple_stats.aggregate_by_group(duration);

        // Build raw records from entries (only if requested)
        let mut records: Vec<StatsRecord> = if include_records {
            tuple_stats
                .entries()
                .iter()
                .map(|(key, entry)| {
                    let total_bytes = (entry.tx_bytes + entry.rx_bytes) as f64;
                    StatsRecord {
                        time_secs: key.time_bucket as f64 * bucket_duration_secs,
                        group_id: key.group_id,
                        connection_id: key.connection_id,
                        request_count: entry.request_count,
                        throughput_rps: entry.request_count as f64 / bucket_duration_secs,
                        throughput_mbps: (total_bytes * 8.0) / (bucket_duration_secs * 1_000_000.0),
                        latency_p50_us: entry.latency.percentile(0.5) as f64 / 1000.0,
                        latency_p99_us: entry.latency.percentile(0.99) as f64 / 1000.0,
                        latency_p999_us: entry.latency.percentile(0.999) as f64 / 1000.0,
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
                .then_with(|| a.connection_id.cmp(&b.connection_id))
        });

        // Build global stats
        let global = GlobalStats {
            total_requests: global_aggregated.total_requests,
            throughput_rps: global_aggregated.throughput_rps,
            throughput_mbps: global_aggregated.throughput_mbps,
            latency: LatencyStats {
                min_us: 0.0,
                max_us: 0.0,
                mean_us: global_aggregated.mean_latency.as_secs_f64() * 1_000_000.0,
                p50_us: global_aggregated.latency_p50.as_secs_f64() * 1_000_000.0,
                p95_us: global_aggregated.latency_p95.as_secs_f64() * 1_000_000.0,
                p99_us: global_aggregated.latency_p99.as_secs_f64() * 1_000_000.0,
                p999_us: global_aggregated.latency_p999.as_secs_f64() * 1_000_000.0,
                p9999_us: global_aggregated.latency_p9999.as_secs_f64() * 1_000_000.0,
                p99999_us: global_aggregated.latency_p99999.as_secs_f64() * 1_000_000.0,
                std_dev_us: global_aggregated.std_dev.as_secs_f64() * 1_000_000.0,
                confidence_interval_us: global_aggregated.confidence_interval.as_secs_f64()
                    * 1_000_000.0,
                sample_count: 0,
            },
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
                        latency: LatencyStats {
                            min_us: 0.0,
                            max_us: 0.0,
                            mean_us: aggregated.mean_latency.as_secs_f64() * 1_000_000.0,
                            p50_us: aggregated.latency_p50.as_secs_f64() * 1_000_000.0,
                            p95_us: aggregated.latency_p95.as_secs_f64() * 1_000_000.0,
                            p99_us: aggregated.latency_p99.as_secs_f64() * 1_000_000.0,
                            p999_us: aggregated.latency_p999.as_secs_f64() * 1_000_000.0,
                            p9999_us: aggregated.latency_p9999.as_secs_f64() * 1_000_000.0,
                            p99999_us: aggregated.latency_p99999.as_secs_f64() * 1_000_000.0,
                            std_dev_us: aggregated.std_dev.as_secs_f64() * 1_000_000.0,
                            confidence_interval_us: aggregated.confidence_interval.as_secs_f64()
                                * 1_000_000.0,
                            sample_count: 0,
                        },
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
        println!("  Mean:          {:.2}", self.global.latency.mean_us);
        println!("  p50:           {:.2}", self.global.latency.p50_us);
        println!("  p95:           {:.2}", self.global.latency.p95_us);
        println!("  p99:           {:.2}", self.global.latency.p99_us);
        println!("  p999:          {:.2}", self.global.latency.p999_us);
        if self.global.latency.std_dev_us > 0.0 {
            println!("  Std Dev:       {:.2}", self.global.latency.std_dev_us);
        }
        if self.global.latency.confidence_interval_us > 0.0 {
            println!("  95% CI:        ±{:.2}", self.global.latency.confidence_interval_us);
        }
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
            println!("    Mean:        {:.2}", group.stats.latency.mean_us);
            println!("    p50:         {:.2}", group.stats.latency.p50_us);
            println!("    p95:         {:.2}", group.stats.latency.p95_us);
            println!("    p99:         {:.2}", group.stats.latency.p99_us);
            println!("    p999:        {:.2}", group.stats.latency.p999_us);

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
            let unique_conns: std::collections::HashSet<_> =
                self.records.iter().map(|r| (r.group_id, r.connection_id)).collect();

            println!("{}", "-".repeat(70));
            println!(
                "RECORDS ({} entries: {} time buckets x {} connections)",
                self.records.len(),
                unique_times.len(),
                unique_conns.len()
            );
            println!("{}", "-".repeat(70));

            // Show first few entries
            println!("Sample records (time, group, conn, rps, p99):");
            for r in self.records.iter().take(10) {
                println!(
                    "  t={:.1}s g={} c={}: {:.0} rps, p99={:.2}μs",
                    r.time_secs, r.group_id, r.connection_id, r.throughput_rps, r.latency_p99_us
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
        let json = serde_json::to_string_pretty(self)?;
        let mut file = File::create(path)?;
        file.write_all(json.as_bytes())?;
        println!("Detailed results written to: {path}");
        Ok(())
    }
}
