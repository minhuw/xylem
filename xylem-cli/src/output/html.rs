//! HTML report generation with charts

use super::DetailedExperimentResults;
use anyhow::Result;
use serde::Serialize;
use std::fs::File;
use std::io::Write;

/// Generate HTML report from experiment results
pub fn generate_html_report(results: &DetailedExperimentResults, path: &str) -> Result<()> {
    let html = format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Xylem Report: {name}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            border-bottom: 3px solid #007bff;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #555;
            margin-top: 30px;
            border-bottom: 2px solid #eee;
            padding-bottom: 8px;
        }}
        .metadata {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 4px;
            margin: 20px 0;
        }}
        .metadata p {{
            margin: 5px 0;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .stat-card {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 4px;
            border-left: 4px solid #007bff;
        }}
        .stat-label {{
            font-size: 14px;
            color: #666;
            margin-bottom: 5px;
        }}
        .stat-value {{
            font-size: 24px;
            font-weight: bold;
            color: #333;
        }}
        .chart-container {{
            position: relative;
            height: 400px;
            margin: 30px 0;
        }}
        .group-section {{
            margin: 40px 0;
            padding: 20px;
            background: #fafafa;
            border-radius: 4px;
        }}
        .group-header {{
            background: #007bff;
            color: white;
            padding: 10px 15px;
            border-radius: 4px;
            margin-bottom: 20px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background: #f8f9fa;
            font-weight: 600;
        }}
        .verdict {{
            padding: 10px 15px;
            border-radius: 4px;
            font-size: 20px;
            font-weight: bold;
        }}
        .verdict-pass {{
            background: #d4edda;
            color: #155724;
        }}
        .verdict-fail {{
            background: #f8d7da;
            color: #721c24;
        }}
        td.fail {{
            color: #721c24;
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Xylem Experiment Report: {name}</h1>

        <div class="metadata">
            <p><strong>Duration:</strong> {duration:.2}s</p>
            <p><strong>Target:</strong> {address} ({protocol})</p>
            <p><strong>Transport:</strong> {transport}</p>
            {description}
        </div>

        {checks}

        <h2>Global Statistics</h2>
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Total Requests</div>
                <div class="stat-value">{requests}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Throughput</div>
                <div class="stat-value">{throughput:.2} req/s</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Bandwidth</div>
                <div class="stat-value">{bandwidth:.2} Mbps</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Median Latency</div>
                <div class="stat-value">{median}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Latency Samples</div>
                <div class="stat-value">{latency_samples}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Latency Percentile Samples</div>
                <div class="stat-value">{latency_percentile_samples}</div>
            </div>
        </div>

        <h3>Requests</h3>
        <table>
            {global_requests}
        </table>

        {global_breakdown}

        <div class="chart-container">
            <canvas id="globalLatencyChart"></canvas>
        </div>

        <h2>Traffic Groups Comparison</h2>
        <div class="chart-container">
            <canvas id="throughputComparisonChart"></canvas>
        </div>

        {group_sections}

    </div>

    <script>
        // Global Latency Chart
        new Chart(document.getElementById('globalLatencyChart'), {{
            type: 'bar',
            data: {{
                labels: ['p50', 'p95', 'p99', 'p999', 'p9999'],
                datasets: [{{
                    label: 'Global Latency (μs)',
                    data: {global_latencies},
                    backgroundColor: 'rgba(54, 162, 235, 0.8)'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{
                    y: {{
                        beginAtZero: true,
                        title: {{
                            display: true,
                            text: 'Latency (μs)'
                        }}
                    }}
                }}
            }}
        }});

        // Throughput Comparison Chart
        new Chart(document.getElementById('throughputComparisonChart'), {{
            type: 'bar',
            data: {{
                labels: {group_labels},
                datasets: [{{
                    label: 'Throughput (req/s)',
                    data: {group_throughputs},
                    backgroundColor: 'rgba(75, 192, 192, 0.8)'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{
                    y: {{
                        beginAtZero: true,
                        title: {{
                            display: true,
                            text: 'Requests/sec'
                        }}
                    }}
                }}
            }}
        }});

        {group_charts}
    </script>
</body>
</html>"#,
        name = escape_html(&results.experiment.name),
        duration = results.experiment.duration_secs,
        address = escape_html(&results.target.address),
        protocol = escape_html(&results.target.protocol),
        transport = escape_html(&results.target.transport),
        description = results
            .experiment
            .description
            .as_ref()
            .map(|d| format!("<p><strong>Description:</strong> {}</p>", escape_html(d)))
            .unwrap_or_default(),
        checks = checks_section(results),
        requests = results.global.total_requests,
        throughput = results.global.throughput_rps,
        bandwidth = results.global.throughput_mbps,
        median = micros_or_na(
            results.global.latency.percentile_for_display(results.global.latency.p50_us)
        ),
        latency_samples = results.global.latency.sample_count,
        latency_percentile_samples =
            super::format_optional_count(results.global.latency.percentile_sample_count),
        global_requests = request_rows(
            &results.global.requests,
            &results.global.error_latency,
            &results.global.send_lag
        ),
        global_breakdown = breakdown_section(&results.global.latency_breakdown),
        group_sections = generate_group_sections(&results.traffic_groups),
        global_latencies = script_json(
            &[
                results.global.latency.p50_us,
                results.global.latency.p95_us,
                results.global.latency.p99_us,
                results.global.latency.p999_us,
                results.global.latency.p9999_us,
            ]
            .map(|value| results.global.latency.percentile_for_display(value))
        )?,
        group_labels =
            script_json(&results.traffic_groups.iter().map(|g| &g.name).collect::<Vec<_>>())?,
        group_throughputs = script_json(
            &results
                .traffic_groups
                .iter()
                .map(|g| g.stats.throughput_rps)
                .collect::<Vec<_>>()
        )?,
        group_charts = generate_group_charts(&results.traffic_groups)?,
    );

    let mut file = File::create(path)?;
    file.write_all(html.as_bytes())?;
    println!("HTML report written to: {path}");
    Ok(())
}

fn generate_group_sections(groups: &[super::TrafficGroupResults]) -> String {
    groups
        .iter()
        .map(|group| {
            format!(
                r#"<div class="group-section">
            <div class="group-header">
                <h3 style="margin:0">Traffic Group {id}: {name}</h3>
            </div>
            <table>
                <tr>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
                <tr>
                    <td>Protocol</td>
                    <td>{protocol}</td>
                </tr>
                <tr>
                    <td>Policy</td>
                    <td>{policy}</td>
                </tr>
                <tr>
                    <td>Threads</td>
                    <td>{threads:?}</td>
                </tr>
                <tr>
                    <td>Connections</td>
                    <td>{connections}</td>
                </tr>
                <tr>
                    <td>Requests</td>
                    <td>{requests}</td>
                </tr>
                <tr>
                    <td>Throughput</td>
                    <td>{throughput:.2} req/s</td>
                </tr>
                <tr>
                    <td>p50 Latency</td>
                    <td>{median}</td>
                </tr>
                <tr>
                    <td>p99 Latency</td>
                    <td>{tail}</td>
                </tr>
                <tr>
                    <td>Mean Latency</td>
                    <td>{mean}</td>
                </tr>
                <tr>
                    <td>Min Latency</td>
                    <td>{min}</td>
                </tr>
                <tr>
                    <td>Max Latency</td>
                    <td>{max}</td>
                </tr>
                <tr>
                    <td>Latency Std Dev</td>
                    <td>{std_dev}</td>
                </tr>
                <tr>
                    <td>Mean 95% CI</td>
                    <td>{ci}</td>
                </tr>
                <tr>
                    <td>Latency Samples</td>
                    <td>{samples}</td>
                </tr>
                <tr>
                    <td>Latency Percentile Samples</td>
                    <td>{percentile_samples}</td>
                </tr>
                {requests_rows}
            </table>
            {breakdown}
            <div class="chart-container">
                <canvas id="groupLatencyChart{id}"></canvas>
            </div>
        </div>"#,
                id = group.id,
                name = escape_html(&group.name),
                protocol = escape_html(&group.protocol),
                policy = escape_html(&group.policy),
                threads = group.threads,
                connections = group.connections,
                requests = group.stats.total_requests,
                throughput = group.stats.throughput_rps,
                median = micros_or_na(
                    group.stats.latency.percentile_for_display(group.stats.latency.p50_us)
                ),
                tail = micros_or_na(
                    group.stats.latency.percentile_for_display(group.stats.latency.p99_us)
                ),
                mean = micros_or_na(group.stats.latency.mean_for_display()),
                min =
                    micros_or_na(group.stats.latency.range_for_display(group.stats.latency.min_us)),
                max =
                    micros_or_na(group.stats.latency.range_for_display(group.stats.latency.max_us)),
                std_dev = micros_or_na(
                    group.stats.latency.dispersion_for_display(group.stats.latency.std_dev_us)
                ),
                ci = group
                    .stats
                    .latency
                    .dispersion_for_display(group.stats.latency.confidence_interval_us)
                    .map_or_else(|| "n/a".to_string(), |ci| format!("±{ci:.2} μs")),
                samples = group.stats.latency.sample_count,
                percentile_samples =
                    super::format_optional_count(group.stats.latency.percentile_sample_count),
                requests_rows = request_rows(
                    &group.stats.requests,
                    &group.stats.error_latency,
                    &group.stats.send_lag
                ),
                breakdown = breakdown_section(&group.stats.latency_breakdown),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The verdict with tables of the validity checks and thresholds
fn checks_section(results: &DetailedExperimentResults) -> String {
    let status = |ok: bool| {
        if ok {
            "<td>ok</td>"
        } else {
            "<td class=\"fail\">FAIL</td>"
        }
    };
    let validity = if results.validity.is_empty() {
        "<p>No validity checks applied.</p>".to_string()
    } else {
        let rows: Vec<String> = results
            .validity
            .iter()
            .map(|check| {
                format!(
                    "<tr>\n{}\n<td>{}</td>\n<td>{}</td>\n<td>{}</td>\n<td>{}</td>\n</tr>",
                    status(check.ok),
                    escape_html(&check.key()),
                    check.observed_text(),
                    check.limit_text(),
                    escape_html(check.reason.as_deref().unwrap_or("")),
                )
            })
            .collect();
        format!(
            "<table>\n<tr><th>Result</th><th>Check</th><th>Observed</th><th>Limit</th>\
             <th>Reason</th></tr>\n{}\n</table>",
            rows.join("\n")
        )
    };
    let thresholds = if results.thresholds.is_empty() {
        "<p>No thresholds configured.</p>".to_string()
    } else {
        let rows: Vec<String> = results
            .thresholds
            .iter()
            .map(|threshold| {
                format!(
                    "<tr>\n{}\n<td>{}</td>\n<td>{}</td>\n<td>{}</td>\n</tr>",
                    status(threshold.ok),
                    escape_html(&threshold.key()),
                    escape_html(&threshold.expression),
                    escape_html(&threshold.observed_text()),
                )
            })
            .collect();
        format!(
            "<table>\n<tr><th>Result</th><th>Metric</th><th>Expression</th>\
             <th>Observed</th></tr>\n{}\n</table>",
            rows.join("\n")
        )
    };
    let class = if results.verdict == crate::thresholds::Verdict::Pass {
        "verdict-pass"
    } else {
        "verdict-fail"
    };
    format!(
        "<h2>Verdict</h2>\n<div class=\"verdict {class}\">{}</div>\n<h3>Validity</h3>\n{validity}\n\
         <h3>Thresholds</h3>\n{thresholds}",
        results.verdict.label()
    )
}

/// Table rows for request counts, error replies and send lags
fn request_rows(
    requests: &xylem_core::stats::RequestCounts,
    error_latency: &super::LatencyStats,
    lag: &super::SendLagStats,
) -> String {
    let capped = if requests.unsent_capped {
        " (lower bound)"
    } else {
        ""
    };
    // Kinds come from the server
    let kinds: Vec<_> = requests
        .errors_by_kind
        .iter()
        .map(|(kind, count)| format!("{}: {count}", escape_html(kind)))
        .collect();
    let errors = if kinds.is_empty() {
        requests.errors.to_string()
    } else {
        format!("{} ({})", requests.errors, kinds.join(", "))
    };
    [
        ("Requests Sent", requests.sent.to_string()),
        ("Requests Completed", requests.completed.to_string()),
        ("Error Replies", errors),
        ("Mean Error Reply Latency", micros_or_na(error_latency.mean_for_display())),
        (
            "p99 Error Reply Latency",
            micros_or_na(error_latency.percentile_for_display(error_latency.p99_us)),
        ),
        ("Error Reply Latency Samples", error_latency.sample_count.to_string()),
        (
            "Error Reply Latency Percentile Samples",
            super::format_optional_count(error_latency.percentile_sample_count),
        ),
        ("Requests Timed Out (no latency)", requests.timed_out.to_string()),
        ("Late Responses (dropped)", requests.late_responses.to_string()),
        ("Requests Failed", requests.failed.to_string()),
        (
            "Late Sends",
            format!("{} (send lag above {:.0} μs)", requests.late_sends, lag.late_threshold_us),
        ),
        ("Unsent Requests", format!("{}{capped}", requests.unsent)),
        ("Mean Send Lag", micros_or_na(lag.mean_us)),
        ("p99 Send Lag", micros_or_na(lag.p99_us)),
        ("Max Send Lag", micros_or_na(lag.max_us)),
    ]
    .iter()
    .map(|(label, value)| format!("<tr>\n<td>{label}</td>\n<td>{value}</td>\n</tr>"))
    .collect::<Vec<_>>()
    .join("\n")
}

/// Heading, clock and table of a latency breakdown: one row per part
fn breakdown_section(breakdown: &super::LatencyBreakdownStats) -> String {
    let rows = breakdown
        .parts()
        .iter()
        .map(|(label, part)| {
            let cells = match part {
                Some(part) => {
                    part.values().map(|value| format!("<td>{}</td>", micros_or_na(value))).concat()
                }
                None => "<td colspan=\"5\">n/a (no kernel split)</td>".to_string(),
            };
            format!("<tr>\n<td>{label}</td>\n{cells}\n</tr>")
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "<h4>Latency Breakdown</h4>\n<p>Clock: {clock}; requests with a kernel split: {split}. \
         {note}</p>\n<table>\n<tr>\n<th>Part</th>\n<th>Mean</th>\n<th>p50</th>\n\
         <th>p99</th>\n<th>p99.9</th>\n<th>Max</th>\n</tr>\n{rows}\n</table>",
        clock = breakdown.clock_name(),
        split = breakdown.kernel_split_count,
        note = super::BREAKDOWN_NOTE,
    )
}

/// Format optional microseconds with two decimals and a unit, or `n/a`.
fn micros_or_na(value: Option<f64>) -> String {
    match value {
        Some(_) => format!("{} μs", super::format_optional(value)),
        None => "n/a".to_string(),
    }
}

/// Escape text nodes, including quotes so this also remains safe in attributes.
fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// JSON is valid JavaScript; escaping `<` prevents the HTML parser from ending
/// an inline script when a string contains `</script>` (in any letter case).
fn script_json(value: &impl Serialize) -> Result<String> {
    Ok(serde_json::to_string(value)?
        .replace('<', "\\u003c")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029"))
}

fn generate_group_charts(groups: &[super::TrafficGroupResults]) -> Result<String> {
    Ok(groups
        .iter()
        .map(|group| {
            Ok(format!(
                r#"new Chart(document.getElementById('groupLatencyChart{id}'), {{
            type: 'bar',
            data: {{
                labels: ['p50', 'p95', 'p99', 'p999'],
                datasets: [{{
                    label: {label},
                    data: {latencies},
                    backgroundColor: 'rgba(255, 159, 64, 0.8)'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{
                    y: {{
                        beginAtZero: true,
                        title: {{
                            display: true,
                            text: 'Latency (μs)'
                        }}
                    }}
                }}
            }}
        }});"#,
                id = group.id,
                label = script_json(&format!("{}  Latency (μs)", group.name))?,
                latencies = script_json(
                    &[
                        group.stats.latency.p50_us,
                        group.stats.latency.p95_us,
                        group.stats.latency.p99_us,
                        group.stats.latency.p999_us,
                    ]
                    .map(|value| group.stats.latency.percentile_for_display(value))
                )?,
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .join("\n"))
}

#[cfg(test)]
mod optional_latency_display_tests {
    use super::*;
    use crate::output::{DetailedExperimentResults, GroupStats, LatencyStats, TrafficGroupResults};
    use std::time::Duration;
    use xylem_core::stats::{SamplingPolicy, TupleStatsCollector};

    fn legacy(sample_count: u64, values: [Option<f64>; 4]) -> LatencyStats {
        let mut json = serde_json::to_value(LatencyStats::default()).unwrap();
        json["sample_count"] = sample_count.into();
        for (field, value) in ["min_us", "max_us", "std_dev_us", "confidence_interval_us"]
            .into_iter()
            .zip(values)
        {
            json[field] = serde_json::to_value(value).unwrap();
        }
        serde_json::from_value(json).unwrap()
    }

    fn group(latency: LatencyStats) -> TrafficGroupResults {
        TrafficGroupResults {
            id: 0,
            name: "group".into(),
            target: "unused".into(),
            protocol: "unused".into(),
            threads: vec![0],
            connections: 1,
            policy: "closed-loop".into(),
            dispatch: Default::default(),
            stats: GroupStats {
                total_requests: 0,
                throughput_rps: 0.0,
                throughput_mbps: 0.0,
                latency,
                error_latency: LatencyStats::default(),
                requests: Default::default(),
                send_lag: Default::default(),
                latency_breakdown: Default::default(),
            },
            protocol_metadata: None,
        }
    }

    fn assert_line(text: &str, label: &str, expected: &str) {
        assert!(
            text.lines()
                .any(|line| line.trim().strip_prefix(label).map(str::trim) == Some(expected)),
            "missing {label} {expected} in {text:?}"
        );
    }

    fn cell<'a>(html: &'a str, label: &str) -> &'a str {
        html.split_once(&format!("<td>{label}</td>"))
            .unwrap()
            .1
            .trim_start()
            .strip_prefix("<td>")
            .unwrap()
            .split_once("</td>")
            .unwrap()
            .0
    }

    #[test]
    fn optional_latency_fields_require_their_sample_counts() {
        let duration = Duration::from_secs(1);
        let mut collector = TupleStatsCollector::new(SamplingPolicy::Unlimited, duration);
        collector.record_latency(0, 0, Duration::ZERO);
        let one_zero =
            DetailedExperimentResults::latency_stats(&collector.aggregate_global(duration));
        collector.record_latency(0, 0, Duration::ZERO);
        let two_zero =
            DetailedExperimentResults::latency_stats(&collector.aggregate_global(duration));
        assert_eq!((one_zero.sample_count, two_zero.sample_count), (1, 2));
        let populated = [Some(7.0), Some(8.0), Some(9.0), Some(10.0)];
        let cases = [
            ("legacy zero count", legacy(0, populated), ["n/a", "n/a", "n/a", "n/a"]),
            ("legacy one count", legacy(1, populated), ["7.00", "8.00", "n/a", "n/a"]),
            ("legacy one zero", legacy(1, [Some(0.0); 4]), ["0.00", "0.00", "n/a", "n/a"]),
            ("legacy two counts", legacy(2, populated), ["7.00", "8.00", "9.00", "±10.00"]),
            ("nulls with two counts", legacy(2, [None; 4]), ["n/a", "n/a", "n/a", "n/a"]),
            ("observed zero once", one_zero, ["0.00", "0.00", "n/a", "n/a"]),
            ("observed zero twice", two_zero, ["0.00", "0.00", "0.00", "±0.00"]),
        ];
        for (name, stats, expected) in cases {
            let json = serde_json::to_value(&stats).unwrap();
            let stats: LatencyStats = serde_json::from_value(json.clone()).unwrap();
            for indent in ["  ", "    "] {
                let text = DetailedExperimentResults::format_latency(&stats, indent);
                for (label, value) in
                    ["Min:", "Max:", "Std Dev:", "95% CI:"].into_iter().zip(expected)
                {
                    assert_line(&text, label, value);
                }
            }
            for indent in ["", "  "] {
                let text = DetailedExperimentResults::format_error_latency(&stats, indent);
                assert_line(&text, "Max:", expected[1]);
            }
            let group = group(stats);
            let html = generate_group_sections(std::slice::from_ref(&group));
            for (label, value) in ["Min Latency", "Max Latency", "Latency Std Dev", "Mean 95% CI"]
                .into_iter()
                .zip(expected)
            {
                let value = if value == "n/a" {
                    value.to_string()
                } else {
                    format!("{value} μs")
                };
                assert_eq!(cell(&html, label), value, "{name} {label}");
            }
            assert_eq!(serde_json::to_value(&group.stats.latency).unwrap(), json, "{name}");
        }
    }
}
