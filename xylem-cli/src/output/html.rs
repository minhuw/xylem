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
                <div class="stat-value">{median:.2} μs</div>
            </div>
        </div>

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
        requests = results.global.total_requests,
        throughput = results.global.throughput_rps,
        bandwidth = results.global.throughput_mbps,
        median = results.global.latency.p50_us,
        group_sections = generate_group_sections(&results.traffic_groups),
        global_latencies = script_json(&[
            results.global.latency.p50_us,
            results.global.latency.p95_us,
            results.global.latency.p99_us,
            results.global.latency.p999_us,
            results.global.latency.p9999_us,
        ])?,
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
                    <td>{median:.2} μs</td>
                </tr>
                <tr>
                    <td>p99 Latency</td>
                    <td>{tail:.2} μs</td>
                </tr>
                <tr>
                    <td>Mean Latency</td>
                    <td>{mean:.2} μs</td>
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
            </table>
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
                median = group.stats.latency.p50_us,
                tail = group.stats.latency.p99_us,
                mean = group.stats.latency.mean_us,
                min = micros_or_na(group.stats.latency.min_us),
                max = micros_or_na(group.stats.latency.max_us),
                std_dev = micros_or_na(group.stats.latency.std_dev_us),
                ci = group
                    .stats
                    .latency
                    .confidence_interval_us
                    .map_or_else(|| "n/a".to_string(), |ci| format!("±{ci:.2} μs")),
                samples = group.stats.latency.sample_count,
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
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
                latencies = script_json(&[
                    group.stats.latency.p50_us,
                    group.stats.latency.p95_us,
                    group.stats.latency.p99_us,
                    group.stats.latency.p999_us,
                ])?,
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .join("\n"))
}
