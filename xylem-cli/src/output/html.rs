//! HTML report generation with charts

use super::DetailedExperimentResults;
use anyhow::Result;
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
    <title>Xylem Report: {}</title>
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
        <h1>Xylem Experiment Report: {}</h1>

        <div class="metadata">
            <p><strong>Duration:</strong> {:.2}s</p>
            <p><strong>Target:</strong> {} ({})</p>
            <p><strong>Transport:</strong> {}</p>
            {}
        </div>

        <h2>Global Statistics</h2>
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Total Requests</div>
                <div class="stat-value">{}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Throughput</div>
                <div class="stat-value">{:.2} req/s</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Bandwidth</div>
                <div class="stat-value">{:.2} Mbps</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Median Latency</div>
                <div class="stat-value">{:.2} μs</div>
            </div>
        </div>

        <div class="chart-container">
            <canvas id="globalLatencyChart"></canvas>
        </div>

        <h2>Traffic Groups Comparison</h2>
        <div class="chart-container">
            <canvas id="throughputComparisonChart"></canvas>
        </div>

        {}

    </div>

    <script>
        // Global Latency Chart
        new Chart(document.getElementById('globalLatencyChart'), {{
            type: 'bar',
            data: {{
                labels: ['p50', 'p95', 'p99', 'p999', 'p9999'],
                datasets: [{{
                    label: 'Global Latency (μs)',
                    data: [{:.2}, {:.2}, {:.2}, {:.2}, {:.2}],
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
                labels: [{}],
                datasets: [{{
                    label: 'Throughput (req/s)',
                    data: [{}],
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

        {}
    </script>
</body>
</html>"#,
        results.experiment.name,
        results.experiment.name,
        results.experiment.duration_secs,
        results.target.address,
        results.target.protocol,
        results.target.transport,
        results
            .experiment
            .description
            .as_ref()
            .map(|d| format!("<p><strong>Description:</strong> {}</p>", d))
            .unwrap_or_default(),
        results.global.total_requests,
        results.global.throughput_rps,
        results.global.throughput_mbps,
        results.global.latency.p50_us,
        results.global.latency.p50_us,
        results.global.latency.p95_us,
        results.global.latency.p99_us,
        results.global.latency.p999_us,
        results.global.latency.p9999_us,
        generate_group_sections(&results.traffic_groups),
        generate_group_labels(&results.traffic_groups),
        generate_group_throughputs(&results.traffic_groups),
        generate_group_charts(&results.traffic_groups),
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
                <h3 style="margin:0">Traffic Group {}: {}</h3>
            </div>
            <table>
                <tr>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
                <tr>
                    <td>Protocol</td>
                    <td>{}</td>
                </tr>
                <tr>
                    <td>Policy</td>
                    <td>{}</td>
                </tr>
                <tr>
                    <td>Threads</td>
                    <td>{:?}</td>
                </tr>
                <tr>
                    <td>Connections</td>
                    <td>{}</td>
                </tr>
                <tr>
                    <td>Requests</td>
                    <td>{}</td>
                </tr>
                <tr>
                    <td>Throughput</td>
                    <td>{:.2} req/s</td>
                </tr>
                <tr>
                    <td>p50 Latency</td>
                    <td>{:.2} μs</td>
                </tr>
                <tr>
                    <td>p99 Latency</td>
                    <td>{:.2} μs</td>
                </tr>
            </table>
            <div class="chart-container">
                <canvas id="groupLatencyChart{}"></canvas>
            </div>
        </div>"#,
                group.id,
                group.name,
                group.protocol,
                group.policy,
                group.threads,
                group.connections,
                group.stats.total_requests,
                group.stats.throughput_rps,
                group.stats.latency.p50_us,
                group.stats.latency.p99_us,
                group.id
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn generate_group_labels(groups: &[super::TrafficGroupResults]) -> String {
    groups.iter().map(|g| format!("'{}'", g.name)).collect::<Vec<_>>().join(", ")
}

fn generate_group_throughputs(groups: &[super::TrafficGroupResults]) -> String {
    groups
        .iter()
        .map(|g| format!("{:.2}", g.stats.throughput_rps))
        .collect::<Vec<_>>()
        .join(", ")
}

fn generate_group_charts(groups: &[super::TrafficGroupResults]) -> String {
    groups
        .iter()
        .map(|group| {
            format!(
                r#"new Chart(document.getElementById('groupLatencyChart{}'), {{
            type: 'bar',
            data: {{
                labels: ['p50', 'p95', 'p99', 'p999'],
                datasets: [{{
                    label: '{}  Latency (μs)',
                    data: [{:.2}, {:.2}, {:.2}, {:.2}],
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
                group.id,
                group.name,
                group.stats.latency.p50_us,
                group.stats.latency.p95_us,
                group.stats.latency.p99_us,
                group.stats.latency.p999_us,
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}
