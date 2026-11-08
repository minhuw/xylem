//! Reports must preserve chart values and treat profile-controlled strings as data.
use std::io::Write;
use std::process::{Command, Stdio};
use std::time::Duration;
use xylem_cli::output::{
    html::generate_html_report, DetailedExperimentResults, GroupStats, TrafficGroupResults,
};
use xylem_core::stats::{SamplingPolicy, TupleStatsCollector};

#[test]
fn html_report_preserves_data_and_escapes_profile_text() {
    let text = "O'Reilly \\\" <b>& </script><script>throw 'injected'</script>\u{2028}\u{2029}";
    let stats = TupleStatsCollector::new(SamplingPolicy::Unlimited, Duration::from_secs(1));
    let mut results = DetailedExperimentResults::from_tuple_stats(
        text.into(),
        Some(text.into()),
        None,
        text.into(),
        text.into(),
        text.into(),
        Duration::from_secs(1),
        &stats,
        &[],
        false,
    );
    results.global.latency.p50_us = 1.25;
    results.global.latency.p95_us = 2.5;
    results.global.latency.p99_us = 3.75;
    results.global.latency.p999_us = 4.125;
    results.global.latency.p9999_us = 5.0625;
    results.traffic_groups.push(TrafficGroupResults {
        id: 0,
        name: text.into(),
        target: text.into(),
        protocol: text.into(),
        threads: vec![0],
        connections: 1,
        policy: text.into(),
        stats: GroupStats {
            total_requests: 1,
            throughput_rps: 12.3456,
            throughput_mbps: 1.0,
            latency: results.global.latency.clone(),
            requests: Default::default(),
            send_lag: Default::default(),
            latency_breakdown: Default::default(),
        },
        protocol_metadata: None,
    });
    let file = tempfile::NamedTempFile::new().unwrap();
    generate_html_report(&results, file.path().to_str().unwrap()).unwrap();
    let html = std::fs::read_to_string(file.path()).unwrap();
    let (body, script) = html.split_once("    <script>\n").unwrap();
    let script = script.split_once("    </script>").unwrap().0;
    assert!(body.contains("<div class=\"group-section\">"));
    for id in ["globalLatencyChart", "throughputComparisonChart", "groupLatencyChart0"] {
        assert!(body.contains(&format!("<canvas id=\"{id}\"></canvas>")));
    }
    assert!(!body.contains(text));
    let escaped = "O&#39;Reilly \\&quot; &lt;b&gt;&amp; &lt;/script&gt;&lt;script&gt;throw &#39;injected&#39;&lt;/script&gt;\u{2028}\u{2029}";
    // Name (title/header), description, address, protocol, transport, group
    // name, group protocol and policy must all be escaped in HTML.
    assert_eq!(body.matches(escaped).count(), 9);
    assert!(!script.contains('<'));
    assert!(!script.contains('\u{2028}'));
    assert!(!script.contains('\u{2029}'));
    assert!(script.contains("data: [1.25,2.5,3.75,4.125,5.0625]"));
    assert!(script.contains("data: [1.25,2.5,3.75,4.125]"));
    assert!(script.contains("data: [12.3456]"));
    // No latency was recorded: unavailable values are n/a, never 0. That is
    // four group latencies, the global and group mean, p99 and max send lag,
    // and the five global and group service values of the latency breakdown.
    assert_eq!(body.matches("<td>n/a</td>").count(), 20);
    assert_eq!(body.matches("<h4>Latency Breakdown</h4>").count(), 2);
    assert_eq!(body.matches("<td colspan=\"5\">n/a (no kernel split)</td>").count(), 4);
    assert_eq!(body.matches("they do not add up to the total").count(), 2);
    assert_eq!(body.matches("<td>Requests Timed Out (no latency)</td>").count(), 2);
    assert!(body.contains("<td>0</td>"), "sample count");
    // Verify JSON labels round-trip even when a JavaScript runtime is unavailable.
    let label_json = script.split("labels: ").nth(2).unwrap().lines().next().unwrap();
    let labels: Vec<String> = serde_json::from_str(label_json.trim_end_matches(',')).unwrap();
    assert_eq!(labels, [text]);

    // Node is optional locally; CI's Ubuntu runner provides it. Execute the full
    // script with a Chart stub to catch syntax errors and positional regressions.
    let mut child = match Command::new("node")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("Node unavailable; JavaScript execution check skipped");
            return;
        }
        Err(e) => panic!("Cannot start Node: {e}"),
    };
    let expected = serde_json::to_string(text).unwrap();
    let harness = format!(
        r#"
const assert = require('node:assert/strict');
const charts = [];
const document = {{ getElementById: id => id }};
function Chart(id, options) {{ charts.push({{id, options}}); }}
{script}
assert.equal(charts.length, 3);
assert.equal(charts[2].id, 'groupLatencyChart0');
assert.deepEqual(charts[0].options.data.datasets[0].data, [1.25,2.5,3.75,4.125,5.0625]);
assert.deepEqual(charts[1].options.data.labels, [{expected}]);
assert.deepEqual(charts[1].options.data.datasets[0].data, [12.3456]);
assert.equal(charts[2].options.data.datasets[0].label, {expected} + '  Latency (μs)');
assert.deepEqual(charts[2].options.data.datasets[0].data, [1.25,2.5,3.75,4.125]);
"#
    );
    child.stdin.take().unwrap().write_all(harness.as_bytes()).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
}

#[test]
fn unavailable_latency_values_serialize_as_null() {
    let stats = TupleStatsCollector::new(SamplingPolicy::None, Duration::from_secs(1));
    let results = DetailedExperimentResults::from_tuple_stats(
        "none".into(),
        None,
        None,
        "127.0.0.1:1".into(),
        "redis".into(),
        "tcp".into(),
        Duration::from_secs(1),
        &stats,
        &[],
        false,
    );
    let json = serde_json::to_value(&results).unwrap();
    let latency = &json["global"]["latency"];
    for key in ["min_us", "max_us", "std_dev_us", "confidence_interval_us"] {
        assert!(latency[key].is_null(), "{key}: {}", latency[key]);
    }
    assert_eq!(latency["sample_count"], 0);
    assert!(latency.get("clamped_samples").is_none());
}
