//! The real executable must keep its console summary, result file names and
//! result file contents for every output format.
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

const DESCRIPTION: &str = "Output format characterization";

/// Echo every byte of one connection back. The xylem-echo protocol accepts its
/// own 16-byte request as the response.
fn serve_echo(listener: TcpListener) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "CLI did not connect");
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("accept: {error}"),
            }
        };
        socket.set_nonblocking(false).unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut buf = [0; 4096];
        loop {
            let n = match socket.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => break,
                Err(error) => panic!("read: {error}"),
            };
            if socket.write_all(&buf[..n]).is_err() {
                break;
            }
        }
    })
}

/// Run a one-second closed-loop echo profile that writes `format` output to
/// `file`. Returns the process output and the echo server address.
fn run_cli(format: &str, file: &Path) -> (Output, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let target = listener.local_addr().unwrap().to_string();
    listener.set_nonblocking(true).unwrap();
    let server = serve_echo(listener);
    let profile_dir = tempfile::tempdir().unwrap();
    let profile = profile_dir.path().join("profile.toml");
    let config = serde_json::json!({
        "experiment": {
            "name": "output-formats", "description": DESCRIPTION, "duration": "1s", "seed": 7
        },
        "traffic_groups": [{
            "name": "echo", "threads": [0], "connections_per_thread": 1,
            "max_pending_per_connection": 1, "target": target, "protocol": "xylem-echo",
            "traffic_policy": { "type": "closed-loop" }
        }],
        "output": { "format": format, "file": file }
    });
    std::fs::write(&profile, toml::to_string(&config).unwrap()).unwrap();
    // Logging is off so stdout holds only the summary and the written-file lines.
    let output = Command::new(env!("CARGO_BIN_EXE_xylem"))
        .arg("-P")
        .arg(&profile)
        .env("RUST_LOG", "off")
        .output()
        .unwrap();
    if server.join().is_err() {
        panic!("echo server failed; CLI stderr: {}", String::from_utf8_lossy(&output.stderr));
    }
    (output, target)
}

fn assert_success(output: &Output) {
    assert_eq!(
        output.status.code(),
        Some(0),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
}

/// Names of the files in `dir`, sorted.
fn file_names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

/// Summary lines that end in a measured value, by their label.
const MEASURED_LABELS: [&str; 26] = [
    "Total Requests:  ",
    "Throughput:      ",
    "Bandwidth:       ",
    "  Mean:          ",
    "  Min:           ",
    "  Max:           ",
    "  p50:           ",
    "  p95:           ",
    "  p99:           ",
    "  p999:          ",
    "  Std Dev:       ",
    "  95% CI:        ",
    "  Samples:       ",
    "  Requests:      ",
    "  Throughput:    ",
    "  Bandwidth:     ",
    "    Mean:        ",
    "    Min:         ",
    "    Max:         ",
    "    p50:         ",
    "    p95:         ",
    "    p99:         ",
    "    p999:        ",
    "    Std Dev:     ",
    "    95% CI:      ",
    "    Samples:     ",
];

/// Mask an integer as `#` and a number with two decimals as `#.##`, keeping a
/// leading `±`. Any other text is returned unchanged so the comparison shows it.
fn mask_number(value: &str) -> String {
    if let Some(magnitude) = value.strip_prefix('±') {
        return format!("±{}", mask_number(magnitude));
    }
    let is_digits = |text: &str| !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit());
    match value.split_once('.') {
        None if is_digits(value) => "#".into(),
        Some((whole, fraction))
            if is_digits(whole) && is_digits(fraction) && fraction.len() == 2 =>
        {
            "#.##".into()
        }
        _ => value.into(),
    }
}

/// Mask the measured values in console output. Returns the masked text and
/// the measured values in output order.
fn mask_summary(stdout: &str) -> (String, Vec<String>) {
    let mut values = Vec::new();
    let mut lines = Vec::new();
    for line in stdout.split('\n') {
        let Some(label) = MEASURED_LABELS.iter().find(|label| line.starts_with(**label)) else {
            lines.push(line.to_string());
            continue;
        };
        let rest = &line[label.len()..];
        let (value, unit) = match rest.split_once(' ') {
            Some((value, unit)) => (value, format!(" {unit}")),
            None => (rest, String::new()),
        };
        values.push(value.to_string());
        lines.push(format!("{label}{}{unit}", mask_number(value)));
    }
    (lines.join("\n"), values)
}

/// The console summary of the echo profile with measured values masked.
fn masked_summary(target: &str) -> String {
    format!(
        "
{double}
Xylem Detailed Experiment Results
{double}

Experiment: output-formats
Description: Output format characterization
Duration: 1.00s
Seed: 7

Transport: tcp
Traffic Groups: 1

{single}
GLOBAL STATISTICS
{single}
Total Requests:  #
Throughput:      #.## req/s
Bandwidth:       #.## Mbps

Latency (μs):
  Mean:          #.##
  Min:           #.##
  Max:           #.##
  p50:           #.##
  p95:           #.##
  p99:           #.##
  p999:          #.##
  Std Dev:       #.##
  95% CI:        ±#.##
  Samples:       #

{single}
TRAFFIC GROUP 0: echo
{single}
Target:          {target} (xylem-echo)
Policy:          closed-loop
Threads:         [0]
Connections:     1

Statistics:
  Requests:      #
  Throughput:    #.## req/s
  Bandwidth:     #.## Mbps

  Latency (μs):
    Mean:        #.##
    Min:         #.##
    Max:         #.##
    p50:         #.##
    p95:         #.##
    p99:         #.##
    p999:        #.##
    Std Dev:     #.##
    95% CI:      ±#.##
    Samples:     #

{double}
",
        double = "=".repeat(70),
        single = "-".repeat(70),
    )
}

/// The number that a section of pretty-printed JSON writes for `key`, as
/// written. The key must appear once in the section.
fn raw_number<'a>(section: &'a str, key: &str) -> &'a str {
    let label = format!("\"{key}\": ");
    let numbers: Vec<&str> = section
        .lines()
        .filter_map(|line| line.trim_start().strip_prefix(label.as_str()))
        .map(|value| value.trim_end_matches(','))
        .collect();
    assert_eq!(numbers.len(), 1, "{key}");
    numbers[0]
}

/// Split JSON results text into the global section and the group section.
fn json_sections(json_text: &str) -> (&str, &str) {
    json_text.split_once("\"traffic_groups\"").unwrap()
}

/// The summary values the console prints for the JSON results, in output
/// order. Numbers are read from the text with Rust's exact parser, because
/// serde_json's default parser may differ in the last bit.
fn summary_values(json_text: &str) -> Vec<String> {
    let (global, group) = json_sections(json_text);
    let mut values = Vec::new();
    for section in [global, group] {
        let requests: u64 = raw_number(section, "total_requests").parse().unwrap();
        values.push(requests.to_string());
        for key in ["throughput_rps", "throughput_mbps"] {
            let value: f64 = raw_number(section, key).parse().unwrap();
            values.push(format!("{value:.2}"));
        }
        for key in [
            "mean_us",
            "min_us",
            "max_us",
            "p50_us",
            "p95_us",
            "p99_us",
            "p999_us",
            "std_dev_us",
        ] {
            let value: f64 = raw_number(section, key).parse().unwrap();
            values.push(format!("{value:.2}"));
        }
        let ci: f64 = raw_number(section, "confidence_interval_us").parse().unwrap();
        values.push(format!("±{ci:.2}"));
        values.push(raw_number(section, "sample_count").to_string());
    }
    values
}

/// JSON fields whose values are measured.
const MEASURED_KEYS: [&str; 15] = [
    "total_requests",
    "min_us",
    "max_us",
    "sample_count",
    "throughput_rps",
    "throughput_mbps",
    "mean_us",
    "p50_us",
    "p95_us",
    "p99_us",
    "p999_us",
    "p9999_us",
    "p99999_us",
    "std_dev_us",
    "confidence_interval_us",
];

/// Mask the numbers of measured fields in pretty-printed JSON as `#`.
fn mask_json(text: &str) -> String {
    let mut lines = Vec::new();
    for line in text.split('\n') {
        let field = line.trim_start();
        let indent = &line[..line.len() - field.len()];
        let mut masked = line.to_string();
        for key in MEASURED_KEYS {
            let label = format!("\"{key}\": ");
            let Some(value) = field.strip_prefix(&label) else {
                continue;
            };
            let (number, comma) = match value.strip_suffix(',') {
                Some(number) => (number, ","),
                None => (value, ""),
            };
            if number.parse::<f64>().is_ok() {
                masked = format!("{indent}{label}#{comma}");
            }
        }
        lines.push(masked);
    }
    lines.join("\n")
}

/// The JSON results file of the echo profile with measured values masked.
const MASKED_JSON: &str = r#"{
  "experiment": {
    "name": "output-formats",
    "description": "Output format characterization",
    "duration_secs": 1.0,
    "seed": 7
  },
  "target": {
    "address": "TARGET",
    "protocol": "xylem-echo",
    "transport": "tcp"
  },
  "global": {
    "total_requests": #,
    "throughput_rps": #,
    "throughput_mbps": #,
    "latency": {
      "min_us": #,
      "max_us": #,
      "mean_us": #,
      "p50_us": #,
      "p95_us": #,
      "p99_us": #,
      "p999_us": #,
      "p9999_us": #,
      "p99999_us": #,
      "std_dev_us": #,
      "confidence_interval_us": #,
      "sample_count": #
    }
  },
  "traffic_groups": [
    {
      "id": 0,
      "name": "echo",
      "target": "TARGET",
      "protocol": "xylem-echo",
      "threads": [
        0
      ],
      "connections": 1,
      "policy": "closed-loop",
      "stats": {
        "total_requests": #,
        "throughput_rps": #,
        "throughput_mbps": #,
        "latency": {
          "min_us": #,
          "max_us": #,
          "mean_us": #,
          "p50_us": #,
          "p95_us": #,
          "p99_us": #,
          "p999_us": #,
          "p9999_us": #,
          "p99999_us": #,
          "std_dev_us": #,
          "confidence_interval_us": #,
          "sample_count": #
        }
      }
    }
  ]
}"#;

fn sorted_keys(value: &serde_json::Value) -> Vec<&str> {
    let mut keys: Vec<&str> = value.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort();
    keys
}

/// Check the JSON results file and return its text.
fn assert_json_results(path: &Path, target: &str) -> String {
    let text = std::fs::read_to_string(path).unwrap();
    assert_eq!(mask_json(&text), MASKED_JSON.replace("TARGET", target));
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(sorted_keys(&json), ["experiment", "global", "target", "traffic_groups"]);
    let groups = json["traffic_groups"].as_array().unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(
        sorted_keys(&groups[0]),
        ["connections", "id", "name", "policy", "protocol", "stats", "target", "threads"]
    );
    for latency in [&json["global"]["latency"], &groups[0]["stats"]["latency"]] {
        // The default HDR histogram records every latency, so min and max
        // bracket the percentiles and the sample count is the request count.
        let value = |key: &str| latency[key].as_f64().unwrap();
        assert!(0.0 < value("min_us") && value("min_us") <= value("p50_us"));
        assert!(value("p999_us") <= value("max_us") * 1.001);
        assert!(value("std_dev_us") >= 0.0 && value("confidence_interval_us") >= 0.0);
        assert!(latency.get("clamped_samples").is_none());
    }
    let requests = &json["global"]["total_requests"];
    assert_eq!(&json["global"]["latency"]["sample_count"], requests);
    assert_eq!(&groups[0]["stats"]["latency"]["sample_count"], requests);
    text
}

/// Check the HTML report describes the echo profile and return its text.
fn assert_html_report(path: &Path, target: &str) -> String {
    let html = std::fs::read_to_string(path).unwrap();
    assert!(html.starts_with("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n"));
    assert!(html.ends_with("    </script>\n</body>\n</html>"));
    let target_line = format!("            <p><strong>Target:</strong> {target} (xylem-echo)</p>");
    for line in [
        "    <title>Xylem Report: output-formats</title>",
        "        <h1>Xylem Experiment Report: output-formats</h1>",
        "            <p><strong>Duration:</strong> 1.00s</p>",
        target_line.as_str(),
        "            <p><strong>Transport:</strong> tcp</p>",
        "            <p><strong>Description:</strong> Output format characterization</p>",
        "                <h3 style=\"margin:0\">Traffic Group 0: echo</h3>",
        "                    <td>xylem-echo</td>",
        "                    <td>closed-loop</td>",
        "                    <td>[0]</td>",
        "                <canvas id=\"groupLatencyChart0\"></canvas>",
        "                labels: [\"echo\"],",
    ] {
        assert_eq!(html.lines().filter(|actual| *actual == line).count(), 1, "{line}");
    }
    html
}

/// The output file paths of each case: no extension, one extension, and a
/// stem that contains a dot.
const FILES: [&str; 3] = ["results", "results.txt", "results.v1.txt"];

#[test]
fn human_format_prints_the_summary_and_writes_no_file() {
    let dir = tempfile::tempdir().unwrap();
    // Human output never opens the file, so a missing directory is not an error.
    let missing = Path::new("missing").join("results.json");
    for file in FILES.iter().map(Path::new).chain([missing.as_path()]) {
        let (output, target) = run_cli("human", &dir.path().join(file));
        assert_success(&output);
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert_eq!(mask_summary(&stdout).0, masked_summary(&target));
        assert_eq!(file_names(dir.path()), Vec::<String>::new());
    }
}

#[test]
fn json_format_replaces_the_extension_with_json() {
    let expected = ["results.json", "results.json", "results.v1.json"];
    for (file, expected) in FILES.into_iter().zip(expected) {
        let dir = tempfile::tempdir().unwrap();
        let (output, target) = run_cli("json", &dir.path().join(file));
        assert_success(&output);
        assert_eq!(file_names(dir.path()), [expected]);
        let json_path = dir.path().join(expected);
        let json_text = assert_json_results(&json_path, &target);
        let stdout = String::from_utf8(output.stdout).unwrap();
        let (masked, values) = mask_summary(&stdout);
        assert_eq!(
            masked,
            format!(
                "{}Detailed results written to: {}\n",
                masked_summary(&target),
                json_path.display()
            )
        );
        assert_eq!(values, summary_values(&json_text));
    }
}

#[test]
fn html_format_replaces_the_extension_with_html() {
    let expected = ["results.html", "results.html", "results.v1.html"];
    for (file, expected) in FILES.into_iter().zip(expected) {
        let dir = tempfile::tempdir().unwrap();
        let (output, target) = run_cli("html", &dir.path().join(file));
        assert_success(&output);
        assert_eq!(file_names(dir.path()), [expected]);
        let html_path = dir.path().join(expected);
        assert_html_report(&html_path, &target);
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert_eq!(
            mask_summary(&stdout).0,
            format!("{}HTML report written to: {}\n", masked_summary(&target), html_path.display())
        );
    }
}

#[test]
fn both_format_strips_one_extension_then_replaces_the_next() {
    // `both` removes the extension and then sets one, so a dotted stem loses
    // its last part: results.v1.txt -> results.v1 -> results.json.
    let expected = [
        ["results.html", "results.json"],
        ["results.html", "results.json"],
        ["results.html", "results.json"],
    ];
    for (file, expected) in FILES.into_iter().zip(expected) {
        let dir = tempfile::tempdir().unwrap();
        let (output, target) = run_cli("both", &dir.path().join(file));
        assert_success(&output);
        assert_eq!(file_names(dir.path()), expected);
        let json_path = dir.path().join("results.json");
        let html_path = dir.path().join("results.html");
        let json_text = assert_json_results(&json_path, &target);
        let html = assert_html_report(&html_path, &target);
        let stdout = String::from_utf8(output.stdout).unwrap();
        let (masked, values) = mask_summary(&stdout);
        assert_eq!(
            masked,
            format!(
                "{}Detailed results written to: {}\nHTML report written to: {}\n",
                masked_summary(&target),
                json_path.display(),
                html_path.display()
            )
        );
        assert_eq!(values, summary_values(&json_text));

        // The HTML report shows the same results as the JSON file. Both write
        // numbers with serde_json, so the chart data repeats the JSON text.
        let (global, group) = json_sections(&json_text);
        let global_latencies: Vec<&str> = ["p50_us", "p95_us", "p99_us", "p999_us", "p9999_us"]
            .iter()
            .map(|key| raw_number(global, key))
            .collect();
        let group_latencies: Vec<&str> = ["p50_us", "p95_us", "p99_us", "p999_us"]
            .iter()
            .map(|key| raw_number(group, key))
            .collect();
        for line in [
            format!(
                "                <div class=\"stat-value\">{}</div>",
                raw_number(global, "total_requests")
            ),
            format!("                    data: [{}],", global_latencies.join(",")),
            format!("                    data: [{}],", raw_number(group, "throughput_rps")),
            format!("                    data: [{}],", group_latencies.join(",")),
        ] {
            assert_eq!(html.lines().filter(|actual| *actual == line).count(), 1, "{line}");
        }
    }
}

#[test]
fn file_formats_fail_after_the_summary_when_the_directory_is_missing() {
    for format in ["json", "html", "both"] {
        let dir = tempfile::tempdir().unwrap();
        let (output, target) = run_cli(format, &dir.path().join("missing").join("results"));
        assert_eq!(output.status.code(), Some(1), "{format}");
        assert_eq!(
            String::from_utf8(output.stderr).unwrap(),
            "Error: No such file or directory (os error 2)\n",
            "{format}"
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert_eq!(mask_summary(&stdout).0, masked_summary(&target), "{format}");
        assert_eq!(file_names(dir.path()), Vec::<String>::new(), "{format}");
    }
}
