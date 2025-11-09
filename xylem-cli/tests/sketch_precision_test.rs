//! Precision comparison test for sketch algorithms
//!
//! This test compares baseline configurations vs high-precision configurations
//! to demonstrate the precision/memory trade-offs.

mod common;

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Serialize, Deserialize)]
struct ExperimentResults {
    protocol: String,
    target: String,
    duration_secs: f64,
    total_requests: u64,
    throughput_rps: f64,
    throughput_mbps: f64,
    latency: LatencyStats,
}

#[derive(Debug, Serialize, Deserialize)]
struct LatencyStats {
    mean_us: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    p999_us: f64,
    p9999_us: f64,
    p99999_us: f64,
    std_dev_us: f64,
    confidence_interval_us: f64,
}

fn create_toml_config(name: &str, port: u16, sampling_policy: &str) -> PathBuf {
    let content = format!(
        r#"[experiment]
name = "{name}"
seed = 42
duration = "20s"

[target]
address = "127.0.0.1:{port}"
protocol = "redis"

[workload]
[workload.keys]
strategy = "random"
max = 10000
value_size = 64

[workload.pattern]
type = "constant"
rate = 10000.0

[[traffic_groups]]
name = "main"
threads = [0]
connections_per_thread = 64
max_pending_per_connection = 1
{sampling_policy}

[traffic_groups.policy]
type = "closed-loop"

[output]
format = "json"
file = "/tmp/xylem-test-{name}.json"
"#
    );

    let path = PathBuf::from(format!("/tmp/xylem-test-{}.toml", name));
    fs::write(&path, content).expect("Failed to write TOML config");
    path
}

fn run_experiment(config_path: &Path) -> ExperimentResults {
    let output = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--package",
            "xylem-cli",
            "--",
            "-P",
            config_path.to_str().unwrap(),
        ])
        .env("RUST_LOG", "error")
        .output()
        .expect("Failed to run xylem");

    if !output.status.success() {
        eprintln!("xylem stdout: {}", String::from_utf8_lossy(&output.stdout));
        eprintln!("xylem stderr: {}", String::from_utf8_lossy(&output.stderr));
        panic!("Xylem experiment failed");
    }

    let output_file = config_path.to_str().unwrap().replace(".toml", ".json");
    let json_content = fs::read_to_string(&output_file)
        .unwrap_or_else(|_| panic!("Failed to read JSON output: {}", output_file));

    serde_json::from_str(&json_content)
        .unwrap_or_else(|_| panic!("Failed to parse JSON output: {}", json_content))
}

fn calculate_error(actual: f64, expected: f64) -> f64 {
    ((actual - expected) / expected * 100.0).abs()
}

#[test]
#[ignore] // Run with: cargo test sketch_precision -- --ignored
fn test_sketch_precision_comparison() {
    let server = common::start_redis().expect("Failed to start Redis server");
    let port = server.port();

    println!("\n╔════════════════════════════════════════════════════════════════╗");
    println!("║         Sketch Precision Comparison - Redis Backend           ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!("\nRedis server running on port {}", port);
    println!("\n📊 Test Configuration:");
    println!("  • Duration: 20 seconds");
    println!("  • Protocol: Redis GET");
    println!("  • Baseline vs High-Precision configs");

    // Create configs for all 9 experiments
    let zero_config =
        create_toml_config("zero", port, r#"sampling_policy = { type = "unlimited" }"#);

    // Baseline configurations
    let limited_1pct_config = create_toml_config(
        "limited-1pct",
        port,
        r#"sampling_policy = { type = "limited", max_samples = 100000, rate = 0.01 }"#,
    );

    let ddsketch_baseline_config = create_toml_config(
        "ddsketch-baseline",
        port,
        r#"sampling_policy = { type = "dd-sketch", alpha = 0.01, max_bins = 1024 }"#,
    );

    let tdigest_baseline_config = create_toml_config(
        "tdigest-baseline",
        port,
        r#"sampling_policy = { type = "t-digest", compression = 1000 }"#,
    );

    let hdr_baseline_config = create_toml_config(
        "hdr-baseline",
        port,
        r#"sampling_policy = { type = "hdr-histogram", sigfigs = 3, max_value_us = 3600000000 }"#,
    );

    // High-precision configurations
    let limited_5pct_config = create_toml_config(
        "limited-5pct",
        port,
        r#"sampling_policy = { type = "limited", max_samples = 100000, rate = 0.05 }"#,
    );

    let ddsketch_hq_config = create_toml_config(
        "ddsketch-hq",
        port,
        r#"sampling_policy = { type = "dd-sketch", alpha = 0.005, max_bins = 2048 }"#,
    );

    let tdigest_hq_config = create_toml_config(
        "tdigest-hq",
        port,
        r#"sampling_policy = { type = "t-digest", compression = 10000 }"#,
    );

    let hdr_hq_config = create_toml_config(
        "hdr-hq",
        port,
        r#"sampling_policy = { type = "hdr-histogram", sigfigs = 4, max_value_us = 3600000000 }"#,
    );

    println!("\n╔════════════════════════════════════════════════════════════════╗");
    println!("║                    RUNNING EXPERIMENTS                         ║");
    println!("╚════════════════════════════════════════════════════════════════╝");

    println!("\n[1/9] Zero Sampling (Ground Truth)");
    let zero_results = run_experiment(&zero_config);
    println!(
        "  → Throughput: {:.0} req/s, Total: {}",
        zero_results.throughput_rps, zero_results.total_requests
    );

    println!("\n[2/9] Limited 1% (baseline)");
    let limited_1pct_results = run_experiment(&limited_1pct_config);
    println!("  → Throughput: {:.0} req/s", limited_1pct_results.throughput_rps);

    println!("\n[3/9] DDSketch α=0.01, 1024 bins (baseline)");
    let ddsketch_baseline_results = run_experiment(&ddsketch_baseline_config);
    println!("  → Throughput: {:.0} req/s", ddsketch_baseline_results.throughput_rps);

    println!("\n[4/9] T-Digest compression=1000 (baseline)");
    let tdigest_baseline_results = run_experiment(&tdigest_baseline_config);
    println!("  → Throughput: {:.0} req/s", tdigest_baseline_results.throughput_rps);

    println!("\n[5/9] HDR Histogram sigfigs=3 (baseline)");
    let hdr_baseline_results = run_experiment(&hdr_baseline_config);
    println!("  → Throughput: {:.0} req/s", hdr_baseline_results.throughput_rps);

    println!("\n[6/9] Limited 5% (high-precision)");
    let limited_5pct_results = run_experiment(&limited_5pct_config);
    println!("  → Throughput: {:.0} req/s", limited_5pct_results.throughput_rps);

    println!("\n[7/9] DDSketch α=0.005, 2048 bins (high-precision)");
    let ddsketch_hq_results = run_experiment(&ddsketch_hq_config);
    println!("  → Throughput: {:.0} req/s", ddsketch_hq_results.throughput_rps);

    println!("\n[8/9] T-Digest compression=10000 (high-precision)");
    let tdigest_hq_results = run_experiment(&tdigest_hq_config);
    println!("  → Throughput: {:.0} req/s", tdigest_hq_results.throughput_rps);

    println!("\n[9/9] HDR Histogram sigfigs=4 (high-precision)");
    let hdr_hq_results = run_experiment(&hdr_hq_config);
    println!("  → Throughput: {:.0} req/s", hdr_hq_results.throughput_rps);

    // Calculate errors for all configs
    macro_rules! calc_errors {
        ($results:expr, $prefix:ident) => {{
            let p50_err = calculate_error($results.latency.p50_us, zero_results.latency.p50_us);
            let p95_err = calculate_error($results.latency.p95_us, zero_results.latency.p95_us);
            let p99_err = calculate_error($results.latency.p99_us, zero_results.latency.p99_us);
            let p999_err = calculate_error($results.latency.p999_us, zero_results.latency.p999_us);
            let p9999_err =
                calculate_error($results.latency.p9999_us, zero_results.latency.p9999_us);
            let p99999_err =
                calculate_error($results.latency.p99999_us, zero_results.latency.p99999_us);
            (p50_err, p95_err, p99_err, p999_err, p9999_err, p99999_err)
        }};
    }

    let (lim1_p50, lim1_p95, lim1_p99, lim1_p999, lim1_p9999, lim1_p99999) =
        calc_errors!(limited_1pct_results, lim1);
    let (dd_p50, dd_p95, dd_p99, dd_p999, dd_p9999, dd_p99999) =
        calc_errors!(ddsketch_baseline_results, dd);
    let (td_p50, td_p95, td_p99, td_p999, td_p9999, td_p99999) =
        calc_errors!(tdigest_baseline_results, td);
    let (hdr_p50, hdr_p95, hdr_p99, hdr_p999, hdr_p9999, hdr_p99999) =
        calc_errors!(hdr_baseline_results, hdr);

    let (lim5_p50, lim5_p95, lim5_p99, lim5_p999, lim5_p9999, lim5_p99999) =
        calc_errors!(limited_5pct_results, lim5);
    let (ddhq_p50, ddhq_p95, ddhq_p99, ddhq_p999, ddhq_p9999, ddhq_p99999) =
        calc_errors!(ddsketch_hq_results, ddhq);
    let (tdhq_p50, tdhq_p95, tdhq_p99, tdhq_p999, tdhq_p9999, tdhq_p99999) =
        calc_errors!(tdigest_hq_results, tdhq);
    let (hdrhq_p50, hdrhq_p95, hdrhq_p99, hdrhq_p999, hdrhq_p9999, hdrhq_p99999) =
        calc_errors!(hdr_hq_results, hdrhq);

    // Print comparison table
    println!("\n╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                                                   BASELINE vs HIGH-PRECISION COMPARISON                                                                               ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝");
    println!(
        "\n{:<30} {:>20} {:>20} {:>20} {:>20} {:>20} {:>20}",
        "Method", "p50 (us)", "p95 (us)", "p99 (us)", "p999 (us)", "p9999 (us)", "p99999 (us)"
    );
    println!("{}", "─".repeat(170));

    println!(
        "{:<30} {:>20.2} {:>20.2} {:>20.2} {:>20.2} {:>20.2} {:>20.2}",
        "Zero (ground truth)",
        zero_results.latency.p50_us,
        zero_results.latency.p95_us,
        zero_results.latency.p99_us,
        zero_results.latency.p999_us,
        zero_results.latency.p9999_us,
        zero_results.latency.p99999_us
    );

    println!("\n--- BASELINE CONFIGURATIONS ---");
    println!("{:<30} {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%)",
        "Limited 1%",
        limited_1pct_results.latency.p50_us, lim1_p50,
        limited_1pct_results.latency.p95_us, lim1_p95,
        limited_1pct_results.latency.p99_us, lim1_p99,
        limited_1pct_results.latency.p999_us, lim1_p999,
        limited_1pct_results.latency.p9999_us, lim1_p9999,
        limited_1pct_results.latency.p99999_us, lim1_p99999);

    println!("{:<30} {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%)",
        "DDSketch α=0.01/1024",
        ddsketch_baseline_results.latency.p50_us, dd_p50,
        ddsketch_baseline_results.latency.p95_us, dd_p95,
        ddsketch_baseline_results.latency.p99_us, dd_p99,
        ddsketch_baseline_results.latency.p999_us, dd_p999,
        ddsketch_baseline_results.latency.p9999_us, dd_p9999,
        ddsketch_baseline_results.latency.p99999_us, dd_p99999);

    println!("{:<30} {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%)",
        "T-Digest comp=1000",
        tdigest_baseline_results.latency.p50_us, td_p50,
        tdigest_baseline_results.latency.p95_us, td_p95,
        tdigest_baseline_results.latency.p99_us, td_p99,
        tdigest_baseline_results.latency.p999_us, td_p999,
        tdigest_baseline_results.latency.p9999_us, td_p9999,
        tdigest_baseline_results.latency.p99999_us, td_p99999);

    println!("{:<30} {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%)",
        "HDR sigfigs=3",
        hdr_baseline_results.latency.p50_us, hdr_p50,
        hdr_baseline_results.latency.p95_us, hdr_p95,
        hdr_baseline_results.latency.p99_us, hdr_p99,
        hdr_baseline_results.latency.p999_us, hdr_p999,
        hdr_baseline_results.latency.p9999_us, hdr_p9999,
        hdr_baseline_results.latency.p99999_us, hdr_p99999);

    println!("\n--- HIGH-PRECISION CONFIGURATIONS ---");
    println!("{:<30} {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%)",
        "Limited 5%",
        limited_5pct_results.latency.p50_us, lim5_p50,
        limited_5pct_results.latency.p95_us, lim5_p95,
        limited_5pct_results.latency.p99_us, lim5_p99,
        limited_5pct_results.latency.p999_us, lim5_p999,
        limited_5pct_results.latency.p9999_us, lim5_p9999,
        limited_5pct_results.latency.p99999_us, lim5_p99999);

    println!("{:<30} {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%)",
        "DDSketch α=0.005/2048",
        ddsketch_hq_results.latency.p50_us, ddhq_p50,
        ddsketch_hq_results.latency.p95_us, ddhq_p95,
        ddsketch_hq_results.latency.p99_us, ddhq_p99,
        ddsketch_hq_results.latency.p999_us, ddhq_p999,
        ddsketch_hq_results.latency.p9999_us, ddhq_p9999,
        ddsketch_hq_results.latency.p99999_us, ddhq_p99999);

    println!("{:<30} {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%)",
        "T-Digest comp=10000",
        tdigest_hq_results.latency.p50_us, tdhq_p50,
        tdigest_hq_results.latency.p95_us, tdhq_p95,
        tdigest_hq_results.latency.p99_us, tdhq_p99,
        tdigest_hq_results.latency.p999_us, tdhq_p999,
        tdigest_hq_results.latency.p9999_us, tdhq_p9999,
        tdigest_hq_results.latency.p99999_us, tdhq_p99999);

    println!("{:<30} {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%)",
        "HDR sigfigs=4",
        hdr_hq_results.latency.p50_us, hdrhq_p50,
        hdr_hq_results.latency.p95_us, hdrhq_p95,
        hdr_hq_results.latency.p99_us, hdrhq_p99,
        hdr_hq_results.latency.p999_us, hdrhq_p999,
        hdr_hq_results.latency.p9999_us, hdrhq_p9999,
        hdr_hq_results.latency.p99999_us, hdrhq_p99999);

    // Assertions - high-precision configs should have better accuracy
    assert!(
        ddhq_p50 < 20.0 && ddhq_p95 < 20.0 && ddhq_p99 < 20.0,
        "DDSketch HQ: p50={:.2}%, p95={:.2}%, p99={:.2}%",
        ddhq_p50,
        ddhq_p95,
        ddhq_p99
    );
    assert!(
        hdrhq_p50 < 20.0 && hdrhq_p95 < 20.0 && hdrhq_p99 < 20.0,
        "HDR HQ: p50={:.2}%, p95={:.2}%, p99={:.2}%",
        hdrhq_p50,
        hdrhq_p95,
        hdrhq_p99
    );

    println!("\n✅ All tests passed!");

    // Cleanup
    drop(server);
    for name in [
        "zero",
        "limited-1pct",
        "ddsketch-baseline",
        "tdigest-baseline",
        "hdr-baseline",
        "limited-5pct",
        "ddsketch-hq",
        "tdigest-hq",
        "hdr-hq",
    ] {
        let _ = fs::remove_file(format!("/tmp/xylem-test-{}.toml", name));
        let _ = fs::remove_file(format!("/tmp/xylem-test-{}.json", name));
    }
}
