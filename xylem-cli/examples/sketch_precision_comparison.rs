//! Sketch Precision Comparison Example
//!
//! This example demonstrates the precision/memory trade-offs of different
//! sampling policies for latency measurement:
//! - Unlimited: Stores all latencies (ground truth, high memory)
//! - Limited: Samples a percentage of requests
//! - DDSketch: Distributed sketch with configurable accuracy
//! - T-Digest: Adaptive histogram with configurable compression
//! - HDR Histogram: High-dynamic-range histogram with fixed precision
//!
//! ## Prerequisites
//!
//! - Redis server running on localhost:6379
//!   Start with Docker: `docker run -d -p 6379:6379 redis:latest`
//!   Or use: `docker compose -f tests/redis/docker-compose.yml up -d`
//!
//! ## Running
//!
//! ```bash
//! # Make sure Redis is running first
//! docker run -d -p 6379:6379 redis:latest
//!
//! # Run the example
//! cargo run --example sketch_precision_comparison --release
//! ```
//!
//! This will run multiple benchmark experiments and compare the results,
//! showing how different sampling policies affect latency measurements.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use xylem_cli::output::DetailedExperimentResults;

fn create_toml_config(name: &str, port: u16, sampling_policy: &str) -> PathBuf {
    let content = format!(
        r#"[experiment]
name = "{name}"
seed = 42
duration = "10s"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = "127.0.0.1:{port}"
transport = "tcp"
threads = [0]
connections_per_thread = 64
max_pending_per_connection = 1
{sampling_policy}

[traffic_groups.protocol_config.keys]
strategy = "random"
max = 10000
value_size = 64

[traffic_groups.protocol_config.operations]
strategy = "fixed"
operation = "get"

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "/tmp/xylem-example-{name}.json"
"#
    );

    let path = PathBuf::from(format!("/tmp/xylem-example-{}.toml", name));
    fs::write(&path, content).expect("Failed to write TOML config");
    path
}

fn run_experiment(config_path: &Path) -> DetailedExperimentResults {
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

fn main() -> anyhow::Result<()> {
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║       Sketch Precision Comparison - Redis Backend             ║");
    println!("╚════════════════════════════════════════════════════════════════╝");

    // Check if Redis is available
    println!("\n🔌 Checking Redis connection...");
    let target_addr = "127.0.0.1:6379".parse()?;

    match std::net::TcpStream::connect_timeout(&target_addr, Duration::from_secs(1)) {
        Ok(_) => println!("✓ Redis is available on 127.0.0.1:6379\n"),
        Err(_) => {
            eprintln!("❌ Error: Redis is not running on 127.0.0.1:6379");
            eprintln!("\nPlease start Redis with Docker:");
            eprintln!("  docker run -d -p 6379:6379 redis:latest");
            eprintln!("\nOr use the just command:");
            eprintln!("  just redis-start");
            std::process::exit(1);
        }
    }

    println!("📊 Test Configuration:");
    println!("  • Duration: 10 seconds per experiment");
    println!("  • Protocol: Redis GET");
    println!("  • Comparing 5 sampling policies\n");

    // Create configs for 5 experiments (reduced from 9 for faster execution)
    let zero_config =
        create_toml_config("zero", 6379, r#"sampling_policy = { type = "unlimited" }"#);

    let ddsketch_config = create_toml_config(
        "ddsketch",
        6379,
        r#"sampling_policy = { type = "dd-sketch", alpha = 0.01, max_bins = 1024 }"#,
    );

    let tdigest_config = create_toml_config(
        "tdigest",
        6379,
        r#"sampling_policy = { type = "t-digest", compression = 1000 }"#,
    );

    let hdr_config = create_toml_config(
        "hdr",
        6379,
        r#"sampling_policy = { type = "hdr-histogram", sigfigs = 3, max_value_us = 3600000000 }"#,
    );

    let limited_config = create_toml_config(
        "limited",
        6379,
        r#"sampling_policy = { type = "limited", max_samples = 100000, rate = 0.05 }"#,
    );

    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                    RUNNING EXPERIMENTS                         ║");
    println!("╚════════════════════════════════════════════════════════════════╝");

    println!("\n[1/5] Unlimited Sampling (Ground Truth)");
    println!("  ℹ️  Stores all latency measurements - highest accuracy, highest memory");
    let zero_results = run_experiment(&zero_config);
    println!(
        "  → Throughput: {:.0} req/s, Total: {}",
        zero_results.global.throughput_rps, zero_results.global.total_requests
    );

    println!("\n[2/5] DDSketch (α=0.01, 1024 bins)");
    println!("  ℹ️  Distributed sketch with relative error guarantees");
    let ddsketch_results = run_experiment(&ddsketch_config);
    println!("  → Throughput: {:.0} req/s", ddsketch_results.global.throughput_rps);

    println!("\n[3/5] T-Digest (compression=1000)");
    println!("  ℹ️  Adaptive histogram optimized for tail percentiles");
    let tdigest_results = run_experiment(&tdigest_config);
    println!("  → Throughput: {:.0} req/s", tdigest_results.global.throughput_rps);

    println!("\n[4/5] HDR Histogram (3 significant figures)");
    println!("  ℹ️  High-dynamic-range histogram with fixed precision");
    let hdr_results = run_experiment(&hdr_config);
    println!("  → Throughput: {:.0} req/s", hdr_results.global.throughput_rps);

    println!("\n[5/5] Limited Sampling (5% of requests)");
    println!("  ℹ️  Random sampling with rate limiting - lower memory");
    let limited_results = run_experiment(&limited_config);
    println!("  → Throughput: {:.0} req/s", limited_results.global.throughput_rps);

    // Calculate errors for all configs
    let dd_p50 =
        calculate_error(ddsketch_results.global.latency.p50_us, zero_results.global.latency.p50_us);
    let dd_p95 =
        calculate_error(ddsketch_results.global.latency.p95_us, zero_results.global.latency.p95_us);
    let dd_p99 =
        calculate_error(ddsketch_results.global.latency.p99_us, zero_results.global.latency.p99_us);
    let dd_p999 = calculate_error(
        ddsketch_results.global.latency.p999_us,
        zero_results.global.latency.p999_us,
    );

    let td_p50 =
        calculate_error(tdigest_results.global.latency.p50_us, zero_results.global.latency.p50_us);
    let td_p95 =
        calculate_error(tdigest_results.global.latency.p95_us, zero_results.global.latency.p95_us);
    let td_p99 =
        calculate_error(tdigest_results.global.latency.p99_us, zero_results.global.latency.p99_us);
    let td_p999 = calculate_error(
        tdigest_results.global.latency.p999_us,
        zero_results.global.latency.p999_us,
    );

    let hdr_p50 =
        calculate_error(hdr_results.global.latency.p50_us, zero_results.global.latency.p50_us);
    let hdr_p95 =
        calculate_error(hdr_results.global.latency.p95_us, zero_results.global.latency.p95_us);
    let hdr_p99 =
        calculate_error(hdr_results.global.latency.p99_us, zero_results.global.latency.p99_us);
    let hdr_p999 =
        calculate_error(hdr_results.global.latency.p999_us, zero_results.global.latency.p999_us);

    let lim_p50 =
        calculate_error(limited_results.global.latency.p50_us, zero_results.global.latency.p50_us);
    let lim_p95 =
        calculate_error(limited_results.global.latency.p95_us, zero_results.global.latency.p95_us);
    let lim_p99 =
        calculate_error(limited_results.global.latency.p99_us, zero_results.global.latency.p99_us);
    let lim_p999 = calculate_error(
        limited_results.global.latency.p999_us,
        zero_results.global.latency.p999_us,
    );

    // Print comparison table
    println!("\n╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                                      PRECISION COMPARISON RESULTS                                         ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════════════════════════════════════╝");
    println!(
        "\n{:<30} {:>20} {:>20} {:>20} {:>20}",
        "Method", "p50 (μs)", "p95 (μs)", "p99 (μs)", "p99.9 (μs)"
    );
    println!("{}", "─".repeat(115));

    println!(
        "{:<30} {:>20.2} {:>20.2} {:>20.2} {:>20.2}",
        "Unlimited (ground truth)",
        zero_results.global.latency.p50_us,
        zero_results.global.latency.p95_us,
        zero_results.global.latency.p99_us,
        zero_results.global.latency.p999_us
    );

    println!(
        "\n{:<30} {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%)",
        "DDSketch α=0.01/1024",
        ddsketch_results.global.latency.p50_us,
        dd_p50,
        ddsketch_results.global.latency.p95_us,
        dd_p95,
        ddsketch_results.global.latency.p99_us,
        dd_p99,
        ddsketch_results.global.latency.p999_us,
        dd_p999
    );

    println!(
        "{:<30} {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%)",
        "T-Digest comp=1000",
        tdigest_results.global.latency.p50_us,
        td_p50,
        tdigest_results.global.latency.p95_us,
        td_p95,
        tdigest_results.global.latency.p99_us,
        td_p99,
        tdigest_results.global.latency.p999_us,
        td_p999
    );

    println!(
        "{:<30} {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%)",
        "HDR sigfigs=3",
        hdr_results.global.latency.p50_us,
        hdr_p50,
        hdr_results.global.latency.p95_us,
        hdr_p95,
        hdr_results.global.latency.p99_us,
        hdr_p99,
        hdr_results.global.latency.p999_us,
        hdr_p999
    );

    println!(
        "{:<30} {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%) {:>13.2} ({:>5.2}%)",
        "Limited 5%",
        limited_results.global.latency.p50_us,
        lim_p50,
        limited_results.global.latency.p95_us,
        lim_p95,
        limited_results.global.latency.p99_us,
        lim_p99,
        limited_results.global.latency.p999_us,
        lim_p999
    );

    println!("\n📊 Key Insights:");
    println!("  • DDSketch: Good tail latency accuracy with predictable memory usage");
    println!("  • T-Digest: Excellent for extreme tail percentiles (p99.9+)");
    println!("  • HDR Histogram: Fixed precision across entire range");
    println!("  • Limited Sampling: Low memory but statistical variance increases");

    println!("\n💡 Next Steps:");
    println!("  • Adjust alpha/compression for different precision/memory trade-offs");
    println!("  • Test with different workload patterns");
    println!("  • Compare memory usage across configurations");

    // Cleanup
    for name in ["zero", "ddsketch", "tdigest", "hdr", "limited"] {
        let _ = fs::remove_file(format!("/tmp/xylem-example-{}.toml", name));
        let _ = fs::remove_file(format!("/tmp/xylem-example-{}.json", name));
    }

    println!("\n✅ Example completed successfully!\n");

    Ok(())
}
