use clap::Parser;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod config;
mod output;

#[derive(Parser)]
#[command(name = "xylem")]
#[command(version, about = "Latency measurement tool", long_about = None)]
struct Cli {
    /// Target server address (e.g., 192.168.1.100:6379)
    #[arg(short, long)]
    target: String,

    /// Protocol to use (echo, redis, memcached-binary, memcached-ascii, http, synthetic, masstree)
    #[arg(short, long)]
    protocol: String,

    /// Number of worker threads
    #[arg(long, default_value = "1")]
    threads: usize,

    /// Connections per thread
    #[arg(long, default_value = "1")]
    connections: usize,

    /// Target request rate (requests per second). If not specified, runs in closed-loop mode
    #[arg(long)]
    rate: Option<f64>,

    /// Experiment duration (e.g., 30s, 1m, 1h)
    #[arg(long, default_value = "10s")]
    duration: String,

    /// Transport protocol (tcp, udp, tls)
    #[arg(long, default_value = "tcp")]
    transport: String,

    /// Pin threads to CPU cores
    #[arg(long)]
    pin_cpus: bool,

    /// Key distribution (sequential, random, zipfian, round-robin)
    #[arg(long, default_value = "random")]
    key_dist: String,

    /// Zipfian theta parameter (0-1, only used with zipfian distribution)
    #[arg(long, default_value = "0.99")]
    key_theta: f64,

    /// Value size in bytes
    #[arg(long, default_value = "64")]
    value_size: usize,

    /// Output file for results (JSON format)
    #[arg(short, long)]
    output: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| cli.log_level.clone().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Xylem latency measurement tool");
    tracing::info!("Target: {}", cli.target);
    tracing::info!("Protocol: {}", cli.protocol);
    tracing::info!("Threads: {}", cli.threads);
    tracing::info!("Connections per thread: {}", cli.connections);
    if let Some(rate) = cli.rate {
        tracing::info!("Target rate: {} req/s", rate);
    } else {
        tracing::info!("Running in closed-loop mode (max throughput)");
    }
    tracing::info!("Duration: {}", cli.duration);

    // TODO: Implement experiment execution
    tracing::warn!("Experiment execution not yet implemented");
    tracing::info!("This is a skeleton implementation - core functionality coming soon");

    Ok(())
}
