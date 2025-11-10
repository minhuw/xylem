# Quick Start

This guide will help you run your first benchmark with Xylem.

## Prerequisites

- Rust 1.70 or later
- A target service to benchmark (e.g., Redis, HTTP server, Memcached)

## Build Xylem

```bash
# Clone the repository
git clone https://github.com/minhuw/xylem.git
cd xylem

# Build in release mode
cargo build --release

# The binary will be at target/release/xylem
```

## Running a Redis Benchmark

### 1. Start a Redis Server

First, ensure you have a Redis server running locally:

```bash
redis-server
```

By default, Redis listens on `localhost:6379`.

### 2. Run the Benchmark

Use one of the included example profiles:

```bash
./target/release/xylem -P profiles/redis-get-zipfian.toml
```

This profile runs a Redis GET benchmark with a Zipfian key distribution.

### 3. Understanding the Output

Xylem will display statistics about the benchmark, including:
- **Latency percentiles** (p50, p95, p99, p99.9, etc.)
- **Throughput** (requests per second)
- **Error rates**
- **Per-thread statistics**

## Configuration-First Design

Xylem uses TOML configuration files (called "profiles") to define experiments. This ensures reproducibility and simplifies complex workload specifications.

### Basic Syntax

```bash
xylem -P <profile.toml>
```

### Example Profiles

Xylem includes example profiles in the `profiles/` directory:

```bash
# Run a Redis GET benchmark with Zipfian distribution
xylem -P profiles/redis-get-zipfian.toml

# Run an HTTP load test
xylem -P profiles/http-spike.toml

# Run a Memcached benchmark
xylem -P profiles/memcached-ramp.toml
```

## Customizing the Benchmark

You can override configuration values using the `--set` flag with dot notation:

```bash
# Change target address
./target/release/xylem -P profiles/redis-get-zipfian.toml \
  --set target.address=192.168.1.100:6379

# Change experiment duration
./target/release/xylem -P profiles/redis-get-zipfian.toml \
  --set experiment.duration=120s

# Change multiple parameters
./target/release/xylem -P profiles/redis-get-zipfian.toml \
  --set experiment.duration=60s \
  --set experiment.seed=42 \
  --set workload.keys.n=1000000
```

## Creating a Custom Profile

Create your own TOML profile file:

```toml
# my-benchmark.toml

[experiment]
duration = "30s"
seed = 123

[target]
protocol = "redis"
transport = "tcp"
address = "localhost:6379"

[workload]
[workload.keys]
strategy = "zipfian"
n = 10000
theta = 0.99
value_size = 100

[[traffic_groups]]
name = "main"
protocol = "redis"
threads = [0, 1, 2, 3]
connections_per_thread = 10
max_pending_per_connection = 1

[traffic_groups.sampling_policy]
type = "unlimited"

[traffic_groups.policy]
type = "closed_loop"
```

Run with:
```bash
./target/release/xylem -P my-benchmark.toml
```

## Profile File Structure

A typical profile file includes:

```toml
# Experiment configuration
[experiment]
duration = "60s"
seed = 123

# Target service configuration
[target]
protocol = "redis"
address = "127.0.0.1:6379"

# Workload configuration
[workload]
# ... workload parameters

# Traffic groups (thread assignment and rate control)
[[traffic_groups]]
name = "group-1"
threads = [0, 1, 2, 3]
# ... rate control parameters
```

## Logging

Control logging verbosity:

```bash
# Debug level logging
xylem -P profiles/redis.toml --log-level debug

# Using RUST_LOG environment variable
RUST_LOG=debug xylem -P profiles/redis.toml
```

## Next Steps

- Explore [CLI Reference](../guide/cli-reference.md) for all available options
- Learn about [Configuration](../guide/configuration.md) file format
- Check out example profiles in the `profiles/` directory
