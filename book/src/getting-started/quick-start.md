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

First, ensure you have a Redis server running. You can use Docker for easy setup:

```bash
docker run -d -p 6379:6379 redis:latest
```

Or use Docker Compose with the provided test configuration:

```bash
docker compose -f tests/redis/docker-compose.yml up -d
```

By default, Redis listens on `localhost:6379`.

### 2. Run the Benchmark

Use one of the included example profiles:

```bash
./target/release/xylem -P tests/redis/redis-get-zipfian.toml
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
xylem -P tests/redis/redis-get-zipfian.toml

# Run an HTTP load test
xylem -P profiles/http-spike.toml

# Run a Memcached benchmark
xylem -P profiles/memcached-ramp.toml
```

## Customizing the Benchmark

You can override configuration values using the `--set` flag with dot notation:

```bash
# Change target address for the first traffic group
./target/release/xylem -P tests/redis/redis-get-zipfian.toml \
  --set traffic_groups.0.target=192.168.1.100:6379

# Change experiment duration
./target/release/xylem -P tests/redis/redis-get-zipfian.toml \
  --set experiment.duration=120s
```

## Creating a Custom Profile

Create your own TOML profile file:

```toml
# my-benchmark.toml

[experiment]
duration = "30s"
seed = 123

[target]
transport = "tcp"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = "localhost:6379"
threads = [0, 1, 2, 3]
connections_per_thread = 10
max_pending_per_connection = 1

[traffic_groups.protocol_config.keys]
strategy = "zipfian"
n = 10000
theta = 0.99
value_size = 100

[traffic_groups.protocol_config.operations]
strategy = "fixed"
operation = "get"

[traffic_groups.sampling_policy]
type = "unlimited"

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "human"
file = "results.json"
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

# Global target configuration (transport only)
[target]
transport = "tcp"

# Traffic groups (each with its own protocol, target, and rate control)
[[traffic_groups]]
name = "group-1"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0, 1, 2, 3]
connections_per_thread = 10
max_pending_per_connection = 1

[traffic_groups.protocol_config.keys]
strategy = "zipfian"
n = 1000000
theta = 0.99
value_size = 64

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "results.json"
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
