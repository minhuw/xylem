# Basic Usage

This chapter covers the fundamental concepts and usage patterns of Xylem.

## Configuration-First Design

Xylem uses TOML configuration files (called "profiles") to define experiments. This approach ensures reproducibility and simplifies complex workload specifications.

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

## Configuration Overrides

You can override any configuration value using the `--set` flag with dot notation:

```bash
# Override target address
xylem -P profiles/redis.toml --set target.address=192.168.1.100:6379

# Override experiment duration and seed
xylem -P profiles/bench.toml --set experiment.duration=120s --set experiment.seed=12345

# Override workload parameters
xylem -P profiles/redis.toml --set workload.keys.n=1000000

# Override thread assignment
xylem -P profiles/multi.toml --set traffic_groups.0.threads=[0,1,2,3]
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

## Understanding Output

Xylem provides detailed statistics after each run:

### Latency Metrics

- **p50 (median)**: 50% of requests complete within this time
- **p95**: 95% of requests complete within this time
- **p99**: 99% of requests complete within this time
- **p99.9**: 99.9% of requests complete within this time
- **max**: Maximum observed latency

### Throughput Metrics

- **Requests/sec**: Actual throughput achieved
- **Target rate**: Configured request rate
- **Success rate**: Percentage of successful requests

### Connection Metrics

- **Active connections**: Number of concurrent connections
- **Connection errors**: Failed connection attempts
- **Request errors**: Failed requests on established connections

## Common Patterns

### Fixed-Rate Benchmarking

Generate a constant load by configuring rate control in the profile:

```toml
[experiment]
duration = "60s"

[[traffic_groups]]
name = "main"
threads = [0, 1, 2, 3]
rate_control = { type = "closed_loop", concurrency = 100 }
```

### Multi-Protocol Testing

Run multiple protocols simultaneously by defining multiple traffic groups in your profile.

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
- See [Examples](../examples/redis.md) for real-world scenarios
