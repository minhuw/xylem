# Configuration Schema

Xylem uses TOML configuration files to define experiments. This reference documents the complete schema for experiment profiles.

## Overview

A complete Xylem configuration file contains the following sections:

```toml
[experiment]       # Experiment metadata and duration
[target]           # Global transport configuration
[[traffic_groups]] # One or more traffic generation groups with protocol_config
[output]           # Output format and destination
```

## Experiment Section

The `[experiment]` section defines metadata and experiment-wide settings.

```toml
[experiment]
name = "redis-bench"
description = "Redis benchmark with latency/throughput agent separation"
duration = "30s"
seed = 42
```

### Fields

- `name` (string, required): Name of the experiment
- `description` (string, optional): Description of the experiment
- `duration` (string, required): Duration in format "Ns", "Nm", "Nh" (seconds, minutes, hours)
- `seed` (integer, optional): Random seed for reproducibility

## Target Section

The `[target]` section specifies global transport settings shared across all traffic groups.

```toml
[target]
transport = "tcp"
```

### Fields

- `transport` (string, required): Transport layer. Supported values:
  - `"tcp"`
  - `"udp"`
  - `"unix"`

Note: The `protocol` and target `address` are now specified per-traffic-group, not globally.

## Traffic Groups

Traffic groups define how workload is distributed across threads and connections. Each group has its own protocol, target, and configuration. You can define multiple `[[traffic_groups]]` sections.

```toml
[[traffic_groups]]
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0, 1, 2, 3]
connections_per_thread = 25
max_pending_per_connection = 10

[traffic_groups.protocol_config.keys]
strategy = "zipfian"
n = 1000000
theta = 0.99
value_size = 64

[traffic_groups.protocol_config.operations]
strategy = "fixed"
operation = "get"

[traffic_groups.traffic_policy]
type = "fixed-rate"
rate = 100.0

[traffic_groups.sampling_policy]
type = "unlimited"
```

### Traffic Group Fields

- `name` (string, required): Name for this traffic group
- `protocol` (string, required): Application protocol for this group. Supported values:
  - `"redis"`
  - `"redis-cluster"`
  - `"http"`
  - `"memcached-binary"`
  - `"memcached-ascii"`
  - `"xylem-echo"` (testing only)
- `target` (string, required): Target address for this group. Format depends on transport:
  - TCP/UDP: `"host:port"` or `"ip:port"`
  - Unix socket: `"/path/to/socket"`
- `threads` (array of integers, required): Thread IDs to use for this group
- `connections_per_thread` (integer, required): Number of connections per thread
- `max_pending_per_connection` (integer, required): Maximum pending requests per connection
  - `1` = no pipelining (accurate latency measurement)
  - Higher values = pipelining enabled

### Protocol Configuration

**`[traffic_groups.protocol_config.keys]`** - Key distribution for KV protocols:

- `strategy` (string, required): Distribution strategy
  - `"sequential"`: Sequential keys starting from `start`
  - `"random"`: Uniform random distribution
  - `"round-robin"`: Cycle through keys
  - `"zipfian"`: Zipfian distribution (power law, hot keys)
  - `"gaussian"`: Normal distribution (temporal locality)
- `n` (integer, required for zipfian): Key space size
- `max` (integer, required for random/round-robin): Maximum key value
- `start` (integer, optional for sequential): Starting key (default: 0)
- `theta` (float, required for zipfian): Skew parameter (0.99 = high skew)
- `mean_pct` (float, required for gaussian): Mean as percentage of keyspace (0.0-1.0)
- `std_dev_pct` (float, required for gaussian): Standard deviation as percentage (0.0-1.0)
- `value_size` (integer, required): Size of values in bytes

**`[traffic_groups.protocol_config.operations]`** - Operation configuration:

Fixed operation:
```toml
[traffic_groups.protocol_config.operations]
strategy = "fixed"
operation = "get"  # or "set", "incr"
```

Weighted operations:
```toml
[traffic_groups.protocol_config.operations]
strategy = "weighted"

[[traffic_groups.protocol_config.operations.commands]]
name = "get"
weight = 0.8

[[traffic_groups.protocol_config.operations.commands]]
name = "set"
weight = 0.2
```

**`[traffic_groups.protocol_config.value_size]`** - Optional value size configuration:

```toml
[traffic_groups.protocol_config.value_size]
strategy = "fixed"
size = 128
```

Or variable sizes:
```toml
[traffic_groups.protocol_config.value_size]
strategy = "uniform"
min = 64
max = 4096
```

### Traffic Policy

**`[traffic_groups.traffic_policy]`** - Rate control:

- `type` (string, required):
  - `"closed-loop"`: Send as fast as possible (max throughput)
  - `"fixed-rate"`: Fixed rate per connection
  - `"poisson"`: Poisson arrival process (open-loop)
- `rate` (float, required for fixed-rate/poisson): Requests per second per connection

```toml
[traffic_groups.traffic_policy]
type = "closed-loop"
```

```toml
[traffic_groups.traffic_policy]
type = "fixed-rate"
rate = 100.0  # 100 req/s per connection
```

```toml
[traffic_groups.traffic_policy]
type = "poisson"
rate = 50.0  # Mean 50 req/s per connection
```

### Sampling Policy

**`[traffic_groups.sampling_policy]`** - Latency sampling:

- `type` (string, required):
  - `"unlimited"`: Sample every request (100% sampling)
  - `"limited"`: Sample a fraction of requests
- `rate` (float, required for limited): Sampling rate (0.0 to 1.0)
- `max_samples` (integer, optional): Maximum samples to store

```toml
[traffic_groups.sampling_policy]
type = "unlimited"
```

```toml
[traffic_groups.sampling_policy]
type = "limited"
rate = 0.01      # 1% sampling
max_samples = 10000
```

## Output Section

The `[output]` section configures where and how results are written.

```toml
[output]
format = "json"
file = "/tmp/results.json"
real_time = false
```

### Fields

- `format` (string, required): Output format
  - `"json"`: Simple JSON format
  - `"human"`: Human-readable console output
  - `"detailed-json"`: Detailed JSON with per-group statistics
  - `"html"`: HTML report with charts
  - `"both"`: Both JSON and HTML
- `file` (string, required): Output file path
- `real_time` (boolean, optional): Print real-time updates during experiment

## Complete Example

```toml
[experiment]
name = "redis-bench"
description = "Redis benchmark with latency/throughput separation"
duration = "30s"
seed = 42

[target]
transport = "tcp"

# Latency agent - accurate latency measurement
[[traffic_groups]]
name = "latency-agent"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0]
connections_per_thread = 10
max_pending_per_connection = 1

[traffic_groups.protocol_config.keys]
strategy = "zipfian"
n = 1000000
theta = 0.99
value_size = 64

[traffic_groups.protocol_config.operations]
strategy = "fixed"
operation = "get"

[traffic_groups.traffic_policy]
type = "poisson"
rate = 100.0

[traffic_groups.sampling_policy]
type = "unlimited"

# Throughput agent - high throughput generation
[[traffic_groups]]
name = "throughput-agent"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [1, 2, 3]
connections_per_thread = 25
max_pending_per_connection = 32

[traffic_groups.protocol_config.keys]
strategy = "zipfian"
n = 1000000
theta = 0.99
value_size = 64

[traffic_groups.protocol_config.operations]
strategy = "weighted"

[[traffic_groups.protocol_config.operations.commands]]
name = "get"
weight = 0.8

[[traffic_groups.protocol_config.operations.commands]]
name = "set"
weight = 0.2

[traffic_groups.traffic_policy]
type = "closed-loop"

[traffic_groups.sampling_policy]
type = "limited"
rate = 0.01
max_samples = 10000

[output]
format = "json"
file = "/tmp/redis-bench-results.json"
real_time = true
```

## See Also

- [Configuration Guide](../guide/configuration.md)
- [Protocol Configuration](../guide/configuration/workload.md)
- [CLI Reference](../guide/cli-reference.md)
- [Example Profiles](https://github.com/minhuw/xylem/tree/main/profiles)
