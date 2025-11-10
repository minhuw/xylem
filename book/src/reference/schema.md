# Configuration Schema

Xylem uses TOML configuration files to define experiments. This reference documents the complete schema for experiment profiles.

## Overview

A complete Xylem configuration file contains the following sections:

```toml
[experiment]      # Experiment metadata and duration
[target]          # Target service address, protocol, and transport
[workload]        # Workload generation parameters
[[traffic_groups]] # One or more traffic generation groups
[output]          # Output format and destination
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

The `[target]` section specifies the target service to benchmark.

```toml
[target]
address = "127.0.0.1:6379"
protocol = "redis"
transport = "tcp"
```

### Fields

- `address` (string, required): Target address. Format depends on transport:
  - TCP/UDP: `"host:port"` or `"ip:port"`
  - Unix socket: `"/path/to/socket"`
- `protocol` (string, required): Application protocol. Supported values:
  - `"redis"`
  - `"http"`
  - `"memcached-binary"`
  - `"memcached-ascii"`
  - `"masstree"`
  - `"xylem-echo"` (testing only)
- `transport` (string, required): Transport layer. Supported values:
  - `"tcp"`
  - `"udp"`
  - `"unix"`

## Workload Section

The `[workload]` section defines the workload pattern and key distribution.

```toml
[workload]

[workload.keys]
strategy = "zipfian"
n = 1000000
theta = 0.99
value_size = 64

[workload.pattern]
type = "constant"
rate = 50000.0
```

### Fields

**`[workload.keys]`** - Key distribution parameters:
- `strategy` (string, required): Distribution strategy
  - `"uniform"`: Uniform distribution
  - `"zipfian"`: Zipfian distribution (power law)
- `n` (integer, required): Key space size (total number of keys)
- `theta` (float, required for zipfian): Skew parameter (0.0 to 1.0)
  - 0.99 = high skew (typical for caches)
  - 0.5 = moderate skew
- `value_size` (integer, required): Size of values in bytes

**`[workload.pattern]`** - Traffic pattern:
- `type` (string, required): Pattern type
  - `"constant"`: Constant rate
  - (other types may be supported)
- `rate` (float, required): Target request rate in requests/second

## Traffic Groups

Traffic groups define how workload is distributed across threads and connections. You can define multiple `[[traffic_groups]]` sections.

```toml
[[traffic_groups]]
name = "latency-agent"
protocol = "redis"
threads = [0]
connections_per_thread = 10
max_pending_per_connection = 1

[traffic_groups.sampling_policy]
type = "unlimited"

[traffic_groups.policy]
type = "poisson"
rate = 100.0
```

### Fields

- `name` (string, required): Name for this traffic group
- `protocol` (string, optional): Override protocol for this group (defaults to `[target]` protocol)
- `threads` (array of integers, required): Thread IDs to use for this group
- `connections_per_thread` (integer, required): Number of connections per thread
- `max_pending_per_connection` (integer, required): Maximum pending requests per connection
  - `1` = no pipelining (accurate latency measurement)
  - Higher values = pipelining enabled

### Sampling Policy

**`[traffic_groups.sampling_policy]`**:
- `type` (string, required):
  - `"unlimited"`: Sample every request (100% sampling)
  - `"limited"`: Sample a fraction of requests
- `rate` (float, required for limited): Sampling rate (0.0 to 1.0)
  - `0.01` = 1% sampling
  - `0.1` = 10% sampling

### Traffic Policy

**`[traffic_groups.policy]`**:
- `type` (string, required):
  - `"poisson"`: Poisson arrival process (open-loop)
  - `"closed-loop"`: Closed-loop (send as fast as possible)
- `rate` (float, required for poisson): Request rate per connection in requests/second

## Output Section

The `[output]` section configures where and how results are written.

```toml
[output]
format = "json"
file = "/tmp/results.json"
```

### Fields

- `format` (string, required): Output format
  - `"json"`: JSON format
  - (other formats may be supported)
- `file` (string, required): Output file path

## Complete Example

```toml
[experiment]
name = "redis-bench"
description = "Redis benchmark"
duration = "30s"
seed = 42

[target]
address = "127.0.0.1:6379"
protocol = "redis"
transport = "tcp"

[workload.keys]
strategy = "zipfian"
n = 1000000
theta = 0.99
value_size = 64

[workload.pattern]
type = "constant"
rate = 50000.0

[[traffic_groups]]
name = "latency-agent"
protocol = "redis"
threads = [0]
connections_per_thread = 10
max_pending_per_connection = 1

[traffic_groups.sampling_policy]
type = "unlimited"

[traffic_groups.policy]
type = "poisson"
rate = 100.0

[[traffic_groups]]
name = "throughput-agent"
protocol = "redis"
threads = [1, 2, 3]
connections_per_thread = 25
max_pending_per_connection = 32

[traffic_groups.sampling_policy]
type = "limited"
rate = 0.01

[traffic_groups.policy]
type = "closed-loop"

[output]
format = "json"
file = "/tmp/redis-bench-results.json"
```

## See Also

- [Configuration Guide](../guide/configuration.md)
- [CLI Reference](../guide/cli-reference.md)
- [Example Profiles](https://github.com/minhuw/xylem/tree/main/profiles)
