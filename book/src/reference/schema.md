# Configuration Schema

Xylem uses TOML configuration files to define experiments. This reference documents the complete schema for experiment profiles.

## Overview

A complete Xylem configuration file contains the following sections:

```text
[experiment]       # Experiment metadata and duration
[[traffic_groups]] # One or more traffic generation groups with protocol_config
[output]           # Output format and destination
[stats]            # Optional statistics settings
[request_dump]     # Optional request CSV dump settings
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

## Transport and Target

Each traffic group specifies its own `transport` (`"tcp"`, `"udp"`, or `"unix"`)
and `target`. Transport defaults to `"tcp"`; there is no global `[target]` section.
Groups sharing a thread must use the same transport. TCP and UDP targets must be
numeric socket addresses, such as `"127.0.0.1:6379"` or `"[::1]:6379"`; hostname
resolution is not supported. Unix targets are filesystem paths.

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
  - `"http"`
  - `"memcached-binary"`
  - `"memcached-ascii"`
  - `"xylem-echo"` (testing only)
- `target` (string, required): Target address for this group. Format depends on transport:
  - TCP/UDP: numeric `"ip:port"` or `"[ipv6]:port"`
  - Unix socket: `"/path/to/socket"`
- `transport` (string, default `"tcp"`): `"tcp"`, `"udp"`, or `"unix"`
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

**`[traffic_groups.sampling_policy]`** - Latency storage. If the section is
omitted, the group uses an auto-resizing HDR histogram with 3 significant
figures, which records every latency and merges independently of order.

- `type` (string, required):
  - `"hdr-histogram"` (default): HDR histogram of every latency.
    - `sigfigs` (integer 1-5, default 3): precision of each value.
    - `max_value_us` (integer, optional): upper bound in microseconds. Unset,
      the histogram grows to fit every value. Set, larger values are recorded
      as the bound and counted in the `clamped_samples` output field.
  - `"unlimited"`: store every latency; exact nearest-rank percentiles, memory
    grows with the request count.
  - `"limited"`: keep a uniform random sample (reservoir) of at most
    `max_samples` latencies per bucket and connection. Merging weights each
    part by the number of latencies it saw, so percentiles describe the whole
    run, not its first requests.
    - `rate` (float, default 1.0): fraction of latencies offered to the sample.
    - `max_samples` (integer, default 131072).
  - `"dd-sketch"` (`alpha`, `max_bins`) and `"t-digest"` (`compression`):
    mergeable quantile sketches.
  - `"none"`: Disable latency sampling (throughput/bytes still recorded)

For every type except `none`, the mean, min, max, standard deviation and
sample count are computed exactly from every recorded latency; only the
percentiles depend on the storage type.

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

```toml
# Disable latency sampling for this group
[traffic_groups.sampling_policy]
type = "none"
```

### Stats Config

Top-level stats configuration (`[stats]`).

- `bucket_duration` (string, default `"1s"`): time bucket for per-connection records
- `include_records` (bool, default false)
- `sampling_policy` (optional): global latency sampling policy; set to `type = "none"` to disable global latency recording; if omitted, the first traffic group’s policy is used

Other keys are rejected. The former `streaming` (Parquet) section was removed, so profiles that still set it fail to load.

## Output Section

The `[output]` section configures where and how results are written.

```toml
[output]
format = "json"
file = "/tmp/results.json"
real_time = false
```

### Fields

- `format` (string, default `"json"`): Output format
  - `"json"`: Detailed JSON with experiment metadata, global and per-group statistics
  - `"human"`: Human-readable console output
  - `"html"`: HTML report with charts
  - `"both"`: Both JSON and HTML
- `file` (string, required): Output file path
- `real_time` (boolean, default false): Must be false; real-time updates are not supported.
- `html.library` (string, default `"chartjs"`): Only `"chartjs"` is supported.
- `html.theme` (string, default `"light"`): Only `"light"` is supported.

## Request Dump

Use `[request_dump]` to record outgoing requests as CSV files:

```toml
[request_dump]
directory = "/tmp/xylem-requests"
prefix = "requests"
rotation = "hourly"
encoding = "hex"
```

The directory is required. Rotation defaults to `"never"`; other values are
`"minutely"`, `"hourly"`, `"daily"`, or `{ size = 1048576 }` for a byte limit.
Encoding defaults to `"hex"`; `"base64"`, `"utf8"`, `"escaped"`, and `"none"` are
also available. `max_files` optionally limits retained dump files.

## Generated JSON Schema

The checked-in [profile schema](https://github.com/minhuw/xylem/blob/main/schema/profile.schema.json)
is generated directly from the Rust configuration types. Regenerate it with:

```bash
cargo run --quiet --bin xylem -- schema > schema/profile.schema.json
```

Keep stdout separate from Cargo diagnostics on stderr. CI checks JSON validity
and compares the committed schema to the command's output. The schema describes
the configuration's shape; runtime validation also checks supported options,
addresses, and protocol settings. `detailed-json` is obsolete; use `json`.

## Complete Example

```toml
[experiment]
name = "redis-bench"
description = "Redis benchmark with latency/throughput separation"
duration = "30s"
seed = 42

# Latency agent - accurate latency measurement
[[traffic_groups]]
transport = "tcp"
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
real_time = false
```

## See Also

- [Configuration Guide](../guide/configuration.md)
- [Protocol Configuration](../guide/configuration/workload.md)
- [CLI Reference](../guide/cli-reference.md)
- [Example Profiles](https://github.com/minhuw/xylem/tree/main/profiles)
