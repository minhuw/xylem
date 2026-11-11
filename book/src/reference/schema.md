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
[thresholds]       # Optional pass/fail limits on the results
[validity]         # Optional checks that the run applied its load
```

Every section, and every table inside one, rejects unknown keys, except that
the keys of `[thresholds]` are metric names; see
[unknown and removed keys](../guide/configuration.md#unknown-and-removed-keys).

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
- `seed` (integer, optional): Random seed for reproducibility. Each traffic
  group's protocol on each thread draws its keys, values and sizes from its
  own seed derived from this one, so threads do not repeat each other's keys,
  and each connection of a `poisson` group draws its arrivals from its own
  derived seed. Without a seed, both come from OS entropy. Earlier versions
  gave every thread this seed itself, so a given seed now generates different
  keys than it did there.
- `drain_timeout` (string, default `"1s"`): after the measurement window, how
  long to wait for responses that are still pending. Nothing is sent during the
  drain. Requests still pending when it ends count as `timed_out`; see
  [run phases](../guide/output-formats.md#run-phases).

## Transport and Target

Each traffic group specifies its own `transport` (`"tcp"`, `"udp"`, `"unix"`, or
`"ntcp"` in builds with the `ntcp` feature)
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
- `transport` (string, default `"tcp"`): `"tcp"`, `"udp"`, `"unix"`, or `"ntcp"`
  (user-space TCP, needs the top-level `[ntcp]` section)
- `timestamps` (default `"user"`): timestamp source, `"user"`, `"kernel"`,
  `{ hardware = "<interface>" }` (`tcp` and `udp` on Linux only), or `"nic"`
  (`ntcp` with the DPDK backend only); the same for groups sharing a thread. See
  [timestamps](../guide/configuration/transport.md#timestamps).
- `threads` (array of integers, required): Thread IDs to use for this group
- `connections_per_thread` (integer, required): Number of connections per thread
- `max_pending_per_connection` (integer, required): Maximum pending requests per connection
  - `1` = no pipelining (accurate latency measurement)
  - Higher values = pipelining enabled
- `on_error` (string, default `"count"`): what an error reply (a Redis error,
  an HTTP 4xx or 5xx, a memcached `SERVER_ERROR`, ...) does
  - `"count"` = it completes its request and counts in `requests.errors`; see
    [error replies](../guide/output-formats.md#error-replies)
  - `"abort"` = the run fails at the first one, naming the group, connection
    and reply; no results are written

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

**Redis connection setup** - Optional fields directly in `[traffic_groups.protocol_config]`, sent once by every connection before any other request:

- `auth` (table, optional): Authenticate with `AUTH`. Set exactly one of `password` and `password_env`.
  - `username` (string, optional): ACL user (Redis 6+); without it, `AUTH <password>` checks `requirepass`
  - `password` (string): Password
  - `password_env` (string): Environment variable holding the password, read when the run starts; an unset variable fails validation
- `db` (integer, optional): Database to `SELECT`, 0 to 2147483647 (default: none, so database 0)
- `resp` (integer, optional): `2` (default, no HELLO) or `3` (sends `HELLO 3`)

```toml
[traffic_groups.protocol_config]
auth = { username = "bench", password_env = "REDIS_PASSWORD" }
db = 3
resp = 3
```

Setup requests are not measured, counted or dumped, and an error reply to one fails the run. See [Redis connection setup](../guide/protocols/redis.md#connection-setup-auth-select-and-hello).

### Traffic Policy

**`[traffic_groups.traffic_policy]`** - Rate control:

- `type` (string, required):
  - `"closed-loop"`: Send as fast as possible (max throughput)
  - `"fixed-rate"`: Evenly spaced sends at a fixed rate
  - `"poisson"`: Poisson arrival process (open-loop)
- `rate` (float, required for fixed-rate/poisson): Requests per second of the
  whole group, across all its threads and connections

Every policy except `closed-loop` is rate-driven, including `adaptive` and the
time-varying `sinusoidal`, `ramp`, `spike` and `sawtooth` patterns. Each
connection of a rate-driven group follows an *intended schedule*: every
request is due one inter-arrival time after the previous request's intended
time, not after its actual send.

The rates are the group's, shared by its `N` connections
(`connections_per_thread` times the length of `threads`), numbered
`c = 0 .. N-1` across its threads. Every connection of every thread and group
counts its schedule from one start time that all threads share, set once every
thread has connected. `fixed-rate` interleaves the connections into one stream:
connection `c` sends its `k`-th request `(c + k·N)/rate` seconds after the
start (to the nanosecond, rounded down, without accumulating error), so the
group sends one request every `1/rate` seconds and no two connections share a
send time. A `poisson` connection is a Poisson process at `rate/N`, its first
arrival one exponential inter-arrival after the start. `adaptive` and the
patterns run each connection at `1/N` of the configured rates, connection `c`
starting `c/N` of a request into its schedule, and a pattern's time counts from
the start. A connection that can first send after the start, because it was
still connecting or setting up, begins at its first intended time from then
on; the sends it missed are not counted as `unsent`.
Inter-arrival times are fixed (`fixed-rate`), exponential samples (`poisson`),
one interval at the current rate (`adaptive`), or, for patterns, the time until
the pattern's rate, held constant over 10 ms steps, accumulates one request.
A pattern that drops to zero schedules nothing, so a pause leaves no backlog.

A connection sends when its next intended time has arrived, its pipeline has
room and its protocol is ready. When the sender falls behind (a full pipeline,
a slow server, preemption), the overdue requests go out back to back as soon as
it can send again; none is skipped. A request's latency is measured from its
intended send time: the *send lag* (actual minus intended send time) plus the
service latency from the send to the response. Without this, the delay of
requests that could not be sent on time would be omitted from the results
(coordinated omission). `adaptive` adjusts its rate from these latencies, so a
growing backlog backs its rate off. `closed-loop` requests have no intended time
and no send lag. See [request accounting](../guide/output-formats.md#request-accounting).

```toml
[traffic_groups.traffic_policy]
type = "closed-loop"
```

```toml
[traffic_groups.traffic_policy]
type = "fixed-rate"
rate = 100.0  # 100 req/s for the group, evenly spaced
```

```toml
[traffic_groups.traffic_policy]
type = "poisson"
rate = 50.0  # Mean 50 req/s for the group
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
    mergeable quantile sketches. T-digest `compression` must be a positive
    integer; it defaults to 1000. Higher values use more memory and generally
    improve accuracy. This requirement applies to both group and global policies.
  - `"none"`: Disable latency sampling (throughput/bytes still recorded)

For every type except `none`, the mean, min, max, standard deviation and
sample count are computed exactly from every recorded latency; only the
percentiles depend on the storage type.

DDSketch 0.4.2 uses floor-based value-to-bin mapping and corresponding bin
representatives; 0.3.x used ceiling-based mapping. Approximate percentiles,
and therefore percentile threshold results, can change for the same samples
and `alpha`. `max_bins` limits the number of logarithmic bins, not a numeric
latency range. When the observed values require more than that many bins, the
lowest bins collapse and their estimates can exceed the configured relative
error. Xylem places values up to and including 1 ns in the zero bucket and
truncates estimated values to whole nanoseconds; these can also prevent a
strict relative error bound, especially near zero.

The dependency's public count saturates at `usize::MAX` on 32-bit targets;
Xylem's independently tracked `u64` sample count preserves the recorded count.

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

## Thresholds and Validity

`[thresholds]` maps a metric, optionally filtered to a traffic group, to a
list of expressions; `[validity]` tunes the checks that a run applied its
configured load:

```toml
[thresholds]
"latency" = ["p99 < 250us", "p99.9 < 1ms"]
"latency{group=latency-agent}" = ["p50 < 80us"]
"requests.timed_out" = ["rate < 0.001"]
"requests.errors" = ["rate < 0.01"]

[validity]
enabled = true
max_unsent_ratio = 0.001
max_late_send_ratio = 0.01
skip = []
```

- `thresholds` (table of string lists, default empty): see
  [Thresholds and Validity](../guide/thresholds.md) for metrics (including
  `requests.errors`, `error_latency` and the `latency_breakdown` parts),
  aggregations, units and evaluation. Its keys are checked as metrics, not as
  profile keys.
- `validity.enabled` (bool, default true): false disables every check.
- `validity.max_unsent_ratio` (number from 0 to 1, default 0.001)
- `validity.max_late_send_ratio` (number from 0 to 1, default 0.01)
- `validity.skip` (list of `"unsent"`, `"late_sends"`, default empty)

Other `[validity]` keys are rejected.

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
- [Thresholds and Validity](../guide/thresholds.md)
- [Protocol Configuration](../guide/configuration/workload.md)
- [CLI Reference](../guide/cli-reference.md)
- [Example Profiles](https://github.com/minhuw/xylem/tree/main/profiles)
