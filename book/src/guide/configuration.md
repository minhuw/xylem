# Configuration

Xylem uses TOML configuration files (called "profiles") for defining experiments.

## Configuration File Structure

A profile is a TOML document with the following top-level sections:

```toml
[experiment]
name = "redis-benchmark"
duration = "60s"
seed = 123

[[traffic_groups]]
transport = "tcp"
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0, 1, 2, 3]
connections_per_thread = 10

[traffic_groups.protocol_config.keys]
strategy = "sequential"
start = 0
value_size = 64

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "results.json"
```

## Sections

The configuration is divided into several sections:

- **[Protocol Configuration](./configuration/workload.md)** - Define key distributions and operations per traffic group
- **[Transport Configuration](./configuration/transport.md)** - Configure network transport options
- **[Protocol Selection](./configuration/protocol.md)** - Protocol-specific settings
- **[Thresholds and Validity](./thresholds.md)** - `[thresholds]` and `[validity]`: pass/fail limits on the results, checks that the run applied its load, and the exit status

## Basic Example

```toml
[experiment]
name = "redis-benchmark"
duration = "60s"
seed = 42

[[traffic_groups]]
transport = "tcp"
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
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
format = "json"
file = "results.json"
```

## Supported Output and Statistics Options

`output.format` accepts `json`, `html`, `both`, and `human`. JSON includes global
and per-group statistics; set `stats.include_records = true` for raw time-bucket
records. HTML currently supports only `output.html.library = "chartjs"` and
`output.html.theme = "light"`. `output.real_time` must be false: the CLI
rejects this unsupported setting instead of silently ignoring it.

TCP and UDP targets require numeric IP addresses, such as `127.0.0.1:6379` or
`[::1]:6379`. Set transport in each traffic group; there is no global `[target]`
configuration section.

## Unknown and Removed Keys

Every table in a profile rejects keys it does not know. That includes each
traffic group's `protocol_config`, whose valid keys are those of the group's
protocol: `keys` in an `http` group is an error, not ignored. A misspelled or
misplaced key stops Xylem before the run with an error that names the key,
where it is, the keys valid there, and the closest one if it looks like a typo.
Xylem checks every key first and reports all unknown keys together, one per
line:

```text
Error: Failed to parse config file: profile.toml
  Caused by: The profile has 2 errors:
  - unknown field `durationn` in experiment (did you mean `duration`?); expected one of `busy_poll_us`, `description`, `drain_timeout`, `duration`, `name`, `numa_membind`, `seed`, `setup_timeout`
  - unknown field `valu_size` in traffic_groups[0].protocol_config.keys (did you mean `value_size`?); expected one of `n`, `prefix`, `strategy`, `theta`, `value_size`
```

The one table whose keys are not fixed is `[thresholds]`: its keys are
metric names, so this check accepts any key there, and the
[thresholds check](./thresholds.md#metrics) rejects an unknown metric with
the value errors. `[validity]`, a group's `timestamps = { hardware = ... }`
table and every other table take only their own keys.

Then it checks the values, and again reports every problem at once, invalid
thresholds and validity limits included. Keys and
values that older versions accepted say what replaced them: the top-level
`[target]` and `[workload]` sections, `stats.streaming`, a traffic group's
`sampling_rate`, the `adaptive` sampling policy, the `redis-cluster` protocol,
and Redis `protocol_config.data_import`.

## Configuration Overrides

You can override any configuration value using the `--set` flag with dot notation:

```bash
# Override target address for a traffic group
xylem -P profile.toml --set traffic_groups.0.target=192.168.1.100:6379

# Override experiment duration
xylem -P profile.toml --set experiment.duration=120s
```

Each override's path must exist in the profile format: a key that does not
belong where the path puts it, or an index past the end of an array, fails
with an error that quotes the override and suggests the closest key. A key
the profile leaves unset, such as `request_dump.directory`, can still be set.
Under `protocol_config` the valid keys are those of the group's protocol, so
switching a group to another protocol may need a new `protocol_config` too,
for example `--set traffic_groups.0.protocol=http --set
traffic_groups.0.protocol_config={}`.

## Loading Configuration

Use the `-P` or `--profile` flag to load a profile file:

```bash
xylem -P tests/redis/redis-get-zipfian.toml
```

## See Also

- [Protocol Configuration](./configuration/workload.md)
- [Transport Configuration](./configuration/transport.md)
- [Protocol Selection](./configuration/protocol.md)
- [Thresholds and Validity](./thresholds.md)
- [Configuration Schema](../reference/schema.md)
