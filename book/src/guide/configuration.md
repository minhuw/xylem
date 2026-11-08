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
rejects this unsupported setting instead of silently ignoring it. The `[stats]`
section rejects unknown keys, including the removed `stats.streaming`.

TCP and UDP targets require numeric IP addresses, such as `127.0.0.1:6379` or
`[::1]:6379`. Set transport in each traffic group; there is no global `[target]`
configuration section.

## Configuration Overrides

You can override any configuration value using the `--set` flag with dot notation:

```bash
# Override target address for a traffic group
xylem -P profile.toml --set traffic_groups.0.target=192.168.1.100:6379

# Override experiment duration
xylem -P profile.toml --set experiment.duration=120s
```

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
