# Configuration

Xylem uses TOML configuration files (called "profiles") for defining experiments.

## Configuration File Structure

A profile is a TOML document with the following top-level sections:

```toml
[experiment]
duration = "60s"
seed = 123

[target]
protocol = "redis"
address = "127.0.0.1:6379"

[workload]
# Workload parameters

[[traffic_groups]]
name = "main"
threads = [0, 1, 2, 3]
# Rate control parameters
```

## Sections

The configuration is divided into several sections:

- **[Workload Configuration](./configuration/workload.md)** - Define workload patterns, duration, and key distributions
- **[Transport Configuration](./configuration/transport.md)** - Configure network transport options
- **[Protocol Configuration](./configuration/protocol.md)** - Protocol-specific settings

## Basic Example

```toml
[experiment]
duration = "60s"
seed = 42

[target]
protocol = "redis"
address = "localhost:6379"

[workload]
keys = { type = "zipfian", n = 10000, s = 0.99 }
value_size = 100

[[traffic_groups]]
name = "group-1"
threads = [0, 1, 2, 3]
rate_control = { type = "closed_loop", concurrency = 50 }

[output]
format = "json"
file = "results.json"
```

## Configuration Overrides

You can override any configuration value using the `--set` flag with dot notation:

```bash
# Override target address
xylem -P profile.toml --set target.address=192.168.1.100:6379

# Override experiment duration
xylem -P profile.toml --set experiment.duration=120s

# Override multiple parameters
xylem -P profile.toml --set experiment.duration=60s --set experiment.seed=999
```

## Loading Configuration

Use the `-P` or `--profile` flag to load a profile file:

```bash
xylem -P profiles/redis-get-zipfian.toml
```

## See Also

- [Workload Configuration](./configuration/workload.md)
- [Transport Configuration](./configuration/transport.md)
- [Protocol Configuration](./configuration/protocol.md)
- [Configuration Schema](../reference/schema.md)
