# Protocol Configuration

Protocol configuration in Xylem is defined per-traffic-group using the `protocol_config` section. This is where you specify how each traffic group generates load: which keys to access, what operations to perform, and how data sizes vary.

## Configuration Structure

All protocol-specific configuration lives under `[[traffic_groups]]` in the `protocol_config` section:

```toml
[[traffic_groups]]
name = "main"
threads = [0, 1, 2, 3]
connections_per_thread = 25
max_pending_per_connection = 10

[traffic_groups.protocol_config.keys]
strategy = "zipfian"
n = 1000000
theta = 0.99
value_size = 128

[traffic_groups.protocol_config.operations]
strategy = "weighted"

[[traffic_groups.protocol_config.operations.commands]]
name = "get"
weight = 0.8

[[traffic_groups.protocol_config.operations.commands]]
name = "set"
weight = 0.2

[traffic_groups.traffic_policy]
type = "fixed-rate"
rate = 100.0
```

## Key Distribution Strategies

The `keys` section determines which keys your benchmark accesses.

### Sequential Keys

Access keys in order - useful for baselines:

```toml
[traffic_groups.protocol_config.keys]
strategy = "sequential"
start = 0
value_size = 128
```

### Random Keys

Uniform random access - worst case for caching:

```toml
[traffic_groups.protocol_config.keys]
strategy = "random"
max = 1000000
value_size = 128
```

### Round-Robin Keys

Cycle through keys predictably:

```toml
[traffic_groups.protocol_config.keys]
strategy = "round-robin"
max = 100000
value_size = 128
```

### Zipfian Distribution

Model real-world hot key patterns where some keys receive far more traffic:

```toml
[traffic_groups.protocol_config.keys]
strategy = "zipfian"
n = 1000000
theta = 0.99      # Higher = more skewed
value_size = 128
```

At theta=0.99, the top 1% of keys might receive 80% of requests.

### Gaussian Distribution

Model temporal locality where recent data is hot:

```toml
[traffic_groups.protocol_config.keys]
strategy = "gaussian"
mean_pct = 0.5      # Center of hot zone (50% of keyspace)
std_dev_pct = 0.1   # How concentrated (10% of keyspace)
max = 10000
value_size = 128
```

## Operations Configuration

Control what operations your benchmark performs.

### Fixed Operation

Single operation type:

```toml
[traffic_groups.protocol_config.operations]
strategy = "fixed"
operation = "get"
```

### Weighted Operations

Mix operations with specified probabilities:

```toml
[traffic_groups.protocol_config.operations]
strategy = "weighted"

[[traffic_groups.protocol_config.operations.commands]]
name = "get"
weight = 0.7  # 70% reads

[[traffic_groups.protocol_config.operations.commands]]
name = "set"
weight = 0.3  # 30% writes
```

Common patterns:
- **Cache workload**: GET 0.95, SET 0.05
- **Session store**: GET 0.7, SET 0.3
- **Counter system**: GET 0.5, INCR 0.5

## Value Size Configuration

Configure value sizes in the `protocol_config`:

```toml
[traffic_groups.protocol_config.value_size]
strategy = "fixed"
size = 128
```

Or use variable sizes:

```toml
[traffic_groups.protocol_config.value_size]
strategy = "uniform"
min = 64
max = 4096
```

```toml
[traffic_groups.protocol_config.value_size]
strategy = "normal"
mean = 512.0
std_dev = 128.0
min = 64
max = 4096
```

## Complete Example

Here's a complete traffic group configuration for a session store workload:

```toml
[[traffic_groups]]
name = "session-store"
threads = [0, 1, 2, 3]
connections_per_thread = 25
max_pending_per_connection = 10

# Temporal locality - recent sessions are hot
[traffic_groups.protocol_config.keys]
strategy = "gaussian"
mean_pct = 0.6
std_dev_pct = 0.15
max = 10000
value_size = 512

# Read-heavy with significant writes
[traffic_groups.protocol_config.operations]
strategy = "weighted"

[[traffic_groups.protocol_config.operations.commands]]
name = "get"
weight = 0.7

[[traffic_groups.protocol_config.operations.commands]]
name = "set"
weight = 0.3

[traffic_groups.traffic_policy]
type = "fixed-rate"
rate = 100.0  # Per-connection rate

[traffic_groups.sampling_policy]
type = "unlimited"
```

## Multiple Traffic Groups

You can define multiple traffic groups with different configurations:

```toml
# Latency measurement agent - low rate, 100% sampling
[[traffic_groups]]
name = "latency-agent"
threads = [0, 1]
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

# Throughput agent - high rate, sampled
[[traffic_groups]]
name = "throughput-agent"
threads = [2, 3, 4, 5, 6, 7]
connections_per_thread = 25
max_pending_per_connection = 32

[traffic_groups.protocol_config.keys]
strategy = "zipfian"
n = 1000000
theta = 0.99
value_size = 64

[traffic_groups.protocol_config.operations]
strategy = "fixed"
operation = "get"

[traffic_groups.traffic_policy]
type = "closed-loop"

[traffic_groups.sampling_policy]
type = "limited"
rate = 0.01
```

## See Also

- [Redis Protocol](../protocols/redis.md) - Redis-specific operations
- [Traffic Groups](./traffic-groups.md) - Traffic group configuration
- [Configuration Schema](../../reference/schema.md) - Complete reference
