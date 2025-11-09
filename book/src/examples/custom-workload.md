# Custom Workload Patterns

Advanced workload patterns for realistic testing scenarios.

## Workload Patterns

### Fixed Rate

Constant request rate:

```json
{
  "workload": {
    "type": "fixed_rate",
    "duration": "60s",
    "rate": 1000
  }
}
```

### Poisson Distribution

Realistic arrival pattern:

```json
{
  "workload": {
    "type": "poisson",
    "duration": "60s",
    "mean_rate": 1000
  }
}
```

### Ramp-Up

Gradually increase load:

```json
{
  "workload": {
    "type": "ramp",
    "duration": "300s",
    "start_rate": 100,
    "end_rate": 5000,
    "step_duration": "30s"
  }
}
```

### Burst Pattern

Periodic load spikes:

```json
{
  "workload": {
    "type": "burst",
    "duration": "300s",
    "base_rate": 1000,
    "burst_rate": 10000,
    "burst_duration": "5s",
    "burst_interval": "60s"
  }
}
```

### Sine Wave

Oscillating load pattern:

```json
{
  "workload": {
    "type": "sine",
    "duration": "600s",
    "min_rate": 500,
    "max_rate": 2000,
    "period": "120s"
  }
}
```

### Trace-Based

Replay from trace file:

```json
{
  "workload": {
    "type": "trace",
    "trace_file": "/path/to/trace.log",
    "time_scale": 1.0
  }
}
```

## Zipfian Distribution

Skewed key access pattern (realistic for caches):

```json
{
  "protocol": "redis",
  "protocol_config": {
    "operation": "get",
    "key_distribution": {
      "type": "zipfian",
      "n": 10000,
      "s": 1.01
    }
  }
}
```

## Think Time

Simulate user think time:

```json
{
  "workload": {
    "type": "closed_loop",
    "connections": 100,
    "think_time": {
      "distribution": "exponential",
      "mean": "100ms"
    }
  }
}
```

## Open vs Closed Loop

### Open Loop

Request rate independent of response time:

```json
{
  "workload": {
    "type": "open_loop",
    "rate": 1000
  }
}
```

### Closed Loop

Fixed number of concurrent requests:

```json
{
  "workload": {
    "type": "closed_loop",
    "connections": 50
  }
}
```

## Coordinated Omission

Avoid coordinated omission by tracking intended vs actual rate:

```json
{
  "workload": {
    "rate": 1000,
    "coordinated_omission_aware": true
  }
}
```

## Warmup Period

Exclude warmup from statistics:

```json
{
  "workload": {
    "duration": "60s",
    "rate": 1000,
    "warmup": "10s"
  }
}
```

## Rate Limiting

Respect rate limits:

```json
{
  "workload": {
    "rate": 1000,
    "rate_limiter": {
      "type": "token_bucket",
      "burst": 100
    }
  }
}
```

## See Also

- [Configuration](../guide/configuration.md)
- [Performance Tuning](../advanced/performance.md)
