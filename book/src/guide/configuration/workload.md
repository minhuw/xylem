# Workload Configuration

Define workload patterns, duration, rate, and other runtime parameters.

## Basic Structure

```json
{
  "workload": {
    "duration": "60s",
    "rate": 5000,
    "connections": 10,
    "warmup": "5s"
  }
}
```

## Options

### `duration`

**Type:** String (duration)  
**Required:** Yes

How long to run the benchmark. Accepts human-readable durations.

**Examples:**
- `"10s"` - 10 seconds
- `"5m"` - 5 minutes
- `"1h"` - 1 hour

### `rate`

**Type:** Integer  
**Required:** Yes

Target request rate in requests per second.

### `connections`

**Type:** Integer  
**Default:** 1

Number of concurrent connections to maintain.

### `warmup`

**Type:** String (duration)  
**Default:** `"0s"`

Warmup period before collecting statistics. Useful for letting the system reach steady state.

## Advanced Patterns

### Closed-Loop Workload

```json
{
  "workload": {
    "type": "closed-loop",
    "duration": "60s",
    "connections": 50,
    "think_time": "10ms"
  }
}
```

### Open-Loop Workload

```json
{
  "workload": {
    "type": "open-loop",
    "duration": "60s",
    "rate": 10000
  }
}
```

### Distribution Patterns

Specify request distribution patterns:

```json
{
  "workload": {
    "duration": "60s",
    "rate_distribution": {
      "type": "poisson",
      "mean": 5000
    }
  }
}
```

## See Also

- [Transport Configuration](./transport.md)
- [Protocol Configuration](./protocol.md)
