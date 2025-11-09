# Basic Usage

This chapter covers the fundamental concepts and usage patterns of Xylem.

## Command-Line Interface

Xylem provides a comprehensive command-line interface for running benchmarks.

### Basic Syntax

```bash
xylem [OPTIONS] --protocol <PROTOCOL> --transport <TRANSPORT>
```

### Common Options

- `--protocol <PROTOCOL>`: Specify the application protocol (redis, http, memcached)
- `--transport <TRANSPORT>`: Specify the transport layer (tcp, udp, unix, tls)
- `--host <HOST>`: Target host address
- `--port <PORT>`: Target port number
- `--duration <DURATION>`: How long to run the benchmark (e.g., 10s, 5m)
- `--rate <RATE>`: Request rate (requests per second)
- `--config <FILE>`: Path to JSON configuration file

## Configuration Files

For complex workloads, use JSON configuration files:

```json
{
  "protocol": "redis",
  "transport": {
    "type": "tcp",
    "host": "localhost",
    "port": 6379
  },
  "workload": {
    "duration": "60s",
    "rate": 10000,
    "connections": 10
  },
  "output": {
    "format": "json",
    "file": "results.json"
  }
}
```

Run with:

```bash
xylem --config workload.json
```

## Understanding Output

Xylem provides detailed statistics after each run:

### Latency Metrics

- **p50 (median)**: 50% of requests complete within this time
- **p95**: 95% of requests complete within this time
- **p99**: 99% of requests complete within this time
- **p99.9**: 99.9% of requests complete within this time
- **max**: Maximum observed latency

### Throughput Metrics

- **Requests/sec**: Actual throughput achieved
- **Target rate**: Configured request rate
- **Success rate**: Percentage of successful requests

### Connection Metrics

- **Active connections**: Number of concurrent connections
- **Connection errors**: Failed connection attempts
- **Request errors**: Failed requests on established connections

## Common Patterns

### Fixed-Rate Benchmarking

Generate a constant load:

```bash
xylem --protocol redis --rate 5000 --duration 60s
```

### Burst Testing

Test system behavior under sudden load spikes (configure via JSON):

```json
{
  "workload": {
    "pattern": "burst",
    "burst_size": 1000,
    "interval": "5s"
  }
}
```

### Multi-Connection Testing

Test with multiple concurrent connections:

```bash
xylem --protocol redis --connections 50 --rate 10000
```

## Next Steps

- Explore [CLI Reference](../guide/cli-reference.md) for all available options
- Learn about [Configuration](../guide/configuration.md) in detail
- See [Examples](../examples/redis.md) for real-world scenarios
