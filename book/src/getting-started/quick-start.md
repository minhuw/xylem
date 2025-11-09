# Quick Start

This guide will help you run your first benchmark with Xylem in just a few minutes.

## Running a Simple Redis Benchmark

Let's start with a simple Redis benchmark using the default configuration.

### 1. Start a Redis Server

First, ensure you have a Redis server running locally:

```bash
redis-server
```

By default, Redis listens on `localhost:6379`.

### 2. Run the Benchmark

Run Xylem with a simple workload:

```bash
xylem --protocol redis --transport tcp --host localhost --port 6379 --duration 10s --rate 1000
```

This command will:
- Use the Redis protocol
- Connect via TCP
- Target `localhost:6379`
- Run for 10 seconds
- Send requests at 1000 requests per second

### 3. Understanding the Output

Xylem will display statistics about the benchmark, including:
- Latency percentiles (p50, p95, p99, etc.)
- Throughput (requests per second)
- Error rates
- Connection statistics

## Configuration File Example

For more complex workloads, you can use a JSON configuration file:

```json
{
  "protocol": "redis",
  "transport": {
    "type": "tcp",
    "host": "localhost",
    "port": 6379
  },
  "workload": {
    "duration": "30s",
    "rate": 5000,
    "operation": "get",
    "key_pattern": "user:*"
  }
}
```

Save this as `config.json` and run:

```bash
xylem --config config.json
```

## Next Steps

- Learn more about [Basic Usage](./basic-usage.md)
- Explore [Configuration options](../guide/configuration.md)
- See more [Examples](../examples/redis.md)
