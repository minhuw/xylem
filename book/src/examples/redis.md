# Redis Benchmarking

Complete examples for benchmarking Redis with Xylem.

## Basic GET Benchmark

Measure Redis GET performance:

```bash
xylem --protocol redis \
      --transport tcp \
      --host localhost \
      --port 6379 \
      --duration 60s \
      --rate 10000
```

## Configuration File

`redis-get.json`:
```json
{
  "protocol": "redis",
  "transport": {
    "type": "tcp",
    "host": "localhost",
    "port": 6379,
    "nodelay": true
  },
  "workload": {
    "duration": "60s",
    "rate": 10000,
    "connections": 10
  },
  "protocol_config": {
    "operation": "get",
    "key_pattern": "user:{id}"
  },
  "output": {
    "format": "json",
    "file": "redis-get-results.json"
  }
}
```

Run with:
```bash
xylem --config redis-get.json
```

## SET Benchmark

Test write performance:

```json
{
  "protocol": "redis",
  "protocol_config": {
    "operation": "set",
    "key_pattern": "cache:item:{id}",
    "value_size": 1024
  },
  "workload": {
    "duration": "30s",
    "rate": 5000
  }
}
```

## Pipelined Requests

Use pipelining for higher throughput:

```json
{
  "protocol": "redis",
  "protocol_config": {
    "operation": "get",
    "key_pattern": "key:*",
    "pipeline": 100
  },
  "workload": {
    "duration": "60s",
    "rate": 50000
  }
}
```

## Mixed Workload

Combine GET and SET operations:

```json
{
  "protocol": "redis",
  "protocol_config": {
    "operations": [
      {"type": "get", "weight": 80},
      {"type": "set", "weight": 20}
    ],
    "key_pattern": "user:*"
  }
}
```

## Over Unix Socket

For lowest latency, use Unix sockets:

```json
{
  "protocol": "redis",
  "transport": {
    "type": "unix",
    "path": "/var/run/redis/redis.sock"
  },
  "workload": {
    "duration": "60s",
    "rate": 50000
  }
}
```

## With TLS

Benchmark Redis with TLS:

```json
{
  "protocol": "redis",
  "transport": {
    "type": "tls",
    "host": "redis.example.com",
    "port": 6380,
    "ca_cert": "/etc/ssl/ca.pem"
  }
}
```

## Latency Testing

Focus on tail latencies:

```json
{
  "protocol": "redis",
  "workload": {
    "duration": "300s",
    "rate": 1000,
    "connections": 1
  },
  "statistics": {
    "sketch": "hdr",
    "percentiles": [50, 95, 99, 99.9, 99.99]
  }
}
```

## Interpreting Results

Example output:
```
Latency Statistics:
  p50:  0.45 ms  <- Half of requests complete in < 0.45ms
  p95:  1.20 ms  <- 95% complete in < 1.20ms
  p99:  2.30 ms  <- 99% complete in < 2.30ms
  p99.9: 5.10 ms <- Tail latency

Throughput: 9,987 req/s
Success Rate: 100.00%
```

## See Also

- [Redis Protocol](../guide/protocols/redis.md)
- [Configuration](../guide/configuration.md)
