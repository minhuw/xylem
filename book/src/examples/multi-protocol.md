# Multi-Protocol Workloads

Run multiple protocols simultaneously to test complex scenarios.

## Overview

Xylem can run multiple protocols concurrently to simulate realistic mixed workloads.

## Configuration

`multi-protocol.json`:
```json
{
  "workloads": [
    {
      "name": "redis-cache",
      "protocol": "redis",
      "transport": {
        "type": "tcp",
        "host": "localhost",
        "port": 6379
      },
      "workload": {
        "duration": "60s",
        "rate": 5000
      },
      "protocol_config": {
        "operation": "get"
      }
    },
    {
      "name": "http-api",
      "protocol": "http",
      "transport": {
        "type": "tcp",
        "host": "localhost",
        "port": 8080
      },
      "workload": {
        "duration": "60s",
        "rate": 1000
      },
      "protocol_config": {
        "method": "GET",
        "path": "/api/users"
      }
    },
    {
      "name": "memcached-session",
      "protocol": "memcached",
      "transport": {
        "type": "tcp",
        "host": "localhost",
        "port": 11211
      },
      "workload": {
        "duration": "60s",
        "rate": 2000
      },
      "protocol_config": {
        "operation": "get"
      }
    }
  ]
}
```

## Running

```bash
xylem --config multi-protocol.json
```

## Coordinated Workloads

Simulate dependent operations:

```json
{
  "workloads": [
    {
      "name": "cache-check",
      "protocol": "redis",
      "workload": {
        "rate": 1000,
        "sequence": 1
      },
      "protocol_config": {
        "operation": "get"
      }
    },
    {
      "name": "db-fallback",
      "protocol": "http",
      "workload": {
        "rate": 100,
        "sequence": 2,
        "condition": "cache-miss"
      }
    }
  ]
}
```

## Mixed Transport Types

Use different transports:

```json
{
  "workloads": [
    {
      "protocol": "redis",
      "transport": {"type": "unix", "path": "/tmp/redis.sock"},
      "workload": {"rate": 10000}
    },
    {
      "protocol": "http",
      "transport": {"type": "tcp", "host": "localhost", "port": 8080},
      "workload": {"rate": 1000}
    }
  ]
}
```

## Monitoring

Each workload reports separate statistics:

```
=== redis-cache ===
p50: 0.5ms, p99: 2.1ms
Throughput: 4,987 req/s

=== http-api ===
p50: 15.3ms, p99: 45.2ms
Throughput: 998 req/s

=== memcached-session ===
p50: 0.8ms, p99: 3.2ms
Throughput: 1,995 req/s
```

## See Also

- [Configuration](../guide/configuration.md)
- [Examples](./redis.md)
