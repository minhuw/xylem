# Redis Protocol

Xylem provides comprehensive support for the Redis protocol.

## Supported Operations

- **GET** - Retrieve value by key
- **SET** - Store key-value pair
- **INCR** - Increment counter
- **DECR** - Decrement counter
- **DEL** - Delete key
- **EXISTS** - Check if key exists
- **PING** - Test connection

## Basic Usage

```bash
xylem --protocol redis --host localhost --port 6379
```

## Configuration

```json
{
  "protocol": "redis",
  "protocol_config": {
    "operation": "get",
    "key_pattern": "user:{id}",
    "value_size": 100
  }
}
```

## Pipelining

Redis pipelining is supported for higher throughput:

```json
{
  "protocol": "redis",
  "protocol_config": {
    "pipeline": 10
  }
}
```

This sends 10 commands before waiting for responses.

## Key Patterns

Use patterns to generate diverse keys:

- `user:{id}` - Sequential IDs
- `item:*` - Random keys
- `cache:{hash}` - Hash-based distribution

## Examples

### GET Benchmark

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
    "rate": 10000
  },
  "protocol_config": {
    "operation": "get",
    "key_pattern": "key:*"
  }
}
```

### SET Benchmark

```json
{
  "protocol": "redis",
  "protocol_config": {
    "operation": "set",
    "key_pattern": "user:{id}",
    "value_size": 256
  }
}
```

## See Also

- [Redis Documentation](https://redis.io/docs/)
- [Examples](../../examples/redis.md)
