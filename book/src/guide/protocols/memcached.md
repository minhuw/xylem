# Memcached Protocol

Support for the Memcached binary and text protocols.

## Supported Operations

- **GET** - Retrieve value by key
- **SET** - Store key-value pair with expiration
- **DELETE** - Remove key
- **ADD** - Add key only if it doesn't exist
- **REPLACE** - Replace existing key
- **INCR** - Increment numeric value
- **DECR** - Decrement numeric value

## Basic Usage

```bash
xylem --protocol memcached --host localhost --port 11211
```

## Configuration

```json
{
  "protocol": "memcached",
  "protocol_config": {
    "operation": "get",
    "key_pattern": "cache:*",
    "value_size": 1024,
    "expiration": 3600
  }
}
```

## Options

### `operation`

Memcached operation to perform.

### `key_pattern`

Pattern for key generation.

### `value_size`

Size of values in bytes for SET operations.

### `expiration`

TTL in seconds for stored items.

## Examples

### Cache GET Benchmark

```json
{
  "protocol": "memcached",
  "transport": {
    "type": "tcp",
    "host": "localhost",
    "port": 11211
  },
  "workload": {
    "duration": "60s",
    "rate": 5000
  },
  "protocol_config": {
    "operation": "get",
    "key_pattern": "item:*"
  }
}
```

### Cache SET Benchmark

```json
{
  "protocol": "memcached",
  "protocol_config": {
    "operation": "set",
    "key_pattern": "cache:{id}",
    "value_size": 2048,
    "expiration": 300
  }
}
```

## See Also

- [Memcached Documentation](https://memcached.org/)
- [Configuration](../configuration.md)
