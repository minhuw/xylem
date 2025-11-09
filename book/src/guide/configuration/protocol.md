# Protocol Configuration

Configure protocol-specific options for Redis, HTTP, Memcached, and custom protocols.

## Redis Protocol

```json
{
  "protocol": "redis",
  "protocol_config": {
    "operation": "get",
    "key_pattern": "user:*",
    "value_size": 100,
    "pipeline": 10
  }
}
```

**Options:**
- `operation` - Redis operation (get, set, incr, etc.)
- `key_pattern` - Key pattern with wildcards
- `value_size` - Size of values for SET operations
- `pipeline` - Number of pipelined requests

## HTTP Protocol

```json
{
  "protocol": "http",
  "protocol_config": {
    "method": "GET",
    "path": "/api/users",
    "headers": {
      "User-Agent": "Xylem",
      "Accept": "application/json"
    },
    "body": null
  }
}
```

**Options:**
- `method` - HTTP method (GET, POST, PUT, DELETE)
- `path` - Request path
- `headers` - Custom HTTP headers
- `body` - Request body (for POST/PUT)

## Memcached Protocol

```json
{
  "protocol": "memcached",
  "protocol_config": {
    "operation": "get",
    "key_pattern": "cache:*",
    "value_size": 1024
  }
}
```

**Options:**
- `operation` - Memcached operation (get, set, delete, etc.)
- `key_pattern` - Key pattern
- `value_size` - Size of values for SET operations

## Custom Protocol

For custom protocol implementations:

```json
{
  "protocol": "custom",
  "protocol_config": {
    "plugin": "/path/to/protocol.so",
    "options": {
      "custom_option": "value"
    }
  }
}
```

## See Also

- [Workload Configuration](./workload.md)
- [Protocols Guide](../protocols.md)
