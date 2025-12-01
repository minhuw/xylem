# Protocols

Xylem supports multiple application protocols for different types of RPC workloads.

## Supported Protocols

- **[Redis](./protocols/redis.md)** - Redis protocol for key-value operations
- **[HTTP](./protocols/http.md)** - HTTP/1.1 protocol for web services
- **[Memcached](./protocols/memcached.md)** - Memcached protocol for caching

## Protocol Selection

Specify the protocol for each traffic group in your TOML profile configuration:

```toml
[[traffic_groups]]
name = "main"
protocol = "redis"
target = "localhost:6379"
# ...
```

## Protocol Architecture

All protocols in Xylem implement a common interface that provides:

1. **Request Generation** - Convert logical operations to wire format
2. **Response Parsing** - Parse responses from the server
3. **State Management** - Track request-response correlation
4. **Error Handling** - Detect and report protocol errors

## See Also

- [Configuration](./configuration.md)
