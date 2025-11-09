# Protocols

Xylem supports multiple application protocols for different types of RPC workloads.

## Supported Protocols

- **[Redis](./protocols/redis.md)** - Redis protocol for key-value operations
- **[HTTP](./protocols/http.md)** - HTTP/1.1 protocol for web services
- **[Memcached](./protocols/memcached.md)** - Memcached protocol for caching
- **[Custom Protocols](./protocols/custom.md)** - Extend Xylem with your own protocols

## Protocol Selection

Specify the protocol using the `--protocol` flag or in the configuration file:

```bash
xylem --protocol redis
```

Or in JSON:

```json
{
  "protocol": "redis"
}
```

## Protocol Architecture

All protocols in Xylem implement a common interface that provides:

1. **Request Encoding** - Convert logical operations to wire format
2. **Response Decoding** - Parse responses from the server
3. **State Management** - Track request-response correlation
4. **Error Handling** - Detect and report protocol errors

## Adding New Protocols

Xylem's modular design makes it easy to add support for new protocols. See the [Extending Xylem](../advanced/extending.md) guide for details.

## See Also

- [Configuration](./configuration.md)
- [Examples](../examples/redis.md)
