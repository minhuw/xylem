# Protocol Configuration

Configure the protocol in the `[target]` section of your TOML profile.

## Specifying Protocol

```toml
[target]
protocol = "redis"  # or "http", "memcached-binary", "memcached-ascii"
address = "localhost:6379"
```

## Supported Protocols

- **redis** - Redis Serialization Protocol (RESP)
- **http** - HTTP/1.1
- **memcached-binary** - Memcached binary protocol
- **memcached-ascii** - Memcached text protocol

See the [Architecture - Protocol Layer](../../architecture/protocols.md) for implementation details.
