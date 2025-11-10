# Protocol Configuration

Configure the protocol in the `[target]` section of your TOML profile.

## Specifying Protocol

```toml
[target]
protocol = "redis"  # or "http", "memcached-binary", "memcached-ascii", "masstree", "xylem-echo"
address = "localhost:6379"
```

## Supported Protocols

- **redis** - Redis Serialization Protocol (RESP)
- **http** - HTTP/1.1
- **memcached-binary** - Memcached binary protocol
- **memcached-ascii** - Memcached text protocol
- **masstree** - Masstree protocol
- **xylem-echo** - Echo protocol (testing/development only)

See the [Architecture - Protocol Layer](../../architecture/protocols.md) for implementation details.
