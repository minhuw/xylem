# Transport Configuration

The transport is specified in the `[target]` section and applies to all traffic groups. Each traffic group specifies its own target address.

## Available Transports

- `tcp` - TCP/IP connections
- `udp` - UDP connections
- `unix` - Unix domain sockets

## Configuration

### TCP (hostname:port)

```toml
[target]
transport = "tcp"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = "localhost:6379"
# ...
```

### Unix Domain Socket (filesystem path)

```toml
[target]
transport = "unix"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = "/var/run/redis/redis.sock"
# ...
```

### UDP (hostname:port)

UDP support depends on the protocol implementation.

```toml
[target]
transport = "udp"

[[traffic_groups]]
name = "main"
protocol = "memcached-binary"
target = "localhost:11211"
# ...
```

See the [Architecture - Transport Layer](../../architecture/transports.md) for implementation details.
