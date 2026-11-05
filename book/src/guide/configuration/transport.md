# Transport Configuration

Each `[[traffic_groups]]` section specifies its own `transport` and `target`. Transport defaults to `"tcp"`. Groups sharing a thread must use the same transport.

## Available Transports

- `tcp` - TCP/IP connections
- `udp` - UDP connections
- `unix` - Unix domain sockets

## Configuration

### TCP (IP:port)

```toml
[[traffic_groups]]
transport = "tcp"
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
# ...
```

### Unix Domain Socket (filesystem path)

```toml
[[traffic_groups]]
transport = "unix"
name = "main"
protocol = "redis"
target = "/var/run/redis/redis.sock"
# ...
```

### UDP (IP:port)

UDP support depends on the protocol implementation.

```toml
[[traffic_groups]]
transport = "udp"
name = "main"
protocol = "memcached-binary"
target = "127.0.0.1:11211"
# ...
```

See the [Architecture - Transport Layer](../../architecture/transports.md) for implementation details.
