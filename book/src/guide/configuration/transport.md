# Transport Configuration

Transport is explicitly specified in the `[target]` section along with the connection address.

## Available Transports

- `tcp` - TCP/IP connections
- `udp` - UDP connections  
- `unix` - Unix domain sockets

## Configuration

### TCP (hostname:port)

```toml
[target]
protocol = "redis"
transport = "tcp"
address = "localhost:6379"
```

### Unix Domain Socket (filesystem path)

```toml
[target]
protocol = "redis"
transport = "unix"
address = "/var/run/redis/redis.sock"
```

### UDP (hostname:port)

UDP support depends on the protocol implementation.

```toml
[target]
protocol = "memcached-binary"
transport = "udp"
address = "localhost:11211"
```

See the [Architecture - Transport Layer](../../architecture/transports.md) for implementation details.
