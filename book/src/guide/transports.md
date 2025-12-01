# Transports

Xylem supports multiple transport layers for network communication.

## Supported Transports

- **[TCP](./transports/tcp.md)** - Reliable byte stream transport
- **[UDP](./transports/udp.md)** - Unreliable datagram transport
- **[Unix Domain Sockets](./transports/unix.md)** - Local inter-process communication

## Transport Selection

Specify the transport in the `[target]` section of your TOML profile configuration. The transport applies to all traffic groups:

```toml
[target]
transport = "tcp"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = "localhost:6379"
# ...
```

For Unix sockets:

```toml
[target]
transport = "unix"

[[traffic_groups]]
name = "main"
protocol = "redis"
target = "/var/run/redis.sock"
# ...
```

## See Also

- [Transport Configuration](./configuration/transport.md)
- [Architecture](../architecture/transports.md)
