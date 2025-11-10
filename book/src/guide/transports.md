# Transports

Xylem supports multiple transport layers for network communication.

## Supported Transports

- **[TCP](./transports/tcp.md)** - Reliable byte stream transport
- **[UDP](./transports/udp.md)** - Unreliable datagram transport
- **[Unix Domain Sockets](./transports/unix.md)** - Local inter-process communication

## Transport Selection

Specify the transport explicitly in your TOML profile configuration:

```toml
[target]
protocol = "redis"
transport = "tcp"
address = "localhost:6379"

# or, for a Unix socket:
[target]
protocol = "redis"
transport = "unix"
address = "/var/run/redis.sock"
```

## See Also

- [Transport Configuration](./configuration/transport.md)
- [Architecture](../architecture/transports.md)
