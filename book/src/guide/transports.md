# Transports

Xylem supports multiple transport layers for network communication.

## Supported Transports

- **[TCP](./transports/tcp.md)** - Reliable byte stream transport
- **[UDP](./transports/udp.md)** - Unreliable datagram transport
- **[Unix Domain Sockets](./transports/unix.md)** - Local inter-process communication

## Transport Selection

Specify the transport in your TOML profile configuration:

```toml
[target]
protocol = "redis"
address = "localhost:6379"  # TCP
# or
address = "/var/run/redis.sock"  # Unix socket
```

## See Also

- [Transport Configuration](./configuration/transport.md)
- [Architecture](../architecture/transports.md)
