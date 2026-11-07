# Transports

Xylem supports multiple transport layers for network communication.

## Supported Transports

- **[TCP](./transports/tcp.md)** - Reliable byte stream transport
- **[UDP](./transports/udp.md)** - Unreliable datagram transport
- **[Unix Domain Sockets](./transports/unix.md)** - Local inter-process communication
- **[ntcp](./transports/ntcp.md)** - User-space TCP over a TUN device (Linux, `ntcp` build feature)

## Transport Selection

Specify `transport` in each `[[traffic_groups]]` section of your TOML profile. It defaults to `"tcp"`. Groups sharing a thread must use the same transport:

```toml
[[traffic_groups]]
transport = "tcp"
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
# ...
```

For Unix sockets:

```toml
[[traffic_groups]]
transport = "unix"
name = "main"
protocol = "redis"
target = "/var/run/redis.sock"
# ...
```

For ntcp, which also needs a top-level `[ntcp]` section with one TUN device and address per thread:

```toml
[[traffic_groups]]
transport = "ntcp"
name = "main"
protocol = "redis"
target = "10.73.0.1:6379"
# ...

[ntcp]
endpoints = [{ ifname = "ntcp0", local_ip = "10.73.0.2" }]
```

## See Also

- [Transport Configuration](./configuration/transport.md)
- [Architecture](../architecture/transports.md)
