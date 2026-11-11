# Protocol Configuration

Configure the protocol and target address for each traffic group in your TOML profile.

## Specifying Protocol

Each traffic group must specify its own `protocol` and `target`:

```toml
[[traffic_groups]]
name = "main"
protocol = "redis"  # or "http", "memcached-binary", "memcached-ascii", "masstree", "dns", "nvme-tcp", "xylem-echo"
target = "127.0.0.1:6379"
threads = [0, 1, 2, 3]
connections_per_thread = 10
max_pending_per_connection = 1
```

## Multi-Protocol Experiments

You can run different protocols in separate traffic groups:

```toml
[[traffic_groups]]
name = "redis-load"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0, 1]
# ...

[[traffic_groups]]
name = "memcached-load"
protocol = "memcached-binary"
target = "127.0.0.1:11211"
threads = [2, 3]
# ...
```

## Supported Protocols

- **redis** - Redis Serialization Protocol (RESP)
- **http** - HTTP/1.1
- **memcached-binary** - Memcached binary protocol
- **memcached-ascii** - Memcached text protocol
- **dns** - DNS queries (see [DNS Protocol](../protocols/dns.md))
- **nvme-tcp** - NVMe over Fabrics on TCP (see [NVMe/TCP Protocol](../protocols/nvme-tcp.md))
- **xylem-echo** - Echo protocol (testing/development only)

See the [Architecture - Protocol Layer](../../architecture/protocols.md) for implementation details.
