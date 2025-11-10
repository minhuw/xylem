# Transport Configuration

Transport is inferred from the `address` field in the `[target]` section.

## Address Format

### TCP (hostname:port)

```toml
[target]
address = "localhost:6379"
```

### Unix Domain Socket (filesystem path)

```toml
[target]
address = "/var/run/redis/redis.sock"
```

### UDP (hostname:port)

UDP support depends on the protocol implementation.

See the [Architecture - Transport Layer](../../architecture/transports.md) for implementation details.
