# Transport Configuration

Transport is configured in the `[target]` section through the `address` field.

## Address Format

The transport type is inferred from the address format:

### TCP

Use hostname:port format:

```toml
[target]
protocol = "redis"
address = "localhost:6379"
```

or

```toml
[target]
protocol = "redis"
address = "192.168.1.100:6379"
```

### Unix Domain Socket

Use a filesystem path:

```toml
[target]
protocol = "redis"
address = "/var/run/redis/redis.sock"
```

### UDP

UDP transport is specified using the same hostname:port format. The protocol implementation determines whether to use TCP or UDP.

## Connection Options

Transport-specific options can be configured through environment variables or extended configuration:

- TCP_NODELAY - Typically enabled by default for low latency
