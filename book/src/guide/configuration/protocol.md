# Protocol Configuration

Configure protocol in the `[target]` section of your profile.

## Protocol Selection

Specify the protocol using the `protocol` field:

```toml
[target]
protocol = "redis"
address = "localhost:6379"
```

## Supported Protocols

### Redis Protocol

```toml
[target]
protocol = "redis"
address = "localhost:6379"
```

The Redis protocol implementation supports standard Redis commands like GET, SET, etc. The specific operation pattern is defined in the workload configuration.

### HTTP Protocol

```toml
[target]
protocol = "http"
address = "localhost:8080"
```

For HTTP workloads, you can configure the request path and method through workload-specific options.

### Memcached Protocol

```toml
[target]
protocol = "memcached-binary"
address = "localhost:11211"
```

or

```toml
[target]
protocol = "memcached-ascii"
address = "localhost:11211"
```
