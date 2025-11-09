# Transport Configuration

Configure transport layer options including TCP, UDP, Unix sockets, and TLS.

## Transport Types

### TCP

Basic TCP transport configuration:

```json
{
  "transport": {
    "type": "tcp",
    "host": "localhost",
    "port": 6379,
    "nodelay": true,
    "keepalive": true
  }
}
```

**Options:**
- `host` - Target host address
- `port` - Target port number
- `nodelay` - Enable TCP_NODELAY (disable Nagle's algorithm)
- `keepalive` - Enable TCP keepalive

### UDP

UDP transport configuration:

```json
{
  "transport": {
    "type": "udp",
    "host": "localhost",
    "port": 8080
  }
}
```

### Unix Domain Socket

Unix socket transport:

```json
{
  "transport": {
    "type": "unix",
    "path": "/var/run/service.sock"
  }
}
```

### TLS

TLS over TCP configuration:

```json
{
  "transport": {
    "type": "tls",
    "host": "localhost",
    "port": 6380,
    "verify": true,
    "ca_cert": "/path/to/ca.pem",
    "client_cert": "/path/to/client.pem",
    "client_key": "/path/to/client-key.pem"
  }
}
```

**Options:**
- `verify` - Verify server certificate
- `ca_cert` - Path to CA certificate
- `client_cert` - Path to client certificate (for mutual TLS)
- `client_key` - Path to client private key

## Connection Pool Options

Configure connection pooling behavior:

```json
{
  "transport": {
    "type": "tcp",
    "host": "localhost",
    "port": 6379,
    "pool": {
      "max_connections": 100,
      "min_connections": 10,
      "connection_timeout": "5s"
    }
  }
}
```

## See Also

- [Workload Configuration](./workload.md)
- [Transports Guide](../transports.md)
