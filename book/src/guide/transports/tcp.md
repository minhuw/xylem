# TCP Transport

TCP (Transmission Control Protocol) provides reliable, ordered, byte-stream communication.

## Basic Configuration

```json
{
  "transport": {
    "type": "tcp",
    "host": "localhost",
    "port": 6379
  }
}
```

## Options

### `host`

**Type:** String  
**Required:** Yes

Target host address. Can be:
- Hostname: `"localhost"`, `"redis.example.com"`
- IPv4 address: `"127.0.0.1"`, `"192.168.1.100"`
- IPv6 address: `"::1"`, `"2001:db8::1"`

### `port`

**Type:** Integer  
**Required:** Yes

Target port number (1-65535).

### `nodelay`

**Type:** Boolean  
**Default:** `true`

Enable TCP_NODELAY to disable Nagle's algorithm. This reduces latency but may decrease throughput for small requests.

```json
{
  "transport": {
    "type": "tcp",
    "nodelay": true
  }
}
```

### `keepalive`

**Type:** Boolean  
**Default:** `false`

Enable TCP keep-alive to detect dead connections.

```json
{
  "transport": {
    "type": "tcp",
    "keepalive": true,
    "keepalive_interval": "60s"
  }
}
```

### `connect_timeout`

**Type:** String (duration)  
**Default:** `"5s"`

Maximum time to wait for connection establishment.

```json
{
  "transport": {
    "type": "tcp",
    "connect_timeout": "10s"
  }
}
```

## Performance Tuning

### Buffer Sizes

Configure send and receive buffer sizes:

```json
{
  "transport": {
    "type": "tcp",
    "send_buffer": 65536,
    "recv_buffer": 65536
  }
}
```

### Connection Pooling

Reuse connections for better performance:

```json
{
  "transport": {
    "type": "tcp",
    "pool_size": 10
  }
}
```

## Examples

### Basic TCP Connection

```bash
xylem --protocol redis --transport tcp --host localhost --port 6379
```

### Optimized for Latency

```json
{
  "transport": {
    "type": "tcp",
    "host": "localhost",
    "port": 6379,
    "nodelay": true,
    "connect_timeout": "1s"
  }
}
```

## See Also

- [TLS Transport](./tls.md) for encrypted TCP
- [Transport Configuration](../configuration/transport.md)
