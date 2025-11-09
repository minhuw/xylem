# Transports

Xylem supports multiple transport layers for network communication.

## Supported Transports

- **[TCP](./transports/tcp.md)** - Reliable byte stream transport
- **[UDP](./transports/udp.md)** - Unreliable datagram transport
- **[Unix Domain Sockets](./transports/unix.md)** - Local inter-process communication
- **[TLS](./transports/tls.md)** - Encrypted TCP transport

## Transport Selection

Specify the transport using the `--transport` flag or in configuration:

```bash
xylem --transport tcp
```

Or in JSON:

```json
{
  "transport": {
    "type": "tcp"
  }
}
```

## Transport Features

### Connection Management

All transports support:
- Connection pooling
- Automatic reconnection
- Connection timeout configuration

### Performance Tuning

Transport-specific options for optimizing performance:
- TCP_NODELAY for latency-sensitive workloads
- Send/receive buffer sizes
- Keep-alive settings

### Security

TLS transport provides:
- Server certificate verification
- Mutual TLS (client certificates)
- Custom CA certificates

## Choosing a Transport

### TCP
- **Use for:** Most protocols, reliable delivery required
- **Pros:** Reliable, ordered delivery, widely supported
- **Cons:** Higher latency than UDP

### UDP
- **Use for:** Low-latency protocols, loss-tolerant workloads
- **Pros:** Lower latency, simpler protocol
- **Cons:** No reliability guarantees

### Unix Sockets
- **Use for:** Local services, highest performance
- **Pros:** Lowest latency, no network overhead
- **Cons:** Only works for local processes

### TLS
- **Use for:** Encrypted communication, production deployments
- **Pros:** Security, authentication
- **Cons:** Higher CPU usage and latency

## See Also

- [Transport Configuration](./configuration/transport.md)
- [Architecture](../architecture/transports.md)
