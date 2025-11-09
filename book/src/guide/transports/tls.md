# TLS Transport

TLS (Transport Layer Security) provides encrypted TCP communication.

## Basic Configuration

```json
{
  "transport": {
    "type": "tls",
    "host": "api.example.com",
    "port": 6380
  }
}
```

## Options

### `host`

**Type:** String  
**Required:** Yes

Target host address. Used for both connection and SNI (Server Name Indication).

### `port`

**Type:** Integer  
**Required:** Yes

Target port number.

### `verify`

**Type:** Boolean  
**Default:** `true`

Verify server certificate against system CA store.

```json
{
  "transport": {
    "type": "tls",
    "verify": true
  }
}
```

### `ca_cert`

**Type:** String (path)  
**Optional**

Path to custom CA certificate file for verification.

```json
{
  "transport": {
    "type": "tls",
    "ca_cert": "/etc/ssl/certs/custom-ca.pem"
  }
}
```

### Mutual TLS (mTLS)

Configure client certificate authentication:

```json
{
  "transport": {
    "type": "tls",
    "host": "secure.example.com",
    "port": 6380,
    "client_cert": "/path/to/client.pem",
    "client_key": "/path/to/client-key.pem"
  }
}
```

**Options:**
- `client_cert` - Path to client certificate (PEM format)
- `client_key` - Path to client private key (PEM format)

### `sni`

**Type:** String  
**Optional**

Override SNI hostname (defaults to `host`).

```json
{
  "transport": {
    "type": "tls",
    "host": "192.168.1.100",
    "sni": "api.example.com"
  }
}
```

## Certificate Formats

Xylem supports PEM-encoded certificates:

```pem
-----BEGIN CERTIFICATE-----
MIIDXTCCAkWgAwIBAgIJAKL...
-----END CERTIFICATE-----
```

## Examples

### Basic TLS Connection

```bash
xylem --protocol redis --transport tls --host redis.example.com --port 6380
```

### With Custom CA

```json
{
  "protocol": "redis",
  "transport": {
    "type": "tls",
    "host": "redis.internal",
    "port": 6380,
    "ca_cert": "/etc/ssl/internal-ca.pem"
  }
}
```

### Mutual TLS

```json
{
  "protocol": "redis",
  "transport": {
    "type": "tls",
    "host": "secure-redis.example.com",
    "port": 6380,
    "ca_cert": "/etc/ssl/ca.pem",
    "client_cert": "/etc/ssl/client.pem",
    "client_key": "/etc/ssl/client-key.pem"
  }
}
```

### Skip Verification (Testing Only)

**Warning:** Only use for testing with self-signed certificates!

```json
{
  "transport": {
    "type": "tls",
    "verify": false
  }
}
```

## Generating Test Certificates

For testing, generate self-signed certificates:

```bash
# Generate CA
openssl req -x509 -newkey rsa:4096 -days 365 -nodes \
  -keyout ca-key.pem -out ca.pem

# Generate server certificate
openssl req -newkey rsa:4096 -nodes \
  -keyout server-key.pem -out server-req.pem
openssl x509 -req -in server-req.pem -days 365 \
  -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out server.pem

# Generate client certificate (for mTLS)
openssl req -newkey rsa:4096 -nodes \
  -keyout client-key.pem -out client-req.pem
openssl x509 -req -in client-req.pem -days 365 \
  -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out client.pem
```

## Performance Considerations

TLS adds overhead:
- **CPU** - Encryption/decryption cost
- **Latency** - Handshake adds ~1 RTT
- **Throughput** - Slightly reduced

For best performance:
- Reuse connections (connection pooling)
- Use hardware acceleration if available
- Consider TLS 1.3 for faster handshakes

## Troubleshooting

### Certificate Verification Failed

```
Error: certificate verify failed
```

Solutions:
- Check CA certificate is correct
- Verify hostname matches certificate
- Check certificate expiration

### Handshake Timeout

Increase connection timeout:
```json
{
  "transport": {
    "type": "tls",
    "connect_timeout": "10s"
  }
}
```

## See Also

- [TCP Transport](./tcp.md)
- [HTTP Protocol](../protocols/http.md)
- [Security Best Practices](../../advanced/performance.md)
