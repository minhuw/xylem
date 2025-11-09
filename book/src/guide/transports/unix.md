# Unix Domain Sockets

Unix domain sockets provide the fastest IPC mechanism for local communication.

## Basic Configuration

```json
{
  "transport": {
    "type": "unix",
    "path": "/var/run/service.sock"
  }
}
```

## Options

### `path`

**Type:** String  
**Required:** Yes

Path to the Unix socket file.

```json
{
  "transport": {
    "type": "unix",
    "path": "/tmp/xylem-test.sock"
  }
}
```

### `connect_timeout`

**Type:** String (duration)  
**Default:** `"5s"`

Maximum time to wait for connection.

## Characteristics

### Advantages

- **Highest Performance** - No network stack overhead
- **Security** - File system permissions control access
- **Reliability** - Same guarantees as TCP
- **No Port Conflicts** - No port number management

### Limitations

- **Local Only** - Only works for processes on same host
- **File System** - Requires file system access
- **Platform** - Unix/Linux only (not Windows)

## Use Cases

Unix sockets are ideal for:

- Local service communication
- Microservices on same host
- Maximum performance benchmarks
- Development and testing

## Examples

### Redis via Unix Socket

```bash
xylem --protocol redis --transport unix --path /var/run/redis/redis.sock
```

### Configuration

```json
{
  "protocol": "redis",
  "transport": {
    "type": "unix",
    "path": "/var/run/redis/redis.sock"
  },
  "workload": {
    "duration": "60s",
    "rate": 50000
  }
}
```

## Server Setup

Ensure your service is configured to listen on a Unix socket:

### Redis

```conf
# redis.conf
unixsocket /var/run/redis/redis.sock
unixsocketperm 777
```

### Nginx

```nginx
upstream backend {
    server unix:/var/run/app.sock;
}
```

## Troubleshooting

### Permission Denied

Check file permissions:
```bash
ls -l /var/run/service.sock
chmod 666 /var/run/service.sock
```

### Socket Not Found

Verify the service is listening:
```bash
ls -l /var/run/service.sock
netstat -lx | grep service.sock
```

## See Also

- [TCP Transport](./tcp.md)
- [Performance Tuning](../../advanced/performance.md)
