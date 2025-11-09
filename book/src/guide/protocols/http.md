# HTTP Protocol

Xylem supports HTTP/1.1 protocol for benchmarking web services and APIs.

## Supported Methods

- GET
- POST
- PUT
- DELETE
- HEAD
- PATCH

## Basic Usage

```bash
xylem --protocol http --host api.example.com --port 80
```

## Configuration

```json
{
  "protocol": "http",
  "protocol_config": {
    "method": "GET",
    "path": "/api/v1/users",
    "headers": {
      "User-Agent": "Xylem/0.1.0",
      "Accept": "application/json"
    }
  }
}
```

## Custom Headers

Add custom headers to requests:

```json
{
  "protocol_config": {
    "headers": {
      "Authorization": "Bearer token123",
      "Content-Type": "application/json",
      "X-Custom-Header": "value"
    }
  }
}
```

## Request Body

For POST/PUT requests:

```json
{
  "protocol_config": {
    "method": "POST",
    "path": "/api/users",
    "headers": {
      "Content-Type": "application/json"
    },
    "body": "{\"name\": \"test\", \"email\": \"test@example.com\"}"
  }
}
```

## Dynamic Paths

Use patterns in paths:

```json
{
  "protocol_config": {
    "path": "/api/users/{id}"
  }
}
```

## HTTPS Support

Use TLS transport for HTTPS:

```json
{
  "protocol": "http",
  "transport": {
    "type": "tls",
    "host": "api.example.com",
    "port": 443
  }
}
```

## Examples

### REST API Benchmark

```json
{
  "protocol": "http",
  "transport": {
    "type": "tcp",
    "host": "localhost",
    "port": 8080
  },
  "workload": {
    "duration": "60s",
    "rate": 1000
  },
  "protocol_config": {
    "method": "GET",
    "path": "/api/products/{id}",
    "headers": {
      "Accept": "application/json"
    }
  }
}
```

## See Also

- [TLS Transport](../transports/tls.md)
- [Examples](../../examples/http.md)
