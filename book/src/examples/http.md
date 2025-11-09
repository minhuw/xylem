# HTTP Load Testing

Examples for load testing HTTP services with Xylem.

## Basic HTTP GET

```bash
xylem --protocol http \
      --transport tcp \
      --host api.example.com \
      --port 80 \
      --duration 60s \
      --rate 1000
```

## REST API Testing

`http-api.json`:
```json
{
  "protocol": "http",
  "transport": {
    "type": "tcp",
    "host": "localhost",
    "port": 8080
  },
  "workload": {
    "duration": "120s",
    "rate": 500,
    "connections": 20
  },
  "protocol_config": {
    "method": "GET",
    "path": "/api/v1/users/{id}",
    "headers": {
      "Accept": "application/json",
      "User-Agent": "Xylem/0.1.0"
    }
  }
}
```

## POST Requests

Test POST endpoints:

```json
{
  "protocol": "http",
  "protocol_config": {
    "method": "POST",
    "path": "/api/users",
    "headers": {
      "Content-Type": "application/json",
      "Accept": "application/json"
    },
    "body": "{\"name\":\"test\",\"email\":\"test@example.com\"}"
  }
}
```

## HTTPS Testing

Use TLS transport for HTTPS:

```json
{
  "protocol": "http",
  "transport": {
    "type": "tls",
    "host": "api.example.com",
    "port": 443,
    "verify": true
  },
  "protocol_config": {
    "method": "GET",
    "path": "/api/status"
  }
}
```

## Authentication

Include authentication headers:

```json
{
  "protocol": "http",
  "protocol_config": {
    "method": "GET",
    "path": "/api/protected",
    "headers": {
      "Authorization": "Bearer YOUR_TOKEN_HERE",
      "Accept": "application/json"
    }
  }
}
```

## Load Testing Scenario

Simulate real user behavior:

```json
{
  "protocol": "http",
  "workload": {
    "duration": "600s",
    "connections": 100,
    "rate_pattern": {
      "type": "ramp",
      "start_rate": 100,
      "end_rate": 1000,
      "ramp_duration": "300s"
    }
  }
}
```

## Health Check Endpoint

Monitor service health:

```json
{
  "protocol": "http",
  "protocol_config": {
    "method": "GET",
    "path": "/health"
  },
  "workload": {
    "duration": "3600s",
    "rate": 10,
    "alert_on_failure": true
  }
}
```

## See Also

- [HTTP Protocol](../guide/protocols/http.md)
- [TLS Transport](../guide/transports/tls.md)
