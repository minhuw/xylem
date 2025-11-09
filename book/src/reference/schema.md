# JSON Schema

Xylem configuration JSON schema reference.

## Schema Location

The JSON schema is available in the repository:
```
schema/config.json
```

## Using the Schema

### Validation

Validate configuration files using tools like `ajv`:

```bash
npm install -g ajv-cli
ajv validate -s schema/config.json -d my-config.json
```

### IDE Integration

Many IDEs support JSON Schema for auto-completion and validation.

#### VS Code

Add to your config file:

```json
{
  "$schema": "file:///path/to/xylem/schema/config.json",
  "protocol": "redis",
  ...
}
```

#### IntelliJ IDEA

Settings → Languages & Frameworks → Schemas and DTDs → JSON Schema Mappings

## Schema Overview

### Top-Level Structure

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Xylem Configuration",
  "type": "object",
  "required": ["protocol", "transport", "workload"],
  "properties": {
    "protocol": { "type": "string" },
    "transport": { "type": "object" },
    "workload": { "type": "object" },
    "protocol_config": { "type": "object" },
    "output": { "type": "object" }
  }
}
```

## Protocol Schema

```json
{
  "protocol": {
    "type": "string",
    "enum": ["redis", "http", "memcached"],
    "description": "Application protocol to use"
  }
}
```

## Transport Schema

### TCP Transport

```json
{
  "transport": {
    "type": "object",
    "required": ["type", "host", "port"],
    "properties": {
      "type": { "const": "tcp" },
      "host": { "type": "string" },
      "port": { "type": "integer", "minimum": 1, "maximum": 65535 },
      "nodelay": { "type": "boolean", "default": true },
      "keepalive": { "type": "boolean", "default": false }
    }
  }
}
```

### TLS Transport

```json
{
  "transport": {
    "type": "object",
    "required": ["type", "host", "port"],
    "properties": {
      "type": { "const": "tls" },
      "host": { "type": "string" },
      "port": { "type": "integer" },
      "verify": { "type": "boolean", "default": true },
      "ca_cert": { "type": "string" },
      "client_cert": { "type": "string" },
      "client_key": { "type": "string" }
    }
  }
}
```

## Workload Schema

```json
{
  "workload": {
    "type": "object",
    "required": ["duration", "rate"],
    "properties": {
      "duration": {
        "type": "string",
        "pattern": "^[0-9]+(ns|us|ms|s|m|h)$",
        "description": "Benchmark duration (e.g., '60s', '5m')"
      },
      "rate": {
        "type": "integer",
        "minimum": 1,
        "description": "Target request rate (req/s)"
      },
      "connections": {
        "type": "integer",
        "minimum": 1,
        "default": 1
      },
      "warmup": {
        "type": "string",
        "pattern": "^[0-9]+(ns|us|ms|s|m|h)$",
        "default": "0s"
      }
    }
  }
}
```

## Protocol Config Schema

### Redis

```json
{
  "protocol_config": {
    "type": "object",
    "properties": {
      "operation": {
        "type": "string",
        "enum": ["get", "set", "incr", "decr"],
        "default": "get"
      },
      "key_pattern": {
        "type": "string",
        "default": "key:*"
      },
      "value_size": {
        "type": "integer",
        "minimum": 0,
        "default": 100
      },
      "pipeline": {
        "type": "integer",
        "minimum": 1,
        "default": 1
      }
    }
  }
}
```

### HTTP

```json
{
  "protocol_config": {
    "type": "object",
    "properties": {
      "method": {
        "type": "string",
        "enum": ["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH"],
        "default": "GET"
      },
      "path": {
        "type": "string",
        "default": "/"
      },
      "headers": {
        "type": "object",
        "additionalProperties": { "type": "string" }
      },
      "body": {
        "type": "string"
      }
    }
  }
}
```

## Output Schema

```json
{
  "output": {
    "type": "object",
    "properties": {
      "format": {
        "type": "string",
        "enum": ["text", "json", "csv"],
        "default": "text"
      },
      "file": {
        "type": "string"
      },
      "pretty": {
        "type": "boolean",
        "default": false
      }
    }
  }
}
```

## Statistics Schema

```json
{
  "statistics": {
    "type": "object",
    "properties": {
      "sketch": {
        "type": "string",
        "enum": ["ddsketch", "tdigest", "hdr"],
        "default": "ddsketch"
      },
      "percentiles": {
        "type": "array",
        "items": {
          "type": "number",
          "minimum": 0,
          "maximum": 100
        }
      }
    }
  }
}
```

## Example Configurations

### Minimal Configuration

```json
{
  "protocol": "redis",
  "transport": {
    "type": "tcp",
    "host": "localhost",
    "port": 6379
  },
  "workload": {
    "duration": "60s",
    "rate": 1000
  }
}
```

### Full Configuration

```json
{
  "$schema": "./schema/config.json",
  "protocol": "redis",
  "transport": {
    "type": "tcp",
    "host": "localhost",
    "port": 6379,
    "nodelay": true,
    "keepalive": true
  },
  "workload": {
    "duration": "300s",
    "rate": 10000,
    "connections": 50,
    "warmup": "30s"
  },
  "protocol_config": {
    "operation": "get",
    "key_pattern": "user:{id}",
    "pipeline": 10
  },
  "output": {
    "format": "json",
    "file": "results.json",
    "pretty": true
  },
  "statistics": {
    "sketch": "ddsketch",
    "percentiles": [50, 95, 99, 99.9]
  }
}
```

## Generating Schema

The schema can be generated from Rust code:

```bash
cargo run --bin generate-schema > schema/config.json
```

## See Also

- [Configuration Guide](../guide/configuration.md)
- [JSON Schema Documentation](https://json-schema.org/)
