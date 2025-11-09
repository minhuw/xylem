# Configuration

Xylem supports configuration via JSON files for complex workload definitions.

## Configuration File Structure

A configuration file is a JSON document with the following top-level fields:

```json
{
  "protocol": "redis",
  "transport": { ... },
  "workload": { ... },
  "output": { ... }
}
```

## Sections

The configuration is divided into several sections:

- **[Workload Configuration](./configuration/workload.md)** - Define workload patterns, duration, and rates
- **[Transport Configuration](./configuration/transport.md)** - Configure transport layer options
- **[Protocol Configuration](./configuration/protocol.md)** - Protocol-specific settings

## Basic Example

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
    "rate": 5000,
    "connections": 10
  },
  "output": {
    "format": "json",
    "file": "results.json"
  }
}
```

## Schema Validation

Xylem provides a JSON schema for configuration validation. The schema file is available in the repository at `schema/config.json`.

You can use tools like [ajv](https://ajv.js.org/) to validate your configuration:

```bash
ajv validate -s schema/config.json -d your-config.json
```

## Configuration Precedence

When both CLI arguments and configuration file are provided:

1. CLI arguments take precedence over configuration file
2. Configuration file provides defaults for unspecified CLI options

Example:

```bash
# rate from CLI (2000) overrides config file
xylem --config config.json --rate 2000
```

## Loading Configuration

Use the `--config` flag to load a configuration file:

```bash
xylem --config workload.json
```

## See Also

- [Workload Configuration](./configuration/workload.md)
- [Transport Configuration](./configuration/transport.md)
- [Protocol Configuration](./configuration/protocol.md)
- [JSON Schema Reference](../reference/schema.md)
