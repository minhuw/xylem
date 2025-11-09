# CLI Reference

Complete reference for Xylem command-line interface.

## Global Options

### `--version`

Display version information.

```bash
xylem --version
```

### `--help`

Display help information.

```bash
xylem --help
```

### `--config <FILE>`

Path to JSON configuration file.

```bash
xylem --config workload.json
```

## Protocol Options

### `--protocol <PROTOCOL>`

Specify the application protocol to use.

**Supported values:**
- `redis` - Redis protocol
- `http` - HTTP protocol
- `memcached` - Memcached protocol

```bash
xylem --protocol redis
```

## Transport Options

### `--transport <TRANSPORT>`

Specify the transport layer.

**Supported values:**
- `tcp` - TCP transport
- `udp` - UDP transport
- `unix` - Unix domain socket
- `tls` - TLS over TCP

```bash
xylem --transport tcp
```

### `--host <HOST>`

Target host address.

```bash
xylem --host localhost
```

### `--port <PORT>`

Target port number.

```bash
xylem --port 6379
```

## Workload Options

### `--duration <DURATION>`

How long to run the benchmark. Accepts human-readable durations.

**Examples:**
- `10s` - 10 seconds
- `5m` - 5 minutes
- `1h` - 1 hour

```bash
xylem --duration 60s
```

### `--rate <RATE>`

Target request rate (requests per second).

```bash
xylem --rate 1000
```

### `--connections <N>`

Number of concurrent connections.

```bash
xylem --connections 10
```

## Output Options

### `--output <FORMAT>`

Output format for results.

**Supported formats:**
- `text` - Human-readable text (default)
- `json` - JSON format
- `csv` - CSV format

```bash
xylem --output json
```

### `--output-file <FILE>`

Write results to a file.

```bash
xylem --output-file results.json
```

## Shell Completion

Generate shell completion scripts:

```bash
xylem --completions bash > xylem.bash
xylem --completions zsh > _xylem
xylem --completions fish > xylem.fish
```

## Environment Variables

Xylem respects the following environment variables:

- `RUST_LOG` - Set logging level (e.g., `debug`, `info`, `warn`, `error`)

```bash
RUST_LOG=debug xylem --protocol redis
```

## See Also

- [Configuration](./configuration.md) - Detailed configuration options
- [Examples](../examples/redis.md) - Common usage examples
