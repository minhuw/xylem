# CLI Reference

Complete reference for Xylem command-line interface.

Xylem uses a config-first design with TOML profile files. This ensures reproducibility and simplifies complex workload specifications.

## Basic Usage

```bash
xylem -P tests/redis/redis-get-zipfian.toml
```

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

### `-P, --profile <FILE>`

Path to TOML profile configuration file.

```bash
xylem -P tests/redis/redis-bench.toml
```

### `--set <KEY=VALUE>`

Override any configuration value using dot notation. Can be specified multiple times.

```bash
xylem -P tests/redis/redis-get-zipfian.toml --set traffic_groups.0.target=192.168.1.100:6379
xylem -P profiles/http-spike.toml --set experiment.duration=60s --set experiment.seed=12345
```

**Examples:**
- `--set traffic_groups.0.target=127.0.0.1:6379`
- `--set traffic_groups.0.protocol=redis`
- `--set experiment.duration=60s`
- `--set experiment.seed=999`
- `--set traffic_groups.0.protocol_config.keys.n=1000000`
- `--set traffic_groups.0.protocol_config.keys.strategy=zipfian`
- `--set traffic_groups.0.threads=[0,1,2,3]`
- `--set output.file=/tmp/results.json`

### `-l, --log-level <LEVEL>`

Set log level (trace, debug, info, warn, error).

**Default:** info

```bash
xylem -P tests/redis/redis-get-zipfian.toml --log-level debug
```

## Subcommands

### `completions <SHELL>`

Generate shell completion scripts.

**Supported shells:**
- bash
- zsh
- fish
- powershell
- elvish

**Examples:**

```bash
# Bash
xylem completions bash > ~/.local/share/bash-completion/completions/xylem

# Zsh
xylem completions zsh > ~/.zsh/completions/_xylem

# Fish
xylem completions fish > ~/.config/fish/completions/xylem.fish
```

### `schema`

Generate JSON Schema for configuration files.

```bash
xylem schema > config-schema.json
```

## Configuration Overrides

The `--set` flag uses dot notation to override any configuration value:

```bash
# Override target address for a traffic group
xylem -P tests/redis/redis-get-zipfian.toml --set traffic_groups.0.target=127.0.0.1:6379

# Override protocol for a traffic group
xylem -P profiles/memcached-ramp.toml --set traffic_groups.0.protocol=memcached-ascii

# Override experiment parameters
xylem -P profiles/memcached-ramp.toml --set experiment.duration=300s --set experiment.seed=42

# Override protocol_config settings (keys, operations, value sizes)
xylem -P tests/redis/redis-get-zipfian.toml --set traffic_groups.0.protocol_config.keys.n=1000000
xylem -P tests/redis/redis-get-zipfian.toml --set traffic_groups.0.protocol_config.keys.strategy=zipfian

# Override traffic group settings
xylem -P profiles/multi-protocol-example.toml --set traffic_groups.0.threads=[0,1,2,3]
```

## Profile Files

Xylem requires a TOML profile file that defines the experiment configuration. See the `profiles/` directory for example configurations.

Example profile structure:
```toml
[experiment]
name = "redis-benchmark"
duration = "60s"
seed = 123

[[traffic_groups]]
transport = "tcp"
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0, 1, 2, 3]
connections_per_thread = 10
max_pending_per_connection = 10

[traffic_groups.protocol_config.keys]
strategy = "zipfian"
n = 1000000
theta = 0.99
value_size = 64

[traffic_groups.protocol_config.operations]
strategy = "fixed"
operation = "get"

[traffic_groups.traffic_policy]
type = "closed-loop"

[traffic_groups.sampling_policy]
type = "unlimited"

[output]
format = "json"
file = "/tmp/results.json"
```

## Exit Status

| Status | Meaning |
|---|---|
| 0 | The run passed its validity checks and thresholds, or none applied |
| 1 | Error, including an invalid profile or unwritable result files |
| 2 | Thresholds failed: a `[thresholds]` expression did not hold |
| 3 | Invalid run: a `[validity]` check failed, so the results do not describe the configured load (takes precedence over 2) |

Result files are written before the exit with 2 or 3. See
[Thresholds and Validity](./thresholds.md).

## Environment Variables

Xylem respects the following environment variables:

- `RUST_LOG` - Set logging level (e.g., `debug`, `info`, `warn`, `error`)

```bash
RUST_LOG=debug xylem -P tests/redis/redis-get-zipfian.toml
```

## See Also

- [Configuration](./configuration.md) - Detailed configuration file format
- [Thresholds and Validity](./thresholds.md) - Verdicts and exit statuses
- [Configuration Schema](../reference/schema.md) - Configuration schema reference
