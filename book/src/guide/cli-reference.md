# CLI Reference

Complete reference for Xylem command-line interface.

Xylem uses a config-first design with TOML profile files. This ensures reproducibility and simplifies complex workload specifications.

## Basic Usage

```bash
xylem -P profiles/redis-get-zipfian.toml
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
xylem -P profiles/redis-bench.toml
```

### `--set <KEY=VALUE>`

Override any configuration value using dot notation. Can be specified multiple times.

```bash
xylem -P profiles/redis.toml --set target.address=192.168.1.100:6379
xylem -P profiles/http.toml --set experiment.duration=120s --set experiment.seed=12345
```

**Examples:**
- `--set target.address=127.0.0.1:6379`
- `--set experiment.duration=60s`
- `--set experiment.seed=999`
- `--set target.protocol=memcached-binary`
- `--set workload.keys.n=1000000`
- `--set traffic_groups.0.threads=[0,1,2,3]`
- `--set output.file=/tmp/results.json`

### `-l, --log-level <LEVEL>`

Set log level (trace, debug, info, warn, error).

**Default:** info

```bash
xylem -P profiles/redis.toml --log-level debug
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
# Override target address
xylem -P profiles/redis.toml --set target.address=localhost:6379

# Override experiment parameters
xylem -P profiles/bench.toml --set experiment.duration=300s --set experiment.seed=42

# Override workload settings
xylem -P profiles/redis.toml --set workload.keys.n=1000000

# Override traffic group settings
xylem -P profiles/multi.toml --set traffic_groups.0.threads=[0,1,2,3]

# Add new traffic group
xylem -P profiles/base.toml --set 'traffic_groups.+={name="new-group",threads=[4,5]}'
```

## Profile Files

Xylem requires a TOML profile file that defines the experiment configuration. See the `profiles/` directory for example configurations.

Example profile structure:
```toml
[experiment]
duration = "60s"
seed = 123

[target]
protocol = "redis"
address = "127.0.0.1:6379"

[workload]
# Workload configuration...

[[traffic_groups]]
name = "group-1"
threads = [0, 1, 2, 3]
```

## Environment Variables

Xylem respects the following environment variables:

- `RUST_LOG` - Set logging level (e.g., `debug`, `info`, `warn`, `error`)

```bash
RUST_LOG=debug xylem -P profiles/redis.toml
```

## See Also

- [Configuration](./configuration.md) - Detailed configuration file format
- [Examples](../examples/redis.md) - Common usage examples
- [JSON Schema](../reference/schema.md) - Configuration schema reference
