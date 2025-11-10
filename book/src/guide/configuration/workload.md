# Workload Configuration

Define workload patterns, key distributions, and runtime parameters in the `[workload]` section of your profile.

## Basic Structure

```toml
[workload]
[workload.keys]
strategy = "zipfian"
n = 10000
theta = 0.99
value_size = 100
```

## Key Distribution

### `strategy`

**Type:** String  
**Required:** Yes

The key distribution pattern to use.

**Options:**
- `"zipfian"` - Zipfian distribution (realistic cache workload with hot keys)
- `"uniform"` - Uniform random distribution
- `"random"` - Random key access

### Zipfian Distribution

```toml
[workload.keys]
strategy = "zipfian"
n = 100000
theta = 0.99
```

**Parameters:**
- `n` - Number of keys in the key space
- `theta` - Skew parameter (higher values = more skewed, typical: 0.99)

### Uniform Distribution

```toml
[workload.keys]
strategy = "uniform"
n = 100000
```

### Random Distribution

```toml
[workload.keys]
strategy = "random"
max = 100000
```

## Value Size

### `value_size`

**Type:** Integer  
**Required:** For protocols that need it (e.g., SET operations)

Size of values in bytes.

```toml
[workload]
value_size = 1024  # 1KB values
```

## See Also

- [Transport Configuration](./transport.md)
- [Protocol Configuration](./protocol.md)
- [Traffic Groups](../cli-reference.md#traffic-groups)
