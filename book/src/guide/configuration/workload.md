# Workload Configuration

Define workload patterns, key distributions, and runtime parameters in the `[workload]` section of your profile.

## Basic Structure

```toml
[workload]
keys = { type = "zipfian", n = 10000, s = 0.99 }
value_size = 100
```

## Key Distribution

### `keys.type`

**Type:** String  
**Required:** Yes

The key distribution pattern to use.

**Options:**
- `"zipfian"` - Zipfian distribution (realistic cache workload with hot keys)
- `"uniform"` - Uniform random distribution
- `"sequential"` - Sequential key access

### Zipfian Distribution

```toml
[workload]
keys = { type = "zipfian", n = 100000, s = 0.99 }
```

**Parameters:**
- `n` - Number of keys in the key space
- `s` - Skew parameter (higher values = more skewed, typical: 0.99)

### Uniform Distribution

```toml
[workload]
keys = { type = "uniform", n = 100000 }
```

### Sequential Distribution

```toml
[workload]
keys = { type = "sequential", start = 0, n = 100000 }
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
