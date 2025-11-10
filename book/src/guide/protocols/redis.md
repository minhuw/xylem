# Redis Protocol

Redis uses the RESP (Redis Serialization Protocol) for client-server communication. This document explains the protocol format, common commands, and how Xylem supports benchmarking Redis workloads.

## What Xylem Supports

Xylem implements RESP and supports:
- Standard Redis commands (GET, SET, INCR, MGET, WAIT)
- Custom command templates for any Redis operation
- Request pipelining for high throughput
- Configurable key distributions and value sizes

Xylem does not currently support:
- RESP3 protocol (only RESP2) - **TODO**: RESP3 adds semantic types (maps, sets, doubles, booleans), push messages, and streaming data. While RESP2 covers all benchmarking needs and works with all Redis versions, RESP3 support could be added for testing RESP3-specific features or comparing protocol performance.
- Pub/Sub commands
- Cluster mode (supports single-instance and simple replication)
- Transactions (MULTI/EXEC)

## Protocol Overview

Redis commands are sent as text-based protocol messages. The key characteristics:

**Text-based** - Human-readable format using printable ASCII characters and CRLF line endings

**Binary-safe** - Bulk strings can contain any binary data with explicit length encoding

**Request-response** - Client sends command, server sends response (pipelining allows multiple pending requests)

**Stateless** - Each command is independent (except for transactions and pub/sub)

## RESP Wire Format

RESP (REdis Serialization Protocol) is Redis's wire protocol. All commands and responses use this format.

### RESP Data Types

RESP has five data types, each identified by its first byte:

**Simple Strings** - Start with `+`, terminated by `\r\n`
```
+OK\r\n
```
Used for status replies.

**Errors** - Start with `-`, terminated by `\r\n`
```
-ERR unknown command\r\n
```
Used for error messages.

**Integers** - Start with `:`, terminated by `\r\n`
```
:42\r\n
```
Used for numeric results (INCR, DECR, counter values).

**Bulk Strings** - Start with `$`, followed by length and data
```
$5\r\nhello\r\n
```
- `$5` - Length in bytes
- `hello` - Actual data
- `\r\n` - Terminator

Null bulk string (key not found):
```
$-1\r\n
```

**Arrays** - Start with `*`, followed by element count
```
*3\r\n$5\r\nhello\r\n$5\r\nworld\r\n:42\r\n
```
- `*3` - Array with 3 elements
- Three elements: two bulk strings and one integer

### Request Format

All Redis commands are sent as RESP arrays of bulk strings.

**GET command:**
```
*2\r\n$3\r\nGET\r\n$8\r\nkey:1234\r\n
```
- `*2` - Array with 2 elements
- `$3\r\nGET\r\n` - Command name (3 bytes)
- `$8\r\nkey:1234\r\n` - Key name (8 bytes)

**SET command:**
```
*3\r\n$3\r\nSET\r\n$8\r\nkey:1234\r\n$128\r\n<128 bytes>\r\n
```
- `*3` - Array with 3 elements
- Command: SET
- Key: key:1234
- Value: 128 bytes of data

**MGET command (multiple keys):**
```
*11\r\n$4\r\nMGET\r\n$8\r\nkey:1000\r\n$8\r\nkey:1001\r\n...<8 more keys>...\r\n
```
- `*11` - Command + 10 keys = 11 elements
- MGET followed by 10 bulk strings

**INCR command:**
```
*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n
```

### Response Format

Response type depends on the command:

**GET (successful):**
```
$128\r\n<128 bytes of data>\r\n
```
Returns bulk string with value.

**GET (key not found):**
```
$-1\r\n
```
Returns null bulk string.

**SET:**
```
+OK\r\n
```
Returns simple string for success.

**INCR:**
```
:43\r\n
```
Returns integer (new counter value).

**MGET:**
```
*3\r\n$5\r\nvalue1\r\n$5\r\nvalue2\r\n$-1\r\n
```
Returns array with:
- Two values (bulk strings)
- One null (missing key)

**WAIT:**
```
:2\r\n
```
Returns integer (number of replicas that acknowledged).

## Common Redis Commands

### GET key

Retrieves the value of a key.

**Request:** `GET key:1234`
**Response:** Bulk string with value, or null if key doesn't exist
**Use case:** Read operations, cache lookups

### SET key value [options]

Sets a key to hold a string value.

**Request:** `SET key:1234 <data>`
**Response:** `+OK` on success
**Options:** Can include EX/PX for expiration, NX/XX for conditional sets
**Use case:** Write operations, cache updates

### INCR key

Increments the integer value of a key by one.

**Request:** `INCR counter:visits`
**Response:** Integer (new value after increment)
**Behavior:** Creates key with value 0 if it doesn't exist, then increments
**Use case:** Counters, rate limiting, analytics

### DECR key

Decrements the integer value of a key by one.

**Request:** `DECR counter:stock`
**Response:** Integer (new value after decrement)
**Use case:** Inventory tracking, quota management

### MGET key [key ...]

Returns values of all specified keys.

**Request:** `MGET key:1 key:2 key:3`
**Response:** Array of bulk strings (null for missing keys)
**Efficiency:** Single round-trip for multiple keys
**Use case:** Batch reads, fetching related data

### WAIT numreplicas timeout

Blocks until all previous write commands are replicated to at least numreplicas.

**Request:** `WAIT 2 1000` (wait for 2 replicas, timeout 1000ms)
**Response:** Integer (number of replicas that acknowledged)
**Use case:** Testing replication lag, consistency verification
**Note:** Only meaningful in replicated Redis setups

### Custom Commands

Redis supports hundreds of commands for different data structures:

**Lists:** LPUSH, RPUSH, LPOP, RPOP, LRANGE
**Sets:** SADD, SREM, SMEMBERS, SINTER
**Sorted Sets:** ZADD, ZREM, ZRANGE, ZRANGEBYSCORE
**Hashes:** HSET, HGET, HMGET, HGETALL
**Strings:** APPEND, STRLEN, GETRANGE, SETRANGE

Xylem supports these through custom command templates (see Command Selection section below).

## Pipelining

Redis supports pipelining: sending multiple commands without waiting for responses.

**Benefits:**
- Reduces round-trip latency
- Increases throughput
- Efficient use of network bandwidth

**How it works:**
```
Client sends:    GET key1    GET key2    GET key3
                 ─────────────────────────────────>
Server responds: <value1>    <value2>    <value3>
                 <─────────────────────────────────
```

**Xylem support:** Configure `max_pending_per_connection` to control pipeline depth.

## Benchmarking with Xylem

Xylem allows you to configure various aspects of Redis workloads:

| Aspect | Candidate Choices | Config Key |
|--------|------------------|------------|
| **Command Selection** | `fixed`, `weighted` | `workload.operations.strategy` |
| **Commands** | `get`, `set`, `incr`, `mget`, `wait`, `custom` | `workload.operations.commands[].name` |
| **Key Distribution** | `sequential`, `random`, `round-robin`, `zipfian`, `gaussian` | `workload.keys.strategy` |
| **Value Size** | `fixed`, `uniform`, `normal`, `per_command` | `workload.value_size.strategy` |
| **Pipelining** | 1-N pending requests per connection | `traffic_groups[].max_pending_per_connection` |
| **Load Pattern** | `constant`, `ramp`, `spike`, `sinusoidal` | `workload.pattern.type` |

### Basic Configuration

```toml
[target]
protocol = "redis"
address = "127.0.0.1:6379"
transport = "tcp"
```

### Command Selection

**Single command:**
```toml
[workload.operations]
strategy = "fixed"
operation = "get"  # Options: get, set, incr
```

**Mixed workload with weighted distribution:**
```toml
[workload.operations]
strategy = "weighted"

[[workload.operations.commands]]
name = "get"
weight = 0.7  # 70% reads

[[workload.operations.commands]]
name = "set"
weight = 0.3  # 30% writes
```

**Batch operations (MGET):**
```toml
[[workload.operations.commands]]
name = "mget"
weight = 0.2
count = 10  # Fetch 10 keys per request
```

**Replication testing (WAIT):**
```toml
[[workload.operations.commands]]
name = "wait"
weight = 0.1
num_replicas = 2
timeout_ms = 1000
```

**Custom commands with templates:**
```toml
[[workload.operations.commands]]
name = "custom"
weight = 0.1
template = "ZADD leaderboard __value_size__ player:__key__"
```

Template variables: `__key__`, `__data__`, `__value_size__`

### Key Distribution

**Sequential access:**
```toml
[workload.keys]
strategy = "sequential"
start = 0
max = 100000
```

**Uniform random:**
```toml
[workload.keys]
strategy = "random"
max = 1000000
```

**Zipfian (hot-key patterns):**
```toml
[workload.keys]
strategy = "zipfian"
exponent = 0.99
max = 1000000
```

**Gaussian (temporal locality):**
```toml
[workload.keys]
strategy = "gaussian"
mean_pct = 0.5
std_dev_pct = 0.1
max = 10000
```

### Value Size Distribution

**Fixed size:**
```toml
[workload.value_size]
strategy = "fixed"
size = 128
```

**Uniform distribution:**
```toml
[workload.value_size]
strategy = "uniform"
min = 64
max = 4096
```

**Normal distribution:**
```toml
[workload.value_size]
strategy = "normal"
mean = 512.0
std_dev = 128.0
min = 64
max = 4096
```

**Per-command sizes:**
```toml
[workload.value_size]
strategy = "per_command"
default = { strategy = "fixed", size = 256 }

[workload.value_size.commands.get]
distribution = "fixed"
size = 64

[workload.value_size.commands.set]
distribution = "uniform"
min = 128
max = 1024
```

## Complete Example

```toml
[experiment]
name = "redis-realistic-workload"
description = "Mixed operations with hot keys and varied sizes"
duration = "60s"
seed = 42  # Reproducible results

[target]
address = "127.0.0.1:6379"
protocol = "redis"
transport = "tcp"

[workload]
# Hot-key pattern: Zipfian distribution
[workload.keys]
strategy = "zipfian"
exponent = 0.99
max = 1000000
value_size = 512

# 70% reads, 30% writes
[workload.operations]
strategy = "weighted"

[[workload.operations.commands]]
name = "get"
weight = 0.7

[[workload.operations.commands]]
name = "set"
weight = 0.3

# Per-command value sizes
[workload.value_size]
strategy = "per_command"
default = { strategy = "fixed", size = 256 }

[workload.value_size.commands.get]
distribution = "fixed"
size = 64

[workload.value_size.commands.set]
distribution = "normal"
mean = 512.0
std_dev = 128.0
min = 128
max = 2048

[workload.pattern]
type = "constant"
rate = 10000.0

[[traffic_groups]]
name = "redis-benchmark"
threads = [0]
connections_per_thread = 20
max_pending_per_connection = 10  # Pipelining depth

[traffic_groups.policy]
type = "closed-loop"

[traffic_groups.sampling_policy]
type = "limited"
rate = 1.0
max_samples = 100000

[output]
format = "json"
file = "results/redis-benchmark.json"
```

## Common Workload Patterns

**Cache workload (CDN, API responses):**
- Commands: 90-95% GET, 5-10% SET
- Keys: Zipfian (persistent hot content)
- Sizes: Small reads (64B), larger writes (256-1024B)

**Session store:**
- Commands: 70% GET, 30% SET
- Keys: Gaussian (temporal locality)
- Sizes: Small lookups (64B), varied session data (128-1024B)

**Counter system (rate limiting, analytics):**
- Commands: 50% GET, 50% INCR
- Keys: Random or uniform
- Sizes: Small fixed (64B)

**Write-heavy (logging, time-series):**
- Commands: 20-30% GET, 70-80% SET
- Keys: Sequential or temporal
- Sizes: Uniform or normal distribution

## See Also

- [Workload Configuration](../configuration/workload.md) - Detailed configuration guide
- [Configuration Schema](../../reference/schema.md) - Complete reference
- [Redis Documentation](https://redis.io/docs/) - Official Redis protocol documentation
