# Benchmarking Masstree with Xylem

When you're benchmarking Masstree, you're speaking MessagePack over TCP—a binary serialization format that's more compact than JSON but still type-safe. Masstree is a research database system designed for high-performance key-value storage with snapshot isolation and multi-version concurrency control. This guide covers the protocol mechanics, supported operations, and how to build workloads that test MVCC behavior and transactional semantics.

## Understanding the Protocol

Masstree's protocol is built on MessagePack, a binary format that's more compact than text-based protocols like RESP. Everything is strongly typed—strings are strings, integers are integers, maps are maps. There's no ambiguity about whether "42" is text or a number.

The communication pattern is straightforward: send a request array, get back a response array. Each request carries a sequence number that matches the response, enabling pipelining and response matching even when they arrive out of order (though Masstree typically preserves order per connection).

Masstree uses numeric command codes instead of text strings. GET is 2, REPLACE is 8, HANDSHAKE is 14. Responses use the command code plus one: GET response is 3, REPLACE response is 9. This makes parsing efficient without string comparisons.

### Connection Handshake

Every connection must handshake before sending operations. This negotiates connection parameters: which CPU core the connection should use (for thread pinning), maximum key length, and protocol version.

The handshake request looks like:
```rust
[0, 14, {"core": -1, "maxkeylen": 255}]
```

That's an array with three elements:
1. Sequence number 0 (handshakes always use seq 0)
2. Command code 14 (CMD_HANDSHAKE)
3. A map with parameters: core=-1 (auto-assign), maxkeylen=255

The server responds with:
```rust
[0, 15, true, 42, "Masstree-0.1"]
```

That's:
1. Sequence number 0
2. Response code 15 (CMD_HANDSHAKE + 1)
3. Success boolean
4. Maximum sequence number (server tells you when to wrap around)
5. Server version string

Xylem handles this automatically for each connection. You never see the handshake—it just works.

### Message Format

Every Masstree message is a MessagePack array. The structure depends on the command:

**GET request**: `[seq, 2, key_string]`
- Three elements: sequence number, command 2, the key

**GET response**: `[seq, 3, value_string]`
- Three elements: sequence number, response 3, the value (or error)

**SET (REPLACE) request**: `[seq, 8, key_string, value_string]`
- Four elements: sequence, command 8, key, value

**SET (REPLACE) response**: `[seq, 9, result_code]`
- Three elements: sequence, response 9, result (Inserted=1, Updated=2)

**PUT request**: `[seq, 6, key_string, col_idx, value, col_idx, value, ...]`
- Variable length: sequence, command 6, key, then pairs of column indices and values

**REMOVE request**: `[seq, 10, key_string]`
- Three elements: sequence, command 10, key

**SCAN request**: `[seq, 4, firstkey_string, count, field_idx, field_idx, ...]`
- Variable length: sequence, command 4, starting key, count, optional field indices

**SCAN response**: `[seq, 5, key1, value1, key2, value2, ...]`
- Variable length: sequence, response 5, then alternating keys and values

MessagePack handles the binary encoding. Strings are length-prefixed (so they're binary-safe like RESP bulk strings), integers use the most compact representation (1-5 bytes depending on size), and arrays/maps have efficient headers. This makes the wire format more compact than RESP for numeric-heavy workloads.

### Sequence Numbers and Pipelining

Each request has a 16-bit sequence number. The client picks the sequence, the server echoes it back. This enables pipelining—send multiple requests without waiting, then match responses by sequence number.

Xylem manages sequences per-connection automatically. It starts at 1 (0 is reserved for handshake), increments on each request, and wraps around at the server's max (typically 2^16-1).

Unlike Redis where responses come back in FIFO order, Masstree can reorder responses since they're tagged by sequence. Single-connection operations usually preserve order, but the protocol allows out-of-order completion for high concurrency scenarios.

## The Operations

Each operation serves specific benchmarking purposes.

**GET** retrieves a value by key. It's your basic read operation. Benchmark GET to measure read latency, snapshot isolation behavior, and how Masstree handles concurrent readers. If the key doesn't exist, you get a NotFound result code (-2).

**SET (REPLACE)** stores or updates a value. Masstree calls it REPLACE because it replaces any existing value atomically. This isn't an upsert in the SQL sense—it's a blind write that doesn't check the old value. Benchmark SET to test write throughput, MVCC overhead, and how concurrent writes interact with readers.

**PUT** updates specific columns within a value. Instead of reading the entire record, modifying a field, and writing it back (read-modify-write), you send only the column updates. For example, updating a user's last login timestamp without touching their profile data. The format is `[seq, 6, key, col0, val0, col1, val1, ...]`. Use this to benchmark partial updates and column-based storage performance.

**REMOVE** deletes a key. Unlike some systems, Masstree removal is immediate (within the current snapshot)—it doesn't wait for garbage collection. Benchmark this to test deletion throughput and how removed keys affect memory and performance.

**SCAN** performs range queries. You specify a starting key ("user:1000"), a count, and optionally which fields to retrieve. The response alternates keys and values: `[seq, 5, "user:1000", {data}, "user:1001", {data}, ...]`. This tests ordered access patterns like leaderboards, time-series queries, and scenarios where sorted iteration matters.

**CHECKPOINT** triggers a database snapshot. Masstree persists state by checkpointing to disk periodically. Calling CHECKPOINT explicitly forces this, which is useful for testing recovery time, checkpoint overhead, and consistency guarantees. The response is immediate, but the actual checkpoint happens asynchronously.

### Result Codes

Masstree responses include result codes that tell you what happened:

- **NotFound (-2)**: GET couldn't find the key
- **Retry (-1)**: Transient error, try again
- **OutOfDate (0)**: Version conflict (rare in basic workloads)
- **Inserted (1)**: SET created a new key
- **Updated (2)**: SET replaced an existing value
- **Found (3)**: GET succeeded
- **ScanDone (4)**: SCAN completed

These codes matter for correctness testing. If you're doing a read-your-writes test, you want to verify that SET returns Inserted or Updated, then GET returns Found with the same value.

## Building Realistic Workloads

Real database workloads exhibit patterns: read-heavy vs write-heavy ratios, hot keys vs uniform access, point queries vs range scans. Xylem provides configuration options to model these patterns.

### Operation Mixes

Start with pure operations. A GET-only workload tests read performance under snapshot isolation:

```toml
[protocol]
type = "masstree"
operation = "get"
```

But production databases handle mixed workloads. Model a typical application with 60% reads, 30% updates, 10% deletes:

```toml
[protocol]
type = "masstree"
operation = "get"  # Default operation

# Define a weighted mix
[workload.operations]
strategy = "weighted"

[[workload.operations.commands]]
name = "get"
weight = 0.6

[[workload.operations.commands]]
name = "set"
weight = 0.3

[[workload.operations.commands]]
name = "remove"
weight = 0.1
```

Add column-based updates to model partial record modifications:

```toml
[[workload.operations.commands]]
name = "put"
weight = 0.1
# Update columns 0 and 2 (e.g., timestamp and counter)
columns = [[0, "2024-11-12T10:30:00Z"], [2, "42"]]
```

Include range queries for analytics workloads:

```toml
[[workload.operations.commands]]
name = "scan"
weight = 0.05
firstkey = "user:0"
count = 100
fields = [0, 1]  # Return only fields 0 and 1
```

Trigger periodic checkpoints to test persistence overhead:

```toml
[[workload.operations.commands]]
name = "checkpoint"
weight = 0.001  # Once per thousand operations
```

Now you're testing a realistic mix: mostly reads, some writes, occasional range scans, and periodic checkpoints.

### Key Distributions

Uniform random keys rarely reflect production workloads. Real systems exhibit skew—some records are hot, others are cold.

**Zipfian** distribution models skewed access. With exponent 0.99, a small fraction of keys receive most of the traffic:

```toml
[workload.keys]
strategy = "zipfian"
exponent = 0.99
max = 1000000
```

This models user databases where active users dominate, e-commerce where bestsellers get most queries, or social networks where popular content drives traffic.

**Gaussian** (normal) distribution clusters keys around a center, modeling temporal locality where recent records are hot:

```toml
[workload.keys]
strategy = "gaussian"
mean_pct = 0.5  # Center of key space
std_dev_pct = 0.1
max = 10000
```

Use this for time-series data where recent timestamps matter, or any sliding-window workload.

**Sequential** access walks through keys in order. Perfect for testing SCAN performance or bulk imports:

```toml
[workload.keys]
strategy = "sequential"
start = 0
max = 100000
```

This is also how you test worst-case behavior in trees—sequential inserts can stress rebalancing.

### Value Sizes

Fixed-size values simplify analysis:

```toml
[workload.value_size]
strategy = "fixed"
size = 256  # All values are 256 bytes
```

But real databases have variation. Some records are small (user preferences), others are large (document content):

```toml
[workload.value_size]
strategy = "normal"
mean = 1024.0
std_dev = 256.0
min = 128
max = 8192
```

Or make sizes operation-specific. Small GETs (hot metadata), large SETs (batch updates):

```toml
[workload.value_size]
strategy = "per_command"

[workload.value_size.commands.get]
distribution = "fixed"
size = 128

[workload.value_size.commands.set]
distribution = "uniform"
min = 512
max = 4096
```

This models caching patterns where reads are metadata lookups and writes are full document stores.

## MVCC and Snapshot Isolation

Masstree's key differentiator is MVCC. Every read sees a consistent snapshot—a point-in-time view of the database. Even if concurrent transactions are modifying data, reads see the version that existed when the transaction started.

GET operations never block on writes, and writes never block reads. This provides true concurrency without read locks. The tradeoff is version maintenance: Masstree maintains multiple versions of each value, and garbage collection cleans up old versions periodically.

### Benchmarking MVCC Workloads

To test snapshot isolation, run concurrent readers and writers on the same keys:

```toml
[runtime]
num_threads = 16
connections_per_thread = 10

[workload.operations]
strategy = "weighted"

[[workload.operations.commands]]
name = "get"
weight = 0.7

[[workload.operations.commands]]
name = "set"
weight = 0.3

[workload.keys]
strategy = "zipfian"
exponent = 1.2  # Very hot keys
max = 1000
```

This creates contention on hot keys. Measure:
- **GET latency distribution**: Does P99 stay low even under write pressure?
- **SET throughput**: How many concurrent updates can Masstree handle?
- **Version chain length**: Are old versions accumulating?

You should see GET latencies remain stable even as SET rate increases, because readers don't block. If P99 GET latency spikes, you might be hitting garbage collection pauses or version chain traversal overhead.

### Read-Your-Writes Testing

Testing read-your-writes consistency verifies MVCC correctness. Write a value, then immediately read it back:

```rust
// Write
protocol.generate_request(conn_id, key, value_size);  // SET
// Read
protocol.generate_request(conn_id, key, 0);  // GET

// Verify response matches written value
```

With correct MVCC implementation, the GET always sees the SET's value, even if other concurrent transactions are modifying the same key.

## Common Workload Patterns

Here are typical workload patterns and how to model them.

**User Session Store**: 70% GET (check sessions), 20% SET (create/update), 10% REMOVE (logout/expire). Gaussian distribution (active users cluster), small uniform values (256-512B for session tokens). Use PUT to update just the "last_seen" timestamp without rewriting the entire session. Measure P99 latency and verify MVCC keeps reads fast during write spikes.

**E-Commerce Inventory**: 80% GET (check stock), 15% PUT (update counts), 5% SCAN (query ranges). Zipfian (popular products dominate), fixed value sizes (structured inventory records). Use PUT with column 0 for stock count. Test throughput under concurrent updates to the same SKU—MVCC should prevent read stalls.

**Time-Series Metrics**: 30% GET (recent values), 60% SET (new datapoints), 10% SCAN (range queries). Sequential or Gaussian keys (recent timestamps are hot), fixed sizes (metric tuples). High write rate with periodic SCAN to fetch time windows. Measure checkpoint overhead and verify SCAN performance with large result sets.

**Analytics Query Engine**: 20% GET (metadata), 10% SET (cache results), 70% SCAN (aggregations). Uniform distribution (analytics queries hit many keys), variable value sizes (small metadata, large aggregated results). Focus on SCAN throughput and latency, especially with large counts (1000+ records per SCAN).

**Transactional Workload (YCSB-style)**: Mix of GET, SET, REMOVE with Zipfian distribution, hot keys creating contention. This tests MVCC's core value proposition: can you sustain high throughput with strong isolation guarantees? Measure throughput vs latency tradeoff and compare with Redis's single-threaded model.

## Comparing Masstree to Redis

Choosing between Masstree and Redis for benchmarking depends on your consistency requirements.

**Use Masstree when**:
- You need snapshot isolation and MVCC semantics
- Your workload has concurrent readers and writers on the same keys
- You want to test column-based updates (partial record modifications)
- Range queries (SCAN) are a significant part of your workload
- You're benchmarking transaction processing systems

**Use Redis when**:
- You need raw speed above all else (single-threaded execution)
- Your workload is primarily read-heavy with occasional writes
- You want to test caching layers where eventual consistency is fine
- You need pub/sub, Lua scripting, or Redis modules
- You're benchmarking web session stores, rate limiters, or CDN caches

**Key differences**:

| Feature | Masstree | Redis |
|---------|----------|-------|
| **Concurrency** | Multi-threaded MVCC | Single-threaded (per core) |
| **Isolation** | Snapshot isolation | No isolation guarantees |
| **Protocol** | MessagePack (binary) | RESP (text-based) |
| **Updates** | Column-based PUT | Full-value SET only |
| **Range queries** | Native SCAN | Requires sorted sets |
| **Use case** | Transactional systems | Caching, pub/sub |

Masstree trades peak throughput for stronger consistency guarantees. Applications requiring "read committed" or "snapshot isolation" semantics should benchmark against Masstree rather than Redis.

## Advanced Patterns

### Range Query Tuning

SCAN performance depends on how much data you're fetching:

```toml
# Small scans (10 records)
[[workload.operations.commands]]
name = "scan"
firstkey = "user:0"
count = 10
fields = [0, 1]  # Only essential fields

# Large scans (1000 records)
[[workload.operations.commands]]
name = "scan"
firstkey = "metrics:2024-11-01"
count = 1000
fields = []  # All fields
```

Measure:
- **Latency vs count**: Does P99 grow linearly or worse?
- **Throughput impact**: Do large SCANs starve other operations?
- **Network overhead**: Are you transfer-bound with large counts?

Tuning SCAN is about balancing batch size (larger = more efficient) vs latency (larger = longer per-operation time).

### Checkpoint Overhead

Checkpoints serialize the database to disk, consuming time and I/O resources:

```toml
# Frequent checkpoints (high durability, low throughput)
[[workload.operations.commands]]
name = "checkpoint"
weight = 0.01  # 1% of operations

# Rare checkpoints (high throughput, lower durability)
[[workload.operations.commands]]
name = "checkpoint"
weight = 0.0001  # 0.01% of operations
```

Measure:
- **Latency during checkpoint**: Do other operations slow down?
- **Throughput degradation**: What's the sustained write rate with frequent checkpoints?
- **Recovery time**: How long to load a checkpoint on restart?

This helps you understand the durability vs performance tradeoff.

### Column-Based Update Efficiency

PUT operations are more efficient than read-modify-write cycles:

```toml
# Inefficient: GET + SET (read full record, modify, write back)
# This requires two network round-trips and full value transfer

# Efficient: PUT (update specific columns)
[[workload.operations.commands]]
name = "put"
columns = [[0, "new_value"]]  # Only update column 0
```

Benchmark the difference:
- **Latency**: PUT should be ~2x faster (one round-trip vs two)
- **Bandwidth**: PUT uses less network (only changed columns vs full record)
- **Contention**: PUT reduces lock time (Masstree only locks affected columns)

For structured data with frequent partial updates, PUT is the right model.

## Wrapping Up

Masstree benchmarking focuses on MVCC behavior and transactional semantics. Xylem provides the configuration primitives: operation mixes, key distributions, value sizes, column-based updates, range queries. The goal is modeling your actual database workload.

Start with baseline measurements. Benchmark pure GET and SET with uniform keys to understand Masstree's baseline performance. Then add realistic complexity: hot keys with Zipfian distribution, mixed operations with production ratios, column updates with PUT, range queries with SCAN. Pipeline aggressively since Masstree handles concurrency well, and watch for MVCC overhead—version chains, garbage collection, checkpoint pauses.

Your benchmarks should provide concrete insights. Not just "Masstree can do 50K QPS," but "our transactional system can handle 10K concurrent users with snapshot isolation, P99 latency under 5ms, and checkpoint every 10 seconds without stalling operations." This data determines whether MVCC overhead is justified for your use case.

## See Also

- [Workload Configuration](../configuration/workload.md) - Detailed configuration guide
- [Configuration Schema](../../reference/schema.md) - Complete reference
- [Masstree Paper](https://pdos.csail.mit.edu/papers/masstree:eurosys12.pdf) - Original research paper
- [MessagePack Format](https://msgpack.org/index.html) - Wire format specification
