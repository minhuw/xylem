# Benchmarking Redis with Xylem

When you're benchmarking Redis, you're speaking RESP—the Redis Serialization Protocol. It's the language Redis uses to understand your commands and send back responses. This guide walks you through how RESP works, what commands Xylem knows how to speak, and how to craft realistic Redis workloads for your benchmarks.

## Understanding RESP

RESP is elegantly simple. Commands look almost like what you'd type at a Redis CLI, but they're formatted in a way that machines can parse efficiently. Everything is text-based—printable ASCII with CRLF line endings—which makes debugging easy. You can literally read RESP messages in a network capture.

But don't let the text format fool you. RESP is binary-safe. When you send a value, you tell Redis exactly how many bytes to expect, so you can store JPEGs, protocol buffers, or any binary blob without escaping special characters. Redis doesn't care what's in your data; it just reads the exact number of bytes you promised and sends it back when you ask for it.

The communication pattern is straightforward: you send a command, Redis sends back a response. One command, one response. But here's the clever part: you don't have to wait for the response before sending the next command. This is called pipelining, and it's how you get serious throughput. Send a batch of GET commands back-to-back, and Redis will send back a batch of responses in the same order. No request IDs needed—FIFO ordering per connection keeps everything straight.

Each command stands alone. Redis doesn't maintain conversation state (except for transactions and pub/sub, which are special cases). This statelessness is what makes Redis fast and scalable.

### The Wire Format

RESP speaks in five types, each starting with a special character:

**Simple strings** (`+`) are for short status messages. When you SET a key, Redis responds with `+OK\r\n`. That's it—a plus sign, the message, and a newline.

**Errors** (`-`) look similar but start with a minus. `-ERR unknown command\r\n` tells you something went wrong. The distinction between simple strings and errors lets clients handle success and failure differently without parsing the message content.

**Integers** (`:`) are for numeric results. When you INCR a counter, Redis might send back `:42\r\n`—that's the new count. The colon says "interpret what follows as a number."

**Bulk strings** (`$`) are where it gets interesting. These can hold any binary data of any size. The format is `$<length>\r\n<data>\r\n`. So `$5\r\nhello\r\n` is a 5-byte string containing "hello". The length-prefix is what makes RESP binary-safe—Redis knows exactly how many bytes to read, regardless of what's in them. A null value is `$-1\r\n`, which is how Redis says "this key doesn't exist."

**Arrays** (`*`) let you send or receive multiple values. The format is `*<count>\r\n` followed by that many elements. Each element can be any RESP type—strings, integers, even nested arrays. This is how commands work: `GET key:1234` becomes `*2\r\n$3\r\nGET\r\n$8\r\nkey:1234\r\n`—an array of two bulk strings.

RESP3 adds more types: true nulls (`_\r\n`), booleans (`#t\r\n` or `#f\r\n`), doubles for floating-point numbers (`,3.14\r\n`), maps (`%<count>\r\n`) for key-value pairs, sets (`~<count>\r\n`) for unique collections, and a few others. These semantic types make client libraries simpler because they don't have to guess whether a bulk string is a number or text.

### Commands on the Wire

Every Redis command is an array of bulk strings. Even simple ones. `GET key:1234` becomes:
```
*2\r\n           # Array with 2 elements
$3\r\nGET\r\n    # First element: "GET" (3 bytes)
$8\r\nkey:1234\r\n  # Second element: "key:1234" (8 bytes)
```

SET looks similar but with three elements:
```
*3\r\n                    # Array with 3 elements
$3\r\nSET\r\n            # Command
$8\r\nkey:1234\r\n       # Key
$128\r\n<128 bytes>\r\n   # Value
```

Responses match what the command returns. A successful GET sends back the value as a bulk string. A missing key sends `$-1\r\n`. SET responds with `+OK\r\n`. INCR returns an integer like `:43\r\n`.

### Pipelining: The Secret to Throughput

Here's where Redis shines. Instead of this:
1. Send GET key1
2. Wait for response
3. Send GET key2
4. Wait for response

You do this:
1. Send GET key1, GET key2, GET key3
2. Read three responses

The latency savings are dramatic. If each round-trip takes 1ms, the first approach takes 2ms for two GETs. Pipelining takes just over 1ms total. With 10 keys, you go from 10ms to ~1ms. The more you pipeline, the closer you get to wire speed.

Redis guarantees responses come back in the same order you sent requests, per connection. You don't need to tag requests with IDs—just keep track of how many you've sent and match them up with responses as they arrive. This is what Xylem does automatically with the `max_pending_per_connection` setting.

## The Commands

Let's talk about what these commands actually do and when you'd use them in benchmarks.

**GET** retrieves a value. It's your bread-and-butter read operation. Benchmark GET to measure read latency, cache hit rates, and how Redis handles concurrent readers. When the key doesn't exist, you get back a null (`$-1\r\n`).

**SET** stores a value. This is your write operation. Redis overwrites any existing value, so SET is idempotent—run it twice and you get the same result. Benchmark SET to test write throughput, persistence overhead (if you have AOF or RDB enabled), and replication lag.

**SETEX** combines SET with an expiration time. `SETEX session:abc123 1800 <data>` stores the session and tells Redis to delete it in 30 minutes. This is atomic—no race between SET and EXPIRE. Use it to benchmark TTL accuracy, eviction behavior, and how expiring keys affect memory management.

**INCR** atomically increments a counter. If the key doesn't exist, Redis treats it as zero and increments to one. This is crucial for testing atomic operations, counters (like rate limiters or page view trackers), and how Redis handles numeric operations under concurrency.

**MGET** fetches multiple keys in one command: `MGET key:1 key:2 key:3`. The response is an array of values (or nulls for missing keys). Use this to benchmark batch operations and see if your network round-trip time dominates your latency.

**WAIT** blocks until previous writes replicate to N replicas or a timeout expires: `WAIT 2 1000` waits for 2 replicas with a 1-second timeout. The response is how many replicas acknowledged. This lets you benchmark replication lag and consistency guarantees.

**SETRANGE** and **GETRANGE** work on offsets within a value. `SETRANGE key:1000 5 hello` overwrites 5 bytes starting at offset 5. `GETRANGE key:1000 5 -1` reads from offset 5 to the end. These are useful for large-value workloads where you only modify or read part of the data—think partial updates to binary structures or reading substrings from log entries.

**AUTH** authenticates your connection. With simple password auth, it's `AUTH mypassword`. With Redis 6.0+ ACL, it's `AUTH username password`. You'll need this to benchmark production-like setups where Redis isn't publicly accessible.

**SELECT** switches databases. Redis has 16 logical databases by default (numbered 0-15). `SELECT 5` moves to database 5. Keys in different databases are isolated. Use this to test multi-tenant scenarios or benchmark how database switching affects performance.

**HELLO** negotiates the protocol version. `HELLO 2` sticks with RESP2. `HELLO 3` upgrades to RESP3. The response includes server version, supported features, and other metadata. Benchmark this if you care about RESP3 adoption or client compatibility.

**CLUSTER SLOTS** returns the cluster topology: which slots are assigned to which nodes. The response is a nested array with start slot, end slot, and node addresses for each range. Use this to benchmark topology discovery and test how clients handle cluster reconfiguration.

### Custom Commands via Templates

Xylem's command templates are your escape hatch for anything not built-in. Want to test sorted sets? Write:
```toml
[[traffic_groups.protocol_config.operations.commands]]
name = "custom"
template = "ZADD leaderboard __value_size__ player:__key__"
```

Xylem replaces `__key__` with the current key, `__value_size__` with the value size, and `__data__` with generated data. The template becomes a proper RESP array. You can benchmark HSET, LPUSH, SADD, GEOADD—anything Redis supports.

## Building Realistic Workloads

Real applications don't just GET and SET uniformly distributed keys. They have patterns: hot keys, read-heavy ratios, varied value sizes, bursts of traffic. Xylem lets you model all of this.

### Command Mixes

All workload configuration goes inside each traffic group's `protocol_config` section. Start simple. A pure GET workload tests read performance:
```toml
[traffic_groups.protocol_config.operations]
strategy = "fixed"
operation = "get"
```

But production is rarely pure reads. Model a cache with 70% reads and 30% writes:
```toml
[traffic_groups.protocol_config.operations]
strategy = "weighted"

[[traffic_groups.protocol_config.operations.commands]]
name = "get"
weight = 0.7

[[traffic_groups.protocol_config.operations.commands]]
name = "set"
weight = 0.3
```

Xylem picks commands randomly based on their weights. Over time, you'll see 70% GETs and 30% SETs.

Add INCR for counters, MGET for batch reads, and WAIT to test replication:
```toml
[[traffic_groups.protocol_config.operations.commands]]
name = "incr"
weight = 0.1

[[traffic_groups.protocol_config.operations.commands]]
name = "mget"
weight = 0.05
params = { count = 10 }  # Fetch 10 keys per MGET

[[traffic_groups.protocol_config.operations.commands]]
name = "wait"
weight = 0.02
params = { num_replicas = 2, timeout_ms = 1000 }
```

Now you're testing a realistic mix: mostly GETs, some SETs, occasional counter increments, batch reads, and periodic replication checks.

### Key Distributions

Uniform random keys are easy to understand but unrealistic. Real workloads have hot keys—some data gets accessed way more than others.

**Zipfian** distribution models this. With a Zipfian exponent of 0.99, a tiny fraction of keys get most of the traffic:
```toml
[traffic_groups.protocol_config.keys]
strategy = "zipfian"
n = 1000000
theta = 0.99
value_size = 64
```

This is perfect for CDN caching (some content is always popular), user sessions (active users generate more requests), or product catalogs (bestsellers dominate).

**Gaussian** (normal) distribution clusters keys around a mean. This models temporal locality—recent data is hot, older data cools off:
```toml
[traffic_groups.protocol_config.keys]
strategy = "gaussian"
mean_pct = 0.5  # Center at 50% of key space
std_dev_pct = 0.1  # 10% spread
max = 10000
value_size = 64
```

Use this for time-series data, sliding windows, or any scenario where "recent" matters.

**Sequential** access walks through keys in order. Good for range scans or import/export workloads:
```toml
[traffic_groups.protocol_config.keys]
strategy = "sequential"
start = 0
value_size = 64
```

**Random** is truly uniform. Every key has equal probability. This is your baseline for understanding Redis's raw performance without hot keys skewing results:
```toml
[traffic_groups.protocol_config.keys]
strategy = "random"
max = 100000
value_size = 64
```

### Value Sizes

The `value_size` in keys configuration sets a fixed size. For variable sizes, add a separate `value_size` section:
```toml
[traffic_groups.protocol_config.value_size]
strategy = "fixed"
size = 128  # All values are 128 bytes
```

But production has variation. Some cache entries are tiny (user preferences), others are huge (HTML fragments). Model this with distributions:
```toml
[traffic_groups.protocol_config.value_size]
strategy = "normal"
mean = 512.0
std_dev = 128.0
min = 64
max = 4096
```

Or use uniform distribution for evenly spread sizes:
```toml
[traffic_groups.protocol_config.value_size]
strategy = "uniform"
min = 256
max = 2048
```

This models scenarios like caching API responses (small reads) and storing rendered pages (large writes).

## Cluster Benchmarking

When you're testing Redis Cluster, you're really testing how well your client handles distributed systems. Redis Cluster shards data across nodes using hash slots. There are 16,384 slots, and every key hashes to one of them. Each node owns a range of slots. When you send a command to the wrong node, Redis tells you where to go with a MOVED or ASK redirect.

Xylem handles this for you. You tell it about your cluster topology—which nodes own which slots—and it routes every request to the right node from the start. No trial and error, no unnecessary redirects.

Here's the setup:
```rust
use xylem_protocols::*;

// Create a cluster-aware protocol
let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
let mut protocol = RedisClusterProtocol::new(selector);

// Register your nodes
protocol.register_connection("127.0.0.1:7000".parse()?, 0);
protocol.register_connection("127.0.0.1:7001".parse()?, 1);
protocol.register_connection("127.0.0.1:7002".parse()?, 2);

// Define the topology
let mut ranges = vec![];
for i in 0..3 {
    let (start, end) = calculate_slot_range(i, 3)?;
    let master = format!("127.0.0.1:{}", 7000 + i).parse()?;
    ranges.push(SlotRange { start, end, master, replicas: vec![] });
}
protocol.update_topology(ClusterTopology::from_slot_ranges(ranges));

// Now every request goes to the right node
let (request, req_id) = protocol.generate_request(0, key, value_size);
```

The slot calculation is CRC16-XMODEM of the key. Xylem does a binary search over slot ranges to find the owner—O(log N) in the number of ranges, but with typical cluster sizes (3-10 nodes), this is effectively free.

### Hash Tags for Multi-Key Operations

Sometimes you need multiple keys on the same node. MGET won't work across nodes—Redis can't fetch `{key:1, key:2, key:3}` if they're on different shards. Hash tags solve this.

When you use curly braces in a key name, Redis only hashes what's inside the braces:
```rust
let key1 = "{user:1000}.profile";
let key2 = "{user:1000}.settings";
let key3 = "{user:1000}.preferences";
```

All three hash to the same slot because they share `{user:1000}`. Now you can MGET them together, or use MULTI/EXEC for atomic multi-key operations.

### Handling Redirects

Even with perfect topology knowledge, clusters reshard. Slots migrate between nodes during scale-up, rebalancing, or failover. When you hit a moving slot, Redis sends a redirect.

**MOVED** means the slot permanently moved. Update your topology and retry. MOVED is a signal to refresh your cluster map.

**ASK** means the slot is temporarily migrating. Retry on the new node (with an ASKING command first), but don't update your topology—the migration might not complete.

Xylem detects both. It parses the redirect response, extracts the target node address, and returns this info so you can decide: update topology, retry immediately, log the event, whatever makes sense for your benchmark.

## Common Workload Patterns

Let me paint some pictures of real-world workloads and how to model them.

**CDN Edge Cache**: 95% GETs, 5% SETs. Zipfian key distribution (popular content dominates). Small reads (64B for metadata), medium writes (512B average, normal distribution for HTML snippets and JSON). High throughput, moderate pipelining. Watch for cache hit rates and P99 latency on those few misses that go to origin.

**Session Store**: 70% GETs, 30% SETs. Gaussian distribution (recent sessions are hot). Small uniform values (256-512B for session tokens and user state). Low latency is critical—sessions are in the critical path. Use SETEX with realistic TTLs (30-60 minutes) and measure how eviction affects memory and performance.

**Rate Limiter**: 50% GET (check current count), 50% INCR (increment count). Random or round-robin keys (different users/IPs). Tiny values (just counters). Extremely high throughput, short TTLs (60 seconds for sliding windows). Focus on atomic operation latency and throughput under contention.

**Write-Heavy Logging**: 80% SET, 20% GET. Sequential keys (time-ordered logs). Uniform value sizes (log entry length is predictable). High write throughput with pipelining. Test persistence overhead (AOF fsync), replication lag, and memory growth.

**E-Commerce Product Catalog**: 90% GET (browsing), 10% SET (inventory updates). Zipfian (bestsellers are hot). Medium values (1-4KB for product details with images). Add MGET for "customers who bought this also bought" features. Benchmark cache stampede behavior when popular items update.

## Wrapping Up

Redis benchmarking is about understanding your workload and translating it into RESP commands. Xylem gives you the tools: command mixes, key distributions, value sizes, pipelining control, cluster routing. The trick is using them to model what your production system actually does.

Start simple. Benchmark pure GETs and SETs with uniform keys to understand Redis's baseline. Then add complexity: hot keys with Zipfian distribution, mixed command ratios, varied value sizes. Pipeline aggressively to see maximum throughput. Test failure modes: what happens during resharding, replication lag, eviction under memory pressure?

Your benchmarks should tell a story. Not just "Redis can do 100K QPS," but "our session store can handle 10K users with P99 latency under 2ms, and we can lose a cluster node without dropping requests." That's the insight that lets you ship with confidence.

## See Also

- [Workload Configuration](../configuration/workload.md) - Detailed configuration guide
- [Configuration Schema](../../reference/schema.md) - Complete reference
- [Redis Documentation](https://redis.io/docs/) - Official Redis protocol documentation
- [Redis Cluster Tutorial](https://redis.io/docs/manual/scaling/) - Official cluster guide
