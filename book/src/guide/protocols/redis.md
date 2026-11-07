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

RESP3 adds more types: true nulls (`_\r\n`), booleans (`#t\r\n` or `#f\r\n`), doubles for floating-point numbers (`,3.14\r\n`), maps (`%<count>\r\n`) for key-value pairs, sets (`~<count>\r\n`) for unique collections, and a few others. A connection speaks RESP2 until it sends `HELLO 3`; see [Connection Setup](#connection-setup-auth-select-and-hello). These semantic types make client libraries simpler because they don't have to guess whether a bulk string is a number or text.

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

A profile can select these commands by name: `get`, `set`, `incr`, `mget`, `setex`, `setrange`, `getrange`, `scan`, `wait`, `multi`, `exec`, `discard`, and `custom` for a command template. Names are case-insensitive. Every other Redis command goes through a template; see [Custom Commands via Templates](#custom-commands-via-templates).

Let's talk about what these commands actually do and when you'd use them in benchmarks.

**GET** retrieves a value. It's your bread-and-butter read operation. Benchmark GET to measure read latency, cache hit rates, and how Redis handles concurrent readers. When the key doesn't exist, you get back a null (`$-1\r\n`).

**SET** stores a value. This is your write operation. Redis overwrites any existing value, so SET is idempotent—run it twice and you get the same result. Benchmark SET to test write throughput, persistence overhead (if you have AOF or RDB enabled), and replication lag.

**SETEX** combines SET with an expiration time. `SETEX session:abc123 1800 <data>` stores the session and tells Redis to delete it in 30 minutes. This is atomic—no race between SET and EXPIRE. Use it to benchmark TTL accuracy, eviction behavior, and how expiring keys affect memory management.

**INCR** atomically increments a counter. If the key doesn't exist, Redis treats it as zero and increments to one. This is crucial for testing atomic operations, counters (like rate limiters or page view trackers), and how Redis handles numeric operations under concurrency.

**MGET** fetches multiple keys in one command: `MGET key:1 key:2 key:3`. The response is an array of values (or nulls for missing keys). Use this to benchmark batch operations and see if your network round-trip time dominates your latency.

**WAIT** blocks until previous writes replicate to N replicas or a timeout expires: `WAIT 2 1000` waits for 2 replicas with a 1-second timeout. The response is how many replicas acknowledged. This lets you benchmark replication lag and consistency guarantees.

**SETRANGE** and **GETRANGE** work on offsets within a value. `SETRANGE key:1000 5 hello` overwrites 5 bytes starting at offset 5. `GETRANGE key:1000 5 -1` reads from offset 5 to the end. These are useful for large-value workloads where you only modify or read part of the data—think partial updates to binary structures or reading substrings from log entries.

**SCAN** iterates over the keyspace: `SCAN 0 COUNT 100 MATCH key:*`. Set `cursor`, `count` and `pattern` with `params = { cursor = 0, count = 100, pattern = "key:*" }`; each request sends the same cursor. Use it to measure the cost of keyspace iteration alongside regular traffic.

**MULTI**, **EXEC** and **DISCARD** start, commit and abort a transaction. Each is sent as a separate weighted command, so commands picked between them are queued and answered with `+QUEUED`.

### Custom Commands via Templates

Command templates send any Redis command the profile can't select by name: DEL, MSET, hashes, lists, sets, sorted sets, CLUSTER SLOTS, GEOADD, and so on. A template is a `custom` command with `params.template`:
```toml
[[traffic_groups.protocol_config.operations.commands]]
name = "custom"
weight = 0.1
params = { template = "ZADD leaderboard __value_size__ __key__" }
```

The template is split on whitespace into the arguments of a RESP array. Xylem replaces `__key__` with the current key (including the key prefix, so `key:42`), `__data__` with a generated value, and `__value_size__` with the value size as a number. A variable is always a whole argument: `player:__key__` sends two arguments, `player:` and `key:42`. So a key can't carry a type prefix such as `list:`. Use a fixed collection name and put `__key__` in a member or field position, or give the data structure commands their own keyspace by putting them in a separate traffic group with a different `keys.prefix`.

Templates for common commands:

| Data type | Templates |
|-----------|-----------|
| Keys | `DEL __key__`, `MSET __key__ __data__ other __data__`, `EXPIRE __key__ 60` |
| Hash | `HSET __key__ field __data__` (one hash per key), `HSET users __key__ __data__` (one shared hash), `HGET __key__ field`, `HGETALL __key__`, `HDEL __key__ field` |
| List | `LPUSH queue __data__`, `RPUSH queue __data__`, `LPOP queue`, `LRANGE queue 0 9`, `LLEN queue` |
| Set | `SADD tags __key__`, `SREM tags __key__`, `SISMEMBER tags __key__`, `SMEMBERS tags`, `SCARD tags` |
| Sorted set | `ZADD leaderboard __value_size__ __key__`, `ZSCORE leaderboard __key__`, `ZRANGE leaderboard 0 9 WITHSCORES`, `ZREM leaderboard __key__`, `ZCARD leaderboard` |
| Cluster | `CLUSTER SLOTS` |

Replies are framed generically, so integers, bulk strings, nils, arrays, nested arrays and RESP3 types all count as one completed request. An error reply, such as `WRONGTYPE` when a template uses a key that already holds another type, counts as a failure. Watch for this when templates and `get`/`set` draw from the same keyspace.

## Connection Setup: AUTH, SELECT and HELLO

AUTH, SELECT and HELLO configure a connection rather than exercise the server, so Xylem never puts them in the command mix. You set them in the traffic group's `protocol_config`, and every connection sends them once, right after it connects and before any insert-phase or measured request:

```toml
[traffic_groups.protocol_config]
auth = { password = "secret" }   # AUTH secret (requirepass)
db = 3                           # SELECT 3
resp = 3                         # HELLO 3
```

**`auth`** authenticates the connection. With only a password, Xylem sends `AUTH <password>`, which checks the server's `requirepass`. Add a username for a Redis 6+ ACL user, and Xylem sends `AUTH <username> <password>`. To keep the password out of the profile, name an environment variable instead with `password_env`; Xylem reads it when the run starts and stops with an error naming the variable if it is not set. Set exactly one of `password` and `password_env`:

```toml
auth = { username = "bench", password = "secret" }
auth = { username = "bench", password_env = "REDIS_PASSWORD" }
```

**`db`** selects a logical database with `SELECT <db>`. Without it, connections stay in the server's default database 0. The server decides how many databases exist (16 by default).

**`resp`** picks the protocol version: 2 (the default, so no HELLO) or 3, which sends `HELLO 3`. Xylem frames every RESP3 reply type, including maps, sets, doubles, booleans, big numbers, verbatim strings and attributes. Out-of-band push messages, such as client-tracking invalidations, answer no request: Xylem skips them instead of matching them to a pending request.

The setup commands go out one at a time, in the order AUTH, SELECT, HELLO, each waiting for its reply. Separate commands, rather than a combined `HELLO 3 AUTH user pass`, keep AUTH and SELECT working on servers without HELLO, and tell you which step failed. Setup requests are warmup: they count toward no latency, throughput, byte or request count, never appear in the request dump, and do not use up a rate-limited connection's schedule, which starts once setup completes. An error reply to any of them, such as `WRONGPASS`, `NOAUTH`, `NOPERM`, an out-of-range database or an unknown HELLO, stops the run with an error naming the command and connection. Xylem never writes the password to its output, logs or errors.

A complete profile for a password-protected server:

```toml
[experiment]
name = "redis-auth"
duration = "30s"

[[traffic_groups]]
name = "bench"
protocol = "redis"
target = "127.0.0.1:6379"
threads = [0]
connections_per_thread = 8
max_pending_per_connection = 4

[traffic_groups.protocol_config]
auth = { username = "bench", password = "secret" }   # or password_env = "REDIS_PASSWORD"
db = 3

[traffic_groups.protocol_config.keys]
strategy = "random"
max = 100000
value_size = 64

[traffic_groups.protocol_config.operations]
strategy = "fixed"
operation = "get"

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "results/redis-auth.json"
```

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

## Common Workload Patterns

Let me paint some pictures of real-world workloads and how to model them.

**CDN Edge Cache**: 95% GETs, 5% SETs. Zipfian key distribution (popular content dominates). Small reads (64B for metadata), medium writes (512B average, normal distribution for HTML snippets and JSON). High throughput, moderate pipelining. Watch for cache hit rates and P99 latency on those few misses that go to origin.

**Session Store**: 70% GETs, 30% SETs. Gaussian distribution (recent sessions are hot). Small uniform values (256-512B for session tokens and user state). Low latency is critical—sessions are in the critical path. Use SETEX with realistic TTLs (30-60 minutes) and measure how eviction affects memory and performance.

**Rate Limiter**: 50% GET (check current count), 50% INCR (increment count). Random or round-robin keys (different users/IPs). Tiny values (just counters). Extremely high throughput, short TTLs (60 seconds for sliding windows). Focus on atomic operation latency and throughput under contention.

**Write-Heavy Logging**: 80% SET, 20% GET. Sequential keys (time-ordered logs). Uniform value sizes (log entry length is predictable). High write throughput with pipelining. Test persistence overhead (AOF fsync), replication lag, and memory growth.

**E-Commerce Product Catalog**: 90% GET (browsing), 10% SET (inventory updates). Zipfian (bestsellers are hot). Medium values (1-4KB for product details with images). Add MGET for "customers who bought this also bought" features. Benchmark cache stampede behavior when popular items update.

## Wrapping Up

Redis benchmarking is about understanding your workload and translating it into RESP commands. Xylem gives you the tools: command mixes, key distributions, value sizes, and pipelining control. The trick is using them to model what your production system actually does.

Start simple. Benchmark pure GETs and SETs with uniform keys to understand Redis's baseline. Then add complexity: hot keys with Zipfian distribution, mixed command ratios, varied value sizes. Pipeline aggressively to see maximum throughput. Test failure modes: what happens during resharding, replication lag, eviction under memory pressure?

Your benchmarks should tell a story. Not just "Redis can do 100K QPS," but "our session store can handle 10K users with P99 latency under 2ms, and we can lose a replica without dropping requests." That's the insight that lets you ship with confidence.

## See Also

- [Workload Configuration](../configuration/workload.md) - Detailed configuration guide
- [Configuration Schema](../../reference/schema.md) - Complete reference
- [Redis Documentation](https://redis.io/docs/) - Official Redis protocol documentation
