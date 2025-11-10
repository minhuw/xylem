# Workload Configuration

The workload configuration is where you define how Xylem generates load against your system. This is where theory meets practice - where you translate your understanding of production traffic into a reproducible benchmark. A well-crafted workload configuration captures the essential characteristics of real user behavior: which keys get accessed, how often different operations occur, and how data sizes vary across your application.

## The Structure of a Workload

Every workload in Xylem consists of three fundamental components that work together to simulate realistic traffic:

**Key distribution** determines which keys your benchmark accesses. Will you hit every key equally (uniform random), focus on a hot set (Zipfian), or model temporal locality where recent keys are hot (Gaussian)? This choice fundamentally shapes your cache hit rates and system behavior.

**Operations configuration** controls what your benchmark does with those keys. Real applications don't just perform one operation - they mix reads and writes, batch operations, and specialized commands in patterns that reflect business logic. You can model a read-heavy cache (90% GET), a balanced session store (70% GET, 30% SET), or even custom workloads using any Redis command.

**Value sizes** determine how much data each operation handles. Production systems rarely use fixed-size values - small cache keys, medium session data, large objects all coexist. Xylem lets you model this complexity, even configuring different sizes for different operations to match your actual data patterns.

Together, these three components create a workload that mirrors reality. Let's explore each in depth.

## Understanding Key Distributions

The way your application accesses keys has profound implications for performance. A cache with perfect hit rates under sequential access might struggle with random patterns. A system optimized for uniform load might behave differently when faced with hot keys. Xylem provides several distribution strategies, each modeling distinct real-world patterns.

### Sequential Keys: The Baseline

Sequential access is the simplest pattern - keys are accessed in order:

```toml
[workload.keys]
strategy = "sequential"
start = 0
max = 100000
value_size = 128
```

Every benchmark accesses keys 0, 1, 2, and so on up to 99,999. This pattern represents the best-case scenario for most systems: perfect predictability, excellent cache locality, and minimal memory pressure. While unrealistic for production, sequential access gives you a performance ceiling - the best your system can possibly do.

Use sequential keys when establishing baseline performance, comparing different configurations, or debugging issues. The predictability makes problems easier to isolate.

### Uniform Random: Pure Chaos

At the opposite extreme, uniform random access treats every key equally:

```toml
[workload.keys]
strategy = "random"
max = 1000000
value_size = 128
```

Each request has an equal chance of accessing any key from 0 to 999,999. This represents the worst case for caching: no locality, maximum memory pressure, and frequent cache misses. If sequential access shows you your ceiling, random access shows you your floor.

Random distributions are perfect for stress testing. They reveal capacity limits, expose memory management issues, and help you understand behavior under the worst possible access patterns. Use this when you need to establish how your system degrades under pressure.

### Round-Robin: Predictable Cycling

Round-robin access cycles through keys in order, wrapping back to the beginning:

```toml
[workload.keys]
strategy = "round-robin"
max = 100000
value_size = 128
```

This pattern accesses keys 0, 1, 2, ... 99,999, then immediately jumps back to 0. It's less common in production but useful when you need predictable, repeating patterns - perhaps for testing cache warm-up behavior or validating eviction policies.

### Zipfian Distribution: The Power Law

Real-world access patterns rarely distribute evenly. Some keys - popular content, active user accounts, frequently accessed configuration - receive far more traffic than others. Zipfian distribution models this fundamental characteristic of production systems:

```toml
[workload.keys]
strategy = "zipfian"
exponent = 0.99
max = 1000000
value_size = 128
```

The exponent controls how skewed the distribution is. At 0.99 (typical for real workloads), the pattern is dramatically skewed: the top 1% of keys might receive 80% of requests, while the bottom 50% collectively receive only a few percent. This mirrors what you see in production - think of popular YouTube videos, trending tweets, or frequently accessed products.

Zipfian is your go-to distribution for realistic cache testing. It reveals how well your system handles hot keys, how effective your caching strategy is, and whether you can maintain performance when traffic concentrates on a small key set. The pattern remains stable over time - if key 1000 is popular now, it stays popular throughout the benchmark.

### Gaussian Distribution: Temporal Locality

Sometimes hotness isn't about persistent popularity - it's about recency. Session stores, time-series databases, and log aggregators often exhibit temporal locality: recent data is hot, older data is cold. Gaussian distribution captures this pattern beautifully:

```toml
[workload.keys]
strategy = "gaussian"
mean_pct = 0.5      # Center of the hot zone
std_dev_pct = 0.1   # How concentrated
max = 10000
value_size = 128
```

The percentages make this intuitive. With `mean_pct = 0.5`, the hot spot centers at 50% of your keyspace - key 5000 in this example. The `std_dev_pct = 0.1` means the standard deviation is 10% of the keyspace (1000 keys). Using the standard rules for normal distributions:

- Roughly 68% of accesses hit the center ±1000 keys (4000-6000)
- About 95% hit the center ±2000 keys (3000-7000)
- Nearly all requests fall within ±3000 keys (2000-8000)

This creates a "warm zone" that moves through your keyspace. It's perfect for modeling recently written data being immediately read (think recent log entries), active user sessions clustering in a time window, or rolling analytics windows where you repeatedly access recent periods.

The key difference from Zipfian: Gaussian models temporal hotness (recent data is hot), while Zipfian models persistent hotness (popular data stays popular). Choose based on whether your hot set is stable (Zipfian) or shifts over time (Gaussian).

## Configuring Operations: What Your Benchmark Does

Real applications don't perform just one operation. They mix reads and writes in patterns that reflect business logic. A content delivery network might be 95% reads, while a logging system might be 80% writes. Xylem lets you model these patterns precisely.

### Single Operation for Simplicity

When you're establishing baselines or focusing on a specific operation's performance, use a fixed operation:

```toml
[workload.operations]
strategy = "fixed"
operation = "get"
```

This configuration generates only GET operations. It's simple, predictable, and useful when you want to isolate and measure one aspect of your system. Change to `"set"` for write testing, or `"incr"` for counter workloads.

### Weighted Operations for Realism

Production systems mix operations in characteristic ratios. Model these patterns with weighted configurations:

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

The weights define the probability of each operation. Xylem normalizes them automatically, so `weight = 0.7` and `weight = 70` work identically. Choose whatever feels natural - percentages often read more clearly.

Common patterns you'll encounter:

**Heavy cache workloads** (CDN, API response caching):
```toml
GET: 0.9-0.95, SET: 0.05-0.1
```

**Session stores** (authentication, user state):
```toml
GET: 0.7, SET: 0.3
```

**Counter systems** (rate limiting, metrics):
```toml
GET: 0.5, INCR: 0.5
```

**Write-heavy systems** (logging, event streams):
```toml
GET: 0.2, SET: 0.8
```

### Batch Operations with MGET

Many applications fetch multiple keys atomically for efficiency. Model this with MGET operations:

```toml
[[workload.operations.commands]]
name = "mget"
weight = 0.2
count = 10  # Ten keys per operation
```

Each MGET request fetches 10 consecutive keys, simulating how your application might retrieve a user's session along with related data, or fetch a time series of recent measurements. This helps you understand pipeline efficiency and batch operation performance.

### Testing Replication with WAIT

Running Redis with replication? The WAIT command helps you measure replication lag under load:

```toml
[[workload.operations.commands]]
name = "wait"
weight = 0.1
num_replicas = 2      # Wait for 2 replicas
timeout_ms = 1000     # Give up after 1 second
```

Include WAIT in your mix to understand consistency guarantees. How often does replication keep up? What's the latency cost of waiting for replicas? WAIT operations answer these questions.

### Custom Commands for Advanced Use Cases

Redis supports hundreds of commands for lists, sets, sorted sets, hashes, and more. Custom templates let you benchmark any operation:

```toml
[[workload.operations.commands]]
name = "custom"
weight = 0.1
template = "HSET sessions:__key__ data __data__"
```

The template system provides three placeholders:
- `__key__` - Current key from your distribution
- `__data__` - Random bytes sized per your value_size config
- `__value_size__` - Numeric size, useful for scores or counts

**Examples that model real workloads:**

Add timestamped events to a sorted set:
```toml
template = "ZADD events __value_size__ event:__key__"
```

Update session data with TTL:
```toml
template = "SETEX session:__key__ 1800 __data__"
```

Push to an activity log:
```toml
template = "LPUSH user:__key__:activity __data__"
```

Track active users:
```toml
template = "SADD active_now user:__key__"
```

## Modeling Value Size Variation

Real data doesn't come in uniform chunks. Cache keys might be 64 bytes, session tokens 256 bytes, and user profiles 2KB. Different operations handle different sizes. Xylem gives you sophisticated tools to model this reality.

### Fixed Sizes: The Known Quantity

When every value has the same size - or when you want consistency for benchmarking - use fixed sizes:

```toml
[workload.value_size]
strategy = "fixed"
size = 128
```

Every operation uses exactly 128 bytes. This is your baseline: consistent, repeatable, and predictable. Use fixed sizes when comparing configurations or establishing capacity limits.

### Uniform Distribution: Exploring a Range

Don't know your exact size distribution but want to test across a range? Uniform distribution gives every size equal probability:

```toml
[workload.value_size]
strategy = "uniform"
min = 64
max = 4096
```

Requests will use sizes anywhere from 64 bytes to 4KB, spread evenly. A request might use 64 bytes, the next 3876 bytes, another 512 bytes - every size in the range has equal probability. This is useful for stress testing memory allocation and fragmentation.

### Normal Distribution: Natural Clustering

Most real data clusters around a typical size. Session data averages 512 bytes with variation. Cache entries typically run 256 bytes but occasionally spike. Normal distribution models this natural clustering:

```toml
[workload.value_size]
strategy = "normal"
mean = 512.0
std_dev = 128.0
min = 64      # Floor
max = 4096    # Ceiling
```

The mean (512 bytes) is your typical value size. The standard deviation (128 bytes) controls spread. Using standard normal distribution rules:

- 68% of values fall within 384-640 bytes (±1σ)
- 95% fall within 256-768 bytes (±2σ)
- 99.7% fall within 128-896 bytes (±3σ)

The min/max bounds prevent extremes. Without them, the normal distribution could occasionally generate tiny (2-byte) or huge (50KB) values. The clamping keeps values practical while preserving the clustering behavior.

### Per-Command Sizes: Maximum Realism

Your most powerful tool is configuring sizes per operation type. Reads often fetch small keys or IDs, while writes store larger data:

```toml
[workload.value_size]
strategy = "per_command"
default = { strategy = "fixed", size = 256 }

[workload.value_size.commands.get]
distribution = "fixed"
size = 64  # GETs fetch small cache keys

[workload.value_size.commands.set]
distribution = "uniform"
min = 128
max = 1024  # SETs write varied data
```

Notice the terminology shift: per-command configs use `distribution` to distinguish them from the top-level `strategy`. Each command can have its own distribution with its own parameters.

The `default` configuration handles operations you haven't explicitly configured. If your workload includes INCR but you haven't specified a size, it uses the default.

**Real-world examples:**

Session store (small IDs, larger session data):
```toml
GET: fixed 64 bytes
SET: normal mean=512, σ=128
```

Content cache (tiny keys, variable content):
```toml
GET: fixed 32 bytes
SET: uniform 1KB-100KB
```

Analytics (small queries, large result sets):
```toml
GET: normal mean=2KB, σ=512
SET: uniform 10KB-1MB
```

## Time-Varying Load Patterns

Beyond key distributions, operations, and sizes, you can vary the overall request rate over time. This is your macro-level control, letting you model daily cycles, traffic spikes, or gradual ramp-ups.

### Constant Load

The simplest pattern maintains steady throughput:

```toml
[workload.pattern]
type = "constant"
rate = 10000.0  # Requests per second
```

Use constant patterns for baseline testing and capacity planning.

### Gradual Ramp-Up

Simulate scaling up or warming caches:

```toml
[workload.pattern]
type = "ramp"
start_rate = 1000.0
end_rate = 10000.0
```

Traffic increases linearly from 1K to 10K requests per second over your experiment duration.

### Traffic Spikes

Model sudden load increases:

```toml
[workload.pattern]
type = "spike"
base_rate = 5000.0
spike_rate = 20000.0
spike_start = "10s"
spike_duration = "5s"
```

Normal traffic at 5K RPS suddenly jumps to 20K RPS at the 10-second mark, holds for 5 seconds, then returns to baseline. Perfect for testing autoscaling, circuit breakers, and graceful degradation.

### Cyclical Patterns

Model daily traffic cycles:

```toml
[workload.pattern]
type = "sinusoidal"
min_rate = 5000.0
max_rate = 15000.0
period = "60s"
```

Traffic oscillates smoothly between 5K and 15K RPS, completing one full cycle every 60 seconds.

## Bringing It All Together

Let's build a complete workload that demonstrates these concepts. Imagine you're testing a session store that exhibits temporal locality (recent sessions are hot), mixed operations (reads dominate but writes are significant), and varied data sizes (small lookups, larger session data):

```toml
[workload]
# Temporal locality - recently created sessions get most traffic
[workload.keys]
strategy = "gaussian"
mean_pct = 0.6       # Hot spot toward recent sessions
std_dev_pct = 0.15   # Moderately concentrated
max = 10000
value_size = 512

# Read-heavy but significant writes
[workload.operations]
strategy = "weighted"

[[workload.operations.commands]]
name = "get"
weight = 0.7

[[workload.operations.commands]]
name = "set"
weight = 0.3

# Small reads (session ID lookups), varied writes (full session data)
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

# Constant baseline load
[workload.pattern]
type = "constant"
rate = 10000.0
```

This configuration captures essential characteristics: temporal hotness (Gaussian keys), realistic operation mix (70/30 read-write), and varied sizes matching actual data patterns. Running this gives you insights you can trust.

## Practical Wisdom

### Ensuring Reproducible Results

Randomness is essential for realistic workloads, but you need reproducible results. Set a seed:

```toml
[experiment]
seed = 42  # Any consistent value works
```

With the same seed, Xylem generates identical sequences of keys and sizes across runs. You can share configurations with colleagues knowing they'll see exactly the same workload pattern.

### Choosing Distributions Wisely

Your distribution choice fundamentally shapes results:

**Gaussian** when you have temporal locality:
- Session stores (recent sessions hot)
- Time-series databases (recent data accessed)
- Rolling analytics (current window active)

**Zipfian** when you have persistent hot keys:
- Content caching (popular items stay popular)
- User data (power users dominate traffic)
- E-commerce (bestsellers get most views)

**Random** when you need worst-case analysis:
- Capacity planning (maximum memory pressure)
- Stress testing (minimum cache effectiveness)
- Establishing performance floors

**Sequential** when you need predictability:
- Baseline measurements
- Configuration comparison
- Debugging and development

### Matching Real Traffic Patterns

Study your production metrics before configuring:

1. **Measure your read-write ratio** from application logs
2. **Sample actual value sizes** from your database
3. **Identify hot keys** through cache hit rate analysis
4. **Understand temporal patterns** in your access logs

Then translate these observations into configuration. A workload that mirrors production gives you results you can trust.

### Starting Simple, Adding Complexity

Begin with simple configurations:
- Fixed sizes
- Single operation type
- Sequential keys

Establish baselines, then add complexity:
- Add operation variety
- Introduce size variation
- Switch to realistic distributions

Each addition reveals new insights. Compare results to understand what each aspect contributes.

## See Also

- [Redis Protocol](../protocols/redis.md) - Detailed Redis operation guide
- [Transport Configuration](./transport.md) - Network settings
- [Protocol Configuration](./protocol.md) - Protocol selection
- [Configuration Schema](../../reference/schema.md) - Complete reference
