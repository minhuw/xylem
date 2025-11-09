# Core Components

The core engine (`xylem-core`) is the heart of Xylem, orchestrating workload generation and statistics collection.

## Key Modules

### Workload Generator

Generates requests according to configured patterns:

```rust
pub trait WorkloadGenerator {
    fn next_request(&mut self) -> Option<Request>;
    fn is_complete(&self) -> bool;
}
```

**Implementations:**
- **FixedRate** - Constant request rate
- **Poisson** - Poisson-distributed arrivals
- **Burst** - Periodic bursts
- **Trace-based** - Replay from trace file

### Connection Manager

Manages connections to target services:

```rust
pub struct ConnectionPool {
    // Connection pool implementation
}
```

**Responsibilities:**
- Connection establishment
- Connection reuse
- Health checking
- Error recovery

### Event Loop

Event-driven I/O using `mio`:

```rust
pub struct EventLoop {
    poll: Poll,
    connections: HashMap<Token, Connection>,
    statistics: Statistics,
}
```

**Features:**
- Edge-triggered polling
- Efficient I/O multiplexing
- Non-blocking operations

### Statistics Collector

Collects and aggregates performance metrics:

```rust
pub struct Statistics {
    latency: LatencyHistogram,
    throughput: ThroughputCounter,
    errors: ErrorCounter,
}
```

**Metrics collected:**
- Request latency (various percentiles)
- Throughput (requests per second)
- Error rates
- Connection statistics

## Latency Measurement

Xylem uses multiple sketch algorithms for accurate latency measurement:

### DDSketch

Relative-error sketch for balanced accuracy:
```rust
pub struct DDSketchLatency {
    sketch: DDSketch,
    alpha: f64,  // Relative error
}
```

### T-Digest

Percentile estimation with high accuracy at extremes:
```rust
pub struct TDigestLatency {
    tdigest: TDigest,
}
```

### HDR Histogram

High dynamic range histogram:
```rust
pub struct HdrLatency {
    histogram: Histogram<u64>,
}
```

## Request Flow

```
1. WorkloadGenerator produces Request
2. EventLoop selects available Connection
3. Protocol encodes Request
4. Transport sends bytes
5. Transport receives response bytes
6. Protocol decodes Response
7. Statistics record latency
8. Repeat
```

## Configuration

Core components are configured via the workload configuration:

```json
{
  "workload": {
    "generator": "fixed_rate",
    "rate": 10000,
    "duration": "60s"
  },
  "statistics": {
    "sketch": "ddsketch",
    "alpha": 0.01
  }
}
```

## Performance Optimizations

### Memory Pooling

Reuse buffers to reduce allocations:
```rust
pub struct BufferPool {
    buffers: Vec<Vec<u8>>,
}
```

### Batch Processing

Process multiple events per poll:
```rust
loop {
    poll.poll(&mut events, timeout)?;
    for event in events.iter() {
        // Process event
    }
}
```

### Lock-Free Statistics

Use atomic operations for concurrent updates:
```rust
pub struct AtomicStatistics {
    count: AtomicU64,
    sum: AtomicU64,
}
```

## Error Handling

Comprehensive error handling with `anyhow` and `thiserror`:

```rust
#[derive(Error, Debug)]
pub enum CoreError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    
    #[error("Timeout exceeded")]
    Timeout,
    
    #[error("Protocol error: {0}")]
    ProtocolError(#[from] ProtocolError),
}
```

## See Also

- [Architecture Overview](./overview.md)
- [Protocol Layer](./protocols.md)
- [Performance Tuning](../advanced/performance.md)
