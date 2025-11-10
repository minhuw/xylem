# Architecture Overview

Xylem is designed with a modular, layered architecture that separates concerns and enables extensibility.

## High-Level Architecture

```
┌─────────────────────────────────────────┐
│           CLI Interface                 │
│         (xylem-cli)                     │
└─────────────────────────────────────────┘
                  │
┌─────────────────────────────────────────┐
│         Core Engine                     │
│       (xylem-core)                      │
│  - Workload Management                  │
│  - Statistics Collection                │
│  - Threading & Event Loop               │
└─────────────────────────────────────────┘
         │                  │
         ▼                  ▼
┌─────────────────┐  ┌──────────────────┐
│   Protocols     │  │   Transports     │
│ (xylem-protocols)│  │(xylem-transport) │
│  - Redis        │  │   - TCP          │
│  - HTTP         │  │   - UDP          │
│  - Memcached    │  │   - Unix Socket  │
│  - Masstree     │  │                  │
└─────────────────┘  └──────────────────┘
```

## Design Principles

Xylem's architecture is built on several core design principles that guide its implementation and ensure it meets the needs of high-performance latency measurement. The system is designed with modularity at its heart, where each component has a single, well-defined responsibility. The CLI layer handles user interaction and TOML configuration parsing, presenting a clean interface to users. The core engine orchestrates workload generation, manages threading, and collects statistics, serving as the central coordinator. Protocol implementations focus solely on encoding and decoding application-level messages, while the transport layer handles all network communication details.

This modular design enables composability, allowing protocols and transports to be freely mixed and matched. Users can run Redis over TCP for standard deployments, switch to UDP for lower latency when appropriate, or use Unix domain sockets for maximum performance in local testing. HTTP can run over TCP or Unix sockets depending on the deployment scenario, and Memcached can be tested over any transport the system supports. This flexibility allows Xylem to adapt to diverse testing requirements without code changes.

Performance is a fundamental consideration throughout the system. The implementation avoids unnecessary data copies wherever possible, using an event-driven architecture built on the `mio` library for efficient I/O multiplexing. Statistics collection uses lock-free data structures to minimize contention in multi-threaded scenarios, and memory allocation patterns are carefully designed to reduce overhead during high-throughput operations.

Finally, the system is built for extensibility through clean trait-based interfaces. Adding new protocols or transports involves implementing well-defined traits with clear contracts. The plugin architecture allows custom implementations to integrate seamlessly with the existing codebase, making it straightforward to extend Xylem for new use cases without modifying the core engine.

### 3. Performance

- **Zero-copy** where possible
- **Event-driven** architecture using `mio`
- **Lock-free** data structures for statistics
- **Efficient** memory allocation patterns

### 4. Extensibility

- Clean trait-based interfaces
- Easy to add new protocols
- Easy to add new transports
- Plugin architecture (planned)

## Key Components

### [Core Engine](./core.md)

The heart of Xylem, responsible for workload generation, connection management, statistics collection, and event loop orchestration.

### [Protocol Layer](./protocols.md)

Implements application protocols through request encoding, response parsing, and protocol state management.

### [Transport Layer](./transports.md)

Handles network communication including connection establishment, data transmission, and error handling.

## Data Flow

The system processes requests through a well-defined pipeline. Users provide workload configuration through TOML profile files, which the core engine reads during initialization to set up the appropriate protocols and transports. During execution, the event loop continuously generates requests according to the configured pattern and collects responses as they arrive. Statistics are gathered throughout the run, tracking latency, throughput, and error rates. When the benchmark completes, final results are formatted according to the output configuration and displayed to the user.

## Threading Model

Xylem uses a single-threaded event loop design:
- **Main thread** - Event loop and I/O multiplexing
- **Optional workers** - For CPU-intensive tasks (planned)

Benefits:
- No locking overhead
- Predictable performance
- Easier to debug

## Performance Characteristics

### Latency

- **Request latency** - Typically < 1μs overhead
- **Measurement overhead** - Minimal impact on results

### Throughput

- **Single connection** - Up to 100K+ req/s
- **Multiple connections** - Limited by network and target

### Memory

- **Fixed overhead** - ~10MB base
- **Per-connection** - ~4KB
- **Statistics** - Depends on sketch algorithm

## Next Steps

- Learn about [Core Components](./core.md)
- Understand [Protocol Layer](./protocols.md)
- Explore [Transport Layer](./transports.md)
