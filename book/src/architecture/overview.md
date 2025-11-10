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

### 1. Modularity

Each component has a single, well-defined responsibility:
- **CLI** - User interface and TOML configuration parsing
- **Core** - Workload orchestration, threading, and statistics
- **Protocols** - Application-level protocol implementation
- **Transports** - Network communication layer

### 2. Composability

Protocols and transports can be mixed and matched:
- Redis over TCP
- Redis over UDP
- HTTP over TCP
- HTTP over Unix sockets
- Memcached over TCP

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

The heart of Xylem, responsible for:
- Workload generation
- Connection management
- Statistics collection
- Event loop orchestration

### [Protocol Layer](./protocols.md)

Implements application protocols:
- Request encoding
- Response parsing
- Protocol state management

### [Transport Layer](./transports.md)

Handles network communication:
- Connection establishment
- Data transmission
- Error handling
- Security (TLS)

## Data Flow

1. **Configuration** - User provides workload configuration
2. **Initialization** - Core engine sets up protocols and transports
3. **Execution** - Event loop generates requests and collects responses
4. **Collection** - Statistics are gathered continuously
5. **Reporting** - Final results are formatted and displayed

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
