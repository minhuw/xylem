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
│  - xylem-echo   │  │                  │
└─────────────────┘  └──────────────────┘
```

## Design Principles

Xylem's architecture follows several core design principles. The system employs a modular design where each component has a well-defined responsibility: the CLI layer handles user interaction and TOML configuration parsing, the core engine orchestrates workload generation and statistics collection, protocol implementations handle message encoding/decoding, and the transport layer manages network communication.

The architecture enables composability through clean interfaces. Protocols and transports can be combined freely - Redis can run over TCP, UDP, or Unix domain sockets; HTTP over TCP or Unix sockets; and Memcached over any supported transport. This flexibility allows Xylem to adapt to different testing scenarios without code modifications.

## Key Components

### Core Engine

The core engine handles workload generation, connection management, statistics collection, and event loop orchestration.

### [Protocol Layer](./protocols.md)

Implements application protocols through request encoding, response parsing, and protocol state management.

### [Transport Layer](./transports.md)

Handles network communication including connection establishment, data transmission, and error handling.

## Data Flow

The system processes requests through a defined pipeline. Users provide workload configuration via TOML profile files. The core engine reads the configuration during initialization to instantiate the appropriate protocols and transports. During execution, the event loop generates requests according to the configured pattern and collects responses as they arrive. Statistics are gathered throughout the run, tracking latency, throughput, and error rates. Upon completion, results are formatted according to the output configuration and presented to the user.

## See Also

- [Protocol Layer](./protocols.md)
- [Transport Layer](./transports.md)
