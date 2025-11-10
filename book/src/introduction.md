# Introduction

Welcome to the **Xylem** documentation!

Xylem is a high-performance and modular traffic generator and measurement tool designed for RPC workloads. It provides a flexible framework for benchmarking and load testing distributed systems with different combinations of application protocols (e.g., Redis, HTTP, Memcached) and transport protocols (e.g., TCP, UDP, Unix Domain Socket).

## Key Features

- **Multi-Protocol Support**: Built-in support for Redis, HTTP, Memcached, Masstree, and xylem-echo protocols
- **Flexible Transport Layer**: Support for TCP, UDP, and Unix Domain Sockets
- **High Performance**: Efficient event-driven architecture for generating high loads
- **Reproducible**: Configuration-first design using TOML profiles ensures reproducibility
- **Detailed Metrics**: Latency measurements and statistics using various sketch algorithms
- **Multi-Threaded**: Support for thread affinity and multi-threaded workload generation

## Use Cases

Xylem is ideal for:

- **Performance Benchmarking**: Measure the performance of your RPC services under various load conditions
- **Load Testing**: Generate realistic traffic patterns to test system behavior under stress
- **Latency Analysis**: Collect detailed latency statistics to identify performance bottlenecks
- **Protocol Testing**: Validate protocol implementations across different transport layers
- **Capacity Planning**: Determine the maximum throughput and optimal configuration for your services

## Project Status

Xylem is actively developed and maintained. The project is licensed under MIT OR Apache-2.0.

## Getting Help

- **GitHub Issues**: Report bugs or request features at [github.com/minhuw/xylem/issues](https://github.com/minhuw/xylem/issues)
- **Source Code**: View the source code at [github.com/minhuw/xylem](https://github.com/minhuw/xylem)

## Next Steps

- Follow the [Quick Start guide](./getting-started/quick-start.md)
- Learn about [CLI Reference](./guide/cli-reference.md)
- Explore the [Architecture](./architecture/overview.md) to understand Xylem's design
