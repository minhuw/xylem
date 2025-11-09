# Introduction

Welcome to the **Xylem** documentation!

Xylem is a high-performance and modular traffic generator and measurement tool designed for RPC workloads. It provides a flexible framework for benchmarking and load testing distributed systems with different combinations of application protocols (e.g., Redis, HTTP, Memcached) and transport protocols (e.g., TCP, UDP, Unix Domain Socket, TLS).

## Key Features

- **Multi-Protocol Support**: Built-in support for Redis, HTTP, Memcached, and extensible for custom protocols
- **Flexible Transport Layer**: Support for TCP, UDP, Unix Domain Sockets, and TLS
- **High Performance**: Efficient event-driven architecture for generating high loads
- **Rich Metrics**: Detailed latency measurements and statistics using various sketch algorithms
- **Configurable Workloads**: JSON-based configuration for complex workload patterns
- **Modular Design**: Clean separation between protocol, transport, and core logic

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

- Learn how to [install Xylem](./getting-started/installation.md)
- Follow the [Quick Start guide](./getting-started/quick-start.md)
- Explore the [Architecture](./architecture/overview.md) to understand Xylem's design
