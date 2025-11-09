# Xylem

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/minhuw/xylem/actions/workflows/ci.yml/badge.svg)](https://github.com/minhuw/xylem/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/minhuw/xylem/branch/main/graph/badge.svg?token=OPF5R7DYXO)](https://codecov.io/gh/minhuw/xylem)

**Xylem** is a high-performance and modular traffic generator and measurement tool for RPC workloads built with different combinations of application **protocol** (e.g., redis, masstree, HTTP) and **transport** protocol (e.g., TCP, UDP, UNIX Domain Socket).

## Documentation

Comprehensive documentation is available in the `book/` directory. To build and view the documentation locally:

```bash
# Install mdBook
cargo install mdbook

# Build and serve the documentation
cd book
mdbook serve --open
```

The documentation includes:
- Getting Started guide
- User guide with CLI reference
- Architecture overview
- Protocol and transport documentation
- Examples and tutorials
- API reference

## Quick Start

```bash
# Build from source
cargo build --release

# Run a simple Redis benchmark
./target/release/xylem --protocol redis --transport tcp --host localhost --port 6379 --duration 10s --rate 1000
```

## License

MIT License - see [LICENSE](LICENSE) file for details.
