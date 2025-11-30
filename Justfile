# Xylem Justfile - Project automation commands
# Run 'just' or 'just --list' to see all available recipes

# Variables
BINARY_NAME := "xylem"
DEFAULT_REDIS_PORT := "6379"
DEFAULT_MEMCACHED_PORT := "11211"

# Default recipe - shows help
_default:
    @just --list

# Run all tests (unit + integration)
test:
    @echo "🚀 Running all tests (unit + integration)..."
    @echo "   Running unit tests first..."
    cargo nextest run --profile unit --workspace
    @echo "   Running integration tests (serially to avoid Docker conflicts)..."
    @just test-cleanup
    cargo nextest run --profile integration --workspace

# Run all examples to validate they work
examples:
    @echo "📚 Running all examples..."
    @echo ""
    @echo "🚀 Starting Redis server for examples..."
    @just redis-start
    @echo ""
    @echo "=== Running redis_basic example ==="
    @cargo run --example redis_basic --release
    @echo ""
    @echo "🧹 Cleaning up: Stopping Redis server..."
    @just redis-stop
    @echo ""
    @echo "✅ All examples completed successfully!"

# Run fast unit tests only (no integration tests)
test-unit:
    @echo "🚀 Running unit tests..."
    cargo nextest run --profile unit --workspace

# Run integration tests only (requires Docker)
test-integration: test-cleanup
    @echo "🔌 Running integration tests only..."
    cargo nextest run --profile integration --workspace

# Clean up test Docker containers (safe to run without Docker)
test-cleanup:
    @echo "🧹 Cleaning up test containers..."
    @(docker ps -aq --filter name=xylem-test | xargs -r docker rm -f || true) 2>/dev/null || true
    @(docker ps -aq --filter name=xylem-redis-cluster | xargs -r docker rm -f || true) 2>/dev/null || true
    @echo "✅ Test containers cleaned up"

# Run Redis-related integration tests
test-redis: test-cleanup
    @echo "🎯 Running Redis integration tests..."
    cargo nextest run --profile integration -E 'test(redis_integration) or test(redis_cluster) or test(pipelining)'

# Run Memcached integration tests
test-memcached: test-cleanup
    @echo "🎯 Running Memcached integration tests..."
    cargo nextest run --profile integration -E 'test(memcached_integration)'

# Run HTTP integration tests
test-http: test-cleanup
    @echo "🌐 Running HTTP integration tests..."
    cargo nextest run --profile integration -E 'test(http_integration)'

# Run Masstree integration tests
test-masstree: test-cleanup
    @echo "🌲 Running Masstree integration tests..."
    cargo nextest run --profile integration -E 'binary(masstree_integration)'

# Build debug version
build:
    @echo "🔨 Building debug version..."
    cargo build --workspace

# Build release (optimized) version
build-release:
    @echo "🔨 Building release version (optimized)..."
    cargo build --workspace --release

# Build and run the CLI in debug mode
run *args:
    @echo "🚀 Running xylem CLI (debug mode)..."
    cargo run --bin {{BINARY_NAME}} -- {{args}}

# Build and run the CLI in release mode
run-release *args:
    @echo "🚀 Running xylem CLI (release mode)..."
    cargo run --bin {{BINARY_NAME}} --release -- {{args}}

# Format all code
fmt:
    @echo "🎨 Formatting code..."
    cargo fmt --all

# Run linter (clippy)
lint:
    @echo "🔍 Running linter (clippy)..."
    cargo clippy --workspace -- -D warnings

# Fix linting issues automatically
lint-fix:
    @echo "🔧 Fixing linting issues..."
    cargo clippy --workspace --fix --allow-dirty

# Type check without building
check:
    @echo "📝 Type checking..."
    cargo check --workspace

# Clean build artifacts
clean:
    @echo "🧹 Cleaning build artifacts..."
    cargo clean

# Pre-commit check - runs everything you should check before committing
precommit: fmt lint test-unit
    @echo "✅ Pre-commit checks passed!"
    @echo "   If you want to run full integration tests too, run: just test"

# Install flamegraph tool (one-time setup)
install-flamegraph:
    @echo "📊 Installing flamegraph tool..."
    cargo install flamegraph

# Generate flamegraph using real xylem Worker with echo protocol
# Uses 1 xylem thread (for clear profiling) and 8 echo server threads
flamegraph-echo:
    #!/usr/bin/env bash
    set -euo pipefail

    echo "🔥 Generating flamegraph with real xylem Worker"
    echo "   Xylem: 1 thread, 1024 connections, pipeline=1 (closed-loop)"
    echo "   Echo server: 8 threads"
    echo "   Duration: 30s"
    echo ""

    # Start echo server in background with 8 threads
    echo "🚀 Starting echo server on port 19999 (8 threads)..."
    cargo run -p xylem-echo-server --release -- --port 19999 --bind 127.0.0.1 --threads 8 &
    SERVER_PID=$!
    sleep 1

    # Ensure server is cleaned up on exit
    cleanup() {
        echo ""
        echo "🧹 Stopping echo server (PID: $SERVER_PID)..."
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
        echo "✅ Cleanup complete"
    }
    trap cleanup EXIT

    # Check server is running
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "❌ Echo server failed to start"
        exit 1
    fi
    echo "✅ Echo server running (PID: $SERVER_PID)"
    echo ""

    # Build benchmark with profiling profile
    echo "🔨 Building echo_benchmark with profiling profile..."
    cargo build --profile profiling -p xylem-core --example echo_benchmark
    echo ""

    # Generate flamegraph (1 xylem thread for clear profiling)
    # Check if kernel symbols are accessible
    if [ "$(cat /proc/sys/kernel/perf_event_paranoid)" -gt 0 ] || [ "$(cat /proc/sys/kernel/kptr_restrict)" -gt 0 ]; then
        echo "⚠️  Kernel symbols may not be available in flamegraph."
        echo "   To enable kernel symbols, run:"
        echo "     sudo sysctl kernel.perf_event_paranoid=-1"
        echo "     sudo sysctl kernel.kptr_restrict=0"
        echo ""
    fi
    echo "🔥 Running benchmark and generating flamegraph..."
    echo "   This will take approximately 30 seconds..."
    cargo flamegraph --profile profiling -p xylem-core --example echo_benchmark -- \
        --target 127.0.0.1:19999 \
        --threads 1 \
        --connections 1024 \
        --pipeline 1 \
        --duration 30

    echo ""
    echo "✅ Flamegraph generated: flamegraph.svg"
    echo "   Open it in a browser to analyze performance"

# Generate flamegraph using Unix domain sockets with echo protocol
# Uses 1 xylem thread (for clear profiling) and 8 echo server threads
flamegraph-echo-unix:
    #!/usr/bin/env bash
    set -euo pipefail

    SOCKET_PATH="/tmp/xylem-echo.sock"

    echo "🔥 Generating flamegraph with Unix domain sockets"
    echo "   Xylem: 1 thread, 1024 connections, pipeline=1 (closed-loop)"
    echo "   Echo server: 8 threads"
    echo "   Socket: $SOCKET_PATH"
    echo "   Duration: 30s"
    echo ""

    # Start echo server in background with Unix socket
    echo "🚀 Starting echo server on $SOCKET_PATH (8 threads)..."
    cargo run -p xylem-echo-server --release -- --unix "$SOCKET_PATH" --threads 8 &
    SERVER_PID=$!
    sleep 1

    # Ensure server and socket are cleaned up on exit
    cleanup() {
        echo ""
        echo "🧹 Stopping echo server (PID: $SERVER_PID)..."
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
        rm -f "$SOCKET_PATH"
        echo "✅ Cleanup complete"
    }
    trap cleanup EXIT

    # Check server is running
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "❌ Echo server failed to start"
        exit 1
    fi
    echo "✅ Echo server running (PID: $SERVER_PID)"
    echo ""

    # Build benchmark with profiling profile
    echo "🔨 Building echo_unix_benchmark with profiling profile..."
    cargo build --profile profiling -p xylem-core --example echo_unix_benchmark
    echo ""

    # Check if kernel symbols are accessible
    if [ "$(cat /proc/sys/kernel/perf_event_paranoid)" -gt 0 ] || [ "$(cat /proc/sys/kernel/kptr_restrict)" -gt 0 ]; then
        echo "⚠️  Kernel symbols may not be available in flamegraph."
        echo "   To enable kernel symbols, run:"
        echo "     sudo sysctl kernel.perf_event_paranoid=-1"
        echo "     sudo sysctl kernel.kptr_restrict=0"
        echo ""
    fi
    echo "🔥 Running benchmark and generating flamegraph..."
    echo "   This will take approximately 30 seconds..."
    cargo flamegraph --profile profiling -p xylem-core --example echo_unix_benchmark -o flamegraph-unix.svg -- \
        --socket "$SOCKET_PATH" \
        --threads 1 \
        --connections 1024 \
        --pipeline 1 \
        --duration 30

    echo ""
    echo "✅ Flamegraph generated: flamegraph-unix.svg"
    echo "   Open it in a browser to analyze performance"

# Run a quick benchmark (Redis, 10k requests, single connection)
bench-quick:
    @echo "⚡ Running quick benchmark..."
    cargo run --release --bin {{BINARY_NAME}} -- -z "127.0.0.1:6379" -n 10000 -c 1

# Full benchmark (Redis, 100k requests, multiple connections, multiple threads)
bench-full:
    @echo "⚡ Running full benchmark..."
    cargo run --release --bin {{BINARY_NAME}} -- -z "127.0.0.1:6379" -n 100000 -c 4 -t 2

# Start Redis server using Docker
redis-start:
    @echo "🎯 Starting Redis server on port {{DEFAULT_REDIS_PORT}} (Docker)..."
    @docker compose -f tests/redis/docker-compose.yml up -d
    @sleep 2
    @echo "✅ Redis started"

# Stop Redis server
redis-stop:
    @echo "🛑 Stopping Redis server..."
    @docker compose -f tests/redis/docker-compose.yml down -v
    @echo "✅ Redis stopped"

# Start Memcached server using Docker
memcached-start:
    @echo "🎯 Starting Memcached server on port {{DEFAULT_MEMCACHED_PORT}} (Docker)..."
    @docker compose -f tests/memcached/docker-compose.yml up -d
    @sleep 2
    @echo "✅ Memcached started"

# Stop Memcached server
memcached-stop:
    @echo "🛑 Stopping Memcached server..."
    @docker compose -f tests/memcached/docker-compose.yml down -v
    @echo "✅ Memcached stopped"

# Build and start Masstree server using Docker
masstree-start:
    @echo "🎯 Building and starting Masstree server on port 2117 (Docker)..."
    @docker compose -f tests/masstree/docker-compose.yml up -d --build
    @sleep 3
    @echo "✅ Masstree started"

# Stop Masstree server
masstree-stop:
    @echo "🛑 Stopping Masstree server..."
    @docker compose -f tests/masstree/docker-compose.yml down -v
    @echo "✅ Masstree stopped"

# Start Redis, Memcached, and Masstree for testing
servers-start: redis-start memcached-start masstree-start
    @echo "✅ All test servers started"

# Stop all servers
servers-stop: redis-stop memcached-stop masstree-stop
    @echo "✅ All test servers stopped"

# Restart test servers
servers-restart: servers-stop servers-start

# Show server status
servers-status:
    @echo "📊 Checking server status..."
    @echo "Redis (port {{DEFAULT_REDIS_PORT}}):"
    @docker ps --filter name=xylem-test-redis --format "{{{{.Status}}}}" | grep -q "Up" && echo "   ✅ Running" || echo "   ❌ Not running"
    @echo "Memcached (port {{DEFAULT_MEMCACHED_PORT}}):"
    @docker ps --filter name=xylem-test-memcached --format "{{{{.Status}}}}" | grep -q "Up" && echo "   ✅ Running" || echo "   ❌ Not running"
    @echo "Masstree (port 2117):"
    @docker ps --filter name=xylem-test-masstree --format "{{{{.Status}}}}" | grep -q "Up" && echo "   ✅ Running" || echo "   ❌ Not running"

# Test release build with current Git commit as version
test-release:
    @echo "🏗️  Testing release build..."
    cargo build --release --bin {{BINARY_NAME}}
    @echo "✅ Release build successful"
    @echo "Binary location: target/release/{{BINARY_NAME}}"

# Install the release binary locally
install: build-release
    @echo "📦 Installing xylem to ~/.cargo/bin/..."
    cp target/release/{{BINARY_NAME}} ~/.cargo/bin/
    @echo "✅ xylem installed! You can run it with: xylem"

# Generate/update JSON Schema for configuration files
schema:
    @echo "📋 Generating JSON Schema..."
    cargo run --bin {{BINARY_NAME}} -- schema > schema/profile.schema.json
    @echo "✅ Schema written to schema/profile.schema.json"

# Generate shell completion scripts
completions-bash:
    @echo "🐚 Generating bash completion..."
    cargo run --bin {{BINARY_NAME}} -- completions bash

completions-zsh:
    @echo "🐚 Generating zsh completion..."
    cargo run --bin {{BINARY_NAME}} -- completions zsh

# Show this help
help:
    @echo "Xylem Justfile - Available Commands"
    @echo "===================================="
    @echo ""
    @echo "📊 Testing:"
    @echo "  just test              - Run ALL tests (unit + integration)"
    @echo "  just test-unit         - Unit tests only (fast, no Docker)"
    @echo "  just test-integration  - All integration tests (requires Docker)"
    @echo "  just test-redis        - Redis-related integration tests"
    @echo "  just test-memcached    - Memcached integration tests"
    @echo "  just test-http         - HTTP integration tests"
    @echo "  just examples          - Run all examples"
    @echo ""
    @echo "🔨 Building:"
    @echo "  just build             - Debug build"
    @echo "  just build-release     - Release build"
    @echo ""
    @echo "🚀 Running:"
    @echo "  just run <args>        - Run CLI in debug mode"
    @echo "  just run-release <args> - Run CLI in release mode"
    @echo ""
    @echo "🎨 Code Quality:"
    @echo "  just fmt               - Format code"
    @echo "  just lint              - Run linter"
    @echo "  just lint-fix          - Auto-fix lint issues"
    @echo "  just check             - Type check"
    @echo "  just precommit         - Run pre-commit checks"
    @echo ""
    @echo "🔥 Performance:"
    @echo "  just flamegraph-echo     - Generate flamegraph with echo benchmark (30s)"
    @echo "  just bench-quick         - Quick benchmark"
    @echo "  just bench-full          - Full benchmark"
    @echo ""
    @echo "🎯 Server Management:"
    @echo "  just servers-start     - Start Redis & Memcached"
    @echo "  just servers-stop      - Stop Redis & Memcached"
    @echo "  just servers-restart   - Restart both servers"
    @echo "  just servers-status    - Check server status"
    @echo ""
    @echo "📦 Other:"
    @echo "  just clean             - Clean build artifacts"
    @echo "  just install           - Install xylem locally"
    @echo "  just schema            - Generate JSON Schema for config files"
    @echo "  just completions-bash  - Generate bash completion script"
    @echo "  just completions-zsh   - Generate zsh completion script"
    @echo "  just help              - Show this help"
