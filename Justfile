# Xylem Justfile - Project automation commands
# Run 'just' or 'just --list' to see all available recipes

# Variables
BINARY_NAME := "xylem"
DEFAULT_REDIS_PORT := "6379"
DEFAULT_MEMCACHED_PORT := "11211"

# Default recipe - lists available recipes
_default:
    @just --list

# Run all tests (unit + integration)
test:
    @echo "🚀 Running all tests (unit + integration)..."
    @echo "   Running unit tests first..."
    cargo nextest run --profile unit --workspace
    @echo "   Running integration tests (serially to avoid Docker conflicts)..."
    cargo nextest run --profile integration --workspace

# Run the redis_basic example against a Docker Redis server
examples:
    @echo "📚 Running the redis_basic example..."
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
    @echo "✅ Example completed successfully!"

# Run fast unit tests only (no integration tests)
test-unit:
    @echo "🚀 Running unit tests..."
    cargo nextest run --profile unit --workspace

# Run doc tests (nextest does not run them)
test-doc:
    @echo "📖 Running doc tests..."
    cargo test --workspace --doc

# Run integration tests only (requires Docker)
test-integration:
    @echo "🔌 Running integration tests only..."
    cargo nextest run --profile integration --workspace

# Run the ntcp (user-space TCP) end-to-end test over TUN devices in an
# unprivileged user + network namespace; needs no root and no Docker
test-ntcp:
    @echo "🧪 Running the ntcp TUN end-to-end test..."
    tests/ntcp/tun_netns.sh

# Run Redis-related integration tests
test-redis:
    @echo "🎯 Running Redis integration tests..."
    cargo nextest run --profile integration -E 'binary(redis_integration) or binary(redis_new_commands_integration) or binary(pipelining_integration)'

# Run Memcached integration tests
test-memcached:
    @echo "🎯 Running Memcached integration tests..."
    cargo nextest run --profile integration -E 'binary(memcached_integration)'

# Run HTTP integration tests
test-http:
    @echo "🌐 Running HTTP integration tests..."
    cargo nextest run --profile integration -E 'binary(http_integration)'

# Run Masstree integration tests
test-masstree:
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
    cargo clippy --all-targets --all-features -- -D warnings

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
precommit: fmt lint test-unit test-doc
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

# Demo: Request dump feature - records all requests to CSV files
# Runs a 10-second Redis benchmark with Poisson arrivals and Zipfian keys
demo-request-dump:
    #!/usr/bin/env bash
    set -euo pipefail

    DUMP_DIR="/tmp/xylem_request_dump"

    echo "📝 Request Dump Demo"
    echo "===================="
    echo ""
    echo "This demo shows how to record all requests to CSV files."
    echo "Configuration: 1 thread, Poisson 1000 req/s, Zipfian keys, 10 seconds"
    echo ""

    # Clean up old dump files
    rm -rf "$DUMP_DIR"
    mkdir -p "$DUMP_DIR"

    # Start Redis
    echo "🚀 Starting Redis server..."
    just redis-start
    echo ""

    # Ensure Redis is cleaned up on exit
    cleanup() {
        echo ""
        echo "🧹 Stopping Redis server..."
        just redis-stop
    }
    trap cleanup EXIT

    # Run the benchmark
    echo "🔥 Running benchmark with request dump enabled..."
    echo ""
    cargo run --release --bin xylem -- -P profiles/request-dump-demo.toml --set output.real_time=false

    echo ""
    echo "📊 Request dump files:"
    ls -la "$DUMP_DIR"
    echo ""

    # Show sample of the dump
    DUMP_FILE=$(ls "$DUMP_DIR"/*.csv 2>/dev/null | head -1)
    if [ -n "$DUMP_FILE" ]; then
        echo "📄 Sample from $DUMP_FILE (first 20 lines):"
        echo "-------------------------------------------"
        head -20 "$DUMP_FILE"
        echo "..."
        echo ""
        echo "📈 Total records: $(wc -l < "$DUMP_FILE") lines"
        echo ""
        echo "💡 CSV columns:"
        echo "   - timestamp_ns: Request timestamp in nanoseconds"
        echo "   - thread_id: Worker thread ID"
        echo "   - group_id: Traffic group ID"
        echo "   - conn_id: Connection ID"
        echo "   - request_id: Unique request identifier"
        echo "   - data_len: Request data length in bytes"
        echo "   - data_utf8: Request data (UTF-8 encoded)"
    fi

    echo ""
    echo "✅ Demo complete! Dump files are in: $DUMP_DIR"

# Run a quick benchmark (Redis, 10 seconds, single connection)
bench-quick:
    @echo "⚡ Running quick benchmark..."
    cargo run --release --bin {{BINARY_NAME}} -- -P tests/redis/redis-get-zipfian.toml --set experiment.duration=10s --set 'traffic_groups.0.threads=[0]' --set traffic_groups.0.connections_per_thread=1 --set traffic_groups.0.max_pending_per_connection=1

# Full benchmark (Redis, 60 seconds, four connections per thread, two threads)
bench-full:
    @echo "⚡ Running full benchmark..."
    cargo run --release --bin {{BINARY_NAME}} -- -P tests/redis/redis-get-zipfian.toml --set experiment.duration=60s --set 'traffic_groups.0.threads=[0,1]' --set traffic_groups.0.connections_per_thread=4

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
    cargo run --quiet --bin {{BINARY_NAME}} -- schema > schema/profile.schema.json
    @echo "✅ Schema written to schema/profile.schema.json"

# Generate shell completion scripts
completions-bash:
    @echo "🐚 Generating bash completion..."
    cargo run --bin {{BINARY_NAME}} -- completions bash

completions-zsh:
    @echo "🐚 Generating zsh completion..."
    cargo run --bin {{BINARY_NAME}} -- completions zsh
