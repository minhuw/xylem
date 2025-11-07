# Xylem Justfile - Project automation commands
# Run 'just' or 'just --list' to see all available recipes

# Variables
BINARY_NAME := "xylem"
DEFAULT_REDIS_PORT := "6379"
DEFAULT_MEMCACHED_PORT := "11211"

# Default recipe - shows help
_default:
    @just --list

# Run all integration tests (Redis, Memcached, etc.)
# This is the main test command - runs comprehensive integration tests
# RUST_LOG=error suppresses verbose dependency logs
test:
    @echo "🔌 Running integration tests (requires Redis/Memcached)..."
    @echo "This will take around 30-60 seconds..."
    RUST_LOG=error cargo test --workspace -- --ignored --test-threads=1

# Run fast unit tests only (no integration tests)
test-unit:
    @echo "🚀 Running unit tests..."
    cargo test --workspace

# Run all tests (unit + integration)
test-all: test-unit test
    @echo "✅ All tests passed!"

# Run only Redis integration tests
test-redis:
    @echo "🎯 Running Redis integration tests..."
    RUST_LOG=error cargo test --test redis_integration -- --ignored --test-threads=1 --nocapture

# Run only Memcached integration tests
test-memcached:
    @echo "🎯 Running Memcached integration tests..."
    RUST_LOG=error cargo test --test memcached_integration -- --ignored --nocapture

# Run scheduler integration tests (these run by default)
# Note: Requires Redis to be running
# If tests fail, try: just redis-start first
test-scheduler:
    @echo "⚙️  Running scheduler integration tests..."
    @echo "Make sure Redis is running (use: just redis-status)"
    RUST_LOG=error cargo test --test scheduler_round_robin -- --nocapture --test-threads=1

# Run rate accuracy tests (timing-sensitive, takes longer)
test-rate:
    @echo "⏱️  Running rate accuracy tests..."
    RUST_LOG=error cargo test --test rate_accuracy -- --ignored --test-threads=1 --nocapture

# Run only pipelined worker tests
test-pipelined:
    @echo "🚰 Running pipelined worker tests..."
    cargo test -p xylem-core --lib threading::pipelined_worker -- --nocapture

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

# Generate flamegraph for the CLI (requires install-flamegraph first)
# Usage: just flamegraph <xylem-args>
flamegraph *args:
    @echo "🔥 Generating flamegraph..."
    @echo "This will run xylem and create a flamegraph.svg file"
    flamegraph --bin {{BINARY_NAME}} -- {{args}}

# Generate flamegraph for integration test (performance profiling)
flamegraph-test:
    @echo "🔥 Generating flamegraph for scheduler test..."
    cargo flamegraph --test scheduler_round_robin -- --nocapture

# Run a quick benchmark (Redis, 10k requests, single connection)
bench-quick:
    @echo "⚡ Running quick benchmark..."
    cargo run --release --bin {{BINARY_NAME}} -- -z "127.0.0.1:6379" -n 10000 -c 1

# Full benchmark (Redis, 100k requests, multiple connections, multiple threads)
bench-full:
    @echo "⚡ Running full benchmark..."
    cargo run --release --bin {{BINARY_NAME}} -- -z "127.0.0.1:6379" -n 100000 -c 4 -t 2

# Start Redis server (if not running)
redis-start:
    @echo "🎯 Starting Redis server on port {{DEFAULT_REDIS_PORT}}..."
    @redis-server --port {{DEFAULT_REDIS_PORT}} --save '' --appendonly no &
    @sleep 1
    @echo "✅ Redis started"

# Stop Redis server
redis-stop:
    @echo "🛑 Stopping Redis server..."
    @redis-cli -p {{DEFAULT_REDIS_PORT}} shutdown || true
    @echo "✅ Redis stopped"

# Start Memcached server (if not running)
memcached-start:
    @echo "🎯 Starting Memcached server on port {{DEFAULT_MEMCACHED_PORT}}..."
    @memcached -p {{DEFAULT_MEMCACHED_PORT}} -m 64 &
    @sleep 1
    @echo "✅ Memcached started"

# Stop Memcached server
memcached-stop:
    @echo "🛑 Stopping Memcached server..."
    @killall memcached || true
    @echo "✅ Memcached stopped"

# Start both Redis and Memcached for testing
servers-start: redis-start memcached-start
    @echo "✅ All test servers started"

# Stop both servers
servers-stop: redis-stop memcached-stop
    @echo "✅ All test servers stopped"

# Restart test servers
servers-restart: servers-stop servers-start

# Show server status
servers-status:
    @echo "📊 Checking server status..."
    @echo "Redis (port {{DEFAULT_REDIS_PORT}}):"
    @redis-cli -p {{DEFAULT_REDIS_PORT}} ping 2>/dev/null || echo "   ❌ Not running"
    @echo "Memcached (port {{DEFAULT_MEMCACHED_PORT}}):"
    @echo "stats" | nc localhost {{DEFAULT_MEMCACHED_PORT}} | head -1 || echo "   ❌ Not running"

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

# Show this help
help:
    @echo "Xylem Justfile - Available Commands"
    @echo "===================================="
    @echo ""
    @echo "📊 Testing:"
    @echo "  just test              - Run ALL integration tests (Redis/Memcached)"
    @echo "  just test-unit         - Fast unit tests only (no integration)"
    @echo "  just test-all          - All tests (unit + integration)"
    @echo "  just test-redis        - Redis tests only"
    @echo "  just test-memcached    - Memcached tests only"
    @echo "  just test-scheduler    - Scheduler integration tests"
    @echo "  just test-pipelined    - Pipelined worker tests"
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
    @echo "  just flamegraph <args> - Generate flamegraph (requires: just install-flamegraph)"
    @echo "  just bench-quick       - Quick benchmark"
    @echo "  just bench-full        - Full benchmark"
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
    @echo "  just help              - Show this help"
