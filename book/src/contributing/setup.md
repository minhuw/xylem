# Development Setup

Get started with Xylem development.

## Prerequisites

- **Rust** 1.70 or later
- **Git**
- **Just** (optional, for task automation)

## Clone the Repository

```bash
git clone https://github.com/minhuw/xylem.git
cd xylem
```

## Build from Source

```bash
# Debug build
cargo build

# Release build
cargo build --release
```

## Running Tests

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_name

# Run with output
cargo test -- --nocapture
```

## Code Coverage

```bash
# Install tarpaulin
cargo install cargo-tarpaulin

# Generate coverage
cargo tarpaulin --out Html
```

## Linting

```bash
# Run clippy
cargo clippy --all-targets --all-features

# Auto-fix issues
cargo clippy --fix
```

## Formatting

```bash
# Check formatting
cargo fmt --check

# Format code
cargo fmt
```

## Using Just

The project includes a `Justfile` for common tasks:

```bash
# Install just
cargo install just

# List available commands
just --list

# Build project
just build

# Run tests
just test

# Run linters
just lint

# Format code
just fmt
```

## Development Workflow

1. **Create a branch**:
   ```bash
   git checkout -b feature/my-feature
   ```

2. **Make changes and test**:
   ```bash
   cargo test
   cargo clippy
   ```

3. **Format code**:
   ```bash
   cargo fmt
   ```

4. **Commit changes**:
   ```bash
   git add .
   git commit -m "Add my feature"
   ```

5. **Push and create PR**:
   ```bash
   git push origin feature/my-feature
   ```

## IDE Setup

### VS Code

Recommended extensions:
- rust-analyzer
- CodeLLDB (debugging)
- Even Better TOML

Settings (`.vscode/settings.json`):
```json
{
  "rust-analyzer.checkOnSave.command": "clippy"
}
```

### IntelliJ IDEA

Install the Rust plugin from JetBrains.

## Debugging

### VS Code

Create `.vscode/launch.json`:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "type": "lldb",
      "request": "launch",
      "name": "Debug xylem",
      "cargo": {
        "args": [
          "build",
          "--bin=xylem",
          "--package=xylem-cli"
        ]
      },
      "args": ["--protocol", "redis"],
      "cwd": "${workspaceFolder}"
    }
  ]
}
```

### Command Line

```bash
# Using rust-gdb
rust-gdb target/debug/xylem
```

## Documentation

Build documentation locally:

```bash
# Build API docs
cargo doc --open

# Build mdBook
cd book
mdbook serve --open
```

## See Also

- [Code Style](./style.md)
- [Testing](./testing.md)
- [Contributing Guidelines](https://github.com/minhuw/xylem/blob/main/CONTRIBUTING.md)
