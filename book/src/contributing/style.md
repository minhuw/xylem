# Code Style

Coding standards and style guidelines for Xylem.

## Rust Style

Follow the [Rust Style Guide](https://doc.rust-lang.org/1.0.0/style/).

### Formatting

Use `rustfmt` with project configuration:

```bash
cargo fmt
```

Configuration is in `rustfmt.toml`.

### Naming Conventions

- **Types**: `PascalCase`
- **Functions**: `snake_case`
- **Constants**: `SCREAMING_SNAKE_CASE`
- **Modules**: `snake_case`

Examples:
```rust
pub struct RedisProtocol { }
pub fn create_protocol() { }
pub const MAX_BUFFER_SIZE: usize = 4096;
pub mod protocol_factory;
```

## Code Organization

### Module Structure

```
crate/
├── src/
│   ├── lib.rs          # Public API
│   ├── protocol.rs     # Protocol implementation
│   ├── transport.rs    # Transport layer
│   └── util.rs         # Utilities
```

### Imports

Group imports in this order:

1. Standard library
2. External crates
3. Internal crates
4. Local modules

```rust
use std::io::{Read, Write};
use std::net::TcpStream;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use xylem_core::Protocol;
use xylem_transport::Transport;

use crate::config::Config;
```

## Documentation

### Module Documentation

```rust
//! Protocol implementations for Xylem.
//!
//! This module provides implementations of various application protocols
//! including Redis, HTTP, and Memcached.
```

### Function Documentation

```rust
/// Encodes a request into wire format.
///
/// # Arguments
///
/// * `request` - The request to encode
///
/// # Returns
///
/// A byte vector containing the encoded request.
///
/// # Errors
///
/// Returns an error if encoding fails.
///
/// # Examples
///
/// ```
/// let request = Request::new("GET", "key");
/// let encoded = encode_request(&request)?;
/// ```
pub fn encode_request(request: &Request) -> Result<Vec<u8>> {
    // ...
}
```

### Type Documentation

```rust
/// Configuration for Redis protocol.
///
/// Specifies operation type, key patterns, and other Redis-specific options.
#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    /// Redis operation to perform (GET, SET, etc.)
    pub operation: String,
    
    /// Pattern for key generation
    pub key_pattern: String,
}
```

## Error Handling

### Use Result Types

```rust
use anyhow::Result;

pub fn connect() -> Result<Connection> {
    // ...
}
```

### Provide Context

```rust
use anyhow::Context;

file::read("config.json")
    .context("Failed to read configuration file")?;
```

### Custom Errors

For library code, use `thiserror`:

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
    
    #[error("Encoding failed")]
    EncodingFailed,
    
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
```

## Testing

### Test Organization

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_encode_request() {
        // ...
    }
    
    #[test]
    fn test_decode_response() {
        // ...
    }
}
```

### Test Naming

Use descriptive names:

```rust
#[test]
fn test_tcp_connect_success() { }

#[test]
fn test_tcp_connect_timeout() { }

#[test]
fn test_redis_pipeline_encoding() { }
```

## Clippy

Address all clippy warnings:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Common rules in `clippy.toml`:
```toml
# Allow uninlined format args
uninlined-format-args = "allow"
```

## Performance

### Avoid Allocations

Prefer borrowing:
```rust
// Good
fn process(data: &[u8]) { }

// Avoid if possible
fn process(data: Vec<u8>) { }
```

### Use Appropriate Data Structures

- `Vec` for sequential data
- `HashMap` for key-value lookups
- `BTreeMap` for ordered data
- `VecDeque` for queues

### Profile Before Optimizing

Use `cargo flamegraph` or `perf` to identify bottlenecks.

## Comments

### When to Comment

- **Complex algorithms**: Explain the approach
- **Non-obvious code**: Clarify intent
- **Workarounds**: Explain why needed

### When NOT to Comment

Don't state the obvious:
```rust
// Bad: Comment duplicates code
// Increment counter
counter += 1;

// Good: No comment needed
counter += 1;
```

## Git Commit Messages

Format:
```
<type>: <subject>

<body>

<footer>
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Formatting
- `refactor`: Code restructuring
- `test`: Tests
- `chore`: Maintenance

Example:
```
feat: Add HTTP/2 protocol support

Implements HTTP/2 protocol using h2 crate. Adds configuration
options for max concurrent streams and initial window size.

Closes #123
```

## See Also

- [Development Setup](./setup.md)
- [Testing Guidelines](./testing.md)
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
