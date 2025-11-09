# Testing

Testing guidelines and best practices for Xylem.

## Test Categories

### Unit Tests

Test individual functions and methods:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_encode_redis_get() {
        let mut proto = RedisProtocol::new_get("key1");
        let encoded = proto.encode_request().unwrap();
        assert_eq!(encoded, b"*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n");
    }
}
```

### Integration Tests

Test component interaction in `tests/`:

```rust
// tests/redis_integration.rs
use xylem_cli::*;

#[test]
fn test_redis_benchmark() {
    // Start test server
    let server = start_test_redis();
    
    // Run benchmark
    let result = run_benchmark(/* ... */);
    
    // Verify results
    assert!(result.success_rate > 0.99);
}
```

### Examples as Tests

Examples in `examples/` are tested:

```rust
// examples/redis_basic.rs
fn main() -> Result<()> {
    // Example code that also serves as test
    Ok(())
}
```

## Running Tests

```bash
# All tests
cargo test

# Unit tests only
cargo test --lib

# Integration tests only
cargo test --test '*'

# Specific test
cargo test test_redis_encoding

# With output
cargo test -- --nocapture

# Single-threaded (for debugging)
cargo test -- --test-threads=1
```

## Test Fixtures

### Shared Test Data

```rust
// tests/common/mod.rs
pub fn sample_config() -> Config {
    Config {
        protocol: "redis".to_string(),
        // ...
    }
}

// tests/my_test.rs
mod common;

#[test]
fn test_with_fixture() {
    let config = common::sample_config();
    // ...
}
```

### Test Servers

For integration tests:

```rust
use std::process::{Command, Child};

struct TestServer {
    process: Child,
    port: u16,
}

impl TestServer {
    fn start() -> Self {
        let port = find_free_port();
        let process = Command::new("redis-server")
            .arg("--port")
            .arg(port.to_string())
            .spawn()
            .unwrap();
        
        // Wait for server to start
        std::thread::sleep(Duration::from_millis(100));
        
        TestServer { process, port }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.process.kill().ok();
    }
}
```

## Mocking

### Mock Transports

```rust
struct MockTransport {
    responses: Vec<Vec<u8>>,
    sent_data: Vec<Vec<u8>>,
}

impl Transport for MockTransport {
    fn send(&mut self, data: &[u8]) -> Result<usize> {
        self.sent_data.push(data.to_vec());
        Ok(data.len())
    }
    
    fn recv(&mut self, buf: &mut [u8]) -> Result<usize> {
        if let Some(response) = self.responses.pop() {
            buf[..response.len()].copy_from_slice(&response);
            Ok(response.len())
        } else {
            Ok(0)
        }
    }
}
```

## Property-Based Testing

Use `proptest` for property-based tests:

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_encode_decode_roundtrip(key in ".*") {
        let mut proto = RedisProtocol::new_get(&key);
        let encoded = proto.encode_request().unwrap();
        
        // Verify we can decode what we encoded
        let decoded_key = parse_redis_get(&encoded).unwrap();
        assert_eq!(decoded_key, key);
    }
}
```

## Benchmarking

Use `criterion` for benchmarks:

```rust
// benches/protocol_bench.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn benchmark_redis_encoding(c: &mut Criterion) {
    let mut proto = RedisProtocol::new_get("test_key");
    
    c.bench_function("redis_encode", |b| {
        b.iter(|| {
            proto.encode_request().unwrap();
            proto.reset();
        })
    });
}

criterion_group!(benches, benchmark_redis_encoding);
criterion_main!(benches);
```

Run benchmarks:
```bash
cargo bench
```

## Coverage

### Generate Coverage

```bash
# Install tarpaulin
cargo install cargo-tarpaulin

# Generate HTML report
cargo tarpaulin --out Html

# Generate and upload to codecov
cargo tarpaulin --out Xml
bash <(curl -s https://codecov.io/bash)
```

### Coverage Goals

- Overall: > 80%
- Core modules: > 90%
- Protocol implementations: > 85%

## Test Best Practices

### Arrange-Act-Assert

```rust
#[test]
fn test_connection_pool() {
    // Arrange
    let mut pool = ConnectionPool::new(10);
    
    // Act
    let conn = pool.acquire().unwrap();
    
    // Assert
    assert!(conn.is_connected());
}
```

### Test One Thing

```rust
// Good: Single concern
#[test]
fn test_connect_success() { /* ... */ }

#[test]
fn test_connect_timeout() { /* ... */ }

// Bad: Multiple concerns
#[test]
fn test_connection() {
    // Tests both success and timeout
}
```

### Use Descriptive Names

```rust
// Good
#[test]
fn test_redis_pipeline_maintains_request_order() { }

// Bad
#[test]
fn test1() { }
```

### Clean Up Resources

```rust
#[test]
fn test_file_operations() {
    let temp_file = create_temp_file();
    
    // Test code
    
    // Cleanup
    std::fs::remove_file(temp_file).ok();
}

// Or use Drop
struct TempFile(PathBuf);

impl Drop for TempFile {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).ok();
    }
}
```

## Continuous Integration

Tests run automatically on CI:

```yaml
# .github/workflows/ci.yml
- name: Run tests
  run: cargo test --all-features
  
- name: Run integration tests
  run: cargo test --test '*'
```

## See Also

- [Development Setup](./setup.md)
- [Code Style](./style.md)
- [Rust Testing Book](https://doc.rust-lang.org/book/ch11-00-testing.html)
