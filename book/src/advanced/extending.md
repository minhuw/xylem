# Extending Xylem

Guide for extending Xylem with custom protocols and transports.

## Adding a New Protocol

### 1. Implement the Protocol Trait

Create a new file in `xylem-protocols/src/`:

```rust
use anyhow::Result;
use xylem_core::Protocol;

pub struct MyProtocol {
    state: ProtocolState,
    // ... protocol-specific fields
}

impl Protocol for MyProtocol {
    fn encode_request(&mut self) -> Result<Vec<u8>> {
        // Encode your request format
        let mut buffer = Vec::new();
        // ... encoding logic
        Ok(buffer)
    }
    
    fn decode_response(&mut self, data: &[u8]) -> Result<usize> {
        // Parse response
        // Return number of bytes consumed
        let consumed = parse_my_protocol(data)?;
        Ok(consumed)
    }
    
    fn is_complete(&self) -> bool {
        matches!(self.state, ProtocolState::Complete)
    }
    
    fn reset(&mut self) {
        self.state = ProtocolState::Idle;
    }
}
```

### 2. Add Configuration Support

Define configuration structure:

```rust
#[derive(Debug, Deserialize)]
pub struct MyProtocolConfig {
    pub operation: String,
    pub custom_option: Option<String>,
}

impl MyProtocol {
    pub fn new(config: &MyProtocolConfig) -> Result<Self> {
        Ok(MyProtocol {
            state: ProtocolState::Idle,
            // ... initialize from config
        })
    }
}
```

### 3. Register the Protocol

Add to the protocol factory in `xylem-protocols/src/lib.rs`:

```rust
pub fn create_protocol(
    name: &str,
    config: &Value,
) -> Result<Box<dyn Protocol>> {
    match name {
        "redis" => { /* ... */ },
        "http" => { /* ... */ },
        "myprotocol" => {
            let cfg: MyProtocolConfig = serde_json::from_value(config.clone())?;
            Ok(Box::new(MyProtocol::new(&cfg)?))
        }
        _ => Err(anyhow!("Unknown protocol: {}", name)),
    }
}
```

### 4. Add Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_encode_request() {
        let mut proto = MyProtocol::new(&MyProtocolConfig::default()).unwrap();
        let req = proto.encode_request().unwrap();
        assert!(!req.is_empty());
    }
    
    #[test]
    fn test_decode_response() {
        let mut proto = MyProtocol::new(&MyProtocolConfig::default()).unwrap();
        let response = b"test response";
        let consumed = proto.decode_response(response).unwrap();
        assert_eq!(consumed, response.len());
        assert!(proto.is_complete());
    }
}
```

## Adding a New Transport

### 1. Implement the Transport Trait

Create a new file in `xylem-transport/src/`:

```rust
use std::io::{Read, Write};
use anyhow::Result;
use xylem_core::Transport;

pub struct MyTransport {
    // Transport-specific fields
}

impl Transport for MyTransport {
    fn connect(&mut self) -> Result<()> {
        // Establish connection
        Ok(())
    }
    
    fn send(&mut self, data: &[u8]) -> Result<usize> {
        // Send data
        Ok(data.len())
    }
    
    fn recv(&mut self, buf: &mut [u8]) -> Result<usize> {
        // Receive data
        Ok(0)
    }
    
    fn close(&mut self) -> Result<()> {
        // Close connection
        Ok(())
    }
    
    fn is_connected(&self) -> bool {
        // Check connection state
        true
    }
}
```

### 2. Add Configuration

```rust
#[derive(Debug, Deserialize)]
pub struct MyTransportConfig {
    pub address: String,
    pub timeout: Option<Duration>,
}

impl MyTransport {
    pub fn new(config: &MyTransportConfig) -> Result<Self> {
        Ok(MyTransport {
            // ... initialize from config
        })
    }
}
```

### 3. Register the Transport

Add to the transport factory in `xylem-transport/src/lib.rs`:

```rust
pub fn create_transport(
    name: &str,
    config: &Value,
) -> Result<Box<dyn Transport>> {
    match name {
        "tcp" => { /* ... */ },
        "udp" => { /* ... */ },
        "mytransport" => {
            let cfg: MyTransportConfig = serde_json::from_value(config.clone())?;
            Ok(Box::new(MyTransport::new(&cfg)?))
        }
        _ => Err(anyhow!("Unknown transport: {}", name)),
    }
}
```

## Custom Statistics

Implement custom statistics collector:

```rust
use xylem_core::Statistics;

pub struct MyStatistics {
    // Custom metrics
}

impl Statistics for MyStatistics {
    fn record_latency(&mut self, latency_ns: u64) {
        // Record latency
    }
    
    fn record_error(&mut self, error: &str) {
        // Record error
    }
    
    fn report(&self) -> StatisticsReport {
        // Generate report
        todo!()
    }
}
```

## Custom Workload Generators

Implement custom workload pattern:

```rust
use xylem_core::WorkloadGenerator;

pub struct MyWorkloadGenerator {
    // Generator state
}

impl WorkloadGenerator for MyWorkloadGenerator {
    fn next_request_time(&mut self) -> Option<Instant> {
        // Calculate next request time
        Some(Instant::now())
    }
    
    fn is_complete(&self) -> bool {
        // Check if workload is complete
        false
    }
}
```

## Best Practices

### Error Handling

Use `anyhow` for errors:

```rust
use anyhow::{Context, Result};

fn my_function() -> Result<()> {
    some_operation()
        .context("Failed to perform operation")?;
    Ok(())
}
```

### Logging

Use `tracing` for logging:

```rust
use tracing::{debug, info, warn, error};

fn process_request() {
    debug!("Processing request");
    info!("Request processed successfully");
    warn!("Slow response detected");
    error!("Request failed");
}
```

### Performance

- Avoid allocations in hot path
- Use buffer pooling
- Prefer stack allocation
- Use `&[u8]` instead of `Vec<u8>` when possible

### Testing

Write comprehensive tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_operation() { /* ... */ }
    
    #[test]
    fn test_error_handling() { /* ... */ }
    
    #[test]
    fn test_edge_cases() { /* ... */ }
}
```

## Contributing

See [Contributing Guide](../contributing/setup.md) for how to contribute your extensions.

## See Also

- [Architecture Overview](../architecture/overview.md)
- [Custom Protocols](../guide/protocols/custom.md)
- [Development Setup](../contributing/setup.md)
