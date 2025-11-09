# Custom Protocols

Extend Xylem with your own protocol implementations.

## Overview

Xylem's modular architecture allows you to implement custom protocols without modifying the core codebase.

## Protocol Trait

To implement a custom protocol, you need to implement the `Protocol` trait:

```rust
pub trait Protocol {
    /// Encode a request
    fn encode_request(&mut self) -> Result<Vec<u8>>;
    
    /// Decode a response
    fn decode_response(&mut self, data: &[u8]) -> Result<usize>;
    
    /// Check if response is complete
    fn is_complete(&self) -> bool;
    
    /// Reset state for next request
    fn reset(&mut self);
}
```

## Example Implementation

Here's a simple example of a custom protocol:

```rust
use xylem_core::Protocol;
use anyhow::Result;

pub struct MyProtocol {
    request_id: u64,
    // ... other fields
}

impl Protocol for MyProtocol {
    fn encode_request(&mut self) -> Result<Vec<u8>> {
        // Build your request
        let mut buf = Vec::new();
        // ... encode logic
        Ok(buf)
    }
    
    fn decode_response(&mut self, data: &[u8]) -> Result<usize> {
        // Parse response
        // Return number of bytes consumed
        Ok(data.len())
    }
    
    fn is_complete(&self) -> bool {
        // Check if full response received
        true
    }
    
    fn reset(&mut self) {
        // Reset state
        self.request_id += 1;
    }
}
```

## Integration

To use your custom protocol:

1. Add it to the `xylem-protocols` crate
2. Register it in the protocol factory
3. Use it via configuration:

```json
{
  "protocol": "my_protocol",
  "protocol_config": {
    // Your protocol-specific options
  }
}
```

## Best Practices

- **Stateless when possible** - Avoid maintaining state between requests
- **Efficient encoding** - Minimize allocations
- **Error handling** - Return descriptive errors
- **Documentation** - Document your protocol options

## See Also

- [Extending Xylem](../../advanced/extending.md)
- [Architecture Overview](../../architecture/overview.md)
