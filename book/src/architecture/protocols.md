# Protocol Layer

The protocol layer (`xylem-protocols`) implements application-level protocols.

## Protocol Trait

All protocols implement the `Protocol` trait:

```rust
pub trait Protocol: Send {
    /// Encode the next request
    fn encode_request(&mut self) -> Result<Vec<u8>>;
    
    /// Decode response data
    fn decode_response(&mut self, data: &[u8]) -> Result<usize>;
    
    /// Check if response is complete
    fn is_complete(&self) -> bool;
    
    /// Reset state for next request
    fn reset(&mut self);
    
    /// Get protocol-specific metrics
    fn metrics(&self) -> ProtocolMetrics;
}
```

## Protocol Implementations

### Redis Protocol

Implements RESP (Redis Serialization Protocol):

```rust
pub struct RedisProtocol {
    operation: RedisOperation,
    key_generator: KeyGenerator,
    state: ProtocolState,
}
```

**Features:**
- RESP2 and RESP3 support
- Pipelining
- Key pattern generation
- All common commands

### HTTP Protocol

Implements HTTP/1.1:

```rust
pub struct HttpProtocol {
    method: Method,
    path: String,
    headers: HeaderMap,
    body: Option<Vec<u8>>,
}
```

**Features:**
- All HTTP methods
- Custom headers
- Request body support
- Keep-alive connections

### Memcached Protocol

Implements binary and text protocols:

```rust
pub struct MemcachedProtocol {
    operation: MemcachedOp,
    key_generator: KeyGenerator,
    binary: bool,
}
```

**Features:**
- Binary and text protocol
- All common operations
- Expiration support
- CAS operations

## State Management

Protocols maintain state for request-response correlation:

```rust
pub enum ProtocolState {
    Idle,
    WaitingResponse { request_id: u64 },
    Complete,
    Error(String),
}
```

## Request Encoding

Efficient request encoding with minimal allocations:

```rust
impl RedisProtocol {
    fn encode_request(&mut self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(64);
        match self.operation {
            RedisOperation::Get(ref key) => {
                write!(buf, "*2\r\n$3\r\nGET\r\n")?;
                write!(buf, "${}\r\n{}\r\n", key.len(), key)?;
            }
            // ... other operations
        }
        Ok(buf)
    }
}
```

## Response Parsing

Incremental parsing for streaming data:

```rust
impl RedisProtocol {
    fn decode_response(&mut self, data: &[u8]) -> Result<usize> {
        match self.state {
            ProtocolState::WaitingResponse { .. } => {
                // Parse RESP format
                let (consumed, response) = parse_resp(data)?;
                self.state = ProtocolState::Complete;
                Ok(consumed)
            }
            _ => Err(anyhow!("Invalid state")),
        }
    }
}
```

## Key Generation

Dynamic key generation for realistic workloads:

```rust
pub enum KeyPattern {
    Sequential { prefix: String, counter: u64 },
    Random { prefix: String },
    Zipfian { prefix: String, n: usize, s: f64 },
}
```

**Patterns:**
- **Sequential** - `key:1`, `key:2`, `key:3`, ...
- **Random** - Random keys from key space
- **Zipfian** - Skewed distribution (popular keys accessed more)

## Protocol Factory

Dynamic protocol creation:

```rust
pub fn create_protocol(config: &ProtocolConfig) -> Result<Box<dyn Protocol>> {
    match config.protocol.as_str() {
        "redis" => Ok(Box::new(RedisProtocol::new(config)?)),
        "http" => Ok(Box::new(HttpProtocol::new(config)?)),
        "memcached" => Ok(Box::new(MemcachedProtocol::new(config)?)),
        _ => Err(anyhow!("Unknown protocol: {}", config.protocol)),
    }
}
```

## Adding a New Protocol

To add a new protocol:

1. **Implement the Protocol trait**:
```rust
pub struct MyProtocol {
    // Protocol state
}

impl Protocol for MyProtocol {
    // Implement required methods
}
```

2. **Add to protocol factory**:
```rust
"myprotocol" => Ok(Box::new(MyProtocol::new(config)?)),
```

3. **Add configuration support**:
```rust
#[derive(Deserialize)]
pub struct MyProtocolConfig {
    // Protocol-specific config
}
```

## Testing

Comprehensive protocol tests:

```rust
#[test]
fn test_redis_get_encode() {
    let mut proto = RedisProtocol::new_get("key1");
    let req = proto.encode_request().unwrap();
    assert_eq!(req, b"*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n");
}

#[test]
fn test_redis_response_decode() {
    let mut proto = RedisProtocol::new_get("key1");
    let _ = proto.encode_request();
    let consumed = proto.decode_response(b"$5\r\nvalue\r\n").unwrap();
    assert_eq!(consumed, 13);
    assert!(proto.is_complete());
}
```

## See Also

- [Architecture Overview](./overview.md)
- [Custom Protocols](../guide/protocols/custom.md)
- [Extending Xylem](../advanced/extending.md)
