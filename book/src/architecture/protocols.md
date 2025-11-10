# Protocol Layer

The protocol layer (`xylem-protocols`) implements application-level protocols for different types of RPC workloads.

## Protocol Trait

All protocols implement a common `Protocol` trait that defines the interface for request generation and response parsing:

```rust
pub trait Protocol: Send {
    type RequestId: Eq + Hash + Clone + Copy + Debug;
    
    /// Generate a request with an ID
    fn generate_request(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
    ) -> (Vec<u8>, Self::RequestId);
    
    /// Parse a response and return the request ID
    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)>;
    
    /// Protocol name
    fn name(&self) -> &'static str;
    
    /// Reset protocol state
    fn reset(&mut self);
}
```

This trait provides a uniform interface for all protocol implementations, allowing the core engine to work with any protocol without knowing its specific details.

## Protocol Design

Protocols in Xylem are designed to be stateless where possible, with minimal memory overhead per request. Each protocol implementation handles the details of encoding requests in the appropriate wire format and parsing responses back into a form the core engine can process. The request ID mechanism allows the system to correlate responses with their corresponding requests, which is essential for accurate latency measurement in pipelined or out-of-order scenarios.

## Supported Protocols

Xylem includes implementations for several common RPC protocols:

- **Redis** - Implements the RESP (Redis Serialization Protocol) for key-value operations
- **HTTP** - Implements HTTP/1.1 for web service testing
- **Memcached** - Implements both binary and text Memcached protocols

Each protocol implementation is documented in detail in the User Guide's [Protocols](../guide/protocols.md) section.

## Request-Response Correlation

The protocol layer maintains the necessary state to correlate responses with their originating requests. This is particularly important for protocols that support pipelining or when multiple requests may be in flight simultaneously. The `RequestId` type allows each protocol to define its own correlation mechanism appropriate to its semantics.

## See Also

- [Protocol implementations in User Guide](../guide/protocols.md)
- [Architecture Overview](./overview.md)
- [Transport Layer](./transports.md)
