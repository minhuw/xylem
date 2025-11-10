# Protocol Layer

The protocol layer (`xylem-protocols`) implements application-level protocols for RPC workload generation and measurement.

## Protocol Trait

All protocol implementations conform to the `Protocol` trait interface:

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

The trait provides a uniform interface for protocol implementations, enabling the core engine to interact with any protocol through a consistent API.

## Protocol Design

Protocol implementations are designed to minimize state and memory overhead. Each implementation encodes requests in the appropriate wire format and parses responses into a form suitable for latency measurement. The `RequestId` mechanism enables request-response correlation, which is required for accurate latency measurement in scenarios involving pipelining or out-of-order responses.

## Supported Protocols

The following protocol implementations are available:

- **Redis** - RESP (Redis Serialization Protocol) for key-value operations
- **HTTP** - HTTP/1.1 for web service testing
- **Memcached** - Binary and text protocol variants for cache testing

Detailed documentation for each protocol is available in the [Protocols](../guide/protocols.md) section of the User Guide.

## Request-Response Correlation

The protocol layer maintains state necessary for correlating responses with their originating requests. This is required for protocols supporting pipelining or scenarios where multiple requests are in flight concurrently. Each protocol defines an appropriate `RequestId` type for its correlation semantics.

## See Also

- [Protocol implementations in User Guide](../guide/protocols.md)
- [Architecture Overview](./overview.md)
- [Transport Layer](./transports.md)
