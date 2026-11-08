# Protocol Layer

The protocol layer (`xylem-protocols`) implements application-level protocols for RPC workload generation and measurement.

## Protocol Trait

All protocol implementations conform to the `Protocol` trait, defined in
[`xylem-common/src/protocol.rs`](https://github.com/minhuw/xylem/blob/main/xylem-common/src/protocol.rs)
and re-exported by `xylem-protocols` and `xylem-core`:

```rust
pub trait Protocol: Send {
    type RequestId: Eq + Hash + Clone + Copy + Debug;

    fn next_request(&mut self, conn_id: usize) -> Request<Self::RequestId>;

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> anyhow::Result<(usize, Option<Self::RequestId>)>;

    fn can_send(&self, _conn_id: usize) -> bool {
        true
    }

    fn setup_pending(&self, _conn_id: usize) -> bool {
        false
    }

    fn name(&self) -> &'static str;

    fn reset(&mut self);
}
```

- `next_request` builds the next request for a connection from the protocol's
  own workload state (command selection, keys, value sizes, insert phase). The
  returned `Request` carries the bytes, the request ID, and metadata that marks
  warm-up requests, whose latency is not recorded.
- `parse_response` returns the bytes consumed and the ID of the completed
  request, or `(0, None)` when the response is still incomplete. Consuming
  bytes without returning an ID is an error.
- `can_send` lets a protocol hold back requests on a connection, for example
  until the Masstree handshake completes.
- `setup_pending` reports a connection that is still in connection setup
  (Redis AUTH, SELECT and HELLO, the Masstree handshake). Its requests go out
  as soon as `can_send` allows, off the connection's intended send schedule,
  which starts once setup is done.
- `name` returns the protocol name, and `reset` clears per-connection state.

The core engine decides when to send; the protocol decides what to send. The
Redis, Memcached, HTTP and Masstree protocols have a `from_config` constructor
that builds them from their `protocol_config` section.

## Protocol Design

Protocol implementations are designed to minimize state and memory overhead. Each implementation encodes requests in the appropriate wire format and parses responses into a form suitable for latency measurement. The `RequestId` mechanism enables request-response correlation, which is required for accurate latency measurement in scenarios involving pipelining or out-of-order responses.

## Supported Protocols

The following protocol implementations are available:

- **Redis** - RESP (Redis Serialization Protocol) for key-value operations
- **HTTP** - HTTP/1.1 for web service testing
- **Memcached** - Binary and text protocol variants for cache testing
- **Masstree** - MessagePack-based protocol for the Masstree key-value store
- **Xylem Echo** - Echo protocol for testing xylem itself

Detailed documentation for each protocol is available in the [Protocols](../guide/protocols.md) section of the User Guide.

## Request-Response Correlation

The protocol layer maintains state necessary for correlating responses with their originating requests. This is required for protocols supporting pipelining or scenarios where multiple requests are in flight concurrently. Each protocol defines an appropriate `RequestId` type for its correlation semantics.

## See Also

- [Protocol implementations in User Guide](../guide/protocols.md)
- [Architecture Overview](./overview.md)
- [Transport Layer](./transports.md)
