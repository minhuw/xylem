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

    fn parse_response<'a>(
        &mut self,
        conn_id: usize,
        data: &'a [u8],
    ) -> anyhow::Result<Parsed<'a, Self::RequestId>>;

    fn can_send(&self, _conn_id: usize) -> bool {
        true
    }

    fn name(&self) -> &'static str;

    fn reset(&mut self);
}
```

- `next_request` builds the next request for a connection from the protocol's
  own workload state (command selection, keys, value sizes, insert phase). The
  returned `Request` carries the bytes, the request ID, and metadata that marks
  warm-up requests, whose latency is not recorded.
- `parse_response` returns `Parsed::Complete { consumed, id, error }` for a
  complete response, or `Parsed::Incomplete`. `error` is set for an error
  reply (a Redis error, an HTTP 4xx or 5xx, ...): an `ErrorReply` with a
  `kind` by which it is counted and a `message`, both borrowed from `data` or
  static, so reporting one allocates nothing. An error reply completes its
  request; `Err` is for bytes that cannot be framed and for failed connection
  setup, and fails the run. `Parsed::Absorbed(n)` consumes bytes that
  complete no request, such as one datagram of a multi-datagram UDP response;
  on a byte stream that is an error.
- `can_send` lets a protocol hold back requests on a connection, for example
  until the Masstree handshake completes.
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
