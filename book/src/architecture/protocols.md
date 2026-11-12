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

    fn setup_pending(&self, _conn_id: usize) -> bool {
        false
    }

    fn connection_roles(&self, load: usize) -> Vec<ConnectionRole> {
        vec![ConnectionRole::Load; load]
    }

    fn bind_connections(&mut self, _conn_ids: &[usize]) {}

    fn drain_outbox(&mut self, _conn_id: usize, _outbox: &mut Outbox) {}

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
  complete no request, such as one datagram of a multi-datagram UDP response,
  an NVMe/TCP R2T, or the leading part of a response longer than the receive
  buffer; they count toward the next completed response's bytes.
- `can_send` lets a protocol hold back requests on a connection, for example
  until the Masstree handshake completes. The worker asks when it schedules
  the connection, so once true it must stay true until that connection's
  next request.
- `setup_pending` reports a connection that is still in connection setup
  (Redis AUTH, SELECT and HELLO, the Masstree handshake). Its requests go out
  as soon as `can_send` allows, off the connection's intended send schedule,
  which starts once setup is done.
- `connection_roles` lets a session span connections. For a group with
  `load` connections per thread it returns the connections to open, in
  order: exactly `load` `Load` connections, which carry the traffic policy
  and statistics, and any `Control` connections the protocol needs for
  itself, such as an NVMe/TCP admin queue per controller. A control
  connection has no policy, takes no group-dispatched arrivals, and may send
  only setup requests. `bind_connections` receives the opened IDs in the same
  order before the run.
- `drain_outbox` runs after each read the worker parsed on a connection. The
  protocol puts in `Outbox::data` bytes to send on that connection that are
  no requests (an NVMe/TCP H2CData PDU answering an R2T, an HTTP/2 SETTINGS
  acknowledgement); the worker sends them before the connection's next
  request, counts them as sent bytes, and gives them no latency. Bytes the
  socket does not take at once wait in the connection's send queue until it
  is writable, ahead of later requests, while the worker keeps reading. In
  `Outbox::wake` it names connections that data on this one unblocked, such
  as I/O queues waiting for their admin queue; the worker reschedules just
  those.
- `name` returns the protocol name, and `reset` clears per-connection state.

The core engine decides when to send; the protocol decides what to send. The
Redis, Memcached, HTTP, HTTP/2, gRPC, Masstree, DNS, NVMe/TCP and ClickHouse
protocols have a `from_config` constructor that builds them from their
`protocol_config` section.

## Protocol Design

Protocol implementations are designed to minimize state and memory overhead. Each implementation encodes requests in the appropriate wire format and parses responses into a form suitable for latency measurement. The `RequestId` mechanism enables request-response correlation, which is required for accurate latency measurement in scenarios involving pipelining or out-of-order responses.

## Supported Protocols

The following protocol implementations are available:

- **Redis** - RESP (Redis Serialization Protocol) for key-value operations
- **HTTP** - HTTP/1.1 for web service testing
- **HTTP/2 and gRPC** - Cleartext HTTP/2 with concurrent streams, flow
  control and HPACK, and gRPC unary calls on top of it
- **Memcached** - Binary and text protocol variants for cache testing
- **Masstree** - MessagePack-based protocol for the Masstree key-value store
- **DNS** - Standard queries with one question, length-prefixed on byte
  streams and bare in UDP datagrams
- **NVMe/TCP** - NVM reads and writes on the I/O queues of NVMe over Fabrics
  controllers
- **ClickHouse** - SQL queries over the native TCP protocol, one at a time
  per connection, with results streamed by column type without decoding
- **Xylem Echo** - Echo protocol for testing xylem itself

Detailed documentation for each protocol is available in the [Protocols](../guide/protocols.md) section of the User Guide.

## Request-Response Correlation

The protocol layer maintains state necessary for correlating responses with their originating requests. This is required for protocols supporting pipelining or scenarios where multiple requests are in flight concurrently. Each protocol defines an appropriate `RequestId` type for its correlation semantics.

## See Also

- [Protocol implementations in User Guide](../guide/protocols.md)
- [Architecture Overview](./overview.md)
- [Transport Layer](./transports.md)
