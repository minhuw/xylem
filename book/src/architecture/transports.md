# Transport Layer

The transport layer (`xylem-transport`) provides network I/O for Xylem: TCP, UDP and Unix domain sockets, driven through one API for groups of connections.

## Connection Groups

A worker thread owns one `ConnectionGroup`, created by a `TransportFactory` (`TcpTransportFactory`, `UdpTransportFactory` or `UnixTransportFactory`). All I/O goes through the group by connection ID:

- `add_connection(target)` opens a connection and returns its ID (`add_connection_path` for Unix sockets).
- `send(conn, data)` writes every byte and returns a `SendReceipt`: the send timestamp and, when TX timestamps are on, the TX range of the send.
- `poll(timeout, &mut ready)` waits once for all connections and fills `ready` with readable connection IDs (level-triggered).
- `recv(conn, buf)` reads what is available and returns a `RecvOutcome`: `Data` with its receive timestamp, `WouldBlock` or `Eof`.
- `poll_tx_timestamps(&mut out)` collects kernel TX timestamps that arrived since the last call.

Each group has one readiness multiplexer, fixed at compile time: epoll on Linux, mio (kqueue) elsewhere. So one syscall checks every connection of a thread. Connection pooling, pipelining and request tracking live in `xylem-core` (`ConnectionPool`), on top of this API. Groups need not be `Send`, so a user-space network stack can implement the trait too.

See the [crate documentation](https://github.com/minhuw/xylem/blob/main/xylem-transport/src/lib.rs) for the full API.

## Timestamps

Every timestamp holds the CPU cycle counter (RDTSC on x86_64) read when the send or receive call completed. By default these user-space timestamps are all Xylem uses.

On Linux, TCP and UDP groups can also use kernel timestamps (`SO_TIMESTAMPING`), configured through `TimestampConfig`:

- **Hardware** timestamps from the NIC, when an interface is configured and has hardware timestamping switched on (checked with `SIOCGHWTSTAMP`).
- **Kernel software** timestamps, when requested explicitly.

Each group logs the source in effect once, at info level. A latency uses kernel timestamps only when the send and the receive both have one from the same clock (the system clock for software stamps, the NIC's clock for hardware stamps). Otherwise it uses the cycle counts of both events.

TX timestamps arrive later than the send that caused them, and are matched to sends by a per-connection counter: bytes for TCP and datagrams for UDP. A TCP stamp covers every send up to the byte it reports. A UDP stamp covers exactly one datagram. The [correlation contract](https://github.com/minhuw/xylem/blob/main/xylem-transport/src/lib.rs) is documented in the crate.

## Supported Transports

- **TCP** - Connection-oriented byte streams using TCP sockets. Every
  connection sets `TCP_NODELAY`, so Nagle's algorithm never holds back a small
  or pipelined request and inflates its latency.
- **UDP** - Connectionless datagram communication
- **Unix Domain Sockets** - Local inter-process communication

Detailed documentation for each transport is available in the [Transports](../guide/transports.md) section of the User Guide.

## See Also

- [Transport implementations in User Guide](../guide/transports.md)
- [Architecture Overview](./overview.md)
- [Protocol Layer](./protocols.md)
