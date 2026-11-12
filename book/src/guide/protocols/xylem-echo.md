# Xylem Echo Protocol

`protocol = "xylem-echo"` loads `xylem-echo-server`, to test Xylem's own
measurement. A request is 16 bytes, `[request_id: u64][delay_us: u64]`
(little-endian), and the server echoes it after `delay_us` microseconds.
Xylem always sends a delay of 0. `protocol_config` takes no keys and may be
left out.

## Transports and the echo server

`xylem-echo-server` listens on TCP and UDP at the same `--bind` address and
`--port`, or on a Unix socket with `--unix`. The protocol works over `tcp`,
`udp`, `unix` and `ntcp`. Over UDP each request is one datagram and the server
answers each with one datagram after the request's delay; replies may arrive
out of order and match their requests by request ID. See the server's
`README.md` and the [UDP transport](../configuration/transport.md#udp-ipport).

## Error Replies

An echo is never an error reply, so `requests.errors` is always zero.

## See Also

- [Protocol Configuration](../configuration/protocol.md)
- [Architecture: Protocol Layer](../../architecture/protocols.md)
