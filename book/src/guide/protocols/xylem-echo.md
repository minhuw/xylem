# Xylem Echo Protocol

> **Note**: This is a testing and development protocol, not intended for production use.

> **TODO**: This page requires validation of TOML configuration format and protocol-specific details.

The xylem-echo protocol is used for testing and development purposes. Full documentation will be added once the TOML configuration format is validated.

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
