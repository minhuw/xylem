# UDP Transport

`transport = "udp"` sends each request as one datagram from its own socket per
connection, and reads each reply datagram on its own. It works with
`memcached-ascii`, `memcached-binary` and `xylem-echo`.

See [Transport Configuration: UDP](../configuration/transport.md#udp-ipport) for
the supported protocols, memcached's frame header, and the `request_timeout`
that recovers from lost datagrams.
