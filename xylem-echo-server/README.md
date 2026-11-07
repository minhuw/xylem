# Xylem Echo Server

A simple test server implementing the Xylem Echo Protocol for validating latency measurement infrastructure.

A request is 16 bytes, `[request_id: u64][delay_us: u64]` (little-endian). The
server waits `delay_us` microseconds, capped at `--max-delay-us`, and then echoes
the request.

## TCP and UDP

By default the server listens on TCP and UDP at the same `--bind` address and
`--port` (default `0.0.0.0:9999`). Both listeners always run; TCP and UDP ports
do not conflict.

- Over TCP, each connection's requests are answered in order, one at a time.
- Over UDP, each datagram is one request, answered by one datagram to its
  sender. Datagrams of any other size are dropped. Delayed replies run
  concurrently, so a short request can overtake a long one; xylem matches
  replies by request ID.

To load the UDP listener, use `transport = "udp"` with `protocol = "xylem-echo"`.

## Unix sockets

Run with `--unix /tmp/xylem-echo.sock` to listen on a Unix socket instead. The server refuses
any existing path, including regular files, symlinks, live sockets, and stale
sockets. It never deletes the supplied path. After stopping the server, confirm
that the socket has no owner before removing it manually and restarting, or use
a new `--unix` path.
