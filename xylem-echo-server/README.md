# Xylem Echo Server

A simple test server implementing the Xylem Echo Protocol for validating latency measurement infrastructure.

Run with `--unix /tmp/xylem-echo.sock` to listen on a Unix socket. The server refuses
any existing path, including regular files, symlinks, live sockets, and stale
sockets. It never deletes the supplied path. After stopping the server, confirm
that the socket has no owner before removing it manually and restarting, or use
a new `--unix` path.
