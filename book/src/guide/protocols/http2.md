# HTTP/2 Protocol

`protocol = "http2"` speaks HTTP/2 over cleartext with prior knowledge
(h2c, RFC 9113 section 3.3): the connection starts with the HTTP/2 preface,
with no TLS and no HTTP/1.1 Upgrade. The server must accept HTTP/2 on a
plain listener, as nginx does with `http2 on;` and h2o does by default.
Every request is the same, with the knobs of the [HTTP/1.1
protocol](./http.md). [gRPC](./grpc.md) runs on the same HTTP/2 layer.

## Configuration

```toml
[experiment]
name = "h2c-get"
duration = "30s"

[[traffic_groups]]
name = "web"
protocol = "http2"
target = "127.0.0.1:8080"
threads = [0, 1]
connections_per_thread = 4
# Streams in flight per connection
max_pending_per_connection = 16

[traffic_groups.protocol_config]
method = "GET"            # GET, POST or PUT
path = "/"                # :path
authority = "example.com" # :authority; defaults to the target
body_size = 64            # POST/PUT body size in bytes

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "results/h2c-get.json"
```

`authority` defaults to the group's `target`, or `localhost` over a Unix
socket. A POST or PUT carries `content-type: application/octet-stream`, a
`content-length` and `body_size` `x` bytes. HTTP/2 works over `tcp`, `unix`
and `ntcp`; `udp` fails validation.

## Connections and streams

Each connection first sends a setup request: the preface, SETTINGS
(server push off, a 1 MiB stream window) and a WINDOW_UPDATE opening the
connection window to 16 MiB. The server's SETTINGS is its reply, so no
request goes out before it; none of this is measured. SETTINGS and PING
from the server are acknowledged at once.

Every request is a stream, and the stream ID is its request ID. Up to
`max_pending_per_connection` streams are in flight on a connection, never
more than the server's `SETTINGS_MAX_CONCURRENT_STREAMS`. Responses of
different streams may interleave; a stream completes at its END_STREAM.
Header blocks may span CONTINUATION frames, and padded frames, PRIORITY
and unknown frame types are handled.

Request headers use only HPACK static table indices and literals without
indexing, so the header block is built once; response headers are decoded
with the server's dynamic table.

## Flow control

Received DATA is discarded as it arrives. A WINDOW_UPDATE gives back a
stream's or the connection's window once half of it is used, so responses
of any size stream through. Request bodies respect the server's connection
and stream windows and its `SETTINGS_MAX_FRAME_SIZE`; what does not fit is
sent when the server opens its windows.

## Errors

A response whose `:status` is not 200 is an error reply whose kind is the
status (`"404"`, `"503"`), counted per the group's `on_error`. Informational
(1xx) responses are skipped. An RST_STREAM from the server ends its stream
as an error reply named after the error code (`"REFUSED_STREAM"`).

A GOAWAY with `NO_ERROR` stops new streams on its connection while the
streams it accepted complete; xylem does not reconnect, so keep the
server's per-connection request limit high (nginx: `keepalive_requests`).
A GOAWAY with another error code or one that refuses streams in flight
fails the run, as does any connection error: malformed frames, a
flow-control violation, an HPACK error, PUSH_PROMISE, or a server that
answers in HTTP/1.1.

## Throughput

`throughput_mbps` counts every byte on the wire both ways, frame headers
and control frames included.

## Testing

`just test-http2` runs the integration tests against nginx with h2c, over
TCP and a Unix socket.
