# HTTP Protocol

Xylem speaks HTTP/1.1 over persistent (keep-alive) connections for
benchmarking web servers and APIs. Every request on a connection reuses it,
and with `max_pending_per_connection` above 1 requests are pipelined:
responses come back in request order, so no request IDs are needed.

## Configuration

```toml
[experiment]
name = "http-get"
duration = "30s"

[[traffic_groups]]
name = "web"
protocol = "http"
target = "127.0.0.1:8080"
threads = [0, 1]
connections_per_thread = 4
max_pending_per_connection = 1

[traffic_groups.protocol_config]
method = "GET"         # GET, POST or PUT
path = "/"             # request target, sent as-is
host = "example.com"   # Host header; defaults to "localhost"
body_size = 64         # POST/PUT body size in bytes

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "results/http-get.json"
```

Every request is the same: the request line, `Host`, `Connection:
keep-alive`, and for POST and PUT a `Content-Length` and a body of
`body_size` `x` bytes.

## Response Framing

Xylem reads responses on a keep-alive connection, so it has to find where
each one ends. It supports these framings (RFC 9112 section 6.3):

- **`Content-Length`**: the body is exactly that many bytes. Repeated
  `Content-Length` headers must agree.
- **`Transfer-Encoding: chunked`**: the body is a series of chunks
  (RFC 9112 section 7.1). Chunk sizes are hexadecimal, in upper or lower
  case. Chunk extensions are ignored, and trailer fields after the
  last-chunk (`0`) are skipped. Every chunk line must end in CRLF.
- **No body**: 204 and 304 responses end at the blank line after their
  headers, whatever their framing headers say.

A response counts as complete only once all of it has arrived, however the
bytes are split across reads. Informational (1xx) responses before the final
response are consumed with it. The bytes counted as received are the whole
response on the wire: interim responses, headers, chunk framing and
trailers.

The following end the run with an error, since xylem cannot frame them on a
keep-alive connection:

- a body delimited by closing the connection (neither `Content-Length` nor
  `Transfer-Encoding`);
- any `Transfer-Encoding` other than a single `chunked`, such as `gzip`,
  `gzip, chunked` or `chunked, chunked`, since xylem does not decode
  content;
- both `Transfer-Encoding` and `Content-Length`: RFC 9112 lets
  `Transfer-Encoding` win but says such a message ought to be handled as an
  error, since it may be an attempt at response smuggling;
- malformed chunked framing: a bad chunk size, a missing CRLF or a malformed
  trailer field;
- an invalid or conflicting `Content-Length`;
- `101 Switching Protocols`;
- a response larger than 64 KiB, the per-response limit of every protocol.
