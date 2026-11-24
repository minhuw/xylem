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

A response with status 400 or above is an error reply: it completes its
request and counts in `requests.errors`, by its status code (`"404"`,
`"503"`), and its latency goes to `error_latency`, not the latency
distribution; see [Output Formats](../output-formats.md#error-replies). 1xx,
2xx and 3xx responses are successful. Set `on_error = "abort"` on the traffic
group to stop the run at the first error reply instead.

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
- a response with `Transfer-Encoding: chunked` (or headers alone) larger than
  64 KiB, the per-read buffer limit. `Content-Length` responses larger than
  64 KiB stream incrementally via `Parsed::Absorbed` in up to 64 KiB chunks.

## Per-Request Key Paths

Add an optional `keys` section to vary the request target per request instead
of sending a fixed `path`:

```toml
[traffic_groups.protocol_config]
method = "GET"
path = "/obj/"
keys = { strategy = "round-robin", max = 1000, value_size = 64, prefix = "k" }
```

Each request sends `{path}{prefix}{key_num}` (for example `/obj/k0`, `/obj/k1`,
...). When `keys` is omitted, xylem prebuilds the request once and sends `path`
unchanged.

## Deterministic Response Body Verification

Set `verify = true` together with `verify_body_size` (and optional `verify_seed`,
default `0`) on a `GET` traffic group to compare every byte of every response
body against a deterministic function of `(verify_seed, path, verify_body_size)`.
`Content-Length` responses of any size (including 64 KiB, 96 KiB, 128 KiB, and
1 MiB+ multi-segment bodies) are verified incrementally over HTTP/1.1 keep-alive
without raising the 64 KiB receive chunk limit:

```toml
[experiment]
name = "http-get-verify"
duration = "10s"

[[traffic_groups]]
name = "web"
protocol = "http"
target = "127.0.0.1:8080"
threads = [0]
connections_per_thread = 4
max_pending_per_connection = 1

[traffic_groups.protocol_config]
method = "GET"
path = "/obj/"
keys = { strategy = "round-robin", max = 64, value_size = 64, prefix = "k" }
verify = true
verify_seed = 42
verify_body_size = 98304

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "results/http-get-verify.json"
```

### Pattern Formula

A server or synthetic responder can reproduce the exact byte sequence for
`(seed: u64, path: &[u8], body_size: usize)` as follows, where `path` is the
exact request target sent on the request line (static `path` when `keys` is
omitted, or `{path}{prefix}{key_num}` when `keys` is configured):

1. Hash `path` with 64-bit FNV-1a (offset basis `0xcbf29ce484222325`, prime
   `0x00000100000001b3`):
   `h = fnv1a_64(path)`
2. Define `splitmix64(x)`:
   - `x = x.wrapping_add(0x9e3779b97f4a7c15)`
   - `x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9)`
   - `x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb)`
   - return `x ^ (x >> 31)`
3. Compute the per-path base word:
   `base = splitmix64(seed ^ h ^ (body_size as u64).wrapping_mul(0x9e3779b97f4a7c15))`
4. For each 64-bit word `w = 0, 1, 2, ...` covering body bytes
   `[8*w .. 8*w + 8)`, emit
   `splitmix64(base.wrapping_add(w.wrapping_mul(0x9e3779b97f4a7c15))).to_le_bytes()`,
   truncated to `body_size` bytes.

### Verification Errors and Run Verdict

When `verify = true`, any failure is counted as a named error in
`requests.errors` / `requests.errors_by_kind`, recorded in
`traffic_groups[].verification` (`verified`, `failures`, and `first_mismatch`),
and fails the `verification` validity check (`verdict = "invalid_run"`, exit
code `3`). Set `on_error = "abort"` on the traffic group to exit immediately
with code `1` on the first verification error reply:

- `BadStatus` — HTTP response status code is not `200`.
- `ContentLengthMismatch` — `Content-Length` does not equal `verify_body_size`,
  or a chunked response decoded to more than `verify_body_size` bytes.
- `BodyMismatch` — a byte in the response body differs from the expected
  pattern (`first_mismatch.offset`, `expected`, and `actual` pinpoint the first
  mismatched byte).
- `ShortBody` — the connection closed before all `Content-Length` body bytes
  arrived, or a chunked response decoded to fewer than `verify_body_size` bytes.
