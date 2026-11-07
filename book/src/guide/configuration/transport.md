# Transport Configuration

Each `[[traffic_groups]]` section specifies its own `transport` and `target`. Transport defaults to `"tcp"`. Groups sharing a thread must use the same transport.

## Available Transports

- `tcp` - TCP/IP connections
- `udp` - UDP connections
- `unix` - Unix domain sockets
- `ntcp` - User-space TCP over a TUN device; needs the `ntcp` build feature and an `[ntcp]` section (see [ntcp](../transports/ntcp.md))

## Configuration

### TCP (IP:port)

```toml
[[traffic_groups]]
transport = "tcp"
name = "main"
protocol = "redis"
target = "127.0.0.1:6379"
# ...
```

### Unix Domain Socket (filesystem path)

```toml
[[traffic_groups]]
transport = "unix"
name = "main"
protocol = "redis"
target = "/var/run/redis/redis.sock"
# ...
```

### UDP (IP:port)

UDP works with the protocols that frame requests as datagrams and match
replies by request ID:

| Protocol | Over UDP |
|----------|----------|
| `memcached-ascii` | memcached's UDP frame header; multi-datagram responses are reassembled |
| `memcached-binary` | the same frame header; verified against memcached 1.6.45 |
| `xylem-echo` | one 16-byte request per datagram, for `xylem-echo-server`'s UDP listener |

`redis`, `http` and `masstree` with `transport = "udp"` fail validation.

```toml
[[traffic_groups]]
transport = "udp"
name = "main"
protocol = "memcached-ascii"
target = "127.0.0.1:11211"
request_timeout = "500ms"   # optional; default 1s for UDP
# ...
```

memcached 1.5.6 and later disable UDP; start it with `-U <port>`.

#### Datagrams

xylem parses each received datagram on its own; bytes of one datagram are
never joined with the next. A protocol may absorb a datagram that completes no
request yet, such as one fragment of a multi-datagram response, a duplicate,
or a reply for an unknown request ID.

#### memcached frame header

Every memcached datagram starts with the 8-byte header of memcached's
`protocol.txt` ("UDP protocol"), big-endian u16 fields:

```text
[request id][sequence number][total datagrams][reserved = 0]
```

A request is one datagram whose request id is the low 16 bits of the
connection's request counter, so a request with its value must fit one
datagram: values above 64,987 bytes fail validation. memcached splits a
response into datagrams of at most 1400 bytes, numbered `0..total`. xylem
reassembles them per connection by request id and sequence number in any
arrival order, and drops duplicates and datagrams for unknown request ids. A
response that lost a datagram never completes; its request times out.

#### Request timeout

UDP does not retransmit, so a lost request or response would leave its request
pending, and a closed-loop connection with pipeline depth D would stall after
D losses. Each UDP group therefore has a `request_timeout` (humantime,
default `1s`), counted from the send. A request still pending when it expires:

- frees its pipeline slot, so the connection sends again;
- counts in `requests.timed_out`;
- records no latency: the latency is censored, not set to the timeout.

A response that arrives later is dropped by request ID and counted in
`requests.late_responses`. Open-loop schedules keep their intended send times;
a timeout only frees the slot that a full pipeline was waiting on, and the
delay still shows as send lag and `unsent`. Timeouts also run during the
drain, and warmup requests time out without being counted. Stream transports
(`tcp`, `unix`, `ntcp`) retransmit, never time out requests, and reject
`request_timeout`.

See the [Architecture - Transport Layer](../../architecture/transports.md) for implementation details.
