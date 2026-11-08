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

## Timestamps

`timestamps` chooses where a traffic group's send and receive timestamps come
from. It applies to `tcp` and `udp` groups on Linux, and `"nic"` to `ntcp`
groups with the DPDK backend:

```toml
[[traffic_groups]]
transport = "udp"
timestamps = "kernel"        # "user" (default), "kernel", or { hardware = "data0" }
# ...
```

- `"user"` (default): only the CPU timestamp counter (RDTSC), read in user
  space when each send and receive call completes. No `SO_TIMESTAMPING` and no
  error-queue reads.
- `"kernel"`: also kernel software timestamps (`SO_TIMESTAMPING`, on the
  system clock): a TX timestamp when the request leaves the kernel's stack for
  the driver, read back from the socket's error queue, and an RX timestamp
  when the response enters the stack. Any interface has them, loopback too.
- `{ hardware = "<interface>" }`: NIC hardware timestamps, on the NIC's PTP
  clock, and every socket is bound to the interface. The interface must
  already have hardware timestamping switched on, for example by a PTP daemon
  or with linuxptp's `hwstamp_ctl -i data0 -t 1 -r 1` as root. When the run
  starts, before any worker, xylem reads the interface's setting with
  `SIOCGHWTSTAMP` and fails with an error if it is off or unsupported. It
  never switches NIC timestamping on, and never falls back to another source.
- `"nic"`: for `ntcp` groups with `ntcp.backend = "dpdk"` only, which own the
  NIC: the NIC's hardware RX timestamps converted to xylem's clock, and the
  time ntcp queued each request to the NIC as its TX timestamp. See
  [ntcp NIC timestamps](../transports/ntcp.md#nic-timestamps-dpdk).

Every timestamp keeps its user-space cycles. A request's latency uses the
kernel timestamps when its send and its response both have one from the same
clock, and the cycles otherwise; either way the
[latency breakdown](../output-formats.md#latency-breakdown) reports the
service latency, and with kernel timestamps it splits the round trip into the
part between the user-space and the kernel timestamps (`host_stack_us`) and
the kernel round trip (`network_server_us`). A
request whose TX or RX timestamp did not arrive is measured in cycles and
counted in `service_us` alone; `kernel_split_count` says how many were split.

- `unix` groups reject anything but `"user"`, and `ntcp` groups anything but
  `"user"` and (with the DPDK backend) `"nic"`: neither has kernel packet
  timestamps.
- Traffic groups that share a thread share its connection group, whose
  sockets are configured alike, so they must use the same `timestamps`.
- Kernel timestamps cost an error-queue read per poll for each connection
  with sends not yet stamped, and a `recvmsg` with ancillary data per read.
  Compare runs with the same source.

See the [Architecture - Transport Layer](../../architecture/transports.md) for implementation details.
