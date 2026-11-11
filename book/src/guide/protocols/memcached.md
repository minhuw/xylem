# Memcached Protocol

xylem speaks memcached's ASCII (`memcached-ascii`) and binary
(`memcached-binary`) protocols over `tcp`, `unix` and `udp`, with GET, SET,
ADD, REPLACE, DELETE, INCR, DECR, TOUCH and multi-key GET.

```toml
[[traffic_groups]]
name = "main"
protocol = "memcached-ascii"
target = "127.0.0.1:11211"
threads = [0]
connections_per_thread = 4
max_pending_per_connection = 8

[traffic_groups.protocol_config]
operation = "GET"   # every request; or a mix with `operations`, below

[traffic_groups.protocol_config.keys]
strategy = "round-robin"
max = 10000
value_size = 4096

[traffic_groups.protocol_config.insert_phase]
key_count = 10000   # SET every key as warmup before measuring

[traffic_groups.traffic_policy]
type = "closed-loop"
```

## Operations

| Name | ASCII request | Binary request | Parameters |
|------|---------------|----------------|------------|
| `get` | `get <key>` | GETK | |
| `set`, `add`, `replace` | `<name> <key> 0 0 <size>` and the value | SET, ADD, REPLACE | |
| `delete` | `delete <key>` | DELETE | |
| `incr`, `decr` | `<name> <key> <delta>` | INCREMENT, DECREMENT; a missing key is not created | `delta` (default 1) |
| `touch` | `touch <key> <exptime>` | TOUCH | `exptime` in seconds (default 0: never expires) |
| `mget` | `get <key> <key> ...` | a GETKQ per key, then NOOP | `count` keys (default 10) |

Every key of a request, each key of an `mget` included, is drawn from the key
distribution. `incr` and `decr` need a numeric value; the values xylem writes
are `x` bytes, so on keys xylem stored they get a `CLIENT_ERROR` (binary:
`DELTA_BADVAL`) error reply, and on missing keys a `NOT_FOUND` result.

`operation` sends one operation in every request, with default parameters.
`operations` takes the shape of [Redis's](redis.md): a fixed operation, or a
weighted random mix whose commands may take `params` and their own `keys`
distribution (with the group's prefix and value size). Set `operation` or
`operations`, not both.

```toml
[traffic_groups.protocol_config.operations]
strategy = "weighted"
commands = [
    { name = "get", weight = 0.80 },
    { name = "set", weight = 0.10 },
    { name = "mget", weight = 0.05, params = { count = 8 } },
    { name = "delete", weight = 0.03 },
    { name = "touch", weight = 0.02, params = { exptime = 300 } },
]
```

The `value_size` section sets the size of SET, ADD and REPLACE values, drawn
per request, in place of `keys.value_size`; see
[Value Size Configuration](../configuration/workload.md#value-size-configuration).
With `per_command`, `set`, `add` and `replace` take their own sizes.

## Error Replies

An error reply completes its request and counts in `requests.errors`, with
its latency in `error_latency` rather than the latency distribution (see
[Output Formats](../output-formats.md#error-replies)):

- ASCII: an `ERROR`, `CLIENT_ERROR ...` or `SERVER_ERROR ...` line, by that
  keyword. An error line also ends a GET response after any values before it.
- Binary: a non-zero response status, by its name in memcached's
  `protocol_binary.h` (`E2BIG`, `DELTA_BADVAL`, `ENOMEM`, ...;
  `UNKNOWN_STATUS` for others). A multi-get with several reports the first.

A miss or a failed condition is a successful reply, not an error: a GET's
`END` without a value, `NOT_FOUND`, `NOT_STORED` and `EXISTS`, and the binary
statuses `KEY_ENOENT`, `NOT_STORED` and `KEY_EEXISTS`.

Error replies are classified the same way over UDP. Set `on_error = "abort"`
on the traffic group to stop the run at the first error reply instead. A
reply xylem cannot parse still stops the run.

## Over UDP

Set `transport = "udp"` on the group; no other setting changes. memcached 1.5.6
and later disable UDP, so start it with `-U <port>`, for example
`memcached -p 11211 -U 11211`.

- Every datagram carries memcached's 8-byte UDP frame header. xylem numbers
  each connection's requests and sends the low 16 bits as the request id.
- Responses are matched by request id, so lost and reordered datagrams are
  tolerated. memcached splits large responses into 1400-byte datagrams; xylem
  reassembles them in any arrival order and drops duplicates.
- A request must fit one datagram, so values, every size a `value_size`
  distribution can draw included, are limited to 64,987 bytes.
- `mget` is TCP- and Unix-only: memcached answers a multi-get over UDP with a
  datagram per value, each marked as a whole response, so its end is unknown.
- Both ASCII and binary work over UDP; binary was verified against
  memcached 1.6.45.
- Lost datagrams time out after the group's `request_timeout` (default 1 s)
  and count in `requests.timed_out`.

See [Transport Configuration: UDP](../configuration/transport.md#udp-ipport).

## See Also

- [Protocol Configuration](../configuration/protocol.md)
