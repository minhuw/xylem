# Memcached Protocol

xylem speaks memcached's ASCII (`memcached-ascii`) and binary
(`memcached-binary`) protocols with GET and SET, over `tcp`, `unix` and `udp`.

```toml
[[traffic_groups]]
name = "main"
protocol = "memcached-ascii"
target = "127.0.0.1:11211"
threads = [0]
connections_per_thread = 4
max_pending_per_connection = 8

[traffic_groups.protocol_config]
operation = "GET"   # or "SET"

[traffic_groups.protocol_config.keys]
strategy = "round-robin"
max = 10000
value_size = 4096

[traffic_groups.protocol_config.insert_phase]
key_count = 10000   # SET every key as warmup before measuring

[traffic_groups.traffic_policy]
type = "closed-loop"
```

## Over UDP

Set `transport = "udp"` on the group; no other setting changes. memcached 1.5.6
and later disable UDP, so start it with `-U <port>`, for example
`memcached -p 11211 -U 11211`.

- Every datagram carries memcached's 8-byte UDP frame header. xylem numbers
  each connection's requests and sends the low 16 bits as the request id.
- Responses are matched by request id, so lost and reordered datagrams are
  tolerated. memcached splits large responses into 1400-byte datagrams; xylem
  reassembles them in any arrival order and drops duplicates.
- A request must fit one datagram, so values are limited to 64,987 bytes.
- Both ASCII and binary work over UDP; binary was verified against
  memcached 1.6.45.
- Lost datagrams time out after the group's `request_timeout` (default 1 s)
  and count in `requests.timed_out`.

See [Transport Configuration: UDP](../configuration/transport.md#udp-ipport).

## See Also

- [Protocol Configuration](../configuration/protocol.md)
