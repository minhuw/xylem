# DNS Protocol

`protocol = "dns"` sends standard DNS queries (RFC 1035) for load against
authoritative or recursive servers, over `tcp`, `unix`, `ntcp` and `udp`.
Each query asks one question in class IN; its name and type are drawn
independently from weighted mixes.

```toml
[experiment]
name = "knot-tcp"
duration = "30s"
seed = 1

[[traffic_groups]]
name = "knot"
protocol = "dns"
target = "127.0.0.1:5353"
threads = [0]
connections_per_thread = 4
max_pending_per_connection = 16

[traffic_groups.protocol_config]
recursion_desired = false                   # RD flag; default true
edns = { udp_size = 1232, dnssec_ok = false }  # OPT record; default none
nxdomain = "success"                        # or "error" (default)
qtypes = [
  { qtype = "A", weight = 8 },
  { qtype = "AAAA", weight = 2 },
  { qtype = "TXT" },                        # weight defaults to 1
]
names = [
  { name = "www.example.test.", weight = 4 },
  { name = "absent.example.test." },
  { pattern = "h{key}.wild.example.test.", keys = { strategy = "zipfian", n = 10000, theta = 0.99 } },
]

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "results/knot-tcp.json"
```

## Queries

- `names` (required): each entry is either a fixed `name` or a `pattern`
  with one `{key}`, which each query replaces with a decimal key from `keys`.
  `keys` takes the strategies of the key-value protocols without their value
  size and prefix: `sequential` (`start`), `random` (`max`), `round-robin`
  (`max`), `zipfian` (`n`, `theta`) and `gaussian` (`mean_pct`,
  `std_dev_pct`, `max`). Names are plain ASCII labels of up to 63 bytes, 255
  bytes in all; the final dot is optional. A pattern must leave room for a
  20-digit key.
- `qtypes`: A, NS, CNAME, SOA, PTR, MX, TXT, AAAA, SRV, NAPTR, DS, RRSIG,
  NSEC, DNSKEY, SVCB, HTTPS, CAA, ANY, or any other type as `TYPE<n>`
  (RFC 3597). Default: A only.
- `recursion_desired`: the RD flag, default true as with dig and dnspython.
- `edns`: adds an EDNS(0) OPT record (RFC 6891) with the UDP payload size
  `udp_size` (512 to 65535, default 1232) and the DO bit `dnssec_ok`.

Every weight must be finite and non-negative, and at least one per mix
positive. `seed` in `[experiment]` makes the draws reproducible.

## Message IDs and Pipelining

The 16-bit message ID is the request ID. Over a stream each connection hands
out IDs in turn and skips those still in flight, so up to
`max_pending_per_connection` queries (at most 65536) are pipelined on a
connection. Responses may come back in any order (RFC 7766) and are matched
by ID.

Over UDP a lost response never frees its ID, so IDs are handed out in turn
without skipping: a query reuses the ID of the query sent 65536 queries
before it on that connection, which has long timed out at any realistic
rate and `request_timeout`.

## Framing

Over `tcp`, `unix` and `ntcp` each message is preceded by its length as a
2-byte big-endian integer (RFC 1035 section 4.2.2, RFC 7766). Over `udp`
each query and each response is one bare datagram.

## Responses and Error Replies

A response must have the QR bit, opcode QUERY and the ID of a query in
flight, echo that query's question (names compare case-insensitively), and
be well formed to its last record. A response without a question is accepted
only with an RCODE other than NOERROR, as servers send for FORMERR. Over a
stream anything else stops the run. Over UDP a response for no query in
flight, or for a different question, is dropped as a duplicate or late
response, and malformed datagrams stop the run.

An error reply completes its query and counts in `requests.errors` by kind
(see [Output Formats](../output-formats.md#error-replies)):

- A response with the TC (truncated) bit counts as kind `TC`. It is common
  over UDP when an answer exceeds 512 bytes, or the `edns.udp_size`.
- Otherwise a non-zero RCODE, including the extended bits of an OPT record,
  counts by its name: `FORMERR`, `SERVFAIL`, `NXDOMAIN`, `NOTIMP`,
  `REFUSED`, `BADVERS`, ... (`UNASSIGNED` for unassigned codes).
- NXDOMAIN counts as success with `nxdomain = "success"`, for a load that
  includes absent names on purpose.

Set `on_error = "abort"` on the traffic group to stop the run at the first
error reply instead.

## Out of Scope

Zone transfers (AXFR, IXFR), NOTIFY, UPDATE, TSIG and TKEY are not
supported: such qtypes fail validation, and every query has opcode QUERY.
DNS over TLS, HTTPS or QUIC is not supported either.

## Testing

The `dns_integration` tests run xylem against Knot DNS 3.6
(`cznic/knot:3.6`) with the zones of Stackbench's knot benchmark, in
`tests/knot`, over TCP and UDP:

```bash
cargo nextest run --profile integration -E 'binary(dns_integration)'
```

## See Also

- [Protocol Configuration](../configuration/protocol.md)
- [Transport Configuration: UDP](../configuration/transport.md#udp-ipport)
