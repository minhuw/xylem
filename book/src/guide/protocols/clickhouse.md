# ClickHouse Protocol

`protocol = "clickhouse"` sends SQL queries over ClickHouse's native TCP
protocol (port 9000), the protocol `clickhouse-client` and
`clickhouse-benchmark` speak. Each connection logs in once, then runs one
query at a time and reads its whole result.

```toml
[experiment]
name = "clickhouse"
duration = "30s"
seed = 1

[[traffic_groups]]
name = "queries"
protocol = "clickhouse"
target = "127.0.0.1:9000"
threads = [1]
connections_per_thread = 4
max_pending_per_connection = 1             # required: one query at a time

[traffic_groups.protocol_config]
database = "bench"                         # default: the user's default database
user = "xylem"                             # default: "default"
password = "xylem-test"                    # or password_env = "VAR"; default: empty
settings = { max_threads = 1 }             # sent with every query; default: none
queries = [
  { sql = "SELECT name, price FROM items WHERE id = 4242", weight = 8 },
  { sql = "SELECT category, count(), avg(price) FROM items GROUP BY category" },
  { sql = "SELECT * FROM items" },           # weight defaults to 1
]

[traffic_groups.traffic_policy]
type = "closed-loop"

[output]
format = "json"
file = "results/clickhouse.json"
```

## Queries

- `query`: the SQL text of every query, or
- `queries`: a weighted mix, each entry an `sql` text and a `weight`
  (default 1), drawn per query with probability weight / total weight by the
  shared weighted selector; `seed` in `[experiment]` makes the draws
  reproducible. Set exactly one of `query` and `queries`.
- `database`, `user` (default `default`) and `password` log in; set
  `password_env` to the name of an environment variable instead of
  `password` to keep the password out of the profile. Logs and errors never
  show it.
- `settings`: query settings such as `max_threads` or `max_block_size`, as
  strings, integers, floats or booleans (sent as 1 and 0). They are marked
  important, so a server that does not know one fails each query with an
  error reply instead of ignoring it.

The SQL text goes to the server as it is. A `FORMAT` clause changes
nothing: over the native protocol the server always sends Native blocks. An
`INSERT` must carry its rows in the query (`INSERT ... VALUES` or
`INSERT ... SELECT`); one that waits for data blocks from the client fails
the run, since xylem sends none.

## Connection Setup

Each connection first sends a Hello with the database, user and password,
and waits for the server's Hello. This setup request is not measured, and a
connection's schedule starts once it is done. A failed login fails the run
with the server's message, such as `ClickHouse refused the connection:
DB::Exception: xylem: Authentication failed: ... (code 516)`.

Xylem speaks protocol revision 54453, and both sides use the lower of it
and the server's revision. 54453 is the last revision before custom
serializations (54454), so the server sends every column in its default
layout and never sparse; Stackbench's ClickHouse 26.8.29.10 speaks 54492.
Servers older than revision 54429 (ClickHouse 20.x) are refused.
Compression stays off: the client chooses it in each query, so a server
cannot turn it on. There is no TLS, and no keepalive Ping: connections stay
busy.

## Results

The server answers a query with packets until EndOfStream: an empty header
block that names the result's columns, the result blocks, Progress,
ProfileInfo, Totals, Extremes, Log and ProfileEvents packets. Xylem reads the
type name of each column and steps over its data without decoding it: the
numeric, date and time, decimal, enum, UUID and IP types, `String`,
`FixedString`, `Nullable`, `Array`, `Tuple`, `Map`, `Nested`,
`LowCardinality`, `SimpleAggregateFunction` and the geo types. A result
with another type (`Dynamic`, `Variant`, `JSON`, `AggregateFunction`, ...)
fails the run naming the type. Results stream through the 64 KiB receive
buffer, so a query may return any number of rows; the latency is the time
to the last byte of EndOfStream.

## Errors

An Exception packet completes its query as an error reply whose kind is the
ClickHouse error code, counted in `errors_by_kind`: `60` for
UNKNOWN_TABLE, `62` for a syntax error, `395` for `throwIf`, and so on. The
connection then goes on with its next query. The traffic group's `on_error`
setting decides whether error replies are counted or fail the run.

## Transports

Over the stream transports `tcp`, `unix` and `ntcp`; `udp` fails
validation. ClickHouse has no Unix socket listener for the native protocol,
so `unix` needs a proxy that offers one. The integration tests run over
`tcp` against clickhouse/clickhouse-server 26.8.29.10.
