# Protocol construction and framing

Use the configuration-aware constructors for workloads driven by configuration:

- `RedisProtocol::from_config(&RedisConfig, seed)`
- `RedisClusterProtocol::from_config(&RedisConfig, seed)`
- `MemcachedAsciiProtocol::from_config(&MemcachedConfig, seed)`
- `MemcachedBinaryProtocol::from_config(&MemcachedConfig, seed)`
- `HttpProtocol::from_config(&HttpConfig)`

Public factories delegate to these constructors. Existing low-level constructors
remain available. Redis callers assembling their own workloads can share
`RedisConfig::command_selector(seed)` and
`factories::parse_redis_operation(command, params)`.

Redis construction retains the configured value size generator, including
inclusive uniform and per-command sizes, instead of choosing a representative
fixed size. Command names for size lookup are case-insensitive. Insertion uses
its explicit size or `keys.value_size`; an unbounded sequential keyspace needs
an explicit insertion count. Per-command key distributions are supported, but
a different per-command prefix or legacy `keys.value_size` is rejected; use
`value_size` for per-command sizing. Imported-data workloads require a separate
driver and are rejected by these constructors. Register actual connection IDs
on cluster protocols after construction using `register_connections`.
The CLI uses `next_request_on_connection` because its worker chooses the socket;
MOVED/ASK retries then use the registered target. Routing-aware callers can keep
using `next_request` or `generate_request_with_key` and send to the target in
`ClusterRequestId.2.0`. Tuple-ID adapters must retain that target connection as
well as its sequence; sequences alone are not unique across targets.

HTTP supports Content-Length framing and bodyless 204/304 responses.
Informational responses are retained until the final response arrives, which
completes exactly one request. Chunked/other Transfer-Encoding, close-delimited
bodies, protocol upgrades, and malformed or conflicting Content-Length headers
produce an explicit error before a request is counted as successful.

Redis maps count pairs. RESP3 attributes are consumed with their attached
reply, including when nested. Cluster redirect parsing consumes only the first
complete response and returns `ParseResult::Retry`; use
`parse_response_extended` to handle redirects. The legacy parser reports
redirects as errors. Retry regeneration requires an outstanding original request
ID and retains its exact bytes and warmup metadata. ASKING preparation replies
are consumed with the retry response so they do not occupy an application
request ID. The caller must successfully send preparation commands before the
regenerated request. The worker propagates preparation-send failures and defers
retries when the target pipeline is full, before sending any preparation bytes.
A fragmented ASKING acknowledgement is buffered with the following response;
it never completes a pending request on its own. Retry metadata holds one copy
of each outstanding cluster request's payload until completion.

Memcached ASCII GET responses use the VALUE byte length and CRLF terminator,
followed by END. Payload newlines and embedded END text do not terminate a
response. Server errors and malformed framing are reported as errors.
