# Protocol construction and framing

Use the configuration-aware constructors for workloads driven by configuration:

- `RedisProtocol::from_config(&RedisConfig, seed)`
- `MemcachedAsciiProtocol::from_config(&MemcachedConfig, seed)`
- `MemcachedBinaryProtocol::from_config(&MemcachedConfig, seed)`
- `MasstreeProtocol::from_config(&MasstreeConfig, seed)`
- `HttpProtocol::from_config(&HttpConfig)`

Public factories delegate to these constructors; Masstree has no factory, and
its constructor sends a configured `value_size` at its representative fixed
size. Existing low-level constructors remain available. Redis callers assembling their own workloads can share
`RedisConfig::command_selector(seed)` and
`factories::parse_redis_operation(command, params)`.

Redis construction retains the configured value size generator, including
inclusive uniform and per-command sizes, instead of choosing a representative
fixed size. Command names for size lookup are case-insensitive. Insertion uses
its explicit size or `keys.value_size`; an unbounded sequential keyspace needs
an explicit insertion count. Per-command key distributions are supported, but
a different per-command prefix or legacy `keys.value_size` is rejected; use
`value_size` for per-command sizing.

HTTP supports Content-Length framing and bodyless 204/304 responses.
Informational responses are retained until the final response arrives, which
completes exactly one request. Chunked/other Transfer-Encoding, close-delimited
bodies, protocol upgrades, and malformed or conflicting Content-Length headers
produce an explicit error before a request is counted as successful.

Redis maps count pairs. RESP3 attributes are consumed with their attached
reply, including when nested. Error replies, including `-MOVED` and `-ASK`,
are reported as errors.

Memcached ASCII GET responses use the VALUE byte length and CRLF terminator,
followed by END. Payload newlines and embedded END text do not terminate a
response. Server errors and malformed framing are reported as errors.
