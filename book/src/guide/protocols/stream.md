# Bulk Stream Protocol

`protocol = "stream"` measures unidirectional bulk byte throughput (a TCP/ntcp
"sink" or reverse stream) with optional deterministic payload verification.
Unlike request/response protocols, `"stream"` transfers raw byte streams
without request framing or per-request latency tracking, reporting exact
`tx_bytes`, `rx_bytes`, `tx_mbps`, and `rx_mbps` in the JSON and console
outputs.

Supported transports: stream transports (`tcp`, `unix`, `ntcp`). Datagram
transports (`udp`) are rejected at profile load.

## Configuration

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `direction` | `"send"` \| `"recv"` | `"send"` | `"send"` streams client → server (one-way TX). `"recv"` reads an unsolicited server → client stream (RX). |
| `chunk_size` | integer (`>= 1`) | `65536` | Chunk size in bytes generated per one-way write in `"send"` mode. |
| `bytes` | integer (`>= 1`, optional) | unset | Measurement byte limit. When set, measurement stops once this many bytes have transferred (or `experiment.duration` elapses first). When every traffic group is a `"stream"` group with `bytes` set, `experiment.duration` may be omitted. |
| `seed` | integer (`u64`) | `0` | Deterministic byte-pattern seed shared between client and server. |
| `verify` | boolean | `true` | Verify received bytes against the deterministic pattern in `"recv"` mode. Mismatches fail the `verification` validity check (exit status `3`, `verdict = "invalid_run"`). |

### Experiment Lifecycle Keys

Two `[experiment]` keys pair naturally with bulk streams and connection
lifecycle testing:

- `experiment.warmup` (default `"0s"`): time-based warm-up before the
  measurement window starts. Traffic during warm-up is excluded from statistics,
  while wire stream offsets remain continuous from byte `0` across the
  warm-up/measurement boundary so peers verify one unbroken stream.
- `experiment.idle_hold` (default `"0s"`): quiescent hold after draining
  responses and before closing connections. Keeps sockets open while polling
  the transport and discarding any inbound bytes without recording late
  responses.

## Deterministic Byte Pattern

Every 8-byte word `w` covering 0-based stream byte offsets `[w * 8 .. w * 8 + 8)`
in little-endian byte order is computed from `seed` and `w` via SplitMix64:

```text
word(seed, w) = splitmix64(seed ^ (w * 0x9e3779b97f4a7c15))
```

where `splitmix64(x)` is:

```rust
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn stream_pattern_word(seed: u64, word_index: u64) -> u64 {
    splitmix64(seed ^ word_index.wrapping_mul(0x9e37_79b9_7f4a_7c15))
}
```

Equivalent C reference implementation for external servers:

```c
#include <stdint.h>

static inline uint64_t splitmix64(uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

static inline uint64_t stream_pattern_word(uint64_t seed, uint64_t word_index) {
    return splitmix64(seed ^ (word_index * 0x9e3779b97f4a7c15ULL));
}
```

For `seed = 0`, `stream_pattern_word(0, 0) == 0xe220a8397b1dcdaf` (first 8 wire
bytes: `af cd 1d 7b 39 a8 20 e2`).

## Examples

### Send by Exact Byte Limit with Warm-Up

`experiment.duration` is omitted; the run warms up for `1s`, then sends
exactly 1 GiB of measured bytes and exits once all bytes are handed to the
transport:

```toml
[experiment]
name = "stream-send-bytes"
warmup = "1s"
idle_hold = "500ms"

[[traffic_groups]]
name = "sink"
protocol = "stream"
target = "127.0.0.1:9000"
transport = "tcp"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1

[traffic_groups.traffic_policy]
type = "closed-loop"

[traffic_groups.protocol_config]
direction = "send"
chunk_size = 65536
bytes = 1073741824
seed = 0

[output]
format = "json"
file = "stream-send-bytes.json"
```

### Send by Duration with Warm-Up

Stream continuously for `10s` after a `1s` warm-up:

```toml
[experiment]
name = "stream-send-duration"
warmup = "1s"
duration = "10s"

[[traffic_groups]]
name = "sink"
protocol = "stream"
target = "127.0.0.1:9000"
transport = "tcp"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1

[traffic_groups.traffic_policy]
type = "closed-loop"

[traffic_groups.protocol_config]
direction = "send"
chunk_size = 65536
seed = 42

[output]
format = "json"
file = "stream-send-duration.json"
```

### Reverse Stream Receive with Payload Verification

Receive 64 MiB from a server and verify every byte from stream offset `0`:

```toml
[experiment]
name = "stream-recv-verify"

[[traffic_groups]]
name = "reverse-stream"
protocol = "stream"
target = "127.0.0.1:9000"
transport = "tcp"
threads = [0]
connections_per_thread = 1
max_pending_per_connection = 1

[traffic_groups.traffic_policy]
type = "closed-loop"

[traffic_groups.protocol_config]
direction = "recv"
bytes = 67108864
seed = 42
verify = true

[output]
format = "json"
file = "stream-recv-verify.json"
```

## See Also

- [Protocol Configuration](../configuration/protocol.md)
- [Architecture: Protocol Layer](../../architecture/protocols.md)
