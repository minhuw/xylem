# Output Formats

Set `format` and `file` in the profile's `[output]` section:

```toml
[output]
format = "both"
file = "results"
```

- `json` (default): Experiment metadata, global statistics, and per-group statistics.
- `html`: A report with latency and throughput charts.
- `both`: JSON and HTML files.
- `human`: Console statistics only.

The CLI sets the file extension to `.json` or `.html` as appropriate. `file` is
required even for `human`. All formats also print a human-readable summary.

## Run phases

Every thread first opens its connections. Once all threads have, the run
starts at one time they share, shortly after the last one is ready; every
rate-driven schedule counts from it (see
[traffic policy](../reference/schema.md#traffic-policy)). Then a run has three
phases:

1. **Warmup**: connection setup (such as Redis AUTH or the Masstree
   handshake) and requests a protocol marks as warmup, such as an insert
   phase. They are excluded from every statistic and count.
2. **Measurement**: from the first measured request to `experiment.duration`
   after the start. Only responses that arrive in this window count toward
   throughput.
3. **Drain**: sending stops, and the run waits up to
   `experiment.drain_timeout` (default 1 s) until no request is pending.
   Requests completed during the drain contribute their latency to the
   percentiles and statistics, but not to throughput. Requests still pending
   when the drain ends count as `timed_out` and have no latency.

## Throughput and the measured window

`experiment.duration_secs` is the configured duration. Throughput
(`throughput_rps`, `throughput_mbps`) is computed over
`experiment.measured_duration_secs`: from the first measured request, after
connection setup and any protocol warmup (such as the Masstree insert phase),
to the end of the measurement phase. `total_requests` counts the requests
completed inside that window, error replies included, which is what
throughput divides; requests
completed during the drain are excluded from it and from the bytes behind
`throughput_mbps`. Their latencies are recorded in the window's last time
bucket. With `stats.include_records = true`, every record's rate divides by
the part of its time bucket inside the measured window, so the first and last
buckets are not understated. All threads' time buckets count from the
shared start.

## Request accounting

Each `requests` object in JSON (global and per group) counts measurement
requests; warmup requests are excluded:

- `sent`: requests sent.
- `completed`: requests whose response arrived, in the window or the drain,
  successful or an [error reply](#error-replies). `completed - errors` equals
  the latency `sample_count` unless the sampling policy is `none` or a
  `limited` rate drops latencies.
- `errors`: completed requests whose response was an error reply. The console
  prints a warning when this is nonzero.
- `errors_by_kind`: `errors` by kind, such as `{"WRONGTYPE": 12, "OOM": 3}`.
- `timed_out`: no response within the group's `request_timeout` (UDP groups,
  default 1 s; see [UDP](configuration/transport.md#udp-ipport)), or still pending
  when the drain timeout passed. Their latency is unknown, so they are not in
  the latency statistics (it is censored, not recorded as the timeout); the
  console prints a warning when this is nonzero.
- `late_responses`: responses that arrived after their request timed out.
  They are dropped, identified by request ID, and add no latency; their
  requests stay in `timed_out`. Only UDP groups time out requests, so this is
  zero for the other transports.
- `failed`: pending on a connection that closed or failed.
- `late_sends`: requests whose send lag exceeded 100 µs.
- `unsent`: sends a rate-driven schedule intended before the end of the
  measurement window that never went out, because the sender could not keep
  up. Counting walks each connection's schedule from its next intended time to
  the end of the window, at most 10,000,000 requests per connection; Poisson
  schedules are sampled forward with the policy's own generator. Connections
  that failed keep counting their schedule. A connection's schedule starts
  once it can send after setup, so a connection still setting up at the end
  counts none, and the slots before a connection's start are not unsent.
- `unsent_capped`: whether that walk stopped at its cap on some connection, so
  `unsent` is a lower bound.

After a run, `sent = completed + timed_out + failed`.

## Error replies

An error reply is a complete response that says its request failed: a Redis
error such as `-WRONGTYPE`, an HTTP status of 400 or above, a memcached
`SERVER_ERROR` line or non-zero binary status, a Masstree `Retry` result.
Each protocol page lists what it counts and how it names the kind. Nil
replies and misses are successful replies.

By default (`on_error = "count"` on the traffic group), an error reply
completes its request:

- It counts in `completed`, `errors` and `errors_by_kind`, and toward
  `total_requests`, throughput and received bytes like any response.
- Its latency goes to `error_latency`, which has the same fields as
  `latency` and uses the group's sampling policy, and not to `latency` or the
  [latency breakdown](#latency-breakdown). Fast rejections would otherwise
  pull the percentiles down, so `latency` and its `sample_count` cover
  successful responses only.
- It is not fed back to an adaptive traffic policy, which would otherwise
  speed up on fast rejections.
- Warmup requests and connection setup are excluded, as from every count.

`errors_by_kind` names at most 32 kinds; error replies of later kinds count
under `"other"`, and a kind is cut to 64 bytes. The bound holds per thread and
after merging threads, so with more than 32 kinds in a run a kind may be split
between its own entry and `other`.

```json
"latency": { "mean_us": 52.7, "p50_us": 50.1, "p99_us": 96.3, "sample_count": 39000, ... },
"error_latency": { "mean_us": 41.2, "p50_us": 39.1, "p99_us": 88.0, "sample_count": 13000, ... },
"requests": {
  "sent": 52000,
  "completed": 52000,
  "errors": 13000,
  "errors_by_kind": { "WRONGTYPE": 13000 },
  "timed_out": 0,
  ...
}
```

With `on_error = "abort"`, the first error reply fails the run, warmup
included, with an error naming the traffic group, the connection and the
reply, and no results are written. Malformed responses and failed connection
setup (a rejected Redis AUTH, a failed Masstree handshake) fail the run
either way.

## Send lag

With a rate-driven traffic policy, every request has an intended send time
(see [traffic policies](../reference/schema.md#traffic-policy)), and its
latency is measured from it: the send lag (actual minus intended send time,
measured on xylem's cycle clock when the send completes) plus the service
latency (from the send, or its kernel TX timestamp, to the response). Each
`send_lag` object reports, in microseconds:

- `sample_count`: open-loop requests sent.
- `mean_us`, `max_us`: exact.
- `p99_us`: from an HDR histogram with three significant digits.
- `late_threshold_us`: the threshold of `late_sends`, 100 µs. An on-time send
  lags by a loop iteration and a send call, a few microseconds; a lag above
  100 µs means the generator could not keep its schedule.

Closed-loop requests have no intended time: their values are `null` and their
latency is the service latency alone.

## Latency breakdown

Each `latency_breakdown` object in JSON (global and per group) says where the
latency of the completed measurement requests went. The send lag is not
repeated there; it stays in `send_lag`. It covers exactly the requests in
`latency`: successful responses, including those completed during the drain.
Warmup and setup requests are excluded, and so are
[error replies](#error-replies), whose latency goes to `error_latency` alone,
so `service_us.sample_count` equals the latency `sample_count` when the
sampling policy records every latency.

```json
"latency_breakdown": {
  "clock": "kernel_software",
  "kernel_split_count": 41230,
  "service_us": {
    "sample_count": 41230, "mean_us": 14.1, "p50_us": 13.9,
    "p99_us": 19.8, "p999_us": 31.2, "max_us": 212.4
  },
  "host_stack_us": {
    "sample_count": 41230, "mean_us": 9.8, "p50_us": 9.6,
    "p99_us": 14.3, "p999_us": 22.0, "max_us": 190.2
  },
  "network_server_us": {
    "sample_count": 41230, "mean_us": 14.1, "p50_us": 13.9,
    "p99_us": 19.8, "p999_us": 31.2, "max_us": 212.4
  }
}
```

The parts of one request are:

- `service_us`: actual receive minus actual send, in the clock the latency
  uses. A request's latency is its send lag plus this. Always present.
- `network_server_us`: present when the send and the receive both carry a
  kernel timestamp from the same clock, which needs kernel or NIC timestamps
  (see [timestamps](configuration/transport.md#timestamps)). It is the kernel
  round trip: the response's kernel RX timestamp minus the request's kernel
  TX timestamp. With kernel software timestamps, that is from the request
  leaving the kernel's stack for the driver to the response entering it, so
  it covers the NICs, the network and the server; with NIC timestamps, from
  the request leaving the client NIC to the response arriving at it. Such a
  request's latency uses this round trip, so its `service_us` equals it.
- `host_stack_us`: for the same requests, the user-space round trip (cycles
  read when the send call and the receive call returned) minus the kernel
  round trip, or zero if that is negative. That is the time from the
  response's kernel RX timestamp to the receive call's return (the client's
  receive stack, wakeup and syscall) minus the time from the request's kernel
  TX timestamp to the send call's return. A kernel software TX timestamp is
  taken inside the send call, so this subtracts the end of that call; a NIC
  usually transmits after the call returned, so with NIC timestamps this adds
  the send path through the stack, driver and NIC queue.

Each part's two terms are measured in one clock; xylem subtracts durations,
never times from different clocks. `host_stack_us` and `network_server_us`
are `null` when no request had a kernel split. Requests whose TX or RX
timestamp did not arrive, or came from different clocks, count in
`service_us` alone.

- `clock`: the clock of the service latencies: `user_space` (no kernel split),
  `kernel_software`, `nic_hardware`, or `mixed` when the scope's requests
  used different ones. `null` when no request completed.
- `kernel_split_count`: requests with a kernel split. Below
  `service_us.sample_count`, some requests lacked a usable kernel timestamp.

Every part reports `sample_count`, an exact `mean_us` and `max_us`, and
`p50_us`, `p99_us` and `p999_us` from an HDR histogram with three significant
digits; the values are `null` without samples. Each part's percentiles are
taken over that part alone, so they do not add up: the p99 of
`host_stack_us` plus the p99 of `network_server_us` is not the p99 of the
latency, and the slowest requests of one part need not be the slowest of
another. Only the means add up: `host_stack_us.mean_us +
network_server_us.mean_us` is the mean user-space round trip of the split
requests, unless some `host_stack_us` saturated at zero.

The console prints the same parts in one row each, and the HTML report in a
table per scope.

## Latency fields

Each `latency` object in JSON (global and per group) holds, in microseconds:

- `p50_us` ... `p99999_us`: percentiles from the group's sampling policy.
- `mean_us`, `min_us`, `max_us`: exact, from every recorded latency.
- `std_dev_us`: sample standard deviation.
- `confidence_interval_us`: half-width of the 95% confidence interval of the
  mean, `1.96 * std_dev / sqrt(sample_count)`.
- `sample_count`: number of latencies recorded, after any `limited` rate.
  `latency` records successful responses only; `error_latency`, with the same
  fields, records [error replies](#error-replies).
- `clamped_samples`: present only for an HDR histogram with `max_value_us`;
  the number of latencies above that bound, recorded as the bound.

A value the run cannot provide is `null` in JSON and `n/a` in the console and
HTML: `min_us` and `max_us` without recorded latencies (for example with
`type = "none"`), and `std_dev_us` and `confidence_interval_us` with fewer than
two.

For raw per-time-bucket, per-connection records in JSON, use:

```toml
[stats]
include_records = true
bucket_duration = "1s"
```

Each record names its connection by `group_id`, `thread_id` and
`connection_id`. Connection IDs restart at 0 on every worker thread, so
`thread_id` (the thread's entry in `threads`) is needed to tell connections
apart.

HTML uses Chart.js (loaded from a CDN) and the light theme. Only
`output.html.library = "chartjs"` and `output.html.theme = "light"` are supported.
`output.real_time` must remain false; the feature is not implemented by the CLI
and validation rejects it.
The obsolete `detailed-json` format has been replaced by `json`.

Request CSV dumps are configured separately through `[request_dump]`; see the
[configuration schema reference](../reference/schema.md#request-dump).
