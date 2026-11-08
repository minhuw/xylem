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
completed inside that window, which is what throughput divides; requests
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
- `completed`: requests whose response arrived, in the window or the drain.
  Equal to the latency `sample_count` unless the sampling policy is `none` or
  a `limited` rate drops latencies.
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

## Latency fields

Each `latency` object in JSON (global and per group) holds, in microseconds:

- `p50_us` ... `p99999_us`: percentiles from the group's sampling policy.
- `mean_us`, `min_us`, `max_us`: exact, from every recorded latency.
- `std_dev_us`: sample standard deviation.
- `confidence_interval_us`: half-width of the 95% confidence interval of the
  mean, `1.96 * std_dev / sqrt(sample_count)`.
- `sample_count`: number of latencies recorded, after any `limited` rate.
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
