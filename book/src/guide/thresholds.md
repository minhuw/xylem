# Thresholds and Validity

A run can gate CI or a regression benchmark. After the run, xylem checks
two things against the final results:

- **Validity**: did the run apply the load it was configured with? If not,
  the run is *invalid*: its statistics describe some other load.
- **Thresholds**: did the metrics stay within the limits in `[thresholds]`?

The outcome is the run's *verdict*: in the results file, the console summary,
the HTML report, and the exit status.

```toml
[experiment]
name = "api-regression"
duration = "30s"

[[traffic_groups]]
name = "latency-agent"
protocol = "xylem-echo"
target = "127.0.0.1:9999"
threads = [0]
connections_per_thread = 4
traffic_policy = { type = "poisson", rate = 20000.0 }

[[traffic_groups]]
name = "load"
protocol = "xylem-echo"
target = "127.0.0.1:9999"
threads = [1, 2]
connections_per_thread = 16
max_pending_per_connection = 8
traffic_policy = { type = "fixed-rate", rate = 80000.0 }

[output]
format = "json"
file = "results.json"

[thresholds]
"latency"                      = ["p99 < 250us", "p99.9 < 1ms"]   # global
"latency{group=latency-agent}" = ["p50 < 80us"]                    # one traffic group
"send_lag"                     = ["p99 < 20us"]
"requests.timed_out"           = ["rate < 0.001"]                  # fraction of requests.sent
"requests.errors"              = ["rate < 0.01"]                   # error replies
"requests.failed"              = ["== 0"]
"throughput"                   = ["> 90000"]

[validity]
max_unsent_ratio = 0.001        # the defaults
max_late_send_ratio = 0.01
```

## Exit status

| Status | Verdict | Meaning |
|---|---|---|
| 0 | `pass` | Every validity check and threshold passed, or none applied (no `[thresholds]`, closed-loop groups only) |
| 1 | | Error: the profile is invalid (including any threshold), the run failed, or a result file could not be written |
| 2 | `thresholds_failed` | The run is valid and a threshold failed |
| 3 | `invalid_run` | A validity check failed. Takes precedence over 2: a threshold of an invalid run says nothing about the configured load |

Every result file is written before xylem exits with 2 or 3, so a CI job
can keep the results of a failed run as artifacts.

## Thresholds

Each key of `[thresholds]` names a metric, optionally filtered to a traffic
group; its value is a list of expressions, each a separate threshold. Every
threshold is evaluated once, against the final results.

### Metrics

A metric is the path of a field in a `stats` object of the
[JSON results](./output-formats.md): `global` by default, or the `stats` of
the traffic group named by `{group=name}`. The group's name must be unique
among the traffic groups; quotes around it are optional
(`'latency{group="api"}'`). Any numeric field there is a metric, including
fields added in later versions:

- **Summaries** are objects of durations in microseconds: `latency`,
  `error_latency` (the latencies of [error replies](./output-formats.md#error-replies)),
  `send_lag`, and the parts of the
  [latency breakdown](./output-formats.md#latency-breakdown),
  `latency_breakdown.service_us`, `latency_breakdown.host_stack_us` and
  `latency_breakdown.network_server_us`. An expression on a summary names an
  aggregation, which picks one of its `_us` fields.
- **Single values** are the other numbers: `total_requests`,
  `throughput_rps` (also written `throughput`), `throughput_mbps`,
  `requests.sent`, `requests.completed`, `requests.errors`,
  `requests.timed_out`, `requests.failed`, `requests.late_responses`,
  `requests.late_sends`, `requests.unsent`, `latency.sample_count`,
  `error_latency.sample_count`, `send_lag.sample_count`,
  `latency_breakdown.kernel_split_count`, the `sample_count` of each
  breakdown part, and any `_us` field written in full, such as
  `latency.p99_us`.

An unknown metric is an error that lists the available ones.

`requests.errors_by_kind` is not a metric. Its keys are the error kinds the
run meets (`WRONGTYPE`, `404`, ...), with at most 32 per run and the rest
under `other`, so a key cannot be checked when the profile loads, and a kind
the run never meets would have no value rather than zero. A threshold on it,
or on one of its kinds, is rejected at load; put the limit on
`requests.errors`, which counts every error reply, and read the kinds from
the results.

```toml
[thresholds]
"requests.errors"                     = ["rate < 0.01"]   # error replies / requests.sent
"error_latency"                       = ["p99 < 5ms"]
"latency_breakdown.network_server_us" = ["p99 < 100us"]   # needs kernel or NIC timestamps
"latency_breakdown.kernel_split_count" = ["rate > 0.99"]  # the split covers the requests
```

### Expressions

An expression is `[aggregation] operator value`:

| Aggregation | Applies to | Compares |
|---|---|---|
| `p50`, `p95`, `p99`, `p99.9`, `p99.99`, `p99.999` | summaries | the percentile field: `p99.9` reads `p999_us` (`p999` works too) |
| `mean`, `min`, `max`, `std_dev`, `confidence_interval` | summaries | `mean_us`, `min_us`, ... |

A latency breakdown part holds `mean`, `p50`, `p99`, `p99.9` and `max` only.
| none | single values | the value |
| `rate` | counts | the count divided by `requests.sent` of the same stats |

Operators are `<`, `<=`, `>`, `>=`, `==` and `!=`. A duration takes a unit:
`ns`, `us` (or `µs`), `ms` or `s`, as in `250us` or `1.5ms`. Counts, rates
and other values are plain numbers: a rate is a fraction, `0.001`, not
`0.1%`. A summary offers only the percentiles its fields hold; `p90` is an
error, as are an unknown aggregation, a missing or extra unit, and a
malformed expression or key.

Every threshold is checked when the profile loads, before any connection
opens: one error lists every invalid threshold, reported together with the
profile's other errors, and xylem exits with 1. The keys of `[thresholds]`
are metric names, so the profile's [unknown-key check](./configuration.md)
accepts any key there and leaves it to this check; `[validity]` is a table
of fixed keys, and an unknown key in it is an error like anywhere else.

### Values the run lacks

A threshold fails when the run has no value for it, and its `observed` is
`null`:

- a `null` field, such as `send_lag` of a closed loop, `latency.min` when
  no latency was recorded (see [latency fields](./output-formats.md#latency-fields)),
  or `latency_breakdown.host_stack_us` and `network_server_us` when no request
  had a kernel split (user-space timestamps, or a transport without kernel
  timestamps);
- a `rate` when nothing was sent;
- a traffic group that recorded no requests, which is not in the results.

The percentiles and mean of `latency` and `error_latency` are 0, not `null`,
without samples: a run without error replies passes `error_latency` ceilings.

`--set` can set a threshold whose key has no dot, such as
`--set 'thresholds.latency=["p99 < 1ms"]'`. A dot in the key, as in
`requests.unsent`, makes `--set` build nested tables, which do not load; put
such thresholds in the profile.

## Validity

A rate-driven traffic policy (every policy except `closed-loop`) gives each
request an intended send time. Two checks compare each rate-driven traffic
group's [request accounting](./output-formats.md#request-accounting) with that
schedule:

| Check | Observed | Default limit |
|---|---|---|
| `unsent` | `unsent / (sent + unsent)`: intended sends that never went out | `max_unsent_ratio = 0.001` |
| `late_sends` | `late_sends / sent`: sends more than 100 µs behind their intended time | `max_late_send_ratio = 0.01` |

A closed-loop group is exempt: it sends whenever a response frees its
pipeline, so it has no schedule to fall behind.

```toml
[validity]
enabled = true                # false: no checks, the run is never invalid
max_unsent_ratio = 0.001
max_late_send_ratio = 0.01
skip = ["late_sends"]         # leave out single checks
```

### What an invalid run tells you

An invalid run did not apply the configured load. When intended sends never
went out, the run offered less than the configured rate, and the requests
that would have waited longest are missing from the latency statistics.
When sends went out late, their latencies (measured from the intended send
time) include that lag, so the generator shaped part of the tail.

It does not tell you why. Both checks fail when the generator is too slow
(too few threads, a busy or preempted CPU) and when the server is: a slow
server keeps each connection's `max_pending_per_connection` requests
pending, so the sender waits for a free slot. Look at `send_lag`, the
generator's CPU, and the server's latency; more connections or a deeper
pipeline help when the server has capacity left.

It does not say the recorded latencies are wrong: they are correct for the
requests that were sent. To measure a server beyond its capacity on
purpose, which leaves requests unsent by design, skip the checks or use a
closed loop. A valid run does not say the server behaved well either:
timeouts, failures and latency are what thresholds are for.

### Why these defaults

- `unsent` at most 0.1%, not zero: the end of the window can cut off an
  intended send that was due in its last moments, about one per connection
  in a healthy run (a 2 s run at 2,000 requests per second over four
  connections showed 0 to 2). 0.1% absorbs that in runs with at least 1,000
  intended sends per connection. If the missing requests were the slowest,
  0.1% of them shifts a percentile by at most 0.1 points: the reported p99.9
  is at worst the true p99.8. Raise the limit for shorter or slower runs.
- `late_sends` at most 1%: an on-time send lags by a few microseconds, and
  100 µs means the sender missed its schedule. 1% tolerates an occasional
  preemption of the sender, but not a schedule kept late throughout.

## Results

The JSON results record the verdict and every check, thresholds in key order:

```json
{
  "verdict": "invalid_run",
  "thresholds": [
    {
      "metric": "latency",
      "group": null,
      "expression": "p99 < 1ms",
      "field": "latency.p99_us",
      "observed": 2325741.567,
      "ok": false
    }
  ],
  "validity": [
    {
      "check": "unsent",
      "group": "echo",
      "observed": 0.7882986861688563,
      "limit": 0.001,
      "ok": false,
      "reason": "472980 of 600001 intended sends never went out (78.83%, limit 0.10%): the pipelines were full or the generator fell behind, so the configured rate was not applied"
    },
    {
      "check": "late_sends",
      "group": "echo",
      "observed": 0.9999921272860394,
      "limit": 0.01,
      "ok": false,
      "reason": "127020 of 127021 sends lagged their intended time by more than 100 µs (100.00%, limit 1.00%): the pipelines were full or the generator fell behind its schedule"
    }
  ]
}
```

- `field` is the stats field compared; a rate shows its division, as in
  `requests.timed_out / requests.sent`.
- `observed` is in the unit of that field (microseconds for durations), a
  fraction for rates and validity checks, or `null` when the run lacks it.
- `group` is `null` for the global stats.
- `reason` is present only for a failed validity check.

The console summary ends with the same checks:

```text
----------------------------------------------------------------------
VERDICT: PASS
----------------------------------------------------------------------
Validity:
  ok    unsent{group=echo}: 0.00% (limit 0.10%)
  ok    late_sends{group=echo}: 0.00% (limit 1.00%)
Thresholds:
  ok    latency: p99 < 1ms (observed 44.90 μs)
  ok    latency: p99.9 < 5ms (observed 87.36 μs)
  ok    latency{group=echo}: p50 < 200us (observed 23.46 μs)
  ok    requests.timed_out: rate < 0.001 (observed 0.000000)
  ok    send_lag: p99 < 50us (observed 14.26 μs)
  ok    throughput: > 1900 (observed 2000.01)
```

The HTML report shows them in a Verdict section above the statistics.
