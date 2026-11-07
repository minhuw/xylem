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
