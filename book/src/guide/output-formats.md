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

For raw per-time-bucket, per-connection records in JSON, use:

```toml
[stats]
include_records = true
bucket_duration = "1s"
```

HTML uses Chart.js (loaded from a CDN) and the light theme. Only
`output.html.library = "chartjs"` and `output.html.theme = "light"` are supported.
`output.real_time` must remain false and `stats.streaming` must be omitted;
these features are not implemented by the CLI and validation rejects them.
The obsolete `detailed-json` format has been replaced by `json`.

Request CSV dumps are configured separately through `[request_dump]`; see the
[configuration schema reference](../reference/schema.md#request-dump).
