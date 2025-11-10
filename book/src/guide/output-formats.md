# Output Formats

Xylem supports multiple output formats for results and metrics, configured in your TOML profile.

## Supported Formats

### Text (Human-Readable)

Default format with human-readable statistics:

```toml
[output]
format = "text"
```

Example output:
```
=== Xylem Benchmark Results ===

Duration: 60.0s
Total Requests: 600000
Successful: 599850 (99.98%)
Failed: 150 (0.02%)

Latency Statistics:
  p50:  1.23 ms
  p95:  3.45 ms
  p99:  5.67 ms
  p99.9: 8.90 ms
  max:  12.34 ms

Throughput: 9997.5 req/s
```

### JSON

Machine-readable JSON output:

```toml
[output]
format = "json"
file = "results.json"
```

Example JSON output:
```json
{
  "duration_secs": 60.0,
  "total_requests": 600000,
  "successful": 599850,
  "failed": 150,
  "latency": {
    "p50": 1.23,
    "p95": 3.45,
    "p99": 5.67,
    "p99_9": 8.90,
    "max": 12.34
  },
  "throughput": 9997.5
}
```

### CSV

Comma-separated values for analysis:

```toml
[output]
format = "csv"
file = "results.csv"
```

Example CSV output:
```csv
metric,value
duration_secs,60.0
total_requests,600000
successful,599850
failed,150
p50_latency_ms,1.23
p95_latency_ms,3.45
p99_latency_ms,5.67
throughput_rps,9997.5
```

## Configuration

Specify output format in your profile:

```toml
[output]
format = "json"
file = "benchmark-results.json"
```

### Options

- `format` - Output format (text, json, csv)
- `file` - Output file path (stdout if not specified)

## Command Line Override

You can override output settings using `--set`:

```bash
xylem -P profile.toml --set output.format=json --set output.file=results.json
```

## See Also

- [Configuration](./configuration.md)
- [CLI Reference](./cli-reference.md)
