# Output Formats

Xylem supports multiple output formats for results and metrics.

## Supported Formats

### Text (Human-Readable)

Default format with human-readable statistics:

```bash
xylem --protocol redis --output text
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

```bash
xylem --protocol redis --output json
```

Example:
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

```bash
xylem --protocol redis --output csv
```

Example:
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

## Writing to File

Save output to a file:

```bash
xylem --protocol redis --output json --output-file results.json
```

## Configuration

Specify output format in configuration:

```json
{
  "output": {
    "format": "json",
    "file": "benchmark-results.json",
    "pretty": true
  }
}
```

### Options

- `format` - Output format (text, json, csv)
- `file` - Output file path (stdout if not specified)
- `pretty` - Pretty-print JSON (JSON only)

## Real-Time Streaming

For long-running benchmarks, stream results:

```json
{
  "output": {
    "format": "json",
    "stream": true,
    "interval": "1s"
  }
}
```

This outputs statistics every second during the run.

## Custom Templates

For advanced formatting, use custom templates:

```json
{
  "output": {
    "format": "custom",
    "template": "/path/to/template.hbs"
  }
}
```

## See Also

- [Metrics and Statistics](../advanced/metrics.md)
- [Configuration](./configuration.md)
