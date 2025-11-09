# Metrics and Statistics

Understanding Xylem's metrics and statistical algorithms.

## Collected Metrics

### Latency Metrics

Xylem measures end-to-end request latency:

- **p50 (median)**: 50th percentile
- **p95**: 95th percentile
- **p99**: 99th percentile
- **p99.9**: 99.9th percentile
- **p99.99**: 99.99th percentile
- **max**: Maximum observed latency
- **min**: Minimum observed latency
- **mean**: Average latency
- **stddev**: Standard deviation

### Throughput Metrics

- **Requests/second**: Actual achieved throughput
- **Target rate**: Configured request rate
- **Utilization**: Actual / Target ratio

### Error Metrics

- **Total requests**: All attempted requests
- **Successful**: Successfully completed requests
- **Failed**: Failed requests
- **Timeout**: Requests that timed out
- **Connection errors**: Connection failures

### Connection Metrics

- **Active connections**: Currently active
- **Total connections**: Created during run
- **Connection errors**: Failed connection attempts
- **Connection reuse**: Reused connections

## Statistical Algorithms

Xylem supports multiple sketch algorithms for latency measurement.

### DDSketch

**Best for**: General purpose, balanced accuracy

Relative-error guarantee for all quantiles:

```json
{
  "statistics": {
    "sketch": "ddsketch",
    "alpha": 0.01  // 1% relative error
  }
}
```

**Characteristics:**
- Memory: O(log(max/min))
- Accuracy: Relative error α
- Merge: Supports merging

### T-Digest

**Best for**: Percentile estimation, especially extreme percentiles

```json
{
  "statistics": {
    "sketch": "tdigest",
    "compression": 100
  }
}
```

**Characteristics:**
- Memory: O(compression)
- Accuracy: Better at extremes
- Merge: Efficient merging

### HDR Histogram

**Best for**: High accuracy, wide range

```json
{
  "statistics": {
    "sketch": "hdr",
    "max_value": 3600000000,  // 1 hour in us
    "significant_figures": 3
  }
}
```

**Characteristics:**
- Memory: O(log(max_value))
- Accuracy: Fixed number of significant figures
- Merge: Supports merging

## Output Formats

### Text Output

Human-readable format:

```
=== Latency Statistics ===
p50:    1.23 ms
p95:    3.45 ms
p99:    5.67 ms
p99.9:  8.90 ms
max:   12.34 ms
mean:   1.50 ms
stddev: 1.20 ms

=== Throughput ===
Actual:  9,987 req/s
Target: 10,000 req/s
Utilization: 99.87%
```

### JSON Output

```json
{
  "latency": {
    "p50": 1.23,
    "p95": 3.45,
    "p99": 5.67,
    "p99_9": 8.90,
    "max": 12.34,
    "mean": 1.50,
    "stddev": 1.20,
    "unit": "ms"
  },
  "throughput": {
    "actual": 9987,
    "target": 10000,
    "unit": "req/s"
  },
  "errors": {
    "total": 600000,
    "successful": 599850,
    "failed": 150,
    "timeout": 0
  }
}
```

### CSV Output

```csv
metric,value
p50_latency_ms,1.23
p95_latency_ms,3.45
p99_latency_ms,5.67
throughput_rps,9987
success_rate,99.98
```

## Time Series Data

Export time-series metrics:

```json
{
  "output": {
    "time_series": true,
    "interval": "1s",
    "file": "timeseries.csv"
  }
}
```

Output:
```csv
timestamp,latency_p50,latency_p99,throughput,errors
1.0,1.2,3.4,9987,0
2.0,1.3,3.5,9982,1
3.0,1.2,3.3,9995,0
...
```

## Histogram Export

Export full histogram:

```json
{
  "output": {
    "histogram": true,
    "file": "histogram.csv"
  }
}
```

## Coordinated Omission

Xylem tracks coordinated omission:

```json
{
  "statistics": {
    "coordinated_omission": true
  }
}
```

This records both:
- **Measured latency**: Time from send to receive
- **Service time**: Corrected for queuing

## Percentile Accuracy

Configure percentiles to report:

```json
{
  "statistics": {
    "percentiles": [50, 90, 95, 99, 99.9, 99.99]
  }
}
```

## Memory Usage

Approximate memory per sketch:

- **DDSketch (α=0.01)**: ~8 KB
- **T-Digest (c=100)**: ~4 KB
- **HDR Histogram**: ~32 KB

## See Also

- [Output Formats](../guide/output-formats.md)
- [Performance Tuning](./performance.md)
