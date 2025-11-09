# Performance Tuning

Optimize Xylem for maximum performance.

## System Tuning

### Increase File Descriptors

```bash
# Check current limit
ulimit -n

# Increase limit (temporary)
ulimit -n 65536

# Permanent (add to /etc/security/limits.conf)
* soft nofile 65536
* hard nofile 65536
```

### TCP Tuning

Optimize TCP for high throughput:

```bash
# Increase TCP buffer sizes
sudo sysctl -w net.core.rmem_max=16777216
sudo sysctl -w net.core.wmem_max=16777216
sudo sysctl -w net.ipv4.tcp_rmem="4096 87380 16777216"
sudo sysctl -w net.ipv4.tcp_wmem="4096 87380 16777216"

# Enable TCP window scaling
sudo sysctl -w net.ipv4.tcp_window_scaling=1

# Reduce TIME_WAIT
sudo sysctl -w net.ipv4.tcp_fin_timeout=30
sudo sysctl -w net.ipv4.tcp_tw_reuse=1
```

### CPU Affinity

Pin to specific CPUs:

```json
{
  "runtime": {
    "cpu_affinity": [0, 1, 2, 3]
  }
}
```

## Xylem Configuration

### Connection Pooling

Reuse connections:

```json
{
  "workload": {
    "connections": 100,
    "connection_reuse": true
  }
}
```

### TCP_NODELAY

Disable Nagle's algorithm for lower latency:

```json
{
  "transport": {
    "type": "tcp",
    "nodelay": true
  }
}
```

### Pipelining

Batch requests (protocol-dependent):

```json
{
  "protocol": "redis",
  "protocol_config": {
    "pipeline": 100
  }
}
```

### Buffer Sizes

Optimize socket buffers:

```json
{
  "transport": {
    "type": "tcp",
    "send_buffer": 65536,
    "recv_buffer": 65536
  }
}
```

## Workload Optimization

### Rate Distribution

Use appropriate distribution:

```json
{
  "workload": {
    "rate_distribution": "poisson"
  }
}
```

### Connection Strategy

Balance connections vs throughput:

```
Low latency: Few connections, pipelining
High throughput: Many connections, parallel
```

### Measurement Overhead

Choose appropriate statistics algorithm:

```json
{
  "statistics": {
    "sketch": "ddsketch",  // Balanced
    // "sketch": "hdr",    // Accurate but slower
    // "sketch": "tdigest" // Fast but less accurate
  }
}
```

## Benchmarking Best Practices

### Warmup

Always include warmup:

```json
{
  "workload": {
    "warmup": "10s",
    "duration": "60s"
  }
}
```

### Duration

Run long enough for stable results:

- Minimum: 30 seconds
- Recommended: 60+ seconds
- Tail latency: 300+ seconds

### Isolation

- Dedicated machines
- Disable power management
- Stop unnecessary services
- Use CPU pinning

### Reproducibility

- Fixed seeds for randomness
- Document system configuration
- Multiple runs

## Monitoring

### System Metrics

Monitor during benchmarks:

```bash
# CPU usage
top -p $(pgrep xylem)

# Network
iftop

# TCP connections
netstat -an | grep ESTABLISHED | wc -l

# Socket buffers
ss -tm
```

### Bottleneck Detection

Common bottlenecks:

1. **CPU**: High CPU usage → Reduce rate or add connections
2. **Network**: Bandwidth saturated → Reduce rate or payload size
3. **Target**: High error rate → Target overloaded
4. **Xylem**: Lower than expected throughput → Increase connections

## Platform-Specific

### Linux

Use epoll (automatic):
```
Event loop: epoll
```

### macOS

Use kqueue (automatic):
```
Event loop: kqueue
```

## See Also

- [Architecture](../architecture/overview.md)
- [Configuration](../guide/configuration.md)
