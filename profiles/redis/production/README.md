# Production Workloads

Research-backed workload patterns from published academic papers.

## Workloads

| File | Name | Read% | Write% | Pattern | Citation |
|------|------|-------|--------|---------|----------|
| `facebook-tao-workload.toml` | Social Graph Cache | 99.8% | 0.2% | Zipfian + Diurnal | Bronson et al. "TAO: Facebook's Distributed Data Store for the Social Graph" (USENIX ATC 2013) |

## Quick Start

```bash
# Facebook TAO: Social graph cache
xylem run profiles/redis/production/facebook-tao-workload.toml
```

## Facebook TAO Workload

**Based on**: TAO (The Associations and Objects) - Facebook's distributed data store for social graph

**Production Characteristics** (from published traces):
- 99.8% reads, 0.2% writes (extremely read-heavy)
- Handles 1 billion+ requests/second globally
- Median object size: ~100 bytes
- Extreme key skew (celebrities = hot keys)
- Multi-get operations common (50% of queries)
- Cache hit rate: 96%+
- Diurnal traffic patterns

**What it models**:
- Friend lists and follower counts
- Photo tags and post metadata
- Page likes and group memberships
- Social graph association queries

**Key Insights**:
- Multi-get batching critical for performance
- Aggressive pipelining needed
- Write-through invalidation strategy
- Strong temporal and spatial locality

## References

- TAO Paper: https://www.usenix.org/conference/atc13/technical-sessions/presentation/bronson
- TAOBench: https://dl.acm.org/doi/10.14778/3641204.3641211
