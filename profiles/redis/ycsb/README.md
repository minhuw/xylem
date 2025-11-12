# YCSB Workloads

Standard YCSB (Yahoo Cloud Serving Benchmark) workloads A-F for Redis.

## Workloads

| File | Name | Read% | Write% | Pattern | Description |
|------|------|-------|--------|---------|-------------|
| `ycsb-workload-a.toml` | Update Heavy | 50% | 50% | Zipfian | Session store recording actions |
| `ycsb-workload-b.toml` | Read Mostly | 95% | 5% | Zipfian | Photo tagging system |
| `ycsb-workload-c.toml` | Read Only | 100% | 0% | Zipfian | User profile cache |
| `ycsb-workload-d.toml` | Read Latest | 95% | 5% | Latest | User status updates / news feed |
| `ycsb-workload-e.toml` | Short Ranges | 95% | 5% | Zipfian | Threaded conversations (SCAN) |
| `ycsb-workload-f.toml` | Read-Modify-Write | 50% | 50% | Zipfian | Atomic RMW with transactions |

## Reference

- Paper: Cooper et al. "Benchmarking Cloud Serving Systems with YCSB" (SoCC 2010)
- Repo: https://github.com/brianfrankcooper/YCSB
