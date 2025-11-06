//! Threading runtime and worker management

use crate::stats::StatsCollector;
use crate::Result;
use std::sync::Arc;
use tokio::sync::Barrier;

pub mod affinity;
pub mod barrier;
pub mod worker;

// Re-export main types
pub use worker::{Worker, WorkerConfig};

/// Multi-threaded runtime for spawning and managing workers
pub struct ThreadingRuntime {
    num_threads: usize,
}

impl ThreadingRuntime {
    /// Create a new threading runtime
    pub fn new(num_threads: usize) -> Self {
        Self { num_threads }
    }

    /// Run multiple workers in parallel and aggregate results
    pub async fn run_workers<F, Fut>(&self, worker_factory: F) -> Result<Vec<StatsCollector>>
    where
        F: Fn(usize) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<StatsCollector>> + Send + 'static,
    {
        // Create synchronization barrier for all threads
        let barrier = Arc::new(Barrier::new(self.num_threads));
        let mut handles = Vec::new();

        // Spawn worker tasks
        for thread_id in 0..self.num_threads {
            let worker_factory = worker_factory.clone();
            let barrier = barrier.clone();

            let handle = tokio::spawn(async move {
                // Wait for all threads to be ready
                barrier.wait().await;

                // Run the worker
                worker_factory(thread_id).await
            });

            handles.push(handle);
        }

        // Collect results from all workers
        let mut results = Vec::new();
        for handle in handles {
            let stats = handle.await??;
            results.push(stats);
        }

        Ok(results)
    }

    /// Get number of threads
    pub fn num_threads(&self) -> usize {
        self.num_threads
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_threading_runtime_basic() {
        let runtime = ThreadingRuntime::new(4);

        let results = runtime
            .run_workers(|thread_id| async move {
                // Simulate some work
                tokio::time::sleep(Duration::from_millis(10)).await;

                // Create a collector with thread-specific data
                let mut collector = StatsCollector::new(100, 1.0);
                collector.record_latency(Duration::from_micros(thread_id as u64 * 10));

                Ok(collector)
            })
            .await
            .unwrap();

        assert_eq!(results.len(), 4);

        // Verify each worker produced results
        for (i, stats) in results.iter().enumerate() {
            assert_eq!(stats.sample_count(), 1);
            assert_eq!(stats.samples().len(), 1);
            assert_eq!(stats.samples()[0], Duration::from_micros(i as u64 * 10));
        }
    }

    #[tokio::test]
    async fn test_threading_runtime_single_thread() {
        let runtime = ThreadingRuntime::new(1);

        let results = runtime
            .run_workers(|_thread_id| async move {
                let mut collector = StatsCollector::default();
                collector.record_latency(Duration::from_micros(100));
                Ok(collector)
            })
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].sample_count(), 1);
    }
}
