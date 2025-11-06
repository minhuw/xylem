//! Threading runtime and worker management
//!
//! Uses native OS threads (std::thread) for maximum control and performance.

use crate::stats::StatsCollector;
use crate::Result;
use std::sync::{Arc, Barrier};
use std::thread;

pub mod affinity;
pub mod barrier;
pub mod pipelined_worker;
pub mod worker;

// Re-export main types
pub use pipelined_worker::{PipelinedWorker, PipelinedWorkerConfig};
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
    ///
    /// Uses native OS threads with a barrier for synchronization.
    /// All threads start execution simultaneously after the barrier.
    pub fn run_workers<F>(&self, worker_factory: F) -> Result<Vec<StatsCollector>>
    where
        F: Fn(usize) -> Result<StatsCollector> + Send + Sync + Clone + 'static,
    {
        // Create synchronization barrier for all threads
        let barrier = Arc::new(Barrier::new(self.num_threads));
        let mut handles = Vec::new();

        // Spawn worker threads
        for thread_id in 0..self.num_threads {
            let worker_factory = worker_factory.clone();
            let barrier = barrier.clone();

            let handle = thread::spawn(move || {
                // Wait for all threads to be ready
                barrier.wait();

                // Run the worker
                worker_factory(thread_id)
            });

            handles.push(handle);
        }

        // Collect results from all workers
        let mut results = Vec::new();
        for handle in handles {
            let stats = handle
                .join()
                .map_err(|e| crate::Error::Other(format!("Thread panicked: {:?}", e)))??;
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
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_threading_runtime_basic() {
        let runtime = ThreadingRuntime::new(4);

        let results = runtime
            .run_workers(|thread_id| {
                // Simulate some work
                thread::sleep(Duration::from_millis(10));

                // Create a collector with thread-specific data
                let mut collector = StatsCollector::new(100, 1.0);
                collector.record_latency(Duration::from_micros(thread_id as u64 * 10));

                Ok(collector)
            })
            .unwrap();

        assert_eq!(results.len(), 4);

        // Verify each worker produced results
        for (i, stats) in results.iter().enumerate() {
            assert_eq!(stats.sample_count(), 1);
            assert_eq!(stats.samples().len(), 1);
            assert_eq!(stats.samples()[0], Duration::from_micros(i as u64 * 10));
        }
    }

    #[test]
    fn test_threading_runtime_single_thread() {
        let runtime = ThreadingRuntime::new(1);

        let results = runtime
            .run_workers(|_thread_id| {
                let mut collector = StatsCollector::default();
                collector.record_latency(Duration::from_micros(100));
                Ok(collector)
            })
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].sample_count(), 1);
    }
}
