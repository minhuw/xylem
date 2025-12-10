//! Threading runtime and worker management
//!
//! Uses native OS threads (std::thread) for maximum control and performance.

use crate::stats::StatsCollector;
use crate::Result;
use std::sync::{Arc, Barrier};
use std::thread;

pub mod affinity;
pub mod barrier;
pub mod worker;

// Re-export main types
pub use affinity::{get_core_count, pin_thread, CpuPinning};
pub use worker::{ParseResult, Protocol, RetryRequest, Worker, WorkerConfig};
pub use xylem_common::RequestMeta;

/// Multi-threaded runtime for spawning and managing workers
pub struct ThreadingRuntime {
    /// Thread IDs (used as CPU core IDs for pinning)
    thread_ids: Vec<usize>,
    cpu_pinning: CpuPinning,
}

impl ThreadingRuntime {
    /// Create a new threading runtime with contiguous thread IDs (0..num_threads)
    pub fn new(num_threads: usize) -> Self {
        Self {
            thread_ids: (0..num_threads).collect(),
            cpu_pinning: CpuPinning::None,
        }
    }

    /// Create a new threading runtime with CPU pinning (contiguous thread IDs)
    pub fn with_cpu_pinning(num_threads: usize, cpu_pinning: CpuPinning) -> Self {
        Self {
            thread_ids: (0..num_threads).collect(),
            cpu_pinning,
        }
    }

    /// Create a new threading runtime with specific thread IDs for CPU pinning
    ///
    /// Thread IDs are used directly as CPU core IDs for pinning.
    /// For example, thread_ids = [2, 3, 4] will pin workers to CPU cores 2, 3, 4.
    pub fn with_thread_ids(thread_ids: Vec<usize>, cpu_pinning: CpuPinning) -> Self {
        Self { thread_ids, cpu_pinning }
    }

    /// Run multiple workers in parallel and aggregate results (generic over stats type)
    ///
    /// Uses native OS threads with a barrier for synchronization.
    /// All threads start execution simultaneously after the barrier.
    /// The callback receives the thread ID (which may be non-contiguous).
    pub fn run_workers_generic<F, T>(&self, worker_factory: F) -> Result<Vec<T>>
    where
        F: Fn(usize) -> Result<T> + Send + Sync + Clone + 'static,
        T: Send + 'static,
    {
        // Create synchronization barrier for all threads
        let barrier = Arc::new(Barrier::new(self.thread_ids.len()));
        let mut handles = Vec::new();

        // Spawn worker threads, one per thread_id
        for &thread_id in &self.thread_ids {
            let worker_factory = worker_factory.clone();
            let barrier = barrier.clone();
            let cpu_pinning = self.cpu_pinning.clone();

            let handle = thread::spawn(move || {
                // Pin thread to CPU core based on thread_id
                // With CpuPinning::Auto, thread_id is used as the core ID
                if let Err(e) = affinity::pin_thread(thread_id, &cpu_pinning) {
                    tracing::warn!(
                        "Failed to pin thread {} to CPU core {}: {}",
                        thread_id,
                        thread_id,
                        e
                    );
                }

                // Wait for all threads to be ready
                barrier.wait();

                // Run the worker with the actual thread_id
                worker_factory(thread_id)
            });

            handles.push(handle);
        }

        // Collect results from all workers
        let mut results = Vec::new();
        for handle in handles {
            let stats = handle
                .join()
                .map_err(|e| crate::Error::Other(format!("Thread panicked: {e:?}")))??;
            results.push(stats);
        }

        Ok(results)
    }

    /// Run multiple workers in parallel and aggregate results
    ///
    /// Uses native OS threads with a barrier for synchronization.
    /// All threads start execution simultaneously after the barrier.
    pub fn run_workers<F>(&self, worker_factory: F) -> Result<Vec<StatsCollector>>
    where
        F: Fn(usize) -> Result<StatsCollector> + Send + Sync + Clone + 'static,
    {
        self.run_workers_generic(worker_factory)
    }

    /// Get number of threads
    pub fn num_threads(&self) -> usize {
        self.thread_ids.len()
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
