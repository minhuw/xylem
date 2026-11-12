//! Threading runtime and worker management
//!
//! Uses native OS threads (std::thread) for maximum control and performance.

use crate::Result;
use std::sync::{Arc, Barrier, Condvar, Mutex, PoisonError};
use std::thread;

pub mod affinity;
pub mod worker;

// Re-export main types
pub use affinity::{get_available_core_ids, get_core_count, pin_thread, CpuPinning};
pub use worker::{Worker, WorkerConfig, DEFAULT_DRAIN_TIMEOUT, DEFAULT_SETUP_TIMEOUT};
pub use xylem_common::{ConnectionRole, ErrorReply, Outbox, Parsed, Protocol, RequestMeta};

/// How long after the last thread reaches the start gate the run starts, so
/// every thread is awake and spinning by then
const START_DELAY_NS: u64 = 10_000_000;

/// Start gate shared by the threads of a run
struct StartGate {
    state: Mutex<GateState>,
    released: Condvar,
}

struct GateState {
    /// Tickets that have neither waited nor been dropped
    outstanding: usize,
    /// The start time, once the last ticket arrived
    start_ns: Option<u64>,
}

impl StartGate {
    fn tickets(count: usize) -> Vec<StartTicket> {
        let gate = Arc::new(Self {
            state: Mutex::new(GateState { outstanding: count, start_ns: None }),
            released: Condvar::new(),
        });
        (0..count).map(|_| StartTicket { gate: Some(Arc::clone(&gate)) }).collect()
    }

    /// Count a ticket in; the last one sets the start time and wakes the rest
    fn arrive(&self) -> std::sync::MutexGuard<'_, GateState> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.outstanding -= 1;
        if state.outstanding == 0 {
            state.start_ns = Some(crate::timing::time_ns().saturating_add(START_DELAY_NS));
            self.released.notify_all();
        }
        state
    }
}

/// A thread's place at the start gate of [`ThreadingRuntime::run_workers_with_start`]
///
/// Once every thread has called [`StartTicket::wait`] or dropped its ticket,
/// for instance by failing or panicking before the start, the start time is
/// set and every waiting thread returns it.
pub struct StartTicket {
    gate: Option<Arc<StartGate>>,
}

impl StartTicket {
    /// Wait until every thread has arrived; returns the shared start time, a
    /// `timing::time_ns` value shortly in the future
    pub fn wait(mut self) -> u64 {
        let gate = self.gate.take().expect("a ticket arrives once");
        let mut state = gate.arrive();
        loop {
            if let Some(start_ns) = state.start_ns {
                return start_ns;
            }
            state = gate.released.wait(state).unwrap_or_else(PoisonError::into_inner);
        }
    }
}

impl Drop for StartTicket {
    /// A thread that never waits must not hold up the others
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            drop(gate.arrive());
        }
    }
}

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

    /// Run multiple workers in parallel that start at one shared time
    ///
    /// Each thread builds its worker (connecting, for instance) and calls
    /// [`StartTicket::wait`], which returns once every thread has done so or
    /// dropped its ticket: the start time, the same for every thread, shortly
    /// after the last one arrived. Pass it to [`Worker::run_at`]. The callback
    /// receives the thread ID (which may be non-contiguous) and its ticket.
    pub fn run_workers_with_start<F, T>(&self, worker_factory: F) -> Result<Vec<T>>
    where
        F: Fn(usize, StartTicket) -> Result<T> + Send + Sync + Clone + 'static,
        T: Send + 'static,
    {
        let tickets = StartGate::tickets(self.thread_ids.len());
        let mut handles = Vec::new();

        for (&thread_id, ticket) in self.thread_ids.iter().zip(tickets) {
            let worker_factory = worker_factory.clone();
            let cpu_pinning = self.cpu_pinning.clone();

            let handle = thread::spawn(move || {
                if let Err(e) = affinity::pin_thread(thread_id, &cpu_pinning) {
                    tracing::warn!(
                        "Failed to pin thread {} to CPU core {}: {}",
                        thread_id,
                        thread_id,
                        e
                    );
                }
                worker_factory(thread_id, ticket)
            });

            handles.push(handle);
        }

        let mut results = Vec::new();
        for handle in handles {
            let stats = handle
                .join()
                .map_err(|e| crate::Error::Other(format!("Thread panicked: {e:?}")))??;
            results.push(stats);
        }

        Ok(results)
    }

    /// Get number of threads
    pub fn num_threads(&self) -> usize {
        self.thread_ids.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::{SamplingPolicy, TupleStatsCollector};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_threading_runtime_basic() {
        let runtime = ThreadingRuntime::new(4);

        let results = runtime
            .run_workers_generic(|thread_id| {
                // Simulate some work
                thread::sleep(Duration::from_millis(10));

                // Create a collector with thread-specific data
                let mut collector = TupleStatsCollector::new(
                    SamplingPolicy::Limited { max_samples: 100, rate: 1.0 },
                    Duration::from_secs(1),
                );
                collector.record_latency(0, 0, Duration::from_micros(thread_id as u64 * 10));

                Ok(collector)
            })
            .unwrap();

        assert_eq!(results.len(), 4);

        // Verify each worker produced results
        for (i, stats) in results.iter().enumerate() {
            let global = stats.aggregate_global(Duration::from_secs(1));
            assert_eq!(global.total_requests, 1);
            assert_eq!(stats.entries().values().map(|entry| entry.latency.count()).sum::<u64>(), 1);
            assert_eq!(global.latency_p50, Duration::from_micros(i as u64 * 10));
        }
    }

    #[test]
    fn test_threading_runtime_single_thread() {
        let runtime = ThreadingRuntime::new(1);

        let results = runtime
            .run_workers_generic(|_thread_id| {
                let mut collector = TupleStatsCollector::default();
                collector.record_latency(0, 0, Duration::from_micros(100));
                Ok(collector)
            })
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].aggregate_global(Duration::from_secs(1)).total_requests, 1);
    }

    #[test]
    fn start_gate_gives_every_thread_one_start_time() {
        let runtime = ThreadingRuntime::new(4);
        let before = crate::timing::time_ns();
        let starts = runtime
            .run_workers_with_start(|thread_id, start| {
                // Threads arrive at different times
                thread::sleep(Duration::from_millis(5 * thread_id as u64));
                Ok(start.wait())
            })
            .unwrap();
        assert!(starts.iter().all(|&start| start == starts[0]), "{starts:?}");
        assert!(starts[0] >= before + 15_000_000 + START_DELAY_NS, "after the last arrival");
    }

    #[test]
    fn start_gate_releases_the_others_when_a_thread_fails_or_panics_first() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let runtime = ThreadingRuntime::new(4);
        let started = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&started);
        // Threads are joined in order, so 0 and 1 finish before 2's error returns
        let result = runtime.run_workers_with_start(move |thread_id, start| match thread_id {
            2 => Err(crate::Error::Other("failed to connect".into())),
            3 => panic!("worker panicked before the start"),
            _ => {
                start.wait();
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });
        assert!(
            matches!(result, Err(crate::Error::Other(message)) if message == "failed to connect")
        );
        assert_eq!(started.load(Ordering::SeqCst), 2, "both waiting threads started");
    }
}
