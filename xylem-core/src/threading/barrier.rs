//! Thread synchronization barriers
//!
//! Provides barrier synchronization for coordinating thread startup and shutdown.
//! Wraps std::sync::Barrier with additional lifecycle management features.

use std::sync::{Arc, Barrier as StdBarrier};

/// A reusable barrier for thread synchronization
///
/// This is a thin wrapper around std::sync::Barrier that provides
/// a more ergonomic API for common threading patterns in Xylem.
#[derive(Clone)]
pub struct Barrier {
    inner: Arc<StdBarrier>,
}

impl Barrier {
    /// Create a new barrier for `n` threads
    ///
    /// # Arguments
    /// * `n` - Number of threads that must wait at the barrier
    ///
    /// # Panics
    /// Panics if `n` is 0
    pub fn new(n: usize) -> Self {
        assert!(n > 0, "Barrier must be created with at least 1 thread");
        Self { inner: Arc::new(StdBarrier::new(n)) }
    }

    /// Wait at the barrier until all threads arrive
    ///
    /// Returns `true` for exactly one thread (the "leader"),
    /// and `false` for all other threads.
    ///
    /// This can be used to execute initialization code on only one thread:
    ///
    /// ```no_run
    /// use xylem_core::threading::barrier::Barrier;
    ///
    /// let barrier = Barrier::new(4);
    /// // ... spawn 4 threads ...
    ///
    /// // In each thread:
    /// if barrier.wait() {
    ///     // Only one thread executes this
    ///     println!("I am the leader!");
    /// }
    /// ```
    pub fn wait(&self) -> bool {
        self.inner.wait().is_leader()
    }

    /// Get the number of threads synchronized by this barrier
    pub fn thread_count(&self) -> usize {
        // Unfortunately std::sync::Barrier doesn't expose this
        // We could track it separately if needed
        // For now, users should track it themselves
        unimplemented!("thread_count() not available from std::sync::Barrier")
    }
}

/// Barrier pair for coordinated startup and shutdown
///
/// Provides two barriers: one for startup synchronization and one for shutdown.
/// This is a common pattern in Xylem's threading model.
pub struct BarrierPair {
    /// Barrier for coordinated startup (all threads start simultaneously)
    pub start: Barrier,
    /// Barrier for coordinated shutdown (all threads finish simultaneously)
    pub finish: Barrier,
}

impl BarrierPair {
    /// Create a new barrier pair for `n` threads
    pub fn new(n: usize) -> Self {
        Self {
            start: Barrier::new(n),
            finish: Barrier::new(n),
        }
    }

    /// Wait at the start barrier
    ///
    /// All threads will wait here until all have arrived, then proceed simultaneously.
    /// Returns true for the leader thread.
    pub fn wait_start(&self) -> bool {
        self.start.wait()
    }

    /// Wait at the finish barrier
    ///
    /// All threads will wait here until all have arrived, ensuring coordinated shutdown.
    /// Returns true for the leader thread.
    pub fn wait_finish(&self) -> bool {
        self.finish.wait()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc as StdArc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_barrier_new() {
        let barrier = Barrier::new(5);
        // Should create successfully
        assert!(format!("{:?}", barrier.inner).contains("Barrier"));
    }

    #[test]
    #[should_panic(expected = "Barrier must be created with at least 1 thread")]
    fn test_barrier_zero_threads() {
        let _barrier = Barrier::new(0);
    }

    #[test]
    fn test_barrier_wait_synchronization() {
        let n_threads = 4;
        let barrier = Barrier::new(n_threads);
        let counter = StdArc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        for _ in 0..n_threads {
            let barrier = barrier.clone();
            let counter = counter.clone();

            let handle = thread::spawn(move || {
                // Simulate some work before barrier
                thread::sleep(Duration::from_millis(10));

                // All threads should arrive at roughly the same time
                barrier.wait();

                // Increment counter after barrier
                counter.fetch_add(1, Ordering::SeqCst);
            });

            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // All threads should have incremented the counter
        assert_eq!(counter.load(Ordering::SeqCst), n_threads);
    }

    #[test]
    fn test_barrier_leader_selection() {
        let n_threads = 8;
        let barrier = Barrier::new(n_threads);
        let leader_count = StdArc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        for _ in 0..n_threads {
            let barrier = barrier.clone();
            let leader_count = leader_count.clone();

            let handle = thread::spawn(move || {
                if barrier.wait() {
                    // Only the leader thread increments
                    leader_count.fetch_add(1, Ordering::SeqCst);
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Exactly one thread should be the leader
        assert_eq!(leader_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_barrier_pair_new() {
        let pair = BarrierPair::new(3);
        // Should create successfully with both barriers
        assert!(format!("{:?}", pair.start.inner).contains("Barrier"));
        assert!(format!("{:?}", pair.finish.inner).contains("Barrier"));
    }

    #[test]
    fn test_barrier_pair_coordination() {
        let n_threads = 4;
        let pair = StdArc::new(BarrierPair::new(n_threads));
        let start_times = StdArc::new(AtomicUsize::new(0));
        let finish_times = StdArc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        for i in 0..n_threads {
            let pair = pair.clone();
            let start_times = start_times.clone();
            let finish_times = finish_times.clone();

            let handle = thread::spawn(move || {
                // Wait at start barrier
                pair.wait_start();
                start_times.fetch_add(1, Ordering::SeqCst);

                // Simulate work with varying duration
                thread::sleep(Duration::from_millis(10 + (i as u64) * 5));

                // Wait at finish barrier
                pair.wait_finish();
                finish_times.fetch_add(1, Ordering::SeqCst);
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // All threads should have hit both barriers
        assert_eq!(start_times.load(Ordering::SeqCst), n_threads);
        assert_eq!(finish_times.load(Ordering::SeqCst), n_threads);
    }

    #[test]
    fn test_barrier_pair_leader() {
        let n_threads = 5;
        let pair = StdArc::new(BarrierPair::new(n_threads));
        let start_leaders = StdArc::new(AtomicUsize::new(0));
        let finish_leaders = StdArc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        for _ in 0..n_threads {
            let pair = pair.clone();
            let start_leaders = start_leaders.clone();
            let finish_leaders = finish_leaders.clone();

            let handle = thread::spawn(move || {
                if pair.wait_start() {
                    start_leaders.fetch_add(1, Ordering::SeqCst);
                }

                thread::sleep(Duration::from_millis(5));

                if pair.wait_finish() {
                    finish_leaders.fetch_add(1, Ordering::SeqCst);
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Exactly one leader for each barrier
        assert_eq!(start_leaders.load(Ordering::SeqCst), 1);
        assert_eq!(finish_leaders.load(Ordering::SeqCst), 1);
    }
}
