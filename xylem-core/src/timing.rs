//! Nanosecond-precision timing utilities
//!
//! Provides timing functions with nanosecond precision for accurate latency measurement.
//! Uses RDTSCP on x86_64 for minimal overhead (~20 cycles vs ~200+ for clock_gettime syscall).
//! The clock and its calibration live in `xylem_transport`, shared with `Timestamp`.

/// Get current time in nanoseconds since calibration
///
/// Uses RDTSCP on x86_64 for minimal overhead (~20 cycles vs ~200+ for syscall).
/// Wraps [`xylem_transport::time_ns`].
///
/// # Example
/// ```
/// use xylem_core::timing::time_ns;
///
/// let start = time_ns();
/// // ... do work ...
/// let elapsed = time_ns() - start;
/// println!("Elapsed: {} ns", elapsed);
/// ```
#[inline(always)]
pub fn time_ns() -> u64 {
    xylem_transport::time_ns()
}

/// Busy-wait until the target time is reached
///
/// This function implements a tight busy-wait loop, providing nanosecond-level precision
/// for scheduling. Unlike `std::thread::sleep` or async timers, this provides deterministic
/// timing at the cost of CPU usage.
///
/// # Arguments
/// * `target_ns` - Target timestamp in nanoseconds (from `time_ns()`)
///
/// # Example
/// ```
/// use xylem_core::timing::{time_ns, busy_wait_until};
///
/// let target = time_ns() + 1_000_000; // 1ms from now
/// busy_wait_until(target);
/// ```
#[inline]
pub fn busy_wait_until(target_ns: u64) {
    while time_ns() < target_ns {
        // Busy wait - yields to OS scheduler but remains active
        std::hint::spin_loop();
    }
}

/// Busy-wait for a duration in nanoseconds
///
/// Convenience wrapper around `busy_wait_until` for relative delays.
///
/// # Arguments
/// * `duration_ns` - Duration to wait in nanoseconds
///
/// # Example
/// ```
/// use xylem_core::timing::busy_wait_ns;
///
/// busy_wait_ns(1_000_000); // Wait 1ms
/// ```
#[inline]
pub fn busy_wait_ns(duration_ns: u64) {
    let target = time_ns() + duration_ns;
    busy_wait_until(target);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Busy waits each overshoot test times
    const TRIALS: usize = 20;

    /// Run `wait` `TRIALS` times, check that every run waited at least
    /// `duration_ns`, and return the smallest overshoot past it
    ///
    /// A busy wait overshoots only by the last iteration of its loop, but the
    /// scheduler can preempt the test thread at any moment, which parallel
    /// tests on a loaded host make common. The best of several runs tests the
    /// wait itself rather than the machine's load. `wait` gets the start time.
    fn best_overshoot(duration_ns: u64, wait: impl Fn(u64)) -> u64 {
        (0..TRIALS)
            .map(|_| {
                let start = time_ns();
                wait(start);
                let elapsed = time_ns() - start;
                assert!(elapsed >= duration_ns, "waited {elapsed} ns, less than {duration_ns} ns");
                elapsed - duration_ns
            })
            .min()
            .expect("TRIALS is not zero")
    }

    #[test]
    fn test_time_ns_monotonic() {
        let t1 = time_ns();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let t2 = time_ns();

        assert!(t2 > t1, "Time should be monotonic");
        assert!(t2 - t1 >= 1_000_000, "Should have elapsed at least 1ms");
    }

    #[test]
    #[cfg(not(tarpaulin))]
    fn test_busy_wait_precision() {
        // Within 20% of 100 microseconds (busy-wait is more accurate than sleep)
        let overshoot = best_overshoot(100_000, |_| busy_wait_ns(100_000));
        assert!(overshoot < 20_000, "overshot 100us by {overshoot} ns even in the best run");
    }

    #[test]
    #[cfg(not(tarpaulin))]
    fn test_busy_wait_until() {
        let overshoot = best_overshoot(50_000, |start| busy_wait_until(start + 50_000));
        assert!(overshoot < 20_000, "overshot 50us by {overshoot} ns even in the best run");
    }
}
