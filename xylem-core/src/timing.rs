//! Nanosecond-precision timing utilities
//!
//! Provides timing functions with nanosecond precision for accurate latency measurement.
//! Uses RDTSCP on x86_64 for minimal overhead (~20 cycles vs ~200+ for clock_gettime syscall).

use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// Calibration: (start_cycles, cycles_per_ns)
static CALIBRATION: OnceLock<(u64, f64)> = OnceLock::new();

fn calibrate() -> (u64, f64) {
    let start_cycles = rdtsc();
    let start_instant = Instant::now();
    std::thread::sleep(Duration::from_millis(10));
    let end_cycles = rdtsc();
    let elapsed_ns = start_instant.elapsed().as_nanos() as f64;
    let cycles_per_ns = (end_cycles - start_cycles) as f64 / elapsed_ns;
    (start_cycles, cycles_per_ns)
}

/// Read the CPU timestamp counter with serialization (RDTSCP)
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn rdtsc() -> u64 {
    unsafe {
        let mut _aux: u32 = 0;
        core::arch::x86_64::__rdtscp(&mut _aux)
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
fn rdtsc() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

/// Get current time in nanoseconds since calibration
///
/// Uses RDTSCP on x86_64 for minimal overhead (~20 cycles vs ~200+ for syscall).
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
    let &(start_cycles, cycles_per_ns) = CALIBRATION.get_or_init(calibrate);
    let cycles = rdtsc() - start_cycles;
    (cycles as f64 / cycles_per_ns) as u64
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
        let start = time_ns();
        busy_wait_ns(100_000); // 100 microseconds
        let elapsed = time_ns() - start;

        // Should be within 10% of target (busy-wait is more accurate than sleep)
        assert!(elapsed >= 100_000, "Should wait at least 100us");
        assert!(elapsed < 120_000, "Should not wait much more than 100us");
    }

    #[test]
    #[cfg(not(tarpaulin))]
    fn test_busy_wait_until() {
        let start = time_ns();
        let target = start + 50_000; // 50 microseconds
        busy_wait_until(target);
        let elapsed = time_ns() - start;

        assert!(elapsed >= 50_000, "Should reach target time");
        assert!(elapsed < 70_000, "Should not overshoot by much");
    }
}
