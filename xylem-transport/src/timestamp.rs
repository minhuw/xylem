//! Timestamp types for latency measurement
//!
//! A [`Timestamp`] always holds the user-space cycle count (RDTSC) read when
//! its send or receive call completed. It may also carry a kernel or NIC
//! timestamp for the same event, tagged with the clock it was taken from.

use std::time::{Duration, Instant};

/// Clock that a kernel-reported timestamp was taken from
///
/// Timestamps from different clocks are not comparable: the NIC's PTP
/// hardware clock is not synchronised to the system clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelClock {
    /// Kernel software timestamp (`SOF_TIMESTAMPING_SOFTWARE`), CLOCK_REALTIME
    Realtime,
    /// Raw NIC hardware timestamp (`SOF_TIMESTAMPING_RAW_HARDWARE`), the NIC's PHC
    NicHardware,
}

/// A timestamp reported by the kernel or the NIC for one send or receive
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelTimestamp {
    /// Nanoseconds since the epoch of `clock`
    pub nanos: u64,
    /// Clock the timestamp was taken from
    pub clock: KernelClock,
}

/// Timestamp for request/response tracking
///
/// Holds two views of one send or receive event:
/// - **Cycles**: the CPU timestamp counter (RDTSC on x86_64, `Instant`
///   elsewhere), read in user space when the call completed. Always present.
/// - **Kernel timestamp**: optional, from SO_TIMESTAMPING (Linux) or another
///   source of NIC/stack timestamps, with the clock it was taken from.
///
/// [`duration_since`](Self::duration_since) uses kernel timestamps only when
/// both events have one from the same clock; otherwise it uses the cycles of
/// both events. It never mixes a kernel timestamp with cycles, or two clocks.
#[derive(Debug, Clone, Copy)]
pub struct Timestamp {
    /// CPU cycle count (RDTSC) or nanoseconds since first use
    cycles: u64,
    /// Kernel or NIC timestamp for the same event, if one was reported
    kernel: Option<KernelTimestamp>,
}

impl Timestamp {
    /// Create a new software timestamp from current time
    ///
    /// Uses RDTSC for minimal overhead (~20 cycles vs ~200+ for clock_gettime).
    #[inline(always)]
    pub fn now() -> Self {
        Self { cycles: rdtsc(), kernel: None }
    }

    /// This event's timestamp with a kernel timestamp for the same event
    ///
    /// Keeps the cycles of `self`, so that the result can still be compared
    /// with timestamps that have no kernel timestamp from the same clock.
    #[inline]
    pub fn with_kernel(self, kernel: KernelTimestamp) -> Self {
        Self { kernel: Some(kernel), ..self }
    }

    /// The kernel timestamp for this event, if one was reported
    #[inline]
    pub fn kernel(&self) -> Option<KernelTimestamp> {
        self.kernel
    }

    /// Check if this timestamp carries a NIC hardware timestamp
    pub fn is_hardware(&self) -> bool {
        matches!(self.kernel, Some(KernelTimestamp { clock: KernelClock::NicHardware, .. }))
    }

    /// Calculate duration since another timestamp
    ///
    /// Uses the kernel timestamps if both have one from the same clock.
    /// Otherwise uses the cycles of both, read when each call completed.
    #[inline]
    pub fn duration_since(&self, earlier: &Timestamp) -> Duration {
        if let (Some(later), Some(earlier)) = (self.kernel, earlier.kernel) {
            if later.clock == earlier.clock {
                return Duration::from_nanos(later.nanos.saturating_sub(earlier.nanos));
            }
        }

        let delta_cycles = self.cycles.saturating_sub(earlier.cycles);
        let nanos = get_calibration().cycles_to_nanos(delta_cycles);
        Duration::from_nanos(nanos)
    }

    /// This event's cycles on the [`time_ns`] clock, in nanoseconds
    #[inline]
    pub fn clock_ns(&self) -> u64 {
        let calibration = get_calibration();
        calibration.cycles_to_nanos(self.cycles.saturating_sub(calibration.start_cycles))
    }

    /// Get the raw cycle count (for comparisons)
    #[inline]
    pub fn cycles(&self) -> u64 {
        self.cycles
    }
}

// =============================================================================
// TSC Calibration (same as before, moved from lib.rs)
// =============================================================================

/// Calibration data for converting RDTSC cycles to nanoseconds
struct TscCalibration {
    /// Cycle count at the start of calibration; `time_ns` counts from here
    start_cycles: u64,
    cycles_per_ns: f64,
}

impl TscCalibration {
    fn calibrate() -> Self {
        // Calibrate by measuring cycles over a known duration, with the
        // counter and the clock read together at both ends
        let (start_cycles, start_instant) = paired_sample();

        // Sleep for a short calibration period
        std::thread::sleep(Duration::from_millis(10));

        let (end_cycles, end_instant) = paired_sample();
        let elapsed_ns = (end_instant - start_instant).as_nanos() as f64;

        let cycles = (end_cycles - start_cycles) as f64;
        let cycles_per_ns = cycles / elapsed_ns;

        Self { start_cycles, cycles_per_ns }
    }

    fn cycles_to_nanos(&self, cycles: u64) -> u64 {
        (cycles as f64 / self.cycles_per_ns) as u64
    }
}

/// Attempts `paired_sample` makes to read the counter and the clock together
const PAIRED_SAMPLE_ATTEMPTS: usize = 32;

/// Read the counter and the monotonic clock at the same moment
///
/// Brackets `Instant::now()` between two counter reads and keeps the attempt
/// with the narrowest bracket, at its midpoint. A preemption, an interrupt or
/// a slow first clock read between the reads widens that attempt's bracket,
/// so it is discarded instead of skewing the calibration. Read one after the
/// other, the pair was off by up to 0.4% on a loaded host.
fn paired_sample() -> (u64, Instant) {
    let mut best: Option<(u64, u64, Instant)> = None;
    for _ in 0..PAIRED_SAMPLE_ATTEMPTS {
        let before = rdtsc();
        let instant = Instant::now();
        let after = rdtsc();
        let width = after.wrapping_sub(before);
        if best.is_none_or(|(best_width, ..)| width < best_width) {
            best = Some((width, before + width / 2, instant));
        }
    }
    let (_, cycles, instant) = best.expect("PAIRED_SAMPLE_ATTEMPTS is not zero");
    (cycles, instant)
}

/// Get calibration data (initialized once)
fn get_calibration() -> &'static TscCalibration {
    static CALIBRATION: std::sync::OnceLock<TscCalibration> = std::sync::OnceLock::new();
    CALIBRATION.get_or_init(TscCalibration::calibrate)
}

/// Get current time in nanoseconds since calibration
///
/// Reads the same counter and calibration as [`Timestamp`], using RDTSCP on
/// x86_64 for minimal overhead (~20 cycles vs ~200+ for syscall).
#[inline(always)]
pub fn time_ns() -> u64 {
    let calibration = get_calibration();
    let cycles = rdtsc() - calibration.start_cycles;
    calibration.cycles_to_nanos(cycles)
}

/// Read the CPU timestamp counter with serialization
///
/// Uses RDTSCP which includes a memory barrier to ensure the timestamp
/// is taken after all previous instructions complete.
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
    // Fallback for non-x86_64: use Instant
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_now() {
        let t1 = Timestamp::now();
        std::thread::sleep(Duration::from_micros(100));
        let t2 = Timestamp::now();

        assert!(t2.cycles() > t1.cycles());
    }

    #[test]
    fn test_duration_since() {
        let t1 = Timestamp::now();
        std::thread::sleep(Duration::from_millis(10));
        let t2 = Timestamp::now();

        let duration = t2.duration_since(&t1);
        // Should be approximately 10ms (allow some variance)
        assert!(duration.as_millis() >= 5 && duration.as_millis() <= 50);
    }

    fn kernel(nanos: u64, clock: KernelClock) -> KernelTimestamp {
        KernelTimestamp { nanos, clock }
    }

    #[test]
    fn test_with_kernel_keeps_event_cycles() {
        let ts = Timestamp::now();
        let stamped = ts.with_kernel(kernel(5, KernelClock::NicHardware));
        assert_eq!(stamped.cycles(), ts.cycles());
        assert_eq!(stamped.kernel(), Some(kernel(5, KernelClock::NicHardware)));
        assert!(stamped.is_hardware());
        assert!(!ts.is_hardware());
        assert!(!ts.with_kernel(kernel(5, KernelClock::Realtime)).is_hardware());
    }

    /// Every pairing of send and receive timestamp sources: kernel time is
    /// used only when both sides come from the same clock, and otherwise each
    /// side's own event cycles are, never a time read at decode.
    #[test]
    fn test_duration_since_every_source_pairing() {
        use KernelClock::{NicHardware as Hw, Realtime as Sw};
        let send = Timestamp::now();
        std::thread::sleep(Duration::from_millis(20));
        let recv = Timestamp::now();
        let cycles = recv.duration_since(&send);
        assert!(cycles >= Duration::from_millis(15), "{cycles:?}");

        // Kernel times 1 ms apart, far from the 20 ms between the cycles
        let kernel_delta = Duration::from_millis(1);
        for (send_clock, recv_clock, kernel_used) in [
            (None, None, false),
            (Some(Sw), None, false),
            (None, Some(Sw), false),
            (Some(Hw), None, false),
            (None, Some(Hw), false),
            (Some(Sw), Some(Hw), false),
            (Some(Hw), Some(Sw), false),
            (Some(Sw), Some(Sw), true),
            (Some(Hw), Some(Hw), true),
        ] {
            let send = match send_clock {
                Some(clock) => send.with_kernel(kernel(1_000_000_000, clock)),
                None => send,
            };
            let recv = match recv_clock {
                Some(clock) => recv.with_kernel(kernel(1_001_000_000, clock)),
                None => recv,
            };
            let expected = if kernel_used { kernel_delta } else { cycles };
            assert_eq!(recv.duration_since(&send), expected, "{send_clock:?} -> {recv_clock:?}");
        }
    }

    #[test]
    fn test_kernel_duration_saturates() {
        let ts = Timestamp::now();
        let earlier = ts.with_kernel(kernel(10, KernelClock::Realtime));
        let later = ts.with_kernel(kernel(5, KernelClock::Realtime));
        assert_eq!(later.duration_since(&earlier), Duration::ZERO);
    }
}
