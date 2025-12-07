//! Timestamp types for latency measurement
//!
//! This module provides timestamp types that support both software (RDTSC-based)
//! and hardware (NIC-based) timestamps for precise latency measurement.

use std::time::{Duration, Instant};

/// Timestamp for request/response tracking
///
/// Supports two timestamp sources:
/// - **Software timestamps**: Uses RDTSC (Read Time-Stamp Counter) on x86_64 for
///   minimal overhead (~20 cycles). Falls back to `Instant` on other architectures.
/// - **Hardware timestamps**: Uses NIC hardware timestamps via SO_TIMESTAMPING
///   (Linux only) for microsecond-precision measurement at the network interface.
///
/// When hardware timestamps are available, they provide more accurate latency
/// measurements by capturing time at the NIC rather than in userspace.
#[derive(Debug, Clone, Copy)]
pub struct Timestamp {
    /// CPU cycle count (RDTSC) or nanoseconds since epoch
    cycles: u64,
    /// Hardware timestamp from NIC (nanoseconds since Unix epoch)
    /// When present, this takes precedence for latency calculation
    #[cfg(target_os = "linux")]
    hw_nanos: Option<u64>,
}

impl Timestamp {
    /// Create a new software timestamp from current time
    ///
    /// Uses RDTSC for minimal overhead (~20 cycles vs ~200+ for clock_gettime).
    #[inline(always)]
    pub fn now() -> Self {
        Self {
            cycles: rdtsc(),
            #[cfg(target_os = "linux")]
            hw_nanos: None,
        }
    }

    /// Create a timestamp from hardware NIC timestamp
    ///
    /// Hardware timestamps are provided by the NIC and represent the time
    /// when the packet was sent/received at the network interface.
    #[cfg(target_os = "linux")]
    pub fn from_hardware(tv_sec: i64, tv_nsec: i64) -> Self {
        let hw_nanos = (tv_sec as u64) * 1_000_000_000 + (tv_nsec as u64);
        Self {
            cycles: rdtsc(), // Also capture SW timestamp for fallback
            hw_nanos: Some(hw_nanos),
        }
    }

    /// Check if this timestamp has hardware timestamp data
    #[cfg(target_os = "linux")]
    pub fn is_hardware(&self) -> bool {
        self.hw_nanos.is_some()
    }

    /// Check if this timestamp has hardware timestamp data (always false without feature)
    #[cfg(not(target_os = "linux"))]
    pub fn is_hardware(&self) -> bool {
        false
    }

    /// Calculate duration since another timestamp
    ///
    /// If both timestamps have hardware timestamps, uses those for calculation.
    /// Otherwise, falls back to software (RDTSC-based) timestamps.
    #[inline]
    pub fn duration_since(&self, earlier: &Timestamp) -> Duration {
        #[cfg(target_os = "linux")]
        {
            // Use hardware timestamps if both are available
            if let (Some(hw_self), Some(hw_earlier)) = (self.hw_nanos, earlier.hw_nanos) {
                let delta_nanos = hw_self.saturating_sub(hw_earlier);
                return Duration::from_nanos(delta_nanos);
            }
        }

        // Fall back to software timestamps
        let delta_cycles = self.cycles.saturating_sub(earlier.cycles);
        let nanos = get_calibration().cycles_to_nanos(delta_cycles);
        Duration::from_nanos(nanos)
    }

    /// Get the raw cycle count (for comparisons)
    #[inline]
    pub fn cycles(&self) -> u64 {
        self.cycles
    }

    /// Get hardware timestamp in nanoseconds (if available)
    #[cfg(target_os = "linux")]
    pub fn hw_nanos(&self) -> Option<u64> {
        self.hw_nanos
    }
}

// =============================================================================
// TSC Calibration (same as before, moved from lib.rs)
// =============================================================================

/// Calibration data for converting RDTSC cycles to nanoseconds
struct TscCalibration {
    cycles_per_ns: f64,
}

impl TscCalibration {
    fn calibrate() -> Self {
        // Calibrate by measuring cycles over a known duration
        let start_cycles = rdtsc();
        let start_instant = Instant::now();

        // Sleep for a short calibration period
        std::thread::sleep(Duration::from_millis(10));

        let end_cycles = rdtsc();
        let elapsed_ns = start_instant.elapsed().as_nanos() as f64;

        let cycles = (end_cycles - start_cycles) as f64;
        let cycles_per_ns = cycles / elapsed_ns;

        Self { cycles_per_ns }
    }

    fn cycles_to_nanos(&self, cycles: u64) -> u64 {
        (cycles as f64 / self.cycles_per_ns) as u64
    }
}

/// Get calibration data (initialized once)
fn get_calibration() -> &'static TscCalibration {
    static CALIBRATION: std::sync::OnceLock<TscCalibration> = std::sync::OnceLock::new();
    CALIBRATION.get_or_init(TscCalibration::calibrate)
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

    #[test]
    fn test_is_hardware_default() {
        let _ts = Timestamp::now();
        // On non-Linux, should always be false
        #[cfg(not(target_os = "linux"))]
        assert!(!_ts.is_hardware());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_hardware_timestamp() {
        // Create a hardware timestamp (simulated)
        let ts = Timestamp::from_hardware(1_700_000_000, 500_000_000);
        assert!(ts.is_hardware());
        assert_eq!(ts.hw_nanos(), Some(1_700_000_000_500_000_000));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_hardware_duration() {
        let t1 = Timestamp::from_hardware(1_700_000_000, 0);
        let t2 = Timestamp::from_hardware(1_700_000_000, 1_000_000); // 1ms later

        let duration = t2.duration_since(&t1);
        assert_eq!(duration, Duration::from_millis(1));
    }
}
