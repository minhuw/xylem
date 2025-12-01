//! Load patterns for time-varying workload intensity
//!
//! Patterns define how the request rate changes over time,
//! enabling realistic scenario testing (daily cycles, flash sales, etc.)

use std::time::Duration;

/// Trait for load patterns
///
/// Load patterns control the target rate over time, enabling time-varying workloads.
/// This is the MACRO-level control (overall traffic intensity), while timing policies
/// provide MICRO-level control (inter-arrival distribution).
pub trait LoadPattern: Send + Sync {
    /// Get target rate at a given time offset
    ///
    /// # Parameters
    /// - `elapsed`: Time since workload started
    ///
    /// # Returns
    /// Target rate in requests per second
    fn rate_at(&self, elapsed: Duration) -> f64;

    /// Get pattern name
    fn name(&self) -> &'static str;

    /// Get the total duration of this pattern (if finite)
    ///
    /// Returns `None` for infinite patterns (like Constant)
    fn duration(&self) -> Option<Duration> {
        None
    }
}

/// Constant rate (no variation)
///
/// Simplest pattern - maintains the same rate throughout the entire duration.
pub struct ConstantPattern {
    rate: f64,
}

impl ConstantPattern {
    /// Create a new constant pattern
    ///
    /// # Parameters
    /// - `rate`: Target rate in requests per second
    pub fn new(rate: f64) -> Self {
        Self { rate }
    }

    /// Get the rate
    pub fn rate(&self) -> f64 {
        self.rate
    }
}

impl LoadPattern for ConstantPattern {
    fn rate_at(&self, _elapsed: Duration) -> f64 {
        self.rate
    }

    fn name(&self) -> &'static str {
        "Constant"
    }
}

/// Ramp pattern (linearly increase/decrease)
///
/// Gradually changes the rate from start_rate to end_rate over a duration.
/// Useful for stress testing and finding saturation points.
pub struct RampPattern {
    start_rate: f64,
    end_rate: f64,
    duration: Duration,
}

impl RampPattern {
    /// Create a new ramp pattern
    ///
    /// # Parameters
    /// - `start_rate`: Initial rate in requests per second
    /// - `end_rate`: Final rate in requests per second
    /// - `duration`: Time to ramp from start to end
    pub fn new(start_rate: f64, end_rate: f64, duration: Duration) -> Self {
        Self { start_rate, end_rate, duration }
    }

    /// Get the start rate
    pub fn start_rate(&self) -> f64 {
        self.start_rate
    }

    /// Get the end rate
    pub fn end_rate(&self) -> f64 {
        self.end_rate
    }
}

impl LoadPattern for RampPattern {
    fn rate_at(&self, elapsed: Duration) -> f64 {
        let progress = (elapsed.as_secs_f64() / self.duration.as_secs_f64()).min(1.0);
        self.start_rate + (self.end_rate - self.start_rate) * progress
    }

    fn name(&self) -> &'static str {
        "Ramp"
    }

    fn duration(&self) -> Option<Duration> {
        Some(self.duration)
    }
}

/// Spike pattern (sudden burst)
///
/// Maintains a normal rate, then spikes to a higher rate for a short duration.
/// Simulates flash sales, sudden traffic bursts, or DDoS attacks.
pub struct SpikePattern {
    normal_rate: f64,
    spike_rate: f64,
    spike_start: Duration,
    spike_duration: Duration,
}

impl SpikePattern {
    /// Create a new spike pattern
    ///
    /// # Parameters
    /// - `normal_rate`: Normal baseline rate in requests per second
    /// - `spike_rate`: Peak rate during spike
    /// - `spike_start`: When to start the spike
    /// - `spike_duration`: How long the spike lasts
    pub fn new(
        normal_rate: f64,
        spike_rate: f64,
        spike_start: Duration,
        spike_duration: Duration,
    ) -> Self {
        Self {
            normal_rate,
            spike_rate,
            spike_start,
            spike_duration,
        }
    }

    /// Get the normal rate
    pub fn normal_rate(&self) -> f64 {
        self.normal_rate
    }

    /// Get the spike rate
    pub fn spike_rate(&self) -> f64 {
        self.spike_rate
    }
}

impl LoadPattern for SpikePattern {
    fn rate_at(&self, elapsed: Duration) -> f64 {
        let spike_end = self.spike_start + self.spike_duration;

        if elapsed >= self.spike_start && elapsed < spike_end {
            self.spike_rate
        } else {
            self.normal_rate
        }
    }

    fn name(&self) -> &'static str {
        "Spike"
    }

    fn duration(&self) -> Option<Duration> {
        Some(self.spike_start + self.spike_duration)
    }
}

/// Sinusoidal pattern (daily cycles)
///
/// Varies the rate following a sine wave, simulating daily traffic patterns.
/// Useful for testing systems under realistic cyclical load.
pub struct SinusoidalPattern {
    base_rate: f64,
    amplitude: f64,
    period: Duration,
    phase_shift: Duration,
}

impl SinusoidalPattern {
    /// Create a new sinusoidal pattern
    ///
    /// # Parameters
    /// - `base_rate`: Average rate (center of oscillation)
    /// - `amplitude`: How much the rate varies above/below base
    /// - `period`: Time for one complete cycle
    ///
    /// The rate will vary between (base_rate - amplitude) and (base_rate + amplitude).
    pub fn new(base_rate: f64, amplitude: f64, period: Duration) -> Self {
        Self {
            base_rate,
            amplitude,
            period,
            phase_shift: Duration::ZERO,
        }
    }

    /// Set a phase shift for the sinusoid
    ///
    /// This shifts the pattern in time. Useful for starting at a different
    /// point in the cycle (e.g., start at the peak instead of middle).
    pub fn with_phase_shift(mut self, phase_shift: Duration) -> Self {
        self.phase_shift = phase_shift;
        self
    }

    /// Get the base rate
    pub fn base_rate(&self) -> f64 {
        self.base_rate
    }

    /// Get the amplitude
    pub fn amplitude(&self) -> f64 {
        self.amplitude
    }

    /// Get the period
    pub fn period(&self) -> Duration {
        self.period
    }
}

impl LoadPattern for SinusoidalPattern {
    fn rate_at(&self, elapsed: Duration) -> f64 {
        let t = (elapsed + self.phase_shift).as_secs_f64();
        let period_secs = self.period.as_secs_f64();

        let angle = 2.0 * std::f64::consts::PI * t / period_secs;
        self.base_rate + self.amplitude * angle.sin()
    }

    fn name(&self) -> &'static str {
        "Sinusoidal"
    }
}

/// Step function pattern (discrete level changes)
///
/// Changes rate in discrete steps at specified times.
/// Useful for simulating different load levels (light, medium, heavy).
pub struct StepPattern {
    steps: Vec<(Duration, f64)>, // (time, rate)
}

impl StepPattern {
    /// Create a new step pattern
    ///
    /// # Parameters
    /// - `steps`: Vector of (time, rate) tuples. Must be sorted by time.
    ///
    /// The pattern will maintain each rate until the next step time.
    /// Before the first step, the rate is 0.0.
    pub fn new(steps: Vec<(Duration, f64)>) -> Self {
        Self { steps }
    }

    /// Get the steps
    pub fn steps(&self) -> &[(Duration, f64)] {
        &self.steps
    }
}

impl LoadPattern for StepPattern {
    fn rate_at(&self, elapsed: Duration) -> f64 {
        // Find the last step that has occurred
        self.steps
            .iter()
            .rev()
            .find(|(time, _)| elapsed >= *time)
            .map(|(_, rate)| *rate)
            .unwrap_or(0.0)
    }

    fn name(&self) -> &'static str {
        "Step"
    }

    fn duration(&self) -> Option<Duration> {
        self.steps.last().map(|(time, _)| *time)
    }
}

/// Sawtooth pattern (repeated ramps)
///
/// Ramps up from min_rate to max_rate, then instantly drops back to min_rate.
/// Repeats this cycle with the given period.
pub struct SawtoothPattern {
    min_rate: f64,
    max_rate: f64,
    period: Duration,
}

impl SawtoothPattern {
    /// Create a new sawtooth pattern
    ///
    /// # Parameters
    /// - `min_rate`: Minimum rate (start of ramp)
    /// - `max_rate`: Maximum rate (end of ramp)
    /// - `period`: Time for one complete cycle (ramp up + drop)
    pub fn new(min_rate: f64, max_rate: f64, period: Duration) -> Self {
        Self { min_rate, max_rate, period }
    }

    /// Get the minimum rate
    pub fn min_rate(&self) -> f64 {
        self.min_rate
    }

    /// Get the maximum rate
    pub fn max_rate(&self) -> f64 {
        self.max_rate
    }

    /// Get the period
    pub fn period(&self) -> Duration {
        self.period
    }
}

impl LoadPattern for SawtoothPattern {
    fn rate_at(&self, elapsed: Duration) -> f64 {
        let period_secs = self.period.as_secs_f64();
        let elapsed_secs = elapsed.as_secs_f64();

        // Position within current cycle (0.0 to 1.0)
        let position = (elapsed_secs % period_secs) / period_secs;

        // Linear ramp from min to max
        self.min_rate + (self.max_rate - self.min_rate) * position
    }

    fn name(&self) -> &'static str {
        "Sawtooth"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_pattern() {
        let pattern = ConstantPattern::new(1000.0);

        assert_eq!(pattern.rate_at(Duration::ZERO), 1000.0);
        assert_eq!(pattern.rate_at(Duration::from_secs(10)), 1000.0);
        assert_eq!(pattern.rate_at(Duration::from_secs(100)), 1000.0);
        assert_eq!(pattern.name(), "Constant");
    }

    #[test]
    fn test_ramp_pattern() {
        let pattern = RampPattern::new(100.0, 1000.0, Duration::from_secs(10));

        // At start
        assert_eq!(pattern.rate_at(Duration::ZERO), 100.0);

        // At middle (5 seconds)
        let mid_rate = pattern.rate_at(Duration::from_secs(5));
        assert!((mid_rate - 550.0).abs() < 1.0, "Mid rate should be ~550, got {}", mid_rate);

        // At end
        assert_eq!(pattern.rate_at(Duration::from_secs(10)), 1000.0);

        // After end (stays at end rate)
        assert_eq!(pattern.rate_at(Duration::from_secs(20)), 1000.0);

        assert_eq!(pattern.name(), "Ramp");
    }

    #[test]
    fn test_spike_pattern() {
        let pattern = SpikePattern::new(
            1000.0,                  // normal rate
            10000.0,                 // spike rate
            Duration::from_secs(10), // spike starts at 10s
            Duration::from_secs(5),  // spike lasts 5s
        );

        // Before spike
        assert_eq!(pattern.rate_at(Duration::from_secs(0)), 1000.0);
        assert_eq!(pattern.rate_at(Duration::from_secs(9)), 1000.0);

        // During spike
        assert_eq!(pattern.rate_at(Duration::from_secs(10)), 10000.0);
        assert_eq!(pattern.rate_at(Duration::from_secs(12)), 10000.0);
        assert_eq!(pattern.rate_at(Duration::from_secs(14)), 10000.0);

        // After spike
        assert_eq!(pattern.rate_at(Duration::from_secs(15)), 1000.0);
        assert_eq!(pattern.rate_at(Duration::from_secs(20)), 1000.0);

        assert_eq!(pattern.name(), "Spike");
    }

    #[test]
    fn test_sinusoidal_pattern() {
        let pattern = SinusoidalPattern::new(
            5000.0,                  // base rate
            3000.0,                  // amplitude
            Duration::from_secs(24), // 24 second period
        );

        // At t=0 (sin(0) = 0)
        let rate_t0 = pattern.rate_at(Duration::ZERO);
        assert!((rate_t0 - 5000.0).abs() < 1.0, "At t=0, rate should be ~5000, got {}", rate_t0);

        // At t=6s (quarter period, sin(π/2) = 1)
        let rate_t6 = pattern.rate_at(Duration::from_secs(6));
        assert!(
            (rate_t6 - 8000.0).abs() < 10.0,
            "At t=6s, rate should be ~8000, got {}",
            rate_t6
        );

        // At t=12s (half period, sin(π) = 0)
        let rate_t12 = pattern.rate_at(Duration::from_secs(12));
        assert!(
            (rate_t12 - 5000.0).abs() < 1.0,
            "At t=12s, rate should be ~5000, got {}",
            rate_t12
        );

        // At t=18s (three-quarter period, sin(3π/2) = -1)
        let rate_t18 = pattern.rate_at(Duration::from_secs(18));
        assert!(
            (rate_t18 - 2000.0).abs() < 10.0,
            "At t=18s, rate should be ~2000, got {}",
            rate_t18
        );

        assert_eq!(pattern.name(), "Sinusoidal");
    }

    #[test]
    fn test_sinusoidal_with_phase_shift() {
        // Start at peak (π/2 phase shift)
        let pattern = SinusoidalPattern::new(5000.0, 3000.0, Duration::from_secs(24))
            .with_phase_shift(Duration::from_secs(6)); // quarter period shift

        // At t=0, should be at peak
        let rate = pattern.rate_at(Duration::ZERO);
        assert!(
            (rate - 8000.0).abs() < 10.0,
            "With phase shift, t=0 should be peak, got {}",
            rate
        );
    }

    #[test]
    fn test_step_pattern() {
        let pattern = StepPattern::new(vec![
            (Duration::from_secs(0), 1000.0),
            (Duration::from_secs(10), 5000.0),
            (Duration::from_secs(20), 10000.0),
        ]);

        // At t=0-9s
        assert_eq!(pattern.rate_at(Duration::from_secs(0)), 1000.0);
        assert_eq!(pattern.rate_at(Duration::from_secs(5)), 1000.0);
        assert_eq!(pattern.rate_at(Duration::from_secs(9)), 1000.0);

        // At t=10-19s
        assert_eq!(pattern.rate_at(Duration::from_secs(10)), 5000.0);
        assert_eq!(pattern.rate_at(Duration::from_secs(15)), 5000.0);
        assert_eq!(pattern.rate_at(Duration::from_secs(19)), 5000.0);

        // At t=20s+
        assert_eq!(pattern.rate_at(Duration::from_secs(20)), 10000.0);
        assert_eq!(pattern.rate_at(Duration::from_secs(30)), 10000.0);

        assert_eq!(pattern.name(), "Step");
    }

    #[test]
    fn test_sawtooth_pattern() {
        let pattern = SawtoothPattern::new(1000.0, 10000.0, Duration::from_secs(10));

        // At start of first cycle
        assert_eq!(pattern.rate_at(Duration::ZERO), 1000.0);

        // At middle of first cycle
        let mid_rate = pattern.rate_at(Duration::from_secs(5));
        assert!((mid_rate - 5500.0).abs() < 10.0, "Mid rate should be ~5500, got {}", mid_rate);

        // At start of second cycle (should reset)
        let reset_rate = pattern.rate_at(Duration::from_secs(10));
        assert!(
            (reset_rate - 1000.0).abs() < 10.0,
            "At start of second cycle, should reset to 1000, got {}",
            reset_rate
        );

        // At middle of second cycle
        let mid_rate2 = pattern.rate_at(Duration::from_secs(15));
        assert!(
            (mid_rate2 - 5500.0).abs() < 10.0,
            "Mid of second cycle should be ~5500, got {}",
            mid_rate2
        );

        assert_eq!(pattern.name(), "Sawtooth");
    }

    #[test]
    fn test_load_pattern_trait() {
        let constant: Box<dyn LoadPattern> = Box::new(ConstantPattern::new(1000.0));
        let ramp: Box<dyn LoadPattern> =
            Box::new(RampPattern::new(100.0, 1000.0, Duration::from_secs(10)));
        let spike: Box<dyn LoadPattern> = Box::new(SpikePattern::new(
            1000.0,
            5000.0,
            Duration::from_secs(5),
            Duration::from_secs(2),
        ));

        // All should implement the trait
        assert_eq!(constant.name(), "Constant");
        assert_eq!(ramp.name(), "Ramp");
        assert_eq!(spike.name(), "Spike");

        // All should be able to compute rate
        let _ = constant.rate_at(Duration::from_secs(5));
        let _ = ramp.rate_at(Duration::from_secs(5));
        let _ = spike.rate_at(Duration::from_secs(5));
    }
}
