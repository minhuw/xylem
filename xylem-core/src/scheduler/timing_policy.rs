//! Timing policy implementations
//!
//! Timing policies control WHEN requests should be sent, implementing different
//! inter-arrival time distributions and rate control strategies.

use super::TimingContext;
use std::time::Duration;

// Import the trait from parent module
use super::TimingPolicy;

/// Closed-loop timing policy (no rate limiting, send when ready)
///
/// This policy always allows sending immediately when a connection is available.
/// It's used for maximum throughput workloads where the system naturally
/// regulates itself through connection availability.
pub struct ClosedLoopTiming;

impl ClosedLoopTiming {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClosedLoopTiming {
    fn default() -> Self {
        Self::new()
    }
}

impl TimingPolicy for ClosedLoopTiming {
    fn delay_until_next(&mut self, _context: &TimingContext) -> Option<Duration> {
        None // No delay, send immediately when connection available
    }

    fn name(&self) -> &'static str {
        "ClosedLoop"
    }
}

/// Fixed-rate timing policy (open-loop with constant rate)
///
/// Sends requests at a fixed rate (requests per second), regardless of
/// connection availability. This creates an open-loop system.
pub struct FixedRateTiming {
    rate: f64,
    inter_arrival_ns: u64,
    last_send_time_ns: Option<u64>,
}

impl FixedRateTiming {
    /// Create a new fixed-rate timing policy
    ///
    /// # Parameters
    /// - `rate`: Target rate in requests per second
    pub fn new(rate: f64) -> Self {
        let inter_arrival_ns = (1_000_000_000.0 / rate) as u64;
        Self {
            rate,
            inter_arrival_ns,
            last_send_time_ns: None,
        }
    }

    pub fn rate(&self) -> f64 {
        self.rate
    }
}

impl TimingPolicy for FixedRateTiming {
    fn delay_until_next(&mut self, context: &TimingContext) -> Option<Duration> {
        if let Some(last_time) = self.last_send_time_ns {
            let elapsed_ns = context.current_time_ns.saturating_sub(last_time);

            if elapsed_ns < self.inter_arrival_ns {
                Some(Duration::from_nanos(self.inter_arrival_ns - elapsed_ns))
            } else {
                Some(Duration::ZERO)
            }
        } else {
            Some(Duration::ZERO)
        }
    }

    fn on_request_sent(&mut self, sent_time_ns: u64) {
        self.last_send_time_ns = Some(sent_time_ns);
    }

    fn reset(&mut self) {
        self.last_send_time_ns = None;
    }

    fn name(&self) -> &'static str {
        "FixedRate"
    }
}

/// Poisson timing policy (exponentially distributed inter-arrivals)
///
/// Models realistic random arrivals using an exponential distribution.
/// This creates a Poisson process with the given rate parameter (lambda).
pub struct PoissonTiming {
    lambda: f64, // Rate parameter (arrivals per second)
    rng: rand::rngs::SmallRng,
    dist: rand_distr::Exp<f64>,
    last_send_time_ns: Option<u64>,
}

impl PoissonTiming {
    /// Create a new Poisson timing policy
    ///
    /// # Parameters
    /// - `lambda`: Average arrival rate in requests per second
    ///
    /// # Returns
    /// Error if lambda <= 0
    pub fn new(lambda: f64) -> anyhow::Result<Self> {
        use rand::SeedableRng;

        if lambda <= 0.0 {
            anyhow::bail!("Lambda must be > 0");
        }

        let rng = rand::rngs::SmallRng::from_entropy();
        let dist = rand_distr::Exp::new(lambda)?;

        Ok(Self {
            lambda,
            rng,
            dist,
            last_send_time_ns: None,
        })
    }

    pub fn lambda(&self) -> f64 {
        self.lambda
    }
}

impl TimingPolicy for PoissonTiming {
    fn delay_until_next(&mut self, context: &TimingContext) -> Option<Duration> {
        use rand_distr::Distribution;

        if let Some(last_time) = self.last_send_time_ns {
            // Sample from exponential distribution
            let inter_arrival_secs = self.dist.sample(&mut self.rng);
            let inter_arrival_ns = (inter_arrival_secs * 1_000_000_000.0) as u64;

            let elapsed_ns = context.current_time_ns.saturating_sub(last_time);

            if elapsed_ns < inter_arrival_ns {
                Some(Duration::from_nanos(inter_arrival_ns - elapsed_ns))
            } else {
                Some(Duration::ZERO)
            }
        } else {
            Some(Duration::ZERO)
        }
    }

    fn on_request_sent(&mut self, sent_time_ns: u64) {
        self.last_send_time_ns = Some(sent_time_ns);
    }

    fn reset(&mut self) {
        self.last_send_time_ns = None;
    }

    fn name(&self) -> &'static str {
        "Poisson"
    }
}

/// Adaptive timing policy (adjusts rate based on latency feedback)
///
/// Dynamically adjusts the request rate based on observed latency:
/// - If latency > target: reduce rate (back off)
/// - If latency < target: increase rate (push harder)
pub struct AdaptiveTiming {
    target_latency: Duration,
    current_rate: f64,
    min_rate: f64,
    max_rate: f64,
    adjustment_factor: f64,
    last_send_time_ns: Option<u64>,
    sample_count: usize,
    latency_sum: Duration,
}

impl AdaptiveTiming {
    /// Create a new adaptive timing policy
    ///
    /// # Parameters
    /// - `initial_rate`: Starting rate in requests per second
    /// - `target_latency`: Target average latency to maintain
    /// - `min_rate`: Minimum rate (won't go below this)
    /// - `max_rate`: Maximum rate (won't go above this)
    pub fn new(initial_rate: f64, target_latency: Duration, min_rate: f64, max_rate: f64) -> Self {
        Self {
            target_latency,
            current_rate: initial_rate,
            min_rate,
            max_rate,
            adjustment_factor: 0.1, // 10% adjustment per iteration
            last_send_time_ns: None,
            sample_count: 0,
            latency_sum: Duration::ZERO,
        }
    }

    pub fn current_rate(&self) -> f64 {
        self.current_rate
    }

    pub fn target_latency(&self) -> Duration {
        self.target_latency
    }

    fn adjust_rate(&mut self, avg_latency: Duration) {
        if avg_latency > self.target_latency {
            // Latency too high, reduce rate
            self.current_rate *= 1.0 - self.adjustment_factor;
        } else {
            // Latency within target, increase rate
            self.current_rate *= 1.0 + self.adjustment_factor;
        }

        self.current_rate = self.current_rate.clamp(self.min_rate, self.max_rate);
    }

    /// Feed latency sample for rate adjustment
    ///
    /// Call this from the worker when a response is received
    pub fn add_latency_sample(&mut self, latency: Duration) {
        self.sample_count += 1;
        self.latency_sum += latency;
    }
}

impl TimingPolicy for AdaptiveTiming {
    fn delay_until_next(&mut self, context: &TimingContext) -> Option<Duration> {
        // Check if we should adjust rate (every 100 requests)
        if self.sample_count >= 100 {
            let avg_latency = self.latency_sum / self.sample_count as u32;
            self.adjust_rate(avg_latency);
            self.sample_count = 0;
            self.latency_sum = Duration::ZERO;
        }

        let inter_arrival_ns = (1_000_000_000.0 / self.current_rate) as u64;

        if let Some(last_time) = self.last_send_time_ns {
            let elapsed_ns = context.current_time_ns.saturating_sub(last_time);

            if elapsed_ns < inter_arrival_ns {
                Some(Duration::from_nanos(inter_arrival_ns - elapsed_ns))
            } else {
                Some(Duration::ZERO)
            }
        } else {
            Some(Duration::ZERO)
        }
    }

    fn on_request_sent(&mut self, sent_time_ns: u64) {
        self.last_send_time_ns = Some(sent_time_ns);
    }

    fn reset(&mut self) {
        self.last_send_time_ns = None;
        self.sample_count = 0;
        self.latency_sum = Duration::ZERO;
    }

    fn name(&self) -> &'static str {
        "Adaptive"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_closed_loop_timing() {
        let mut policy = ClosedLoopTiming::new();

        let ctx = TimingContext {
            time_since_last: Duration::ZERO,
            current_time_ns: 0,
            requests_sent: 0,
            global_in_flight: 0,
        };

        // Always returns None (no delay)
        assert_eq!(policy.delay_until_next(&ctx), None);
    }

    #[test]
    fn test_fixed_rate_timing() {
        let mut policy = FixedRateTiming::new(1000.0); // 1000 req/s = 1ms per request

        let ctx = TimingContext {
            time_since_last: Duration::ZERO,
            current_time_ns: 1_000_000_000, // 1 second
            requests_sent: 0,
            global_in_flight: 0,
        };

        // First request: immediate
        let delay = policy.delay_until_next(&ctx);
        assert_eq!(delay, Some(Duration::ZERO));

        policy.on_request_sent(1_000_000_000);

        // Second request: should wait ~1ms
        let ctx2 = TimingContext {
            time_since_last: Duration::from_micros(500),
            current_time_ns: 1_000_500_000, // 1.0005 seconds
            requests_sent: 1,
            global_in_flight: 1,
        };

        let delay2 = policy.delay_until_next(&ctx2);
        assert!(delay2.is_some());
        assert!(delay2.unwrap() > Duration::ZERO);
    }

    #[test]
    fn test_poisson_timing() {
        let mut policy = PoissonTiming::new(1000.0).expect("Failed to create Poisson timing");

        let ctx = TimingContext {
            time_since_last: Duration::ZERO,
            current_time_ns: 1_000_000_000,
            requests_sent: 0,
            global_in_flight: 0,
        };

        // First request: immediate
        let delay = policy.delay_until_next(&ctx);
        assert_eq!(delay, Some(Duration::ZERO));

        policy.on_request_sent(1_000_000_000);

        // Second request: should have some delay (exponentially distributed)
        let ctx2 = TimingContext {
            time_since_last: Duration::ZERO,
            current_time_ns: 1_001_000_000, // 1ms later
            requests_sent: 1,
            global_in_flight: 1,
        };

        let delay2 = policy.delay_until_next(&ctx2);
        assert!(delay2.is_some());
        // Delay is random, just check it's reasonable
    }

    #[test]
    fn test_adaptive_timing() {
        let target_latency = Duration::from_millis(10);
        let mut policy = AdaptiveTiming::new(1000.0, target_latency, 100.0, 10000.0);

        let initial_rate = policy.current_rate();
        assert_eq!(initial_rate, 1000.0);

        // Simulate high latency feedback
        for _ in 0..100 {
            policy.add_latency_sample(Duration::from_millis(20)); // 2x target
        }

        let ctx = TimingContext {
            time_since_last: Duration::ZERO,
            current_time_ns: 1_000_000_000,
            requests_sent: 100,
            global_in_flight: 5,
        };

        policy.delay_until_next(&ctx); // Triggers adjustment

        // Rate should decrease
        assert!(policy.current_rate() < initial_rate);
    }

    #[test]
    fn test_adaptive_timing_rate_bounds() {
        let target_latency = Duration::from_millis(10);
        let mut policy = AdaptiveTiming::new(1000.0, target_latency, 500.0, 2000.0);

        // Simulate very high latency (should clamp to min_rate)
        for _ in 0..1000 {
            policy.add_latency_sample(Duration::from_secs(1));
            let ctx = TimingContext {
                time_since_last: Duration::ZERO,
                current_time_ns: 1_000_000_000,
                requests_sent: 0,
                global_in_flight: 0,
            };
            policy.delay_until_next(&ctx);
        }

        assert!(policy.current_rate() >= 500.0);

        // Reset and simulate very low latency (should clamp to max_rate)
        policy.reset();
        for _ in 0..1000 {
            policy.add_latency_sample(Duration::from_micros(1));
            let ctx = TimingContext {
                time_since_last: Duration::ZERO,
                current_time_ns: 1_000_000_000,
                requests_sent: 0,
                global_in_flight: 0,
            };
            policy.delay_until_next(&ctx);
        }

        assert!(policy.current_rate() <= 2000.0);
    }
}
