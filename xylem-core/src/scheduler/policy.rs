//! Per-Connection Traffic Policies
//!
//! Policies control WHEN a connection should send its next request.
//! Each connection gets its own policy instance, enabling truly independent traffic models.
//!
//! This is the renamed and redesigned version of the former "TimingPolicy" system.
//! Key changes:
//! - Renamed from `TimingPolicy` to `Policy` (more general)
//! - Designed for per-connection use (not global)
//! - Simpler API: `next_send_time()` instead of complex `delay_until_next()`
//! - Uses shared distributions from `workload::distributions`

use std::time::Duration;
use xylem_common::ExponentialDistribution;

/// Policy trait for per-connection traffic models
///
/// Each connection has its own Policy instance that determines when it should send
/// its next request. This enables fully independent traffic on each connection.
///
/// Think of this like a coroutine in an async runtime - each connection is independent,
/// and the scheduler just picks whichever connection's next request is ready.
pub trait Policy: Send {
    /// Get the absolute time (in nanoseconds) when this connection should next wake
    ///
    /// # Parameters
    /// - `current_time_ns`: Current time in nanoseconds (from `timing::time_ns()`)
    ///
    /// # Returns
    /// - `Some(time_ns)`: Wake at this absolute time, then check `ready_to_send`.
    /// - `None`: Send immediately (closed-loop, always ready)
    fn next_send_time(&mut self, current_time_ns: u64) -> Option<u64>;

    /// Check whether a scheduled wakeup permits sending. Dynamic policies may
    /// wake up just to refresh their rate; the pool reschedules them when false.
    /// After returning false, `next_send_time` must return a future wakeup.
    /// The default preserves the already sampled deadline of fixed/Poisson policies.
    fn ready_to_send(&mut self, _current_time_ns: u64) -> bool {
        true
    }

    /// Feedback from a completed measurement response. Warmup responses are excluded.
    fn on_response(&mut self, _latency: Duration) {}

    /// Notify policy that a request was sent on this connection
    ///
    /// # Parameters
    /// - `sent_time_ns`: When the request was sent (nanoseconds)
    fn on_request_sent(&mut self, sent_time_ns: u64);

    /// Reset policy state
    fn reset(&mut self);

    /// Get policy name for debugging
    fn name(&self) -> &'static str;
}

/// Closed-loop policy (max throughput, send as fast as possible)
///
/// Always returns `None` (send immediately when connection is available).
/// This creates a closed-loop system where the connection's pending limit
/// naturally regulates the rate.
pub struct ClosedLoopPolicy;

impl ClosedLoopPolicy {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClosedLoopPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl Policy for ClosedLoopPolicy {
    fn next_send_time(&mut self, _current_time_ns: u64) -> Option<u64> {
        None // Always ready to send
    }

    fn on_request_sent(&mut self, _sent_time_ns: u64) {
        // No state to track
    }

    fn reset(&mut self) {
        // No state to reset
    }

    fn name(&self) -> &'static str {
        "ClosedLoop"
    }
}

/// Fixed-rate policy (constant inter-arrival time)
///
/// Sends requests at a fixed rate (requests per second) with deterministic spacing.
/// Each request is exactly `1/rate` seconds after the previous one.
pub struct FixedRatePolicy {
    rate: f64,
    inter_arrival_ns: u64,
    last_send_time_ns: Option<u64>,
}

impl FixedRatePolicy {
    /// Create a new fixed-rate policy
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

    /// Get the current rate
    pub fn rate(&self) -> f64 {
        self.rate
    }
}

impl Policy for FixedRatePolicy {
    fn next_send_time(&mut self, _current_time_ns: u64) -> Option<u64> {
        match self.last_send_time_ns {
            Some(last_time) => Some(last_time + self.inter_arrival_ns),
            None => Some(0), // First request: send immediately (time 0 is always in past)
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

/// Poisson policy (exponentially distributed inter-arrivals)
///
/// Models realistic bursty traffic using exponential distribution.
/// Creates a Poisson process with the given rate parameter (lambda).
pub struct PoissonPolicy {
    dist: ExponentialDistribution,
    last_send_time_ns: Option<u64>,
}

impl PoissonPolicy {
    /// Create a new Poisson policy with entropy-based seed
    ///
    /// # Parameters
    /// - `lambda`: Average arrival rate in requests per second
    ///
    /// # Returns
    /// Error if lambda <= 0
    pub fn new(lambda: f64) -> anyhow::Result<Self> {
        Self::with_seed(lambda, None)
    }

    /// Create a new Poisson policy with explicit seed
    ///
    /// # Parameters
    /// - `lambda`: Average arrival rate in requests per second
    /// - `seed`: Optional seed for reproducibility (None = use entropy)
    ///
    /// # Returns
    /// Error if lambda <= 0
    pub fn with_seed(lambda: f64, seed: Option<u64>) -> anyhow::Result<Self> {
        let dist = ExponentialDistribution::with_seed(lambda, seed)?;
        Ok(Self { dist, last_send_time_ns: None })
    }

    /// Get the current lambda (rate parameter)
    pub fn lambda(&self) -> f64 {
        self.dist.lambda()
    }
}

impl Policy for PoissonPolicy {
    fn next_send_time(&mut self, _current_time_ns: u64) -> Option<u64> {
        match self.last_send_time_ns {
            Some(last_time) => {
                // Sample inter-arrival time from exponential distribution
                let inter_arrival_secs = self.dist.sample_inter_arrival();
                let inter_arrival_ns = (inter_arrival_secs * 1_000_000_000.0) as u64;
                Some(last_time + inter_arrival_ns)
            }
            None => Some(0), // First request: send immediately
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

/// Adaptive policy (adjusts rate based on latency feedback)
///
/// Dynamically adjusts the request rate based on observed latency:
/// - If latency > target: reduce rate (back off)
/// - If latency < target: increase rate (push harder)
pub struct AdaptivePolicy {
    target_latency: Duration,
    current_rate: f64,
    min_rate: f64,
    max_rate: f64,
    adjustment_factor: f64,
    last_send_time_ns: Option<u64>,
    sample_count: usize,
    latency_sum: Duration,
}

impl AdaptivePolicy {
    /// Create a new adaptive policy
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

    /// Get the current rate
    pub fn current_rate(&self) -> f64 {
        self.current_rate
    }

    /// Get the target latency
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
    /// Call this after receiving a response on this connection
    pub fn add_latency_sample(&mut self, latency: Duration) {
        self.sample_count += 1;
        self.latency_sum += latency;
    }
}

impl Policy for AdaptivePolicy {
    fn on_response(&mut self, latency: Duration) {
        self.add_latency_sample(latency);
    }

    fn next_send_time(&mut self, _current_time_ns: u64) -> Option<u64> {
        // Check if we should adjust rate (every 100 requests)
        if self.sample_count >= 100 {
            let avg_latency = self.latency_sum / self.sample_count as u32;
            self.adjust_rate(avg_latency);
            self.sample_count = 0;
            self.latency_sum = Duration::ZERO;
        }

        let inter_arrival_ns = (1_000_000_000.0 / self.current_rate) as u64;

        match self.last_send_time_ns {
            Some(last_time) => Some(last_time + inter_arrival_ns),
            None => Some(0), // First request: send immediately
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

/// PolicyScheduler trait for assigning policies to connections
///
/// This trait provides a strategy for assigning traffic policies to connections.
/// Think of it as a factory pattern for policies - each time a new connection is created,
/// the scheduler asks the PolicyScheduler "what policy should this connection use?"
///
/// Different implementations allow different assignment strategies:
/// - Uniform: All connections use the same policy type (e.g., all Poisson at 1000 req/s)
/// - Pattern: All connections follow a time-varying `LoadPattern`
pub trait PolicyScheduler: Send {
    /// Assign a policy to a newly created connection
    ///
    /// # Parameters
    /// - `conn_id`: Connection ID (0-indexed)
    ///
    /// # Returns
    /// A boxed Policy instance for this connection
    fn assign_policy(&mut self, conn_id: usize) -> Box<dyn Policy>;

    /// Get the scheduler name for debugging
    fn name(&self) -> &'static str;
}

/// Uniform policy scheduler (all connections use the same policy)
///
/// Creates a clone/copy of the same policy for every connection.
/// Useful when you want identical traffic patterns on all connections.
///
/// # Example
/// ```
/// use xylem_core::scheduler::{UniformPolicyScheduler, FixedRatePolicy};
/// // All connections will get FixedRate at 1000 req/s
/// let scheduler = UniformPolicyScheduler::fixed_rate(1000.0);
/// // Or use the general constructor with a closure:
/// let scheduler2 = UniformPolicyScheduler::new(|| Box::new(FixedRatePolicy::new(1000.0)));
/// ```
pub struct UniformPolicyScheduler {
    /// Policy factory function
    factory: Box<dyn Fn() -> Box<dyn Policy> + Send>,
}

impl UniformPolicyScheduler {
    /// Create a uniform scheduler with a factory function
    ///
    /// # Parameters
    /// - `factory`: Function that creates a new policy instance
    pub fn new<F>(factory: F) -> Self
    where
        F: Fn() -> Box<dyn Policy> + Send + 'static,
    {
        Self { factory: Box::new(factory) }
    }

    /// Create a uniform scheduler for closed-loop policy
    pub fn closed_loop() -> Self {
        Self::new(|| Box::new(ClosedLoopPolicy::new()))
    }

    /// Create a uniform scheduler for fixed-rate policy
    pub fn fixed_rate(rate: f64) -> Self {
        Self::new(move || Box::new(FixedRatePolicy::new(rate)))
    }

    /// Create a uniform scheduler for Poisson policy
    pub fn poisson(lambda: f64) -> anyhow::Result<Self> {
        // Validate lambda immediately
        let _ = PoissonPolicy::new(lambda)?;
        Ok(Self::new(move || {
            Box::new(PoissonPolicy::new(lambda).expect("Lambda already validated"))
        }))
    }

    /// Create a uniform scheduler for adaptive policy
    pub fn adaptive(
        initial_rate: f64,
        target_latency: Duration,
        min_rate: f64,
        max_rate: f64,
    ) -> Self {
        Self::new(move || {
            Box::new(AdaptivePolicy::new(initial_rate, target_latency, min_rate, max_rate))
        })
    }
}

impl PolicyScheduler for UniformPolicyScheduler {
    fn assign_policy(&mut self, _conn_id: usize) -> Box<dyn Policy> {
        (self.factory)()
    }

    fn name(&self) -> &'static str {
        "Uniform"
    }
}

/// Pattern-aware policy that dynamically adjusts rate based on elapsed time
///
/// This policy wraps a `LoadPattern` and updates its target rate on each
/// `next_send_time` call based on the pattern's rate at the current elapsed time.
/// This enables truly time-varying workloads like sinusoidal, ramp, spike patterns.
pub struct PatternAwarePolicy {
    pattern: std::sync::Arc<dyn crate::workload::LoadPattern>,
    start_time_ns: u64,
    /// None means paused, not an infinite interval to add to a timestamp.
    inter_arrival_ns: Option<u64>,
    last_send_time_ns: Option<u64>,
    last_rate_update_ns: u64,
}

const PATTERN_REFRESH_NS: u64 = 10_000_000;

impl PatternAwarePolicy {
    /// Create a policy whose pattern starts at the given absolute time.
    pub fn new(
        pattern: std::sync::Arc<dyn crate::workload::LoadPattern>,
        start_time_ns: u64,
    ) -> Self {
        let inter_arrival_ns = Self::interval(pattern.rate_at(Duration::ZERO));
        Self {
            pattern,
            start_time_ns,
            inter_arrival_ns,
            last_send_time_ns: None,
            last_rate_update_ns: start_time_ns,
        }
    }

    fn interval(rate: f64) -> Option<u64> {
        (rate > 0.0).then(|| (1_000_000_000.0 / rate) as u64)
    }

    fn update_rate(&mut self, current_time_ns: u64) {
        if current_time_ns.saturating_sub(self.last_rate_update_ns) < PATTERN_REFRESH_NS {
            return;
        }
        let elapsed = Duration::from_nanos(current_time_ns.saturating_sub(self.start_time_ns));
        self.inter_arrival_ns = Self::interval(self.pattern.rate_at(elapsed));
        self.last_rate_update_ns = current_time_ns;
    }

    fn send_deadline(&self) -> Option<u64> {
        self.inter_arrival_ns
            .map(|interval| self.last_send_time_ns.map_or(0, |last| last.saturating_add(interval)))
    }
}

impl Policy for PatternAwarePolicy {
    fn next_send_time(&mut self, current_time_ns: u64) -> Option<u64> {
        self.update_rate(current_time_ns);
        let refresh = self.last_rate_update_ns.saturating_add(PATTERN_REFRESH_NS);
        // A refresh wakeup is not permission to send, including during idle phases.
        Some(self.send_deadline().map_or(refresh, |send| send.min(refresh)))
    }

    fn ready_to_send(&mut self, current_time_ns: u64) -> bool {
        self.update_rate(current_time_ns);
        self.send_deadline().is_some_and(|send| send <= current_time_ns)
    }

    fn on_request_sent(&mut self, sent_time_ns: u64) {
        self.last_send_time_ns = Some(sent_time_ns);
    }

    fn reset(&mut self) {
        self.last_send_time_ns = None;
        self.last_rate_update_ns = self.start_time_ns;
        self.inter_arrival_ns = Self::interval(self.pattern.rate_at(Duration::ZERO));
    }

    fn name(&self) -> &'static str {
        "PatternAware"
    }
}

/// Pattern-based policy scheduler for time-varying traffic
///
/// Uses a `LoadPattern` to determine the target rate at each point in time.
/// Each connection gets a `PatternAwarePolicy` that dynamically updates its
/// rate based on elapsed time since the experiment started.
///
/// This enables realistic time-varying workloads like:
/// - Sinusoidal (diurnal traffic patterns)
/// - Ramp (gradual increase/decrease)
/// - Spike (sudden bursts)
/// - Sawtooth (repeated ramps)
pub struct PatternPolicyScheduler {
    pattern: std::sync::Arc<dyn crate::workload::LoadPattern>,
    start_time_ns: Option<u64>,
}

impl PatternPolicyScheduler {
    /// Create a new pattern-based scheduler
    ///
    /// # Parameters
    /// - `pattern`: The load pattern that determines rate over time
    pub fn new(pattern: Box<dyn crate::workload::LoadPattern>) -> Self {
        Self {
            pattern: std::sync::Arc::from(pattern),
            start_time_ns: None,
        }
    }
}

impl PolicyScheduler for PatternPolicyScheduler {
    fn assign_policy(&mut self, _conn_id: usize) -> Box<dyn Policy> {
        // Initialize start time on first policy assignment
        let start_time = *self.start_time_ns.get_or_insert_with(crate::timing::time_ns);

        // Create a pattern-aware policy that dynamically updates rate
        Box::new(PatternAwarePolicy::new(self.pattern.clone(), start_time))
    }

    fn name(&self) -> &'static str {
        "Pattern"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_closed_loop_policy() {
        let mut policy = ClosedLoopPolicy::new();

        // Always returns None (always ready)
        assert_eq!(policy.next_send_time(1000), None);
        assert_eq!(policy.next_send_time(2000), None);

        policy.on_request_sent(1000);
        assert_eq!(policy.next_send_time(3000), None);
    }

    #[test]
    fn test_fixed_rate_policy() {
        let mut policy = FixedRatePolicy::new(1000.0); // 1000 req/s = 1ms per request

        // First request: time 0 (send immediately)
        let next = policy.next_send_time(100_000);
        assert_eq!(next, Some(0));

        policy.on_request_sent(100_000); // Sent at t=100μs

        // Second request: should be at t=100μs + 1000μs = 1100μs
        let next2 = policy.next_send_time(200_000);
        assert_eq!(next2, Some(100_000 + 1_000_000)); // +1ms in nanoseconds

        assert_eq!(policy.name(), "FixedRate");
    }

    #[test]
    fn test_poisson_policy() {
        let mut policy = PoissonPolicy::new(1000.0).expect("Failed to create Poisson");

        // First request
        let next = policy.next_send_time(0);
        assert_eq!(next, Some(0));

        policy.on_request_sent(0);

        // Second request: should have some delay (random)
        let next2 = policy.next_send_time(0);
        assert!(next2.is_some());
        assert!(next2.unwrap() > 0);

        assert_eq!(policy.name(), "Poisson");
    }

    #[test]
    fn test_adaptive_policy() {
        let target_latency = Duration::from_millis(10);
        let mut policy = AdaptivePolicy::new(1000.0, target_latency, 100.0, 10000.0);

        let initial_rate = policy.current_rate();
        assert_eq!(initial_rate, 1000.0);

        // First request
        let next = policy.next_send_time(0);
        assert_eq!(next, Some(0));

        policy.on_request_sent(0);

        // Simulate high latency feedback (above target)
        for _ in 0..100 {
            policy.add_latency_sample(Duration::from_millis(20)); // 2x target
        }

        // Next call to next_send_time() triggers adjustment
        policy.next_send_time(1_000_000_000);

        // Rate should have decreased
        assert!(policy.current_rate() < initial_rate);

        assert_eq!(policy.name(), "Adaptive");
    }

    #[test]
    fn test_adaptive_policy_rate_bounds() {
        let target_latency = Duration::from_millis(10);
        let mut policy = AdaptivePolicy::new(1000.0, target_latency, 500.0, 2000.0);

        // Simulate very high latency (should clamp to min_rate)
        for _ in 0..1000 {
            policy.add_latency_sample(Duration::from_secs(1));
            policy.next_send_time(0);
        }

        assert!(policy.current_rate() >= 500.0);
        assert!(policy.current_rate() <= 2000.0);
    }

    #[test]
    fn test_policy_trait() {
        let mut closed_loop: Box<dyn Policy> = Box::new(ClosedLoopPolicy::new());
        let mut fixed_rate: Box<dyn Policy> = Box::new(FixedRatePolicy::new(1000.0));
        let mut poisson: Box<dyn Policy> = Box::new(PoissonPolicy::new(1000.0).unwrap());

        // All should implement the trait
        assert_eq!(closed_loop.name(), "ClosedLoop");
        assert_eq!(fixed_rate.name(), "FixedRate");
        assert_eq!(poisson.name(), "Poisson");

        // All should be able to compute next send time
        let _ = closed_loop.next_send_time(0);
        let _ = fixed_rate.next_send_time(0);
        let _ = poisson.next_send_time(0);
    }

    #[test]
    fn test_uniform_policy_scheduler() {
        let mut scheduler = UniformPolicyScheduler::fixed_rate(1000.0);

        // All connections should get the same policy type
        let policy0 = scheduler.assign_policy(0);
        let policy1 = scheduler.assign_policy(1);
        let policy2 = scheduler.assign_policy(2);

        assert_eq!(policy0.name(), "FixedRate");
        assert_eq!(policy1.name(), "FixedRate");
        assert_eq!(policy2.name(), "FixedRate");
        assert_eq!(scheduler.name(), "Uniform");
    }

    #[test]
    fn test_uniform_policy_scheduler_closed_loop() {
        let mut scheduler = UniformPolicyScheduler::closed_loop();

        let policy0 = scheduler.assign_policy(0);
        let policy1 = scheduler.assign_policy(1);

        assert_eq!(policy0.name(), "ClosedLoop");
        assert_eq!(policy1.name(), "ClosedLoop");
    }

    #[test]
    fn test_uniform_policy_scheduler_poisson() {
        let mut scheduler = UniformPolicyScheduler::poisson(1000.0).expect("Failed to create");

        let policy0 = scheduler.assign_policy(0);
        let policy1 = scheduler.assign_policy(1);

        assert_eq!(policy0.name(), "Poisson");
        assert_eq!(policy1.name(), "Poisson");
    }

    #[test]
    fn test_uniform_policy_scheduler_adaptive() {
        let mut scheduler =
            UniformPolicyScheduler::adaptive(1000.0, Duration::from_millis(10), 100.0, 10000.0);

        let policy0 = scheduler.assign_policy(0);
        let policy1 = scheduler.assign_policy(1);

        assert_eq!(policy0.name(), "Adaptive");
        assert_eq!(policy1.name(), "Adaptive");
    }

    #[test]
    fn test_policy_scheduler_trait() {
        let mut uniform: Box<dyn PolicyScheduler> =
            Box::new(UniformPolicyScheduler::fixed_rate(1000.0));

        assert_eq!(uniform.name(), "Uniform");

        // Should be able to assign policies through the trait object
        let _ = uniform.assign_policy(0);
    }
}
