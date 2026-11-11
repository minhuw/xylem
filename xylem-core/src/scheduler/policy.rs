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
//! - Uses shared distributions from `xylem_common`
//!
//! # Open-loop schedules
//!
//! Every rate-driven policy (fixed rate, Poisson, adaptive and load patterns)
//! follows an *intended schedule*: `intended[k + 1] = intended[k] + interarrival[k]`,
//! starting at the connection's start time. The schedule advances by intended
//! times, never by actual send times. When
//! the sender falls behind (full pipeline, slow loop, preemption, a server
//! stall), the next intended time is simply in the past: the overdue requests
//! go out back to back as soon as the connection can send, each carrying its
//! own intended time, and nothing is skipped. The worker adds each request's
//! send lag (actual minus intended send time) to its latency, so the delay of
//! requests that could not go out on time is not omitted (coordinated omission).
//!
//! The closed-loop policy has no schedule: its requests have no intended time.
//!
//! # Shared start and phases
//!
//! A worker anchors every schedule at one start time that all connections of
//! all threads share ([`Policy::anchor`]). The connections of a traffic group
//! are numbered `c = 0..N` across its threads and split its rate `R`. Fixed-rate
//! connection `c` sends its `k`-th request at
//! `start + ⌊(c + k·N)·10⁹/R⌋` ns, so the merged stream is evenly spaced at
//! `R`. Adaptive and pattern connections run at `R/N` each and start `c/N` of
//! a request into their schedule. A connection that can first send after the
//! start begins at its first intended time from then on. An unanchored
//! schedule starts at its first `next_send_time` call.

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
    /// The first call starts a rate-driven policy's intended schedule: at its
    /// first intended time at or after both the anchor and `current_time_ns`,
    /// or, without an anchor, at `current_time_ns`.
    ///
    /// # Parameters
    /// - `current_time_ns`: Current time in nanoseconds (from `timing::time_ns()`)
    ///
    /// # Returns
    /// - `Some(time_ns)`: Wake at this absolute time, then check `ready_to_send`.
    ///   It may be in the past when the connection is behind its schedule.
    /// - `None`: Send immediately (closed-loop, always ready)
    fn next_send_time(&mut self, current_time_ns: u64) -> Option<u64>;

    /// Check whether a scheduled wakeup permits sending. Dynamic policies may
    /// wake up just to refresh their rate; the pool reschedules them when false.
    /// After returning false, `next_send_time` must return a future wakeup.
    /// The default preserves the already sampled deadline of fixed/Poisson policies.
    fn ready_to_send(&mut self, _current_time_ns: u64) -> bool {
        true
    }

    /// Intended send time of the next request, for a connection whose wakeup
    /// permitted a send. `None` for policies without a schedule (closed loop).
    fn intended_send_time(&self) -> Option<u64> {
        None
    }

    /// Feedback from a completed measurement response, with its latency
    /// measured from the intended send time. Warmup responses and error
    /// replies are excluded.
    fn on_response(&mut self, _latency: Duration) {}

    /// Notify policy that a request was sent on this connection
    ///
    /// Rate-driven policies advance their intended schedule by one request;
    /// `sent_time_ns` only starts a schedule that `next_send_time` never started.
    ///
    /// # Parameters
    /// - `sent_time_ns`: When the request was sent (nanoseconds)
    fn on_request_sent(&mut self, sent_time_ns: u64);

    /// Count the intended sends from the next one up to, but excluding,
    /// `end_ns`, stopping at `cap`. Returns the count and whether the cap
    /// stopped the walk. Consumes the schedule, so call it only once the
    /// connection stops sending. Policies without a schedule return `(0, false)`.
    fn count_intended_before(&mut self, _end_ns: u64, _cap: u64) -> (u64, bool) {
        (0, false)
    }

    /// Anchor the intended schedule at `start_ns`, the start time that every
    /// connection of every thread shares, and restart it. Phases count from
    /// the anchor, and the schedule starts at the next `next_send_time` call.
    /// Policies without a schedule ignore it.
    fn anchor(&mut self, _start_ns: u64) {}

    /// Restart the intended schedule from the next `next_send_time` call,
    /// keeping any anchor
    fn reset(&mut self);

    /// Get policy name for debugging
    fn name(&self) -> &'static str;
}

/// Convert a rate in requests per second into nanoseconds between sends
///
/// Every rate-based policy uses this one rule. Positive rates truncate to whole
/// nanoseconds, and rates too small to represent saturate at `u64::MAX`.
///
/// Returns `None` unless `rate > 0`, so zero, negative, and NaN rates pause the
/// policy instead of producing an interval that overflows or is zero, either of
/// which would let the connection send unthrottled. A paused policy never permits
/// a send, not even the first request:
/// - `PatternAwarePolicy` keeps waking at its refresh cadence, because its pattern
///   can raise the rate again.
/// - `FixedRatePolicy` and `AdaptivePolicy` have no rate to refresh, so their
///   `next_send_time` returns `u64::MAX`, a wakeup that never arrives. The
///   connection stays idle for the rest of the run.
fn rate_to_interval_ns(rate: f64) -> Option<u64> {
    (rate > 0.0).then(|| (1_000_000_000.0 / rate) as u64)
}

/// Intended schedule with a constant interval between consecutive requests
/// at any moment (the interval may change between requests)
#[derive(Debug, Clone, Copy, Default)]
struct IntervalSchedule {
    /// Start time shared by every connection, once anchored
    anchor_ns: Option<u64>,
    /// Start of the schedule: the intended time of the first request
    start_ns: Option<u64>,
    /// Intended time of the last request sent
    last_intended_ns: Option<u64>,
}

impl IntervalSchedule {
    /// Start at `offset_ns` after the anchor (or `current_time_ns` without
    /// one), or at the first time on that grid of `interval_ns` steps that is
    /// not before `current_time_ns`.
    fn start(&mut self, current_time_ns: u64, offset_ns: u64, interval_ns: u64) {
        if self.start_ns.is_some() {
            return;
        }
        let first = self.anchor_ns.unwrap_or(current_time_ns).saturating_add(offset_ns);
        let behind = current_time_ns.saturating_sub(first);
        let steps = if interval_ns == 0 {
            0
        } else {
            behind.div_ceil(interval_ns)
        };
        self.start_ns = Some(first.saturating_add(steps.saturating_mul(interval_ns)));
    }

    fn anchor(&mut self, start_ns: u64) {
        *self = Self {
            anchor_ns: Some(start_ns),
            ..Self::default()
        };
    }

    fn reset(&mut self) {
        *self = Self {
            anchor_ns: self.anchor_ns,
            ..Self::default()
        };
    }

    /// Intended time of the next request: the start, then one interval after
    /// the last intended time, saturating so a huge interval cannot wrap.
    fn next(&self, interval_ns: u64) -> Option<u64> {
        match self.last_intended_ns {
            Some(last) => Some(last.saturating_add(interval_ns)),
            None => self.start_ns,
        }
    }

    fn advance(&mut self, interval_ns: u64, sent_time_ns: u64) {
        self.last_intended_ns = Some(self.next(interval_ns).unwrap_or(sent_time_ns));
    }

    fn count_before(&self, interval_ns: u64, end_ns: u64, cap: u64) -> (u64, bool) {
        let Some(next) = self.next(interval_ns) else {
            return (0, false);
        };
        if next >= end_ns {
            return (0, false);
        }
        let count = match (end_ns - next - 1).checked_div(interval_ns) {
            Some(steps) => steps.saturating_add(1),
            // A zero interval schedules every request at once.
            None => u64::MAX,
        };
        if count > cap {
            (cap, true)
        } else {
            (count, false)
        }
    }
}

/// Closed-loop policy (max throughput, send as fast as possible)
///
/// Always returns `None` (send immediately when connection is available).
/// This creates a closed-loop system where the connection's pending limit
/// naturally regulates the rate. Its requests have no intended send time.
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

const NANOS_PER_SEC: u128 = 1_000_000_000;

/// A positive, finite rate held exactly as `mantissa · 2^exponent` requests
/// per second, the value of its `f64`. Times derived from it are exact
/// functions of slot numbers, so a schedule never drifts.
#[derive(Debug, Clone, Copy)]
struct ExactRate {
    /// Odd, below 2^53
    mantissa: u64,
    exponent: i32,
}

impl ExactRate {
    /// `None` unless `rate` is positive and finite
    fn new(rate: f64) -> Option<Self> {
        if !(rate > 0.0 && rate.is_finite()) {
            return None;
        }
        let bits = rate.to_bits();
        let biased = ((bits >> 52) & 0x7ff) as i32;
        let fraction = bits & ((1 << 52) - 1);
        let (mantissa, exponent) = if biased == 0 {
            (fraction, -1074) // subnormal
        } else {
            (fraction | 1 << 52, biased - 1075)
        };
        let zeros = mantissa.trailing_zeros();
        Some(Self {
            mantissa: mantissa >> zeros,
            exponent: exponent + zeros as i32,
        })
    }

    /// `⌊slot · 10⁹ / rate⌋`: nanoseconds from a schedule's origin to slot
    /// number `slot`, saturating at `u64::MAX`
    fn offset_ns(self, slot: u64) -> u64 {
        let mantissa = self.mantissa as u128;
        // Below 2^94
        let scaled = slot as u128 * NANOS_PER_SEC;
        if self.exponent >= 0 {
            let quotient = (scaled / mantissa).checked_shr(self.exponent as u32).unwrap_or(0);
            return u64::try_from(quotient).unwrap_or(u64::MAX);
        }
        // ⌊scaled · 2^shift / mantissa⌋ by long division, at most 64 bits a
        // step, so the remainder (below the mantissa, so 2^53) never overflows
        let (mut quotient, mut remainder) = (scaled / mantissa, scaled % mantissa);
        let mut shift = self.exponent.unsigned_abs();
        while shift > 0 {
            let step = shift.min(64);
            if quotient > (u64::MAX as u128) >> step {
                return u64::MAX;
            }
            let wide = remainder << step;
            quotient = (quotient << step) | (wide / mantissa);
            remainder = wide % mantissa;
            shift -= step;
        }
        u64::try_from(quotient).unwrap_or(u64::MAX)
    }

    /// `⌈offset_ns · rate / 10⁹⌉`: the number of slots whose offset is below
    /// `offset_ns`, saturating at `u128::MAX`. Slot `j` is before `offset_ns`
    /// exactly when `j · 10⁹ / rate < offset_ns`, that is `j < offset_ns · rate / 10⁹`.
    fn slots_before(self, offset_ns: u64) -> u128 {
        if offset_ns == 0 {
            return 0;
        }
        // Below 2^117
        let product = offset_ns as u128 * self.mantissa as u128;
        let scaled = if self.exponent >= 0 {
            let shift = self.exponent as u32;
            if shift > product.leading_zeros() {
                return u128::MAX;
            }
            product << shift
        } else {
            // ⌈product / 2^shift⌉; ceilings of successive divisions compose
            let shift = self.exponent.unsigned_abs();
            if shift >= 128 {
                1
            } else {
                (product >> shift) + u128::from(product & ((1 << shift) - 1) != 0)
            }
        };
        scaled.div_ceil(NANOS_PER_SEC)
    }
}

/// Fixed-rate policy (constant inter-arrival time)
///
/// One connection of `connections` (N) that together send `rate` (R)
/// requests per second with deterministic, evenly interleaved spacing. The
/// slots of the merged stream are `origin + ⌊j · 10⁹ / R⌋` ns for `j = 0, 1, ...`;
/// connection `c` takes slots `c, c + N, c + 2N, ...`, so its own requests
/// are `N/R` seconds apart and no two connections share a slot. Every time is
/// computed from its slot number with `10⁹/R` held exactly, so the schedule
/// does not drift. The origin is the anchor, or the first `next_send_time`
/// call without one. [`FixedRatePolicy::new`] is a single connection (N = 1).
///
/// A rate that is not greater than zero never sends (see `rate_to_interval_ns`).
pub struct FixedRatePolicy {
    rate: f64,
    /// None means paused, not an infinite interval to add to a timestamp.
    exact: Option<ExactRate>,
    /// This connection's number `c` among the `connections` sharing the rate
    connection: u64,
    connections: u64,
    /// Start time shared by every connection, once anchored
    anchor_ns: Option<u64>,
    /// The slots' origin and the next request's number `k` among this
    /// connection's slots, once the schedule started
    started: Option<(u64, u64)>,
}

impl FixedRatePolicy {
    /// Create a new fixed-rate policy for a single connection
    ///
    /// # Parameters
    /// - `rate`: Target rate in requests per second
    pub fn new(rate: f64) -> Self {
        Self::striped(rate, 0, 1)
    }

    /// Connection `connection` of `connections` that together send `rate`
    /// requests per second in one evenly spaced stream
    pub fn striped(rate: f64, connection: usize, connections: usize) -> Self {
        let connections = connections.max(1);
        assert!(connection < connections, "connection {connection} of {connections}");
        Self {
            rate,
            exact: ExactRate::new(rate),
            connection: connection as u64,
            connections: connections as u64,
            anchor_ns: None,
            started: None,
        }
    }

    /// Get the rate the policy's connections share
    pub fn rate(&self) -> f64 {
        self.rate
    }

    /// Number of this connection's slots intended before `time_ns`
    fn slots_before(&self, rate: ExactRate, origin_ns: u64, time_ns: u64) -> u64 {
        let slots = rate.slots_before(time_ns.saturating_sub(origin_ns));
        let connection = self.connection as u128;
        if slots <= connection {
            return 0;
        }
        u64::try_from((slots - connection).div_ceil(self.connections as u128)).unwrap_or(u64::MAX)
    }

    /// Intended time of this connection's `k`-th slot
    fn slot_time(&self, rate: ExactRate, origin_ns: u64, k: u64) -> u64 {
        let slot = self.connection as u128 + k as u128 * self.connections as u128;
        match u64::try_from(slot) {
            Ok(slot) => origin_ns.saturating_add(rate.offset_ns(slot)),
            Err(_) => u64::MAX,
        }
    }

    /// Start the schedule at the first slot at or after both the origin and
    /// `current_time_ns`; returns the origin and the next slot number
    fn start(&mut self, rate: ExactRate, current_time_ns: u64) -> (u64, u64) {
        if let Some(started) = self.started {
            return started;
        }
        let origin_ns = self.anchor_ns.unwrap_or(current_time_ns);
        let next = self.slots_before(rate, origin_ns, current_time_ns.max(origin_ns));
        *self.started.insert((origin_ns, next))
    }
}

impl Policy for FixedRatePolicy {
    fn next_send_time(&mut self, current_time_ns: u64) -> Option<u64> {
        let Some(rate) = self.exact else {
            return Some(u64::MAX); // Paused: a wakeup that never arrives
        };
        let (origin_ns, next) = self.start(rate, current_time_ns);
        Some(self.slot_time(rate, origin_ns, next))
    }

    fn intended_send_time(&self) -> Option<u64> {
        let (origin_ns, next) = self.started?;
        Some(self.slot_time(self.exact?, origin_ns, next))
    }

    fn on_request_sent(&mut self, sent_time_ns: u64) {
        if let Some(rate) = self.exact {
            self.start(rate, sent_time_ns);
            if let Some((_, next)) = &mut self.started {
                *next = next.saturating_add(1);
            }
        }
    }

    /// Counts by formula; the schedule is left as it is.
    fn count_intended_before(&mut self, end_ns: u64, cap: u64) -> (u64, bool) {
        let (Some(rate), Some((origin_ns, next))) = (self.exact, self.started) else {
            return (0, false);
        };
        let count = self.slots_before(rate, origin_ns, end_ns).saturating_sub(next);
        if count > cap {
            (cap, true)
        } else {
            (count, false)
        }
    }

    fn anchor(&mut self, start_ns: u64) {
        self.anchor_ns = Some(start_ns);
        self.started = None;
    }

    fn reset(&mut self) {
        self.started = None;
    }

    fn name(&self) -> &'static str {
        "FixedRate"
    }
}

/// Poisson policy (exponentially distributed inter-arrivals)
///
/// Models realistic bursty traffic using exponential distribution.
/// Creates a Poisson process with the given rate parameter (lambda): each
/// intended send time is one sampled inter-arrival after the previous one,
/// the first one inter-arrival after the origin (the anchor, or the first
/// `next_send_time` call without one). A connection that can first send
/// later skips the arrivals before then, so with a seeded generator its
/// intended times do not depend on when it became ready.
pub struct PoissonPolicy {
    dist: ExponentialDistribution,
    /// Start time shared by every connection, once anchored
    anchor_ns: Option<u64>,
    /// Intended time of the next request, once the schedule has started
    next_intended_ns: Option<u64>,
}

/// Most arrivals a Poisson schedule that starts late skips one at a time
/// before it restarts, memorylessly, at the current time instead
const POISSON_SKIP_LIMIT: u32 = 1_000_000;

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
        Ok(Self {
            dist,
            anchor_ns: None,
            next_intended_ns: None,
        })
    }

    /// Get the current lambda (rate parameter)
    pub fn lambda(&self) -> f64 {
        self.dist.lambda()
    }

    /// Sample one inter-arrival time in nanoseconds
    fn sample_ns(&mut self) -> u64 {
        (self.dist.sample_inter_arrival() * 1_000_000_000.0) as u64
    }

    /// Start the schedule at its first arrival at or after `current_time_ns`
    fn start(&mut self, current_time_ns: u64) -> u64 {
        if let Some(next) = self.next_intended_ns {
            return next;
        }
        let mut next = self.anchor_ns.unwrap_or(current_time_ns).saturating_add(self.sample_ns());
        let mut skipped = 0;
        while next < current_time_ns {
            if skipped == POISSON_SKIP_LIMIT {
                next = current_time_ns.saturating_add(self.sample_ns());
                break;
            }
            next = next.saturating_add(self.sample_ns());
            skipped += 1;
        }
        *self.next_intended_ns.insert(next)
    }
}

impl Policy for PoissonPolicy {
    fn next_send_time(&mut self, current_time_ns: u64) -> Option<u64> {
        Some(self.start(current_time_ns))
    }

    fn intended_send_time(&self) -> Option<u64> {
        self.next_intended_ns
    }

    fn on_request_sent(&mut self, sent_time_ns: u64) {
        let intended = self.next_intended_ns.unwrap_or(sent_time_ns);
        self.next_intended_ns = Some(intended.saturating_add(self.sample_ns()));
    }

    /// Samples the remaining schedule forward with the policy's own generator.
    fn count_intended_before(&mut self, end_ns: u64, cap: u64) -> (u64, bool) {
        let Some(mut next) = self.next_intended_ns else {
            return (0, false);
        };
        let mut count = 0;
        while next < end_ns {
            if count == cap {
                return (count, true);
            }
            count += 1;
            next = next.saturating_add(self.sample_ns());
        }
        self.next_intended_ns = Some(next);
        (count, false)
    }

    fn anchor(&mut self, start_ns: u64) {
        self.anchor_ns = Some(start_ns);
        self.next_intended_ns = None;
    }

    fn reset(&mut self) {
        self.next_intended_ns = None;
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
///
/// The latency it receives is measured from each request's intended send
/// time, so a backlog of overdue requests counts as high latency and backs
/// the rate off. Each request is intended one interval at the current rate
/// after the previous one's intended time. The first is `c/N` of an interval
/// after the origin for connection `c` of `N` (see [`AdaptivePolicy::with_phase`]).
///
/// A current rate that is not greater than zero never sends (see `rate_to_interval_ns`).
pub struct AdaptivePolicy {
    target_latency: Duration,
    current_rate: f64,
    min_rate: f64,
    max_rate: f64,
    adjustment_factor: f64,
    schedule: IntervalSchedule,
    /// This connection's number and the number of connections, for its phase
    phase: (u64, u64),
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
            schedule: IntervalSchedule::default(),
            phase: (0, 1),
            sample_count: 0,
            latency_sum: Duration::ZERO,
        }
    }

    /// Start connection `connection` of `connections` `connection/connections`
    /// of an interval into the schedule, so the connections of a group
    /// interleave instead of sending together
    pub fn with_phase(mut self, connection: usize, connections: usize) -> Self {
        let connections = connections.max(1);
        assert!(connection < connections, "connection {connection} of {connections}");
        self.phase = (connection as u64, connections as u64);
        self
    }

    fn phase_ns(&self, interval_ns: u64) -> u64 {
        let (connection, connections) = self.phase;
        (interval_ns as u128 * connection as u128 / connections as u128) as u64
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

    fn next_send_time(&mut self, current_time_ns: u64) -> Option<u64> {
        // Check if we should adjust rate (every 100 requests)
        if self.sample_count >= 100 {
            let avg_latency = self.latency_sum / self.sample_count as u32;
            self.adjust_rate(avg_latency);
            self.sample_count = 0;
            self.latency_sum = Duration::ZERO;
        }

        match rate_to_interval_ns(self.current_rate) {
            Some(inter_arrival_ns) => {
                let phase_ns = self.phase_ns(inter_arrival_ns);
                self.schedule.start(current_time_ns, phase_ns, inter_arrival_ns);
                self.schedule.next(inter_arrival_ns)
            }
            None => Some(u64::MAX), // Paused: a wakeup that never arrives
        }
    }

    fn intended_send_time(&self) -> Option<u64> {
        self.schedule.next(rate_to_interval_ns(self.current_rate)?)
    }

    fn on_request_sent(&mut self, sent_time_ns: u64) {
        if let Some(inter_arrival_ns) = rate_to_interval_ns(self.current_rate) {
            self.schedule.advance(inter_arrival_ns, sent_time_ns);
        }
    }

    /// Counts at the current rate, which no longer changes once sending stops.
    fn count_intended_before(&mut self, end_ns: u64, cap: u64) -> (u64, bool) {
        match rate_to_interval_ns(self.current_rate) {
            Some(inter_arrival_ns) => self.schedule.count_before(inter_arrival_ns, end_ns, cap),
            None => (0, false),
        }
    }

    fn anchor(&mut self, start_ns: u64) {
        self.reset();
        self.schedule.anchor(start_ns);
    }

    fn reset(&mut self) {
        self.schedule.reset();
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
    /// - `conn_id`: Index of the connection among the connections this
    ///   scheduler serves (0-indexed); a pool counts each group's connections
    ///   from 0
    ///
    /// # Returns
    /// A boxed Policy instance for this connection
    fn assign_policy(&mut self, conn_id: usize) -> Box<dyn Policy>;

    /// The arrival process the scheduler's connections share under group
    /// dispatch (see [`super::dispatch`]), taken once when the pool is built;
    /// `None` (the default) gives each connection its own schedule
    fn dispatch_policy(&mut self) -> Option<Box<dyn Policy>> {
        None
    }

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

/// Policy scheduler for one thread's connections of a traffic group
///
/// A traffic group numbers its connections across all its threads; this
/// thread's are `first_connection`, `first_connection + 1`, ... Each policy
/// is built from its connection's group-wide number, so the connections can
/// share the group's rate: striped fixed-rate slots, phase offsets, and
/// per-connection Poisson seeds.
pub struct GroupPolicyScheduler {
    first_connection: usize,
    factory: Box<dyn Fn(usize) -> Box<dyn Policy> + Send>,
}

impl GroupPolicyScheduler {
    /// Build the policy of connection `conn_id` with `factory(first_connection + conn_id)`
    pub fn new<F>(first_connection: usize, factory: F) -> Self
    where
        F: Fn(usize) -> Box<dyn Policy> + Send + 'static,
    {
        Self {
            first_connection,
            factory: Box::new(factory),
        }
    }
}

impl PolicyScheduler for GroupPolicyScheduler {
    fn assign_policy(&mut self, conn_id: usize) -> Box<dyn Policy> {
        (self.factory)(self.first_connection + conn_id)
    }

    fn name(&self) -> &'static str {
        "Group"
    }
}

/// Pattern-aware policy that follows a time-varying rate
///
/// The pattern's rate is sampled at the start of every 10 ms step (aligned to
/// the pattern's start) and held for that step. The intended schedule is a
/// non-homogeneous process over that rate: the first request is intended once
/// the rate integrated from the schedule's origin reaches the connection's
/// phase (zero by default, so at the first moment the rate is positive; `c/N`
/// for connection `c` of `N`, see [`PatternAwarePolicy::with_phase`]), and
/// each later request once the rate integrated from the previous intended
/// time reaches one request. For a constant rate this is one `1/rate`
/// interval. The origin is the anchor, which also becomes the pattern's
/// start, or the first `next_send_time` call without one; a connection that
/// can first send later starts at its next intended time from then on.
/// Paused (zero-rate) steps contribute nothing, so a
/// pause schedules no backlog, while a short spike is observed as soon as it
/// accumulates a request even after a long low-rate gap. Intended times that
/// passed while the connection could not send still go out, like every
/// rate-driven policy.
///
/// This enables truly time-varying workloads like sinusoidal, ramp, spike patterns.
pub struct PatternAwarePolicy {
    pattern: std::sync::Arc<dyn crate::workload::LoadPattern>,
    start_time_ns: u64,
    /// Whether `start_time_ns` is the anchor, the origin of the schedule
    anchored: bool,
    /// Requests to integrate from the origin before the first intended time
    phase: f64,
    /// Time up to which the rate has been integrated; None before the
    /// schedule starts
    cursor_ns: Option<u64>,
    /// Requests still to integrate before the next intended time; zero for
    /// the first request, which is due as soon as the rate is positive
    credit_needed: f64,
    /// Intended time of the next request, once integration has found it
    next_intended_ns: Option<u64>,
}

/// Length of the steps over which a pattern's rate is held constant, and the
/// furthest ahead a pattern policy integrates its rate on each wakeup.
const PATTERN_REFRESH_NS: u64 = 10_000_000;

impl PatternAwarePolicy {
    /// Create a policy whose pattern starts at the given absolute time.
    pub fn new(
        pattern: std::sync::Arc<dyn crate::workload::LoadPattern>,
        start_time_ns: u64,
    ) -> Self {
        Self {
            pattern,
            start_time_ns,
            anchored: false,
            phase: 0.0,
            cursor_ns: None,
            credit_needed: 0.0,
            next_intended_ns: None,
        }
    }

    /// Start connection `connection` of `connections` `connection/connections`
    /// of a request into the schedule, so the connections of a group
    /// interleave instead of sending together
    pub fn with_phase(mut self, connection: usize, connections: usize) -> Self {
        let connections = connections.max(1);
        assert!(connection < connections, "connection {connection} of {connections}");
        self.phase = connection as f64 / connections as f64;
        self
    }

    /// Start the schedule at `current_time_ns`, or at the origin if that is
    /// later, owing the phase less the requests the rate scheduled since the
    /// origin; the phase points missed before `current_time_ns` are skipped
    fn start(&mut self, current_time_ns: u64) {
        if self.cursor_ns.is_some() {
            return;
        }
        let origin = if self.anchored {
            self.start_time_ns
        } else {
            current_time_ns
        };
        let from = origin.max(current_time_ns);
        let owed = self.phase - self.integrate(origin, from);
        self.credit_needed = if owed >= 0.0 {
            owed
        } else {
            owed.rem_euclid(1.0)
        };
        self.cursor_ns = Some(from);
    }

    /// Requests the rate schedules from `from_ns` to `to_ns`
    fn integrate(&self, from_ns: u64, to_ns: u64) -> f64 {
        let mut requests = 0.0;
        let mut cursor = from_ns;
        while cursor < to_ns {
            let offset = cursor.saturating_sub(self.start_time_ns);
            let step_start = cursor - offset % PATTERN_REFRESH_NS;
            let step_end = step_start.saturating_add(PATTERN_REFRESH_NS).min(to_ns);
            let rate = self
                .pattern
                .rate_at(Duration::from_nanos(step_start.saturating_sub(self.start_time_ns)));
            if rate > 0.0 {
                requests += rate * (step_end - cursor) as f64 / 1_000_000_000.0;
            }
            cursor = step_end;
        }
        requests
    }

    /// Integrate the rate until the next intended time is found or the
    /// cursor reaches `horizon_ns`.
    fn integrate_until(&mut self, horizon_ns: u64) {
        let Some(mut cursor) = self.cursor_ns else {
            return;
        };
        while self.next_intended_ns.is_none() && cursor < horizon_ns {
            let offset = cursor.saturating_sub(self.start_time_ns);
            let step_start = cursor - offset % PATTERN_REFRESH_NS;
            let step_end = step_start.saturating_add(PATTERN_REFRESH_NS);
            let rate = self
                .pattern
                .rate_at(Duration::from_nanos(step_start.saturating_sub(self.start_time_ns)));
            if rate > 0.0 {
                let needed_ns = self.credit_needed * 1_000_000_000.0 / rate;
                // A request that completes its step is due at the next
                // step's start, if that step's rate is still positive.
                if needed_ns < (step_end - cursor) as f64 {
                    let next = cursor.saturating_add(needed_ns as u64);
                    self.next_intended_ns = Some(next);
                    cursor = next;
                    break;
                }
                self.credit_needed -= rate * (step_end - cursor) as f64 / 1_000_000_000.0;
            }
            if step_end == u64::MAX {
                // Nothing can be scheduled past the end of time.
                cursor = u64::MAX;
                break;
            }
            cursor = step_end;
        }
        self.cursor_ns = Some(cursor);
    }

    fn advance(&mut self, sent_time_ns: u64) {
        let intended = self.next_intended_ns.take().unwrap_or(sent_time_ns);
        self.cursor_ns = Some(intended);
        self.credit_needed = 1.0;
    }
}

impl Policy for PatternAwarePolicy {
    fn next_send_time(&mut self, current_time_ns: u64) -> Option<u64> {
        self.start(current_time_ns);
        let horizon = current_time_ns.saturating_add(PATTERN_REFRESH_NS);
        self.integrate_until(horizon);
        // Without a request due by the horizon, wake there to integrate on.
        // That refresh wakeup is not permission to send.
        Some(self.next_intended_ns.unwrap_or(horizon))
    }

    fn ready_to_send(&mut self, current_time_ns: u64) -> bool {
        self.start(current_time_ns);
        self.integrate_until(current_time_ns.saturating_add(1));
        self.next_intended_ns.is_some_and(|next| next <= current_time_ns)
    }

    fn intended_send_time(&self) -> Option<u64> {
        self.next_intended_ns
    }

    fn on_request_sent(&mut self, sent_time_ns: u64) {
        self.advance(sent_time_ns);
    }

    fn count_intended_before(&mut self, end_ns: u64, cap: u64) -> (u64, bool) {
        if self.cursor_ns.is_none() {
            return (0, false);
        }
        let mut count = 0;
        loop {
            self.integrate_until(end_ns);
            match self.next_intended_ns {
                Some(next) if next < end_ns => {
                    if count == cap {
                        return (count, true);
                    }
                    count += 1;
                    self.advance(next);
                }
                _ => return (count, false),
            }
        }
    }

    fn anchor(&mut self, start_ns: u64) {
        self.start_time_ns = start_ns;
        self.anchored = true;
        self.reset();
    }

    fn reset(&mut self) {
        self.cursor_ns = None;
        self.credit_needed = 0.0;
        self.next_intended_ns = None;
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

        // The schedule starts at the first call: the first request is due then
        let next = policy.next_send_time(100_000);
        assert_eq!(next, Some(100_000));
        assert_eq!(policy.intended_send_time(), Some(100_000));

        policy.on_request_sent(150_000); // Sent 50μs late

        // The next request is 1ms after the intended time, not the actual send
        let next2 = policy.next_send_time(200_000);
        assert_eq!(next2, Some(100_000 + 1_000_000)); // +1ms in nanoseconds

        assert_eq!(policy.name(), "FixedRate");
    }

    #[test]
    fn test_fixed_rate_policy_non_positive_rate_never_sends() {
        for rate in [0.0, -1.0, f64::NAN] {
            let mut policy = FixedRatePolicy::new(rate);

            // Not even the first request is due; the wakeup never arrives.
            assert_eq!(policy.next_send_time(100), Some(u64::MAX), "rate {rate}");

            policy.on_request_sent(100);
            assert_eq!(policy.next_send_time(200), Some(u64::MAX), "rate {rate}");
        }
    }

    #[test]
    fn test_fixed_rate_policy_huge_interval_saturates() {
        // An interval too large for u64 must not wrap the deadline into the past.
        let mut policy = FixedRatePolicy::new(1e-300);
        policy.on_request_sent(100);
        assert_eq!(policy.next_send_time(100), Some(u64::MAX));
    }

    #[test]
    fn test_poisson_policy() {
        let mut policy = PoissonPolicy::new(1000.0).expect("Failed to create Poisson");

        // First request: one sampled inter-arrival after the start, like
        // every arrival of a Poisson process
        let first = policy.next_send_time(0).unwrap();
        assert_eq!(policy.intended_send_time(), Some(first));
        assert_eq!(policy.next_send_time(1), Some(first), "the schedule started at 0");

        policy.on_request_sent(first);

        // Second request: one more sampled inter-arrival later
        let next2 = policy.next_send_time(0);
        assert!(next2.is_some());
        assert!(next2.unwrap() >= first);

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
    fn test_adaptive_policy_non_positive_rate_never_sends() {
        let target_latency = Duration::from_millis(10);
        for rate in [0.0, -1.0] {
            let mut policy = AdaptivePolicy::new(rate, target_latency, rate, rate);

            // Not even the first request is due; the wakeup never arrives.
            assert_eq!(policy.next_send_time(100), Some(u64::MAX), "rate {rate}");

            // A rate adjustment stays within the non-positive bounds.
            policy.on_request_sent(100);
            for _ in 0..100 {
                policy.add_latency_sample(Duration::ZERO);
            }
            assert_eq!(policy.next_send_time(200), Some(u64::MAX), "rate {rate}");
            assert_eq!(policy.current_rate(), rate);
        }
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

    /// Sends the requests due at `now` the way the pool and worker do,
    /// returning their intended times.
    fn send_due(policy: &mut dyn Policy, now: u64) -> Vec<u64> {
        let mut intended = Vec::new();
        while policy.next_send_time(now).is_some_and(|next| next <= now)
            && policy.ready_to_send(now)
        {
            intended.push(policy.intended_send_time().unwrap());
            policy.on_request_sent(now);
        }
        intended
    }

    #[test]
    fn stalled_sender_sends_overdue_requests_with_their_intended_times() {
        // 1000 req/s, then the sender stalls for 100 ms after its first send
        let mut policy = FixedRatePolicy::new(1000.0);
        assert_eq!(send_due(&mut policy, 0), vec![0]);
        let overdue = send_due(&mut policy, 100_000_000);
        // Nothing is skipped: every intended time up to now goes out
        assert_eq!(overdue, (1..=100).map(|k| k * 1_000_000).collect::<Vec<_>>());
        // Then the schedule continues where it was, not from the late send
        assert_eq!(policy.next_send_time(100_000_000), Some(101_000_000));
        assert_eq!(policy.count_intended_before(110_000_000, 1000), (9, false));
    }

    #[test]
    fn closed_loop_has_no_intended_time_or_unsent_requests() {
        let mut policy = ClosedLoopPolicy::new();
        assert_eq!(policy.next_send_time(10), None);
        assert_eq!(policy.intended_send_time(), None);
        policy.on_request_sent(10);
        assert_eq!(policy.count_intended_before(u64::MAX, 10), (0, false));
    }

    #[test]
    fn poisson_schedule_keeps_its_mean_rate_when_sends_are_late() {
        let mut policy = PoissonPolicy::with_seed(1000.0, Some(7)).unwrap();
        policy.next_send_time(0);
        let n = 20_000u64;
        for k in 0..n {
            // Every send is 5 ms late; the schedule must not drift with it.
            let intended = policy.intended_send_time().unwrap();
            policy.on_request_sent(intended + 5_000_000 + k);
        }
        let mean_interval = policy.intended_send_time().unwrap() as f64 / n as f64;
        assert!((mean_interval - 1_000_000.0).abs() < 30_000.0, "{mean_interval}");

        // Sampling the rest of a 1 s window forward counts about 1000 sends
        let mut policy = PoissonPolicy::with_seed(1000.0, Some(9)).unwrap();
        policy.next_send_time(0);
        let (count, capped) = policy.count_intended_before(1_000_000_000, u64::MAX);
        assert!(!capped && (900..=1100).contains(&count), "{count}");
    }

    #[test]
    fn unsent_count_includes_the_next_intended_time_and_respects_the_cap() {
        let mut policy = FixedRatePolicy::new(1000.0);
        // Never started: nothing was intended
        assert_eq!(policy.count_intended_before(10_000_000, 100), (0, false));
        policy.next_send_time(0);
        policy.on_request_sent(0);
        // Due at 1, 2, ..., 9 ms; 10 ms is not before the end
        assert_eq!(policy.count_intended_before(10_000_000, 100), (9, false));
        assert_eq!(policy.count_intended_before(10_000_001, 100), (10, false));
        assert_eq!(policy.count_intended_before(1_000_000, 100), (0, false));
        assert_eq!(policy.count_intended_before(u64::MAX, 100), (100, true));

        let mut paused = FixedRatePolicy::new(0.0);
        paused.next_send_time(0);
        assert_eq!(paused.count_intended_before(u64::MAX, 100), (0, false));

        let mut poisson = PoissonPolicy::with_seed(1e6, Some(1)).unwrap();
        poisson.next_send_time(0);
        assert_eq!(poisson.count_intended_before(u64::MAX, 50), (50, true));
    }

    fn pattern(rate: f64) -> PatternAwarePolicy {
        PatternAwarePolicy::new(
            std::sync::Arc::new(crate::workload::patterns::SpikePattern::new(
                rate,
                1000.0,
                Duration::from_millis(20),
                Duration::from_millis(20),
            )),
            0,
        )
    }

    #[test]
    fn pattern_schedule_integrates_its_rate() {
        // Constant 1000 req/s before the spike: one request per millisecond
        let mut policy = pattern(1000.0);
        policy.next_send_time(0);
        assert_eq!(send_due(&mut policy, 5_000_000), [0, 1, 2, 3, 4, 5].map(|k| k * 1_000_000));

        // A 20 ms spike at 1000 req/s after a pause schedules 20 requests,
        // and the pause schedules none, even when the sender is late
        let mut policy = pattern(0.0);
        policy.next_send_time(0);
        assert!(send_due(&mut policy, 19_999_999).is_empty());
        let spike = send_due(&mut policy, 100_000_000);
        assert_eq!(spike, (20..40).map(|k| k * 1_000_000).collect::<Vec<_>>());
        assert_eq!(policy.count_intended_before(1_000_000_000, 100), (0, false));

        let mut policy = pattern(0.0);
        policy.next_send_time(0);
        assert_eq!(policy.count_intended_before(1_000_000_000, 100), (20, false));
        let mut policy = pattern(0.0);
        policy.next_send_time(0);
        assert_eq!(policy.count_intended_before(1_000_000_000, 5), (5, true));
    }

    /// Intended times of a policy's first `sends` requests, each sent on
    /// time, with the schedule started at `ready_ns`
    fn intended_times(policy: &mut dyn Policy, ready_ns: u64, sends: usize) -> Vec<u64> {
        policy.next_send_time(ready_ns);
        (0..sends)
            .map(|_| {
                let intended = policy.intended_send_time().unwrap();
                policy.on_request_sent(intended);
                intended
            })
            .collect()
    }

    #[test]
    fn exact_rate_offsets_are_exact_and_agree_with_slot_counts() {
        // Rates that are integers over a power of two, checked against
        // rational arithmetic: ⌊slot · 10⁹ · denominator / numerator⌋
        for (numerator, denominator) in [
            (1000u128, 1u128),
            (300_000, 1),
            (24_691, 2),
            (1, 4),
            (7_000_000, 1),
            (1 << 60, 1),
        ] {
            let rate = ExactRate::new(numerator as f64 / denominator as f64).unwrap();
            for slot in [0u64, 1, 2, 3, 999, 1_000_000, 123_456_789_012, u64::MAX / 1_000_000] {
                let expected = slot as u128 * 1_000_000_000 * denominator / numerator;
                assert_eq!(
                    rate.offset_ns(slot) as u128,
                    expected.min(u64::MAX as u128),
                    "{numerator}/{denominator} req/s, slot {slot}"
                );
            }
        }
        // For any rate, a slot is before a time exactly when its offset is
        for value in [
            1000.0,
            1.0 / 3.0,
            0.001,
            99_999.999,
            33_333.333_333,
            2.5e9,
            1e-300,
            1e300,
            f64::MIN_POSITIVE,
            5e-324,
        ] {
            let rate = ExactRate::new(value).unwrap();
            for slot in [0u64, 1, 2, 7, 1000, 123_456_789, 1 << 40] {
                let offset = rate.offset_ns(slot);
                if offset == u64::MAX {
                    continue; // saturated: past the end of time
                }
                assert!(rate.slots_before(offset) <= slot as u128, "{value} req/s, slot {slot}");
                assert!(rate.slots_before(offset + 1) > slot as u128, "{value} req/s, slot {slot}");
            }
        }
        for value in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(ExactRate::new(value).is_none(), "{value}");
        }
    }

    #[test]
    fn striped_fixed_rate_connections_merge_into_one_evenly_spaced_stream() {
        // 12345.5 req/s is 81001.98... ns between slots, so a truncated
        // interval would drift by 183 µs over these 1,050,000 sends
        let (rate, connections, start) = (12_345.5, 7, 5_000_000_000u64);
        let slot = |j: u128| start + (j * 2_000_000_000 / 24_691) as u64;
        let mut merged = Vec::new();
        for c in 0..connections {
            let mut policy = FixedRatePolicy::striped(rate, c, connections);
            policy.anchor(start);
            merged.extend(intended_times(&mut policy, start - 1_000_000, 150_000));
        }
        merged.sort_unstable();
        let expected: Vec<u64> = (0..merged.len() as u128).map(slot).collect();
        assert!(
            merged == expected,
            "first difference at send {:?}",
            merged.iter().zip(&expected).position(|(a, b)| a != b)
        );
        assert_eq!(*merged.last().unwrap(), start + 85_051_152_241, "no drift");

        // Over any window the connections' intended sends add up to the rate's
        for window in [1_000_000_000, 1_234_567, 81_002, 81_001, 1] {
            let total: u64 = (0..connections)
                .map(|c| {
                    let mut policy = FixedRatePolicy::striped(rate, c, connections);
                    policy.anchor(start);
                    policy.next_send_time(start);
                    policy.count_intended_before(start + window, u64::MAX).0
                })
                .sum();
            let expected = (0u128..).take_while(|&j| slot(j) < start + window).count();
            assert_eq!(total as usize, expected, "window {window} ns");
        }
    }

    #[test]
    fn late_fixed_rate_connection_starts_at_its_next_slot_and_counts_unsent_from_there() {
        // Connection 1 of 4 sharing 4000 req/s: the group's slots are 250 µs
        // apart and its own are at 0.25 ms, 1.25 ms, 2.25 ms, ...
        let start = 1_000_000_000;
        let mut policy = FixedRatePolicy::striped(4000.0, 1, 4);
        policy.anchor(start);
        // Not started: nothing is intended, so nothing is unsent
        assert_eq!(policy.intended_send_time(), None);
        assert_eq!(policy.count_intended_before(start + 10_000_000, 100), (0, false));
        // Ready 1.3 ms after the start, it starts at 2.25 ms
        assert_eq!(policy.next_send_time(start + 1_300_000), Some(start + 2_250_000));
        // Unsent counts its slots from there: 2.25 ms, 3.25 ms, ..., 9.25 ms
        assert_eq!(policy.count_intended_before(start + 10_000_000, 100), (8, false));
        policy.on_request_sent(start + 2_300_000);
        assert_eq!(policy.next_send_time(start + 2_300_000), Some(start + 3_250_000));
        assert_eq!(policy.count_intended_before(start + 10_000_000, 100), (7, false));
        assert_eq!(policy.count_intended_before(start + 3_250_000, 100), (0, false));
        assert_eq!(policy.count_intended_before(start + 3_250_001, 100), (1, false));
        assert_eq!(policy.count_intended_before(u64::MAX, 100), (100, true));
        // A restart (connection setup) leaves nothing intended until it
        // starts again, at its slot exactly when ready on one
        policy.reset();
        assert_eq!(policy.count_intended_before(u64::MAX, 100), (0, false));
        assert_eq!(policy.next_send_time(start + 5_250_000), Some(start + 5_250_000));
        // Ready before the start, it starts at its first slot
        policy.anchor(start);
        assert_eq!(policy.next_send_time(0), Some(start + 250_000));
    }

    #[test]
    fn seeded_poisson_schedules_repeat_and_late_starts_skip_ahead() {
        let start = 1_000_000_000;
        let schedule = |seed: u64| {
            let mut policy = PoissonPolicy::with_seed(1000.0, Some(seed)).unwrap();
            policy.anchor(start);
            intended_times(&mut policy, start - 5, 1000)
        };
        let first = schedule(1);
        assert_eq!(first, schedule(1), "same seed, same schedule");
        assert_ne!(first, schedule(2));
        assert!(first[0] > start, "the first arrival is an inter-arrival after the start");

        // Ready late, a connection skips the arrivals before then and keeps
        // the rest of the same schedule
        assert!(first[10] > first[9] + 1);
        let mut late = PoissonPolicy::with_seed(1000.0, Some(1)).unwrap();
        late.anchor(start);
        assert_eq!(intended_times(&mut late, first[9] + 1, 990), first[10..]);
    }

    #[test]
    fn seeded_poisson_connections_have_distinct_streams_at_the_group_rate() {
        let (rate, connections, start, window) = (10_000.0, 8, 1_000_000_000u64, 10_000_000_000);
        let mut firsts = std::collections::HashSet::new();
        let mut total = 0;
        for c in 0..connections {
            let seed = crate::seed::poisson_seed(42, 0, c);
            let mut policy =
                PoissonPolicy::with_seed(rate / connections as f64, Some(seed)).unwrap();
            policy.anchor(start);
            firsts.insert(policy.next_send_time(start).unwrap());
            total += policy.count_intended_before(start + window, u64::MAX).0;
        }
        assert_eq!(firsts.len(), connections, "every connection has its own stream");
        // 100,000 arrivals expected, with a standard deviation of about 316
        assert!((total as f64 - 100_000.0).abs() < 1_500.0, "{total}");
    }

    #[test]
    fn pattern_connections_interleave_by_phase_from_the_anchor() {
        // 1000 req/s per connection, 4000 req/s for the four, until the
        // spike 20 ms after the anchor
        let start = 1_000_000_000;
        let mut merged = Vec::new();
        for c in 0..4 {
            let mut policy = pattern(1000.0).with_phase(c, 4);
            policy.anchor(start);
            policy.next_send_time(start - 1_000);
            merged.extend(send_due(&mut policy, start + 4_999_999));
        }
        merged.sort_unstable();
        assert_eq!(merged.len(), 20);
        for (j, intended) in merged.iter().enumerate() {
            let expected = start + j as u64 * 250_000;
            assert!(intended.abs_diff(expected) <= 1, "request {j}: {intended} vs {expected}");
        }

        // Ready 1.3 ms after the start, connection 1 starts at its next
        // phase point, 2.25 ms
        let mut late = pattern(1000.0).with_phase(1, 4);
        late.anchor(start);
        let first = late.next_send_time(start + 1_300_000).unwrap();
        assert!(first.abs_diff(start + 2_250_000) <= 1, "{first}");
    }

    #[test]
    fn adaptive_connections_start_at_their_phase() {
        let start = 1_000_000_000;
        let policy = || {
            let mut policy =
                AdaptivePolicy::new(1000.0, Duration::from_millis(1), 1.0, 1e6).with_phase(3, 4);
            policy.anchor(start);
            policy
        };
        assert_eq!(policy().next_send_time(start - 10), Some(start + 750_000));
        // Ready late: the next time on its grid
        assert_eq!(policy().next_send_time(start + 2_000_000), Some(start + 2_750_000));
        assert_eq!(policy().next_send_time(start + 2_750_000), Some(start + 2_750_000));
    }
}
