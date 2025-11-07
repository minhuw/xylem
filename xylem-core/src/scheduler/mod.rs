//! Unified scheduler architecture with composable timing and connection selection
//!
//! This module provides a flexible scheduler abstraction that separates two orthogonal concerns:
//!
//! 1. **Timing Policy** (temporal): WHEN to send the next request
//!    - Examples: ClosedLoop, FixedRate, Poisson arrivals, Adaptive rate
//!
//! 2. **Connection Selector** (spatial): WHICH connection to use
//!    - Examples: RoundRobin, KeyAffinity, LoadBased, Zipfian, LatencyAware
//!
//! These can be composed to create sophisticated scheduling strategies:
//! - FixedRate + KeyAffinity: Steady load with key-based routing
//! - Poisson + Zipfian: Realistic arrivals with hot-key optimization
//! - Adaptive + LatencyAware: Dynamic rate adjustment with latency-based routing
//!
//! ## Architecture
//!
//! ```text
//! UnifiedScheduler
//! ├── TimingPolicy (when)
//! │   ├── ClosedLoop
//! │   ├── FixedRate
//! │   ├── Poisson
//! │   └── Adaptive
//! └── ConnectionSelector (which)
//!     ├── RoundRobin
//!     ├── KeyAffinity
//!     ├── LoadBased
//!     ├── Zipfian
//!     ├── Random
//!     ├── LatencyAware
//!     └── ClosedLoop
//! ```

use std::time::Duration;

pub mod connection_selector;
pub mod timing_policy;

/// Timing policy trait - decides WHEN to send the next request
///
/// Timing policies control the inter-arrival time between requests, implementing
/// various rate control and arrival distribution strategies.
pub trait TimingPolicy: Send {
    /// Calculate delay until next request should be sent
    ///
    /// # Parameters
    /// - `context`: Information about timing state and request history
    ///
    /// # Returns
    /// - `None`: Send immediately (closed-loop, no delay)
    /// - `Some(Duration::ZERO)`: Send immediately but timing is tracked
    /// - `Some(duration)`: Wait this amount before sending
    fn delay_until_next(&mut self, context: &TimingContext) -> Option<Duration>;

    /// Notify that a request was sent (for rate tracking)
    fn on_request_sent(&mut self, sent_time_ns: u64) {
        let _ = sent_time_ns; // Default: no-op
    }

    /// Reset timing state
    fn reset(&mut self) {
        // Default: no-op
    }

    /// Get the name of this timing policy
    fn name(&self) -> &'static str;
}

/// Connection selector trait - decides WHICH connection to use
///
/// Connection selectors implement various strategies for distributing requests
/// across multiple connections based on workload characteristics and connection state.
pub trait ConnectionSelector: Send {
    /// Select which connection to use for the next request
    ///
    /// # Parameters
    /// - `context`: Information about the request and connection states
    ///
    /// # Returns
    /// Connection index, or `None` if no connection is available
    fn select_connection(&mut self, context: &ConnectionContext) -> Option<usize>;

    /// Notify selector that a request was sent (optional feedback)
    fn on_request_sent(&mut self, conn_idx: usize, key: u64) {
        let _ = (conn_idx, key); // Default: no-op
    }

    /// Notify selector that a response was received (optional feedback)
    fn on_response_received(&mut self, conn_idx: usize, key: u64, latency: Duration) {
        let _ = (conn_idx, key, latency); // Default: no-op
    }

    /// Reset selector state
    fn reset(&mut self) {
        // Default: no-op
    }

    /// Get the name of this connection selector
    fn name(&self) -> &'static str;
}

// Re-export for convenience
pub use connection_selector::{
    ClosedLoopSelector, KeyAffinitySelector, LatencyAwareSelector, LoadBasedSelector,
    MaxConcurrentRequests, RandomSelector, RoundRobinSelector, ZipfianSelector,
};

pub use timing_policy::{AdaptiveTiming, ClosedLoopTiming, FixedRateTiming, PoissonTiming};

/// Context for timing policy decisions
#[derive(Debug, Clone)]
pub struct TimingContext {
    /// Time since last request was sent
    pub time_since_last: Duration,
    /// Current time in nanoseconds (from timing::time_ns())
    pub current_time_ns: u64,
    /// Number of requests sent so far
    pub requests_sent: u64,
    /// Number of in-flight requests across all connections
    pub global_in_flight: usize,
}

/// Context for connection selection decisions
#[derive(Debug)]
pub struct ConnectionContext<'a> {
    /// Key being requested (for affinity-based routing)
    pub key: u64,
    /// Connection states
    pub connections: &'a [ConnState],
    /// Maximum pending requests per connection
    pub max_pending_per_conn: usize,
    /// Current time in nanoseconds
    pub current_time_ns: u64,
}

/// Connection state snapshot for scheduler decisions
#[derive(Debug, Clone)]
pub struct ConnState {
    /// Connection index
    pub idx: usize,
    /// Number of pending requests
    pub pending_count: usize,
    /// Whether connection is closed
    pub closed: bool,
    /// Recent average latency (optional, for latency-aware scheduling)
    pub avg_latency: Option<Duration>,
    /// Whether connection is ready to accept requests
    ///
    /// For open-loop: typically true (ready if not at max pending)
    /// For closed-loop: true only if connection has no outstanding requests
    pub ready: bool,
}

/// Unified scheduler combining timing policy and connection selection
///
/// This is the main scheduler type that composes a `TimingPolicy` with a `ConnectionSelector`
/// to make holistic scheduling decisions about both WHEN and WHERE to send requests.
///
/// # Example
///
/// ```ignore
/// use xylem_core::scheduler::*;
///
/// // Create fixed-rate timing with key-affinity routing
/// let timing = Box::new(FixedRateTiming::new(1000.0)); // 1000 req/s
/// let selector = Box::new(KeyAffinitySelector::new());
/// let scheduler = UnifiedScheduler::new(timing, selector);
/// ```
pub struct UnifiedScheduler {
    timing_policy: Box<dyn TimingPolicy>,
    connection_selector: Box<dyn ConnectionSelector>,

    // Timing state
    last_request_time_ns: Option<u64>,
    requests_sent: u64,
}

impl UnifiedScheduler {
    /// Create a new unified scheduler
    ///
    /// # Parameters
    /// - `timing_policy`: Determines when to send requests
    /// - `connection_selector`: Determines which connection to use
    pub fn new(
        timing_policy: Box<dyn TimingPolicy>,
        connection_selector: Box<dyn ConnectionSelector>,
    ) -> Self {
        Self {
            timing_policy,
            connection_selector,
            last_request_time_ns: None,
            requests_sent: 0,
        }
    }

    /// Check if we should send a request now based on timing policy
    ///
    /// # Parameters
    /// - `current_time_ns`: Current time in nanoseconds
    /// - `global_in_flight`: Number of in-flight requests across all connections
    ///
    /// # Returns
    /// `true` if the timing policy allows sending now, `false` otherwise
    pub fn should_send_now(&mut self, current_time_ns: u64, global_in_flight: usize) -> bool {
        let time_since_last = self
            .last_request_time_ns
            .map(|last| Duration::from_nanos(current_time_ns.saturating_sub(last)))
            .unwrap_or(Duration::ZERO);

        let timing_ctx = TimingContext {
            time_since_last,
            current_time_ns,
            requests_sent: self.requests_sent,
            global_in_flight,
        };

        match self.timing_policy.delay_until_next(&timing_ctx) {
            None => true,                                 // Closed-loop: always ready when called
            Some(duration) if duration.is_zero() => true, // Ready now
            Some(delay) => time_since_last >= delay,      // Check if enough time has passed
        }
    }

    /// Select a connection for the next request
    ///
    /// # Parameters
    /// - `key`: The key being requested
    /// - `connections`: Current state of all connections
    /// - `max_pending_per_conn`: Maximum pending requests per connection
    /// - `current_time_ns`: Current time in nanoseconds
    ///
    /// # Returns
    /// Connection index, or `None` if no connection is available
    pub fn select_connection(
        &mut self,
        key: u64,
        connections: &[ConnState],
        max_pending_per_conn: usize,
        current_time_ns: u64,
    ) -> Option<usize> {
        let conn_ctx = ConnectionContext {
            key,
            connections,
            max_pending_per_conn,
            current_time_ns,
        };

        self.connection_selector.select_connection(&conn_ctx)
    }

    /// Notify that a request was sent
    ///
    /// This updates internal state and notifies both timing policy and connection selector.
    ///
    /// # Parameters
    /// - `conn_idx`: Index of the connection that was used
    /// - `key`: The key that was requested
    /// - `sent_time_ns`: Time when the request was sent (nanoseconds)
    pub fn on_request_sent(&mut self, conn_idx: usize, key: u64, sent_time_ns: u64) {
        self.last_request_time_ns = Some(sent_time_ns);
        self.requests_sent += 1;
        self.timing_policy.on_request_sent(sent_time_ns);
        self.connection_selector.on_request_sent(conn_idx, key);
    }

    /// Notify that a response was received
    ///
    /// This allows the connection selector to track latency and adjust routing.
    ///
    /// # Parameters
    /// - `conn_idx`: Index of the connection that received the response
    /// - `key`: The key that was requested
    /// - `latency`: Round-trip latency for this request
    pub fn on_response_received(&mut self, conn_idx: usize, key: u64, latency: Duration) {
        self.connection_selector.on_response_received(conn_idx, key, latency);
    }

    /// Reset scheduler state
    ///
    /// Clears all tracking state and resets both timing policy and connection selector.
    pub fn reset(&mut self) {
        self.last_request_time_ns = None;
        self.requests_sent = 0;
        self.timing_policy.reset();
        self.connection_selector.reset();
    }

    /// Get timing policy name for debugging/logging
    pub fn timing_policy_name(&self) -> &str {
        self.timing_policy.name()
    }

    /// Get connection selector name for debugging/logging
    pub fn connection_selector_name(&self) -> &str {
        self.connection_selector.name()
    }

    /// Get number of requests sent so far
    pub fn requests_sent(&self) -> u64 {
        self.requests_sent
    }
}

impl std::fmt::Debug for UnifiedScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnifiedScheduler")
            .field("timing_policy", &self.timing_policy_name())
            .field("connection_selector", &self.connection_selector_name())
            .field("requests_sent", &self.requests_sent)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unified_scheduler_creation() {
        let timing = Box::new(timing_policy::ClosedLoopTiming::new());
        let selector = Box::new(connection_selector::RoundRobinSelector::new());
        let scheduler = UnifiedScheduler::new(timing, selector);

        assert_eq!(scheduler.timing_policy_name(), "ClosedLoop");
        assert_eq!(scheduler.connection_selector_name(), "RoundRobin");
        assert_eq!(scheduler.requests_sent(), 0);
    }

    #[test]
    fn test_unified_scheduler_state_tracking() {
        let timing = Box::new(timing_policy::ClosedLoopTiming::new());
        let selector = Box::new(connection_selector::RoundRobinSelector::new());
        let mut scheduler = UnifiedScheduler::new(timing, selector);

        // Initial state
        assert_eq!(scheduler.requests_sent(), 0);

        // Send a request
        scheduler.on_request_sent(0, 42, 1000);
        assert_eq!(scheduler.requests_sent(), 1);

        // Send another
        scheduler.on_request_sent(1, 43, 2000);
        assert_eq!(scheduler.requests_sent(), 2);

        // Reset
        scheduler.reset();
        assert_eq!(scheduler.requests_sent(), 0);
    }
}
