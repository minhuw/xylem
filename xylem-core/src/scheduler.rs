//! Connection scheduling strategies
//!
//! This module provides a flexible scheduler abstraction for selecting which connection
//! to use for each request. Different scheduling strategies can optimize for different
//! workload patterns:
//!
//! - **Round-robin**: Simple, cache-friendly, default Lancet behavior
//! - **Key-affinity**: Routes same keys to same connections for cache locality
//! - **Load-based**: Routes to least-loaded connections for balance
//! - **Zipfian-aware**: Fast lanes for hot keys, reduces head-of-line blocking
//! - **Random**: Random selection for testing
//! - **Latency-aware**: Routes to faster connections based on history

use rand::seq::SliceRandom;
use std::collections::VecDeque;
use std::time::Duration;

/// Context provided to scheduler for making decisions
#[derive(Debug)]
pub struct SchedulerContext<'a> {
    /// Key being requested (for affinity-based routing)
    pub key: u64,
    /// Connection states
    pub connections: &'a [ConnectionState],
    /// Maximum pending requests per connection
    pub max_pending_per_conn: usize,
}

/// Connection state snapshot for scheduler decisions
#[derive(Debug, Clone)]
pub struct ConnectionState {
    /// Connection index
    pub idx: usize,
    /// Number of pending requests
    pub pending_count: usize,
    /// Whether connection is closed
    pub closed: bool,
    /// Recent average latency (optional, for latency-aware scheduling)
    pub avg_latency: Option<Duration>,
    /// Whether connection is ready to accept requests (for closed-loop scheduling)
    ///
    /// For open-loop: typically true (ready if not at max pending)
    /// For closed-loop: true only if connection has no outstanding requests or response was received
    pub ready: bool,
}

/// Scheduler trait for connection selection strategies
///
/// Schedulers decide which connection to use for each request based on
/// workload characteristics, connection state, and performance goals.
pub trait Scheduler: Send {
    /// Select which connection to use for the next request
    ///
    /// Returns connection index, or None if no connection is available.
    ///
    /// # Parameters
    /// - `context`: Information about the request and connection states
    fn select_connection(&mut self, context: &SchedulerContext) -> Option<usize>;

    /// Notify scheduler that a request was sent (optional feedback)
    #[allow(unused_variables)]
    fn on_request_sent(&mut self, conn_idx: usize, key: u64) {
        // Default: no-op
    }

    /// Notify scheduler that a response was received (optional feedback)
    #[allow(unused_variables)]
    fn on_response_received(&mut self, conn_idx: usize, key: u64, latency: Duration) {
        // Default: no-op
    }

    /// Reset scheduler state
    fn reset(&mut self) {
        // Default: no-op
    }
}

// ========================================
// Common Scheduler Implementations
// ========================================

/// Round-robin scheduler (Lancet-style, cache-friendly)
///
/// Cycles through connections in order. Simple and effective for most workloads.
/// Matches the default behavior of the original Lancet implementation.
pub struct RoundRobinScheduler {
    counter: usize,
}

impl RoundRobinScheduler {
    /// Create a new round-robin scheduler
    pub fn new() -> Self {
        Self { counter: 0 }
    }
}

impl Default for RoundRobinScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Scheduler for RoundRobinScheduler {
    fn select_connection(&mut self, context: &SchedulerContext) -> Option<usize> {
        let total = context.connections.len();

        // Try all connections starting from current position
        for _ in 0..total {
            let idx = self.counter % total;
            self.counter = self.counter.wrapping_add(1);

            let conn = &context.connections[idx];
            if !conn.closed && conn.pending_count < context.max_pending_per_conn {
                return Some(idx);
            }
        }

        None // All connections busy or closed
    }

    fn reset(&mut self) {
        self.counter = 0;
    }
}

/// Key-affinity scheduler (same key → same connection)
///
/// Routes requests for the same key to the same connection, improving
/// server-side cache locality. Falls back to round-robin if the affinity
/// connection is unavailable.
pub struct KeyAffinityScheduler {
    /// Fallback to round-robin if affinity connection unavailable
    fallback_counter: usize,
}

impl KeyAffinityScheduler {
    /// Create a new key-affinity scheduler
    pub fn new() -> Self {
        Self { fallback_counter: 0 }
    }

    /// Hash a key to a connection index
    fn hash_key(key: u64, num_conns: usize) -> usize {
        (key as usize) % num_conns
    }
}

impl Default for KeyAffinityScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Scheduler for KeyAffinityScheduler {
    fn select_connection(&mut self, context: &SchedulerContext) -> Option<usize> {
        let total = context.connections.len();

        // Try affinity connection first
        let affinity_idx = Self::hash_key(context.key, total);
        let conn = &context.connections[affinity_idx];

        if !conn.closed && conn.pending_count < context.max_pending_per_conn {
            return Some(affinity_idx);
        }

        // Fallback to round-robin if affinity connection busy
        for _ in 0..total {
            let idx = self.fallback_counter % total;
            self.fallback_counter = self.fallback_counter.wrapping_add(1);

            if idx == affinity_idx {
                continue; // Already tried
            }

            let conn = &context.connections[idx];
            if !conn.closed && conn.pending_count < context.max_pending_per_conn {
                return Some(idx);
            }
        }

        None
    }

    fn reset(&mut self) {
        self.fallback_counter = 0;
    }
}

/// Load-based scheduler (picks least-loaded connection)
///
/// Always routes to the connection with the fewest pending requests.
/// Good for balancing load when connections have varying latencies.
pub struct LoadBasedScheduler;

impl LoadBasedScheduler {
    /// Create a new load-based scheduler
    pub fn new() -> Self {
        Self
    }
}

impl Default for LoadBasedScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Scheduler for LoadBasedScheduler {
    fn select_connection(&mut self, context: &SchedulerContext) -> Option<usize> {
        context
            .connections
            .iter()
            .filter(|conn| !conn.closed && conn.pending_count < context.max_pending_per_conn)
            .min_by_key(|conn| conn.pending_count)
            .map(|conn| conn.idx)
    }
}

/// Zipfian-aware scheduler
///
/// Hot keys (frequent, low-numbered keys in Zipfian distribution)
/// are routed to dedicated "fast lane" connections to reduce
/// head-of-line blocking for hot data.
///
/// This scheduler partitions connections into two groups:
/// - **Fast lanes**: First N connections, dedicated to hot keys
/// - **Normal lanes**: Remaining connections, for cold keys
///
/// This prevents cold (infrequent) keys from blocking hot keys in the
/// request queue, which is important for Zipfian workloads where a small
/// number of keys receive the majority of requests.
pub struct ZipfianScheduler {
    /// Number of fast-lane connections for hot keys
    num_fast_lanes: usize,
    /// Threshold: keys below this are "hot"
    hot_key_threshold: u64,
    /// Counter for fast lanes
    fast_lane_counter: usize,
    /// Counter for normal lanes
    normal_lane_counter: usize,
}

impl ZipfianScheduler {
    /// Create a Zipfian-aware scheduler
    ///
    /// # Parameters
    /// - `num_fast_lanes`: Number of connections dedicated to hot keys
    /// - `hot_key_threshold`: Keys < threshold are considered hot
    ///
    /// # Example
    /// ```
    /// # use xylem_core::scheduler::ZipfianScheduler;
    /// // Dedicate 2 connections to keys 0-99, others for keys 100+
    /// let scheduler = ZipfianScheduler::new(2, 100);
    /// ```
    pub fn new(num_fast_lanes: usize, hot_key_threshold: u64) -> Self {
        Self {
            num_fast_lanes,
            hot_key_threshold,
            fast_lane_counter: 0,
            normal_lane_counter: 0,
        }
    }

    /// Get the number of fast lanes
    pub fn num_fast_lanes(&self) -> usize {
        self.num_fast_lanes
    }

    /// Get the hot key threshold
    pub fn hot_key_threshold(&self) -> u64 {
        self.hot_key_threshold
    }
}

impl Scheduler for ZipfianScheduler {
    fn select_connection(&mut self, context: &SchedulerContext) -> Option<usize> {
        let total = context.connections.len();

        if total <= self.num_fast_lanes {
            // Not enough connections for partitioning, use round-robin
            for _ in 0..total {
                let idx = self.fast_lane_counter % total;
                self.fast_lane_counter = self.fast_lane_counter.wrapping_add(1);

                let conn = &context.connections[idx];
                if !conn.closed && conn.pending_count < context.max_pending_per_conn {
                    return Some(idx);
                }
            }
            return None;
        }

        let is_hot_key = context.key < self.hot_key_threshold;

        if is_hot_key {
            // Route hot keys to fast lanes (first N connections)
            for _ in 0..self.num_fast_lanes {
                let idx = self.fast_lane_counter % self.num_fast_lanes;
                self.fast_lane_counter = self.fast_lane_counter.wrapping_add(1);

                let conn = &context.connections[idx];
                if !conn.closed && conn.pending_count < context.max_pending_per_conn {
                    return Some(idx);
                }
            }
        } else {
            // Route cold keys to normal lanes (remaining connections)
            let start = self.num_fast_lanes;
            let count = total - self.num_fast_lanes;

            for _ in 0..count {
                let offset = self.normal_lane_counter % count;
                self.normal_lane_counter = self.normal_lane_counter.wrapping_add(1);
                let idx = start + offset;

                let conn = &context.connections[idx];
                if !conn.closed && conn.pending_count < context.max_pending_per_conn {
                    return Some(idx);
                }
            }
        }

        None
    }

    fn reset(&mut self) {
        self.fast_lane_counter = 0;
        self.normal_lane_counter = 0;
    }
}

/// Random scheduler (useful for testing)
///
/// Randomly selects an available connection. Useful for testing
/// and comparing against deterministic strategies.
///
/// Note: Uses thread_rng() for simplicity. For performance-critical
/// paths, consider using a deterministic scheduler like round-robin.
pub struct RandomScheduler;

impl RandomScheduler {
    /// Create a new random scheduler
    pub fn new() -> Self {
        Self
    }
}

impl Default for RandomScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Scheduler for RandomScheduler {
    fn select_connection(&mut self, context: &SchedulerContext) -> Option<usize> {
        use rand::thread_rng;

        // Collect available connections
        let available: Vec<usize> = context
            .connections
            .iter()
            .filter(|conn| !conn.closed && conn.pending_count < context.max_pending_per_conn)
            .map(|conn| conn.idx)
            .collect();

        available.choose(&mut thread_rng()).copied()
    }
}

/// Open-loop round-robin scheduler
///
/// Designed for open-loop (time-driven) workloads where requests are sent
/// at a fixed rate regardless of response timing. Supports pipelining with
/// multiple outstanding requests per connection.
///
/// **Characteristics:**
/// - Time-driven request sending
/// - Round-robin connection selection
/// - Supports high pipelining depth
/// - Ignores connection readiness (relies only on pending_count)
/// - Independent of response arrival timing
///
/// **Use when:** You want to measure system performance under sustained load
/// at a specific request rate, similar to Lancet's THROUGHPUT_AGENT mode.
pub struct OpenLoopRoundRobinScheduler {
    counter: usize,
}

impl OpenLoopRoundRobinScheduler {
    /// Create a new open-loop round-robin scheduler
    pub fn new() -> Self {
        Self { counter: 0 }
    }
}

impl Default for OpenLoopRoundRobinScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Scheduler for OpenLoopRoundRobinScheduler {
    fn select_connection(&mut self, context: &SchedulerContext) -> Option<usize> {
        let total = context.connections.len();

        // Try all connections starting from current position
        // Ignores ready flag - only checks pending_count and closed status
        for _ in 0..total {
            let idx = self.counter % total;
            self.counter = self.counter.wrapping_add(1);

            let conn = &context.connections[idx];
            if !conn.closed && conn.pending_count < context.max_pending_per_conn {
                return Some(idx);
            }
        }

        None // All connections busy or closed
    }

    fn reset(&mut self) {
        self.counter = 0;
    }
}

/// Maximum concurrent requests configuration for closed-loop scheduler
#[derive(Debug, Clone, Copy)]
pub enum MaxConcurrentRequests {
    /// Allow N requests per connection (parallel closed-loop)
    ///
    /// Example: PerConnection(1) with 10 connections = 10 total in-flight requests
    /// This is Lancet's LATENCY_AGENT mode with multiple connections.
    PerConnection(usize),

    /// Allow N requests globally across all connections (strict serial closed-loop)
    ///
    /// Example: Global(1) = only 1 request in-flight across entire pool
    /// Pure request-response serialization.
    Global(usize),
}

/// Closed-loop round-robin scheduler
///
/// Designed for closed-loop (request-response) workloads where each request
/// waits for its response before sending the next. Supports configurable
/// concurrency limits.
///
/// **Characteristics:**
/// - Request-response synchronous per connection
/// - Round-robin among ready connections only
/// - Configurable max concurrent requests (per-connection or global)
/// - Tracks connection readiness via callbacks
/// - Only selects ready connections
///
/// **Use when:** You want to measure per-request latency without queueing
/// effects, similar to Lancet's LATENCY_AGENT mode.
pub struct ClosedLoopRoundRobinScheduler {
    /// Round-robin counter
    counter: usize,
    /// Maximum concurrent requests configuration
    max_concurrent: MaxConcurrentRequests,
    /// Track connection readiness internally
    ready_flags: Vec<bool>,
    /// Track global in-flight count (for Global mode)
    global_in_flight: usize,
}

impl ClosedLoopRoundRobinScheduler {
    /// Create a new closed-loop round-robin scheduler
    ///
    /// # Parameters
    /// - `num_connections`: Number of connections in the pool
    /// - `max_concurrent`: Maximum concurrent requests configuration
    ///
    /// # Example
    /// ```
    /// # use xylem_core::scheduler::{ClosedLoopRoundRobinScheduler, MaxConcurrentRequests};
    /// // Per-connection: 1 request per connection, 10 connections = 10 in-flight
    /// let scheduler = ClosedLoopRoundRobinScheduler::new(10, MaxConcurrentRequests::PerConnection(1));
    ///
    /// // Global: 1 request total across all connections
    /// let scheduler = ClosedLoopRoundRobinScheduler::new(10, MaxConcurrentRequests::Global(1));
    /// ```
    pub fn new(num_connections: usize, max_concurrent: MaxConcurrentRequests) -> Self {
        Self {
            counter: 0,
            max_concurrent,
            ready_flags: vec![true; num_connections], // All connections start ready
            global_in_flight: 0,
        }
    }

    /// Create with per-connection limit (convenience constructor)
    pub fn per_connection(num_connections: usize, max_per_conn: usize) -> Self {
        Self::new(num_connections, MaxConcurrentRequests::PerConnection(max_per_conn))
    }

    /// Create with global limit (convenience constructor)
    pub fn global(num_connections: usize, max_global: usize) -> Self {
        Self::new(num_connections, MaxConcurrentRequests::Global(max_global))
    }
}

impl Scheduler for ClosedLoopRoundRobinScheduler {
    fn select_connection(&mut self, context: &SchedulerContext) -> Option<usize> {
        let total = context.connections.len();

        // Check global limit first (if applicable)
        if let MaxConcurrentRequests::Global(max) = self.max_concurrent {
            if self.global_in_flight >= max {
                return None; // Global limit reached
            }
        }

        // Resize ready_flags if needed (dynamic connection pool)
        if self.ready_flags.len() != total {
            self.ready_flags.resize(total, true);
        }

        // Try all connections starting from current position
        // Only select connections that are ready AND have capacity
        for _ in 0..total {
            let idx = self.counter % total;
            self.counter = self.counter.wrapping_add(1);

            let conn = &context.connections[idx];

            // Connection must be: not closed, ready, and have capacity
            if conn.closed {
                continue;
            }

            // Check readiness (from ConnectionState)
            if !conn.ready {
                continue;
            }

            // Check internal ready flag (from callbacks)
            if idx < self.ready_flags.len() && !self.ready_flags[idx] {
                continue;
            }

            // Check per-connection limit (if applicable)
            if let MaxConcurrentRequests::PerConnection(max_per_conn) = self.max_concurrent {
                if conn.pending_count >= max_per_conn {
                    continue;
                }
            }

            return Some(idx);
        }

        None // No ready connections available
    }

    fn on_request_sent(&mut self, conn_idx: usize, _key: u64) {
        // Mark connection as not ready until response arrives
        if conn_idx < self.ready_flags.len() {
            self.ready_flags[conn_idx] = false;
        }

        // Increment global counter
        self.global_in_flight += 1;
    }

    fn on_response_received(&mut self, conn_idx: usize, _key: u64, _latency: Duration) {
        // Mark connection as ready again
        if conn_idx < self.ready_flags.len() {
            self.ready_flags[conn_idx] = true;
        }

        // Decrement global counter
        self.global_in_flight = self.global_in_flight.saturating_sub(1);
    }

    fn reset(&mut self) {
        self.counter = 0;
        self.ready_flags.fill(true);
        self.global_in_flight = 0;
    }
}

/// Latency-aware scheduler (experimental)
///
/// Tracks per-connection latency and routes to faster connections.
/// This can help avoid slow connections and improve tail latencies.
///
/// **Note**: Requires tracking average latency per connection, which
/// may add overhead. Use only when latency variance between connections
/// is significant.
pub struct LatencyAwareScheduler {
    /// Recent latency samples per connection
    latency_history: Vec<VecDeque<Duration>>,
    /// Window size for averaging
    window_size: usize,
    /// Fallback counter for round-robin when latencies are equal
    fallback_counter: usize,
}

impl LatencyAwareScheduler {
    /// Create a new latency-aware scheduler
    ///
    /// # Parameters
    /// - `num_connections`: Number of connections in the pool
    /// - `window_size`: Number of recent samples to track per connection
    pub fn new(num_connections: usize, window_size: usize) -> Self {
        Self {
            latency_history: vec![VecDeque::with_capacity(window_size); num_connections],
            window_size,
            fallback_counter: 0,
        }
    }

    /// Calculate average latency for a connection
    fn avg_latency(&self, conn_idx: usize) -> Option<Duration> {
        if conn_idx >= self.latency_history.len() {
            return None;
        }

        let history = &self.latency_history[conn_idx];
        if history.is_empty() {
            return None;
        }

        let sum: Duration = history.iter().sum();
        Some(sum / history.len() as u32)
    }
}

impl Scheduler for LatencyAwareScheduler {
    fn select_connection(&mut self, context: &SchedulerContext) -> Option<usize> {
        // Find available connections
        let available: Vec<usize> = context
            .connections
            .iter()
            .filter(|conn| !conn.closed && conn.pending_count < context.max_pending_per_conn)
            .map(|conn| conn.idx)
            .collect();

        if available.is_empty() {
            return None;
        }

        // Select connection with lowest average latency
        let best = available.iter().min_by_key(|&&idx| self.avg_latency(idx)).copied();

        // Fallback to round-robin if no latency data
        best.or_else(|| {
            let idx = available[self.fallback_counter % available.len()];
            self.fallback_counter = self.fallback_counter.wrapping_add(1);
            Some(idx)
        })
    }

    fn on_response_received(&mut self, conn_idx: usize, _key: u64, latency: Duration) {
        if conn_idx < self.latency_history.len() {
            let history = &mut self.latency_history[conn_idx];

            if history.len() >= self.window_size {
                history.pop_front();
            }
            history.push_back(latency);
        }
    }

    fn reset(&mut self) {
        for history in &mut self.latency_history {
            history.clear();
        }
        self.fallback_counter = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_context(conn_states: Vec<ConnectionState>) -> SchedulerContext<'static> {
        let leaked: &'static [ConnectionState] = Box::leak(conn_states.into_boxed_slice());
        SchedulerContext {
            key: 42,
            connections: leaked,
            max_pending_per_conn: 10,
        }
    }

    #[test]
    fn test_round_robin_basic() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 1,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 2,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
        ];

        let context = make_test_context(connections);
        let mut scheduler = RoundRobinScheduler::new();

        // Should cycle through 0, 1, 2, 0, 1, 2...
        assert_eq!(scheduler.select_connection(&context), Some(0));
        assert_eq!(scheduler.select_connection(&context), Some(1));
        assert_eq!(scheduler.select_connection(&context), Some(2));
        assert_eq!(scheduler.select_connection(&context), Some(0));
    }

    #[test]
    fn test_round_robin_skips_full() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 10, // Full
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 1,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
        ];

        let context = make_test_context(connections);
        let mut scheduler = RoundRobinScheduler::new();

        // Should skip connection 0 (full) and select 1
        assert_eq!(scheduler.select_connection(&context), Some(1));
    }

    #[test]
    fn test_key_affinity() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 1,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 2,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
        ];

        let context = make_test_context(connections);
        let mut scheduler = KeyAffinityScheduler::new();

        // Key 42 % 3 = 0, should always select connection 0
        let mut test_context = SchedulerContext {
            key: 42,
            connections: context.connections,
            max_pending_per_conn: 10,
        };

        assert_eq!(scheduler.select_connection(&test_context), Some(0));
        assert_eq!(scheduler.select_connection(&test_context), Some(0));

        // Key 43 % 3 = 1, should select connection 1
        test_context.key = 43;
        assert_eq!(scheduler.select_connection(&test_context), Some(1));
    }

    #[test]
    fn test_load_based() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 5,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 1,
                pending_count: 2, // Least loaded
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 2,
                pending_count: 8,
                closed: false,
                avg_latency: None,
                ready: true,
            },
        ];

        let context = make_test_context(connections);
        let mut scheduler = LoadBasedScheduler::new();

        // Should always select connection 1 (least loaded)
        assert_eq!(scheduler.select_connection(&context), Some(1));
    }

    #[test]
    fn test_zipfian_hot_keys() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            }, // Fast lane
            ConnectionState {
                idx: 1,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            }, // Fast lane
            ConnectionState {
                idx: 2,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            }, // Normal
            ConnectionState {
                idx: 3,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            }, // Normal
        ];

        let context = make_test_context(connections);
        let mut scheduler = ZipfianScheduler::new(2, 100); // 2 fast lanes, keys < 100 are hot

        // Hot key (50 < 100) should go to fast lanes (0 or 1)
        let mut hot_context = SchedulerContext {
            key: 50,
            connections: context.connections,
            max_pending_per_conn: 10,
        };

        let conn = scheduler.select_connection(&hot_context).unwrap();
        assert!(conn < 2, "Hot key should use fast lane (0 or 1)");

        // Cold key (150 >= 100) should go to normal lanes (2 or 3)
        hot_context.key = 150;
        let conn = scheduler.select_connection(&hot_context).unwrap();
        assert!(conn >= 2, "Cold key should use normal lane (2 or 3)");
    }

    #[test]
    fn test_random_scheduler() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 1,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 2,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
        ];

        let context = make_test_context(connections);
        let mut scheduler = RandomScheduler::new();

        // Should select valid connections (0, 1, or 2)
        for _ in 0..10 {
            let conn = scheduler.select_connection(&context).unwrap();
            assert!(conn < 3);
        }
    }

    #[test]
    fn test_latency_aware() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 1,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
        ];

        let context = make_test_context(connections);
        let mut scheduler = LatencyAwareScheduler::new(2, 10);

        // Initially no latency data, should select something
        assert!(scheduler.select_connection(&context).is_some());

        // Record some latencies
        scheduler.on_response_received(0, 1, Duration::from_millis(10));
        scheduler.on_response_received(0, 2, Duration::from_millis(12));
        scheduler.on_response_received(1, 3, Duration::from_millis(5));
        scheduler.on_response_received(1, 4, Duration::from_millis(6));

        // Connection 1 has lower average latency (5.5ms vs 11ms), should prefer it
        assert_eq!(scheduler.select_connection(&context), Some(1));
    }

    #[test]
    fn test_all_connections_busy() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 10, // Full
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 1,
                pending_count: 10, // Full
                closed: false,
                avg_latency: None,
                ready: true,
            },
        ];

        let context = make_test_context(connections);

        // All schedulers should return None when all connections busy
        assert_eq!(RoundRobinScheduler::new().select_connection(&context), None);
        assert_eq!(KeyAffinityScheduler::new().select_connection(&context), None);
        assert_eq!(LoadBasedScheduler::new().select_connection(&context), None);
    }

    #[test]
    fn test_closed_connections_skipped() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 0,
                closed: true, // Closed
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 1,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
        ];

        let context = make_test_context(connections);
        let mut scheduler = RoundRobinScheduler::new();

        // Should skip closed connection 0 and select 1
        assert_eq!(scheduler.select_connection(&context), Some(1));
    }

    #[test]
    fn test_open_loop_round_robin() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 1,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 2,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
        ];

        let context = make_test_context(connections);
        let mut scheduler = OpenLoopRoundRobinScheduler::new();

        // Should cycle through 0, 1, 2, 0, 1, 2...
        assert_eq!(scheduler.select_connection(&context), Some(0));
        assert_eq!(scheduler.select_connection(&context), Some(1));
        assert_eq!(scheduler.select_connection(&context), Some(2));
        assert_eq!(scheduler.select_connection(&context), Some(0));
    }

    #[test]
    fn test_open_loop_ignores_ready_flag() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: false, // Not ready, but open-loop should still use it
            },
            ConnectionState {
                idx: 1,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: false,
            },
        ];

        let context = make_test_context(connections);
        let mut scheduler = OpenLoopRoundRobinScheduler::new();

        // Open-loop ignores ready flag, should still select connections
        assert_eq!(scheduler.select_connection(&context), Some(0));
        assert_eq!(scheduler.select_connection(&context), Some(1));
    }

    #[test]
    fn test_open_loop_respects_pending_limit() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 10, // At max
                closed: false,
                avg_latency: None,
                ready: false,
            },
            ConnectionState {
                idx: 1,
                pending_count: 5, // Has capacity
                closed: false,
                avg_latency: None,
                ready: true,
            },
        ];

        let context = make_test_context(connections);
        let mut scheduler = OpenLoopRoundRobinScheduler::new();

        // Should skip connection 0 (full) and select 1
        assert_eq!(scheduler.select_connection(&context), Some(1));
    }

    #[test]
    fn test_closed_loop_per_connection() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 1,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 2,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
        ];

        let context = make_test_context(connections);
        let mut scheduler = ClosedLoopRoundRobinScheduler::per_connection(3, 1);

        // Round-robin among ready connections
        assert_eq!(scheduler.select_connection(&context), Some(0));
        assert_eq!(scheduler.select_connection(&context), Some(1));
        assert_eq!(scheduler.select_connection(&context), Some(2));
        assert_eq!(scheduler.select_connection(&context), Some(0));
    }

    #[test]
    fn test_closed_loop_respects_ready_flag() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: false, // Not ready
            },
            ConnectionState {
                idx: 1,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true, // Ready
            },
        ];

        let context = make_test_context(connections);
        let mut scheduler = ClosedLoopRoundRobinScheduler::per_connection(2, 1);

        // Should only select ready connection (1)
        assert_eq!(scheduler.select_connection(&context), Some(1));
        assert_eq!(scheduler.select_connection(&context), Some(1));
    }

    #[test]
    fn test_closed_loop_internal_ready_tracking() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 1,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
        ];

        let context = make_test_context(connections);
        let mut scheduler = ClosedLoopRoundRobinScheduler::per_connection(2, 1);

        // Select connection 0
        assert_eq!(scheduler.select_connection(&context), Some(0));

        // Mark connection 0 as sent (becomes not ready internally)
        scheduler.on_request_sent(0, 1);

        // Should now select connection 1 (connection 0 not ready internally)
        assert_eq!(scheduler.select_connection(&context), Some(1));

        // Mark connection 0 as received response (becomes ready again)
        scheduler.on_response_received(0, 1, Duration::from_millis(10));

        // Should now be able to select connection 0 again
        assert_eq!(scheduler.select_connection(&context), Some(0));
    }

    #[test]
    fn test_closed_loop_global_limit() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 1,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 2,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
        ];

        let context = make_test_context(connections);
        let mut scheduler = ClosedLoopRoundRobinScheduler::global(3, 1);

        // Can select first connection
        assert_eq!(scheduler.select_connection(&context), Some(0));

        // Mark as sent - global limit reached
        scheduler.on_request_sent(0, 1);

        // Should return None (global limit of 1 reached)
        assert_eq!(scheduler.select_connection(&context), None);

        // Receive response - global limit freed
        scheduler.on_response_received(0, 1, Duration::from_millis(10));

        // Can select again (should be connection 1 due to round-robin)
        assert_eq!(scheduler.select_connection(&context), Some(1));
    }

    #[test]
    fn test_closed_loop_global_limit_multiple() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 1,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 2,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
        ];

        let context = make_test_context(connections);
        let mut scheduler = ClosedLoopRoundRobinScheduler::global(3, 2);

        // Can select two connections
        assert_eq!(scheduler.select_connection(&context), Some(0));
        scheduler.on_request_sent(0, 1);

        assert_eq!(scheduler.select_connection(&context), Some(1));
        scheduler.on_request_sent(1, 2);

        // Global limit reached (2 in-flight)
        assert_eq!(scheduler.select_connection(&context), None);

        // Receive one response
        scheduler.on_response_received(0, 1, Duration::from_millis(10));

        // Can select again (global count now 1)
        assert_eq!(scheduler.select_connection(&context), Some(2));
    }

    #[test]
    fn test_closed_loop_reset() {
        let connections = vec![
            ConnectionState {
                idx: 0,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
            ConnectionState {
                idx: 1,
                pending_count: 0,
                closed: false,
                avg_latency: None,
                ready: true,
            },
        ];

        let context = make_test_context(connections);
        let mut scheduler = ClosedLoopRoundRobinScheduler::global(2, 1);

        // Send request
        scheduler.select_connection(&context);
        scheduler.on_request_sent(0, 1);

        // Global limit reached
        assert_eq!(scheduler.select_connection(&context), None);

        // Reset
        scheduler.reset();

        // Should be able to select again
        assert_eq!(scheduler.select_connection(&context), Some(0));
    }
}
