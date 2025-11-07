//! Connection selector implementations
//!
//! Connection selectors decide WHICH connection to use for each request,
//! implementing various routing and load-balancing strategies.

use super::{ConnectionContext, ConnectionSelector};
use rand::seq::SliceRandom;
use std::collections::VecDeque;
use std::time::Duration;

/// Round-robin connection selector
///
/// Cycles through connections in order. Simple and effective for most workloads.
/// Matches the default behavior of the original Lancet implementation.
pub struct RoundRobinSelector {
    counter: usize,
}

impl RoundRobinSelector {
    pub fn new() -> Self {
        Self { counter: 0 }
    }
}

impl Default for RoundRobinSelector {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionSelector for RoundRobinSelector {
    fn select_connection(&mut self, context: &ConnectionContext) -> Option<usize> {
        let total = context.connections.len();

        for _ in 0..total {
            let idx = self.counter % total;
            self.counter = self.counter.wrapping_add(1);

            let conn = &context.connections[idx];
            if !conn.closed && conn.pending_count < context.max_pending_per_conn {
                return Some(idx);
            }
        }

        None
    }

    fn reset(&mut self) {
        self.counter = 0;
    }

    fn name(&self) -> &'static str {
        "RoundRobin"
    }
}

/// Key-affinity connection selector
///
/// Routes requests for the same key to the same connection, improving
/// server-side cache locality. Falls back to round-robin if the affinity
/// connection is unavailable.
pub struct KeyAffinitySelector {
    fallback_counter: usize,
}

impl KeyAffinitySelector {
    pub fn new() -> Self {
        Self { fallback_counter: 0 }
    }

    fn hash_key(key: u64, num_conns: usize) -> usize {
        (key as usize) % num_conns
    }
}

impl Default for KeyAffinitySelector {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionSelector for KeyAffinitySelector {
    fn select_connection(&mut self, context: &ConnectionContext) -> Option<usize> {
        let total = context.connections.len();

        // Try affinity connection first
        let affinity_idx = Self::hash_key(context.key, total);
        let conn = &context.connections[affinity_idx];

        if !conn.closed && conn.pending_count < context.max_pending_per_conn {
            return Some(affinity_idx);
        }

        // Fallback to round-robin
        for _ in 0..total {
            let idx = self.fallback_counter % total;
            self.fallback_counter = self.fallback_counter.wrapping_add(1);

            if idx == affinity_idx {
                continue;
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

    fn name(&self) -> &'static str {
        "KeyAffinity"
    }
}

/// Load-based connection selector
///
/// Routes to the connection with the fewest pending requests.
pub struct LoadBasedSelector;

impl LoadBasedSelector {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LoadBasedSelector {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionSelector for LoadBasedSelector {
    fn select_connection(&mut self, context: &ConnectionContext) -> Option<usize> {
        context
            .connections
            .iter()
            .filter(|conn| !conn.closed && conn.pending_count < context.max_pending_per_conn)
            .min_by_key(|conn| conn.pending_count)
            .map(|conn| conn.idx)
    }

    fn name(&self) -> &'static str {
        "LoadBased"
    }
}

/// Zipfian-aware connection selector with fast lanes for hot keys
///
/// Partitions connections into "fast lanes" for hot keys and "normal lanes" for cold keys.
/// This reduces head-of-line blocking in Zipfian workloads.
pub struct ZipfianSelector {
    num_fast_lanes: usize,
    hot_key_threshold: u64,
    fast_lane_counter: usize,
    normal_lane_counter: usize,
}

impl ZipfianSelector {
    pub fn new(num_fast_lanes: usize, hot_key_threshold: u64) -> Self {
        Self {
            num_fast_lanes,
            hot_key_threshold,
            fast_lane_counter: 0,
            normal_lane_counter: 0,
        }
    }

    pub fn num_fast_lanes(&self) -> usize {
        self.num_fast_lanes
    }

    pub fn hot_key_threshold(&self) -> u64 {
        self.hot_key_threshold
    }
}

impl ConnectionSelector for ZipfianSelector {
    fn select_connection(&mut self, context: &ConnectionContext) -> Option<usize> {
        let total = context.connections.len();

        if total <= self.num_fast_lanes {
            // Not enough connections for partitioning
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
            // Route hot keys to fast lanes
            for _ in 0..self.num_fast_lanes {
                let idx = self.fast_lane_counter % self.num_fast_lanes;
                self.fast_lane_counter = self.fast_lane_counter.wrapping_add(1);

                let conn = &context.connections[idx];
                if !conn.closed && conn.pending_count < context.max_pending_per_conn {
                    return Some(idx);
                }
            }
        } else {
            // Route cold keys to normal lanes
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

    fn name(&self) -> &'static str {
        "Zipfian"
    }
}

/// Random connection selector
///
/// Selects a random available connection. Useful for testing.
pub struct RandomSelector;

impl RandomSelector {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RandomSelector {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionSelector for RandomSelector {
    fn select_connection(&mut self, context: &ConnectionContext) -> Option<usize> {
        use rand::thread_rng;

        let available: Vec<usize> = context
            .connections
            .iter()
            .filter(|conn| !conn.closed && conn.pending_count < context.max_pending_per_conn)
            .map(|conn| conn.idx)
            .collect();

        available.choose(&mut thread_rng()).copied()
    }

    fn name(&self) -> &'static str {
        "Random"
    }
}

/// Latency-aware connection selector
///
/// Routes requests to connections with lower average latency.
pub struct LatencyAwareSelector {
    latency_history: Vec<VecDeque<Duration>>,
    window_size: usize,
    fallback_counter: usize,
}

impl LatencyAwareSelector {
    pub fn new(num_connections: usize, window_size: usize) -> Self {
        Self {
            latency_history: vec![VecDeque::with_capacity(window_size); num_connections],
            window_size,
            fallback_counter: 0,
        }
    }

    fn avg_latency(&self, conn_idx: usize) -> Option<Duration> {
        if conn_idx >= self.latency_history.len() {
            return None;
        }

        let history = &self.latency_history[conn_idx];
        if history.is_empty() {
            return None;
        }

        let sum: Duration = history.iter().copied().sum();
        Some(sum / history.len() as u32)
    }
}

impl ConnectionSelector for LatencyAwareSelector {
    fn select_connection(&mut self, context: &ConnectionContext) -> Option<usize> {
        let available: Vec<usize> = context
            .connections
            .iter()
            .filter(|conn| !conn.closed && conn.pending_count < context.max_pending_per_conn)
            .map(|conn| conn.idx)
            .collect();

        if available.is_empty() {
            return None;
        }

        let best = available.iter().min_by_key(|&&idx| self.avg_latency(idx)).copied();

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

    fn name(&self) -> &'static str {
        "LatencyAware"
    }
}

/// Closed-loop connection selector (respects readiness flags)
///
/// Only selects connections that are marked as ready. Tracks ready flags
/// and enforces concurrency limits (per-connection or global).
pub struct ClosedLoopSelector {
    counter: usize,
    ready_flags: Vec<bool>,
    global_in_flight: usize,
    max_concurrent: MaxConcurrentRequests,
}

#[derive(Debug, Clone, Copy)]
pub enum MaxConcurrentRequests {
    PerConnection(usize),
    Global(usize),
}

impl ClosedLoopSelector {
    pub fn new(num_connections: usize, max_concurrent: MaxConcurrentRequests) -> Self {
        Self {
            counter: 0,
            ready_flags: vec![true; num_connections],
            global_in_flight: 0,
            max_concurrent,
        }
    }

    pub fn per_connection(num_connections: usize, max_per_conn: usize) -> Self {
        Self::new(num_connections, MaxConcurrentRequests::PerConnection(max_per_conn))
    }

    pub fn global(num_connections: usize, max_global: usize) -> Self {
        Self::new(num_connections, MaxConcurrentRequests::Global(max_global))
    }
}

impl ConnectionSelector for ClosedLoopSelector {
    fn select_connection(&mut self, context: &ConnectionContext) -> Option<usize> {
        let total = context.connections.len();

        // Check global limit
        if let MaxConcurrentRequests::Global(max) = self.max_concurrent {
            if self.global_in_flight >= max {
                return None;
            }
        }

        // Resize ready_flags if needed
        if self.ready_flags.len() != total {
            self.ready_flags.resize(total, true);
        }

        for _ in 0..total {
            let idx = self.counter % total;
            self.counter = self.counter.wrapping_add(1);

            let conn = &context.connections[idx];

            if conn.closed || !conn.ready {
                continue;
            }

            if idx < self.ready_flags.len() && !self.ready_flags[idx] {
                continue;
            }

            if let MaxConcurrentRequests::PerConnection(max_per_conn) = self.max_concurrent {
                if conn.pending_count >= max_per_conn {
                    continue;
                }
            }

            return Some(idx);
        }

        None
    }

    fn on_request_sent(&mut self, conn_idx: usize, _key: u64) {
        if conn_idx < self.ready_flags.len() {
            self.ready_flags[conn_idx] = false;
        }
        self.global_in_flight += 1;
    }

    fn on_response_received(&mut self, conn_idx: usize, _key: u64, _latency: Duration) {
        if conn_idx < self.ready_flags.len() {
            self.ready_flags[conn_idx] = true;
        }
        self.global_in_flight = self.global_in_flight.saturating_sub(1);
    }

    fn reset(&mut self) {
        self.counter = 0;
        self.ready_flags.fill(true);
        self.global_in_flight = 0;
    }

    fn name(&self) -> &'static str {
        "ClosedLoop"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::ConnState;

    fn make_test_connections(states: Vec<(usize, usize, bool, bool)>) -> Vec<ConnState> {
        // states: (idx, pending_count, closed, ready)
        states
            .into_iter()
            .map(|(idx, pending, closed, ready)| ConnState {
                idx,
                pending_count: pending,
                closed,
                avg_latency: None,
                ready,
            })
            .collect()
    }

    #[test]
    fn test_round_robin_selector() {
        let connections = make_test_connections(vec![
            (0, 0, false, true),
            (1, 0, false, true),
            (2, 0, false, true),
        ]);

        let ctx = ConnectionContext {
            key: 42,
            connections: &connections,
            max_pending_per_conn: 10,
            current_time_ns: 0,
        };

        let mut selector = RoundRobinSelector::new();

        assert_eq!(selector.select_connection(&ctx), Some(0));
        assert_eq!(selector.select_connection(&ctx), Some(1));
        assert_eq!(selector.select_connection(&ctx), Some(2));
        assert_eq!(selector.select_connection(&ctx), Some(0));
    }

    #[test]
    fn test_key_affinity_selector() {
        let connections = make_test_connections(vec![
            (0, 0, false, true),
            (1, 0, false, true),
            (2, 0, false, true),
        ]);

        let mut selector = KeyAffinitySelector::new();

        // Key 42 % 3 = 0
        let ctx = ConnectionContext {
            key: 42,
            connections: &connections,
            max_pending_per_conn: 10,
            current_time_ns: 0,
        };

        assert_eq!(selector.select_connection(&ctx), Some(0));
        assert_eq!(selector.select_connection(&ctx), Some(0));
    }

    #[test]
    fn test_closed_loop_selector_respects_ready() {
        let connections = make_test_connections(vec![
            (0, 0, false, false), // Not ready
            (1, 0, false, true),  // Ready
        ]);

        let ctx = ConnectionContext {
            key: 42,
            connections: &connections,
            max_pending_per_conn: 10,
            current_time_ns: 0,
        };

        let mut selector = ClosedLoopSelector::per_connection(2, 1);

        // Should only select connection 1 (ready)
        assert_eq!(selector.select_connection(&ctx), Some(1));
    }

    #[test]
    fn test_closed_loop_selector_global_limit() {
        let connections = make_test_connections(vec![(0, 0, false, true), (1, 0, false, true)]);

        let ctx = ConnectionContext {
            key: 42,
            connections: &connections,
            max_pending_per_conn: 10,
            current_time_ns: 0,
        };

        let mut selector = ClosedLoopSelector::global(2, 1);

        // Can select first
        assert_eq!(selector.select_connection(&ctx), Some(0));
        selector.on_request_sent(0, 42);

        // Global limit reached
        assert_eq!(selector.select_connection(&ctx), None);

        // After response, can select again
        selector.on_response_received(0, 42, Duration::from_millis(10));
        assert_eq!(selector.select_connection(&ctx), Some(1));
    }
}
