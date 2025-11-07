//! Temporal Scheduler - Per-Connection Traffic Model
//!
//! This scheduler implements the "async runtime" model where each connection has its own
//! independent traffic policy. The scheduler simply picks whichever connection's next
//! request is due soonest (like an async runtime picking the next ready coroutine).
//!
//! Key differences from UnifiedScheduler:
//! - No global timing state (each connection is independent)
//! - No ConnectionSelector (pure temporal scheduling)
//! - Uses a min-heap to efficiently find the connection with earliest ready time
//!
//! ## Architecture
//!
//! ```text
//! TemporalScheduler
//! ├── Min-Heap of (next_send_time, conn_idx)
//! └── Connections (each with own Policy)
//!     ├── Connection 0: FixedRatePolicy(1000 req/s)
//!     ├── Connection 1: PoissonPolicy(500 req/s)
//!     └── Connection 2: ClosedLoopPolicy
//! ```

use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Entry in the temporal scheduler's priority queue
///
/// Orders connections by their next send time (earliest first).
/// Connections with `None` (closed-loop, always ready) have highest priority.
#[derive(Debug, Clone, Eq, PartialEq)]
struct SchedulerEntry {
    /// When this connection should send next (None = ready immediately)
    next_send_time_ns: Option<u64>,
    /// Connection index
    conn_idx: usize,
}

impl SchedulerEntry {
    fn new(next_send_time_ns: Option<u64>, conn_idx: usize) -> Self {
        Self { next_send_time_ns, conn_idx }
    }
}

impl Ord for SchedulerEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap (BinaryHeap is max-heap by default)
        // None (closed-loop) has highest priority (smallest)
        match (self.next_send_time_ns, other.next_send_time_ns) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater, // None > Some for max-heap = highest priority
            (Some(_), None) => Ordering::Less,
            (Some(a), Some(b)) => b.cmp(&a), // Reverse order for min-heap
        }
    }
}

impl PartialOrd for SchedulerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Temporal scheduler for per-connection traffic models
///
/// This scheduler maintains a min-heap of connections ordered by their next send time.
/// Each connection has its own independent policy that determines when it should send.
///
/// # Example
///
/// ```ignore
/// // Create scheduler
/// let mut scheduler = TemporalScheduler::new();
///
/// // Add connections (with their ready times computed from their policies)
/// scheduler.add_connection(0, Some(1000)); // conn 0 ready at t=1000ns
/// scheduler.add_connection(1, Some(2000)); // conn 1 ready at t=2000ns
/// scheduler.add_connection(2, None);       // conn 2 ready immediately (closed-loop)
///
/// // Pick next ready connection
/// let current_time = 500;
/// if let Some(conn_idx) = scheduler.pick_ready_connection(current_time) {
///     println!("Send on connection {}", conn_idx);
/// }
/// ```
pub struct TemporalScheduler {
    /// Min-heap of connections ordered by next send time
    heap: BinaryHeap<SchedulerEntry>,
}

impl TemporalScheduler {
    /// Create a new temporal scheduler
    pub fn new() -> Self {
        Self { heap: BinaryHeap::new() }
    }

    /// Add or update a connection's next send time in the scheduler
    ///
    /// # Parameters
    /// - `conn_idx`: Connection index
    /// - `next_send_time_ns`: When this connection should send next (None = ready immediately)
    pub fn update_connection(&mut self, conn_idx: usize, next_send_time_ns: Option<u64>) {
        self.heap.push(SchedulerEntry::new(next_send_time_ns, conn_idx));
    }

    /// Pick the next connection that is ready to send
    ///
    /// Returns the connection index whose next send time is <= current_time,
    /// or None if no connection is ready.
    ///
    /// # Parameters
    /// - `current_time_ns`: Current time in nanoseconds
    ///
    /// # Returns
    /// - `Some(conn_idx)`: Connection index that is ready to send
    /// - `None`: No connection is ready yet
    pub fn pick_ready_connection(&mut self, current_time_ns: u64) -> Option<usize> {
        // Peek at the top of the heap (earliest ready time)
        if let Some(entry) = self.heap.peek() {
            match entry.next_send_time_ns {
                None => {
                    // Closed-loop: always ready
                    self.heap.pop().map(|e| e.conn_idx)
                }
                Some(ready_time) if ready_time <= current_time_ns => {
                    // Ready to send
                    self.heap.pop().map(|e| e.conn_idx)
                }
                Some(_) => {
                    // Not ready yet (and nothing else in heap will be ready either)
                    None
                }
            }
        } else {
            None
        }
    }

    /// Get the next ready time (when the next connection will be ready to send)
    ///
    /// Useful for sleep/busy-wait decisions.
    ///
    /// # Returns
    /// - `Some(time_ns)`: Next time when a connection will be ready
    /// - `None`: A connection is ready now (closed-loop) or no connections
    pub fn next_ready_time(&self) -> Option<u64> {
        self.heap.peek()?.next_send_time_ns
    }

    /// Check if any connection is ready to send now
    ///
    /// # Parameters
    /// - `current_time_ns`: Current time in nanoseconds
    pub fn has_ready_connection(&self, current_time_ns: u64) -> bool {
        if let Some(entry) = self.heap.peek() {
            match entry.next_send_time_ns {
                None => true, // Closed-loop: always ready
                Some(ready_time) => ready_time <= current_time_ns,
            }
        } else {
            false
        }
    }

    /// Clear all connections from the scheduler
    pub fn clear(&mut self) {
        self.heap.clear();
    }

    /// Get the number of connections in the scheduler
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Check if the scheduler is empty
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

impl Default for TemporalScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scheduler_entry_ordering() {
        // None (closed-loop) should have highest priority
        let closed_loop = SchedulerEntry::new(None, 0);
        let timed = SchedulerEntry::new(Some(1000), 1);

        // In max-heap, greater means higher priority (popped first)
        assert!(closed_loop > timed);
    }

    #[test]
    fn test_scheduler_entry_ordering_times() {
        // Earlier times should have higher priority
        let early = SchedulerEntry::new(Some(1000), 0);
        let late = SchedulerEntry::new(Some(2000), 1);

        assert!(early > late); // early has higher priority
    }

    #[test]
    fn test_temporal_scheduler_closed_loop() {
        let mut scheduler = TemporalScheduler::new();

        // Add closed-loop connection (always ready)
        scheduler.update_connection(0, None);

        // Should be ready immediately
        let ready = scheduler.pick_ready_connection(0);
        assert_eq!(ready, Some(0));
    }

    #[test]
    fn test_temporal_scheduler_timed() {
        let mut scheduler = TemporalScheduler::new();

        // Add connection ready at t=1000
        scheduler.update_connection(0, Some(1000));

        // Not ready at t=500
        let ready = scheduler.pick_ready_connection(500);
        assert_eq!(ready, None);

        // Ready at t=1000
        scheduler.update_connection(0, Some(1000));
        let ready = scheduler.pick_ready_connection(1000);
        assert_eq!(ready, Some(0));

        // Ready at t=1500 (past ready time)
        scheduler.update_connection(0, Some(1000));
        let ready = scheduler.pick_ready_connection(1500);
        assert_eq!(ready, Some(0));
    }

    #[test]
    fn test_temporal_scheduler_multiple_connections() {
        // Test 1: Pick earliest ready connection
        let mut scheduler = TemporalScheduler::new();
        scheduler.update_connection(0, Some(3000));
        scheduler.update_connection(1, Some(1000)); // Earliest
        scheduler.update_connection(2, Some(2000));

        // At t=500: none ready
        assert_eq!(scheduler.pick_ready_connection(500), None);

        // At t=1000: conn 1 ready (earliest), heap is cleared after pick
        assert_eq!(scheduler.pick_ready_connection(1000), Some(1));

        // Test 2: Pick second earliest
        scheduler.clear();
        scheduler.update_connection(0, Some(3000));
        scheduler.update_connection(2, Some(2000)); // Now earliest

        assert_eq!(scheduler.pick_ready_connection(2000), Some(2));

        // Test 3: Pick last one
        scheduler.clear();
        scheduler.update_connection(0, Some(3000));

        assert_eq!(scheduler.pick_ready_connection(3000), Some(0));
    }

    #[test]
    fn test_temporal_scheduler_closed_loop_priority() {
        let mut scheduler = TemporalScheduler::new();

        // Add timed and closed-loop connections
        scheduler.update_connection(0, Some(1000)); // Timed
        scheduler.update_connection(1, None); // Closed-loop (highest priority)
        scheduler.update_connection(2, Some(500)); // Timed (earlier)

        // Closed-loop should be picked first
        assert_eq!(scheduler.pick_ready_connection(0), Some(1));
    }

    #[test]
    fn test_temporal_scheduler_next_ready_time() {
        let mut scheduler = TemporalScheduler::new();

        // Empty scheduler
        assert_eq!(scheduler.next_ready_time(), None);

        // Add connections
        scheduler.update_connection(0, Some(3000));
        scheduler.update_connection(1, Some(1000)); // Earliest
        scheduler.update_connection(2, Some(2000));

        // Should return earliest time
        assert_eq!(scheduler.next_ready_time(), Some(1000));
    }

    #[test]
    fn test_temporal_scheduler_next_ready_time_closed_loop() {
        let mut scheduler = TemporalScheduler::new();

        // Add closed-loop connection
        scheduler.update_connection(0, None);
        scheduler.update_connection(1, Some(1000));

        // Closed-loop means ready now (None)
        assert_eq!(scheduler.next_ready_time(), None);
    }

    #[test]
    fn test_temporal_scheduler_has_ready_connection() {
        let mut scheduler = TemporalScheduler::new();

        // Empty scheduler
        assert!(!scheduler.has_ready_connection(0));

        // Add connection ready at t=1000
        scheduler.update_connection(0, Some(1000));

        assert!(!scheduler.has_ready_connection(500)); // Not ready
        assert!(scheduler.has_ready_connection(1000)); // Ready
        assert!(scheduler.has_ready_connection(1500)); // Ready (past)
    }

    #[test]
    fn test_temporal_scheduler_clear() {
        let mut scheduler = TemporalScheduler::new();

        scheduler.update_connection(0, Some(1000));
        scheduler.update_connection(1, Some(2000));

        assert_eq!(scheduler.len(), 2);

        scheduler.clear();

        assert_eq!(scheduler.len(), 0);
        assert!(scheduler.is_empty());
    }

    #[test]
    fn test_temporal_scheduler_update_same_connection() {
        let mut scheduler = TemporalScheduler::new();

        // Add connection
        scheduler.update_connection(0, Some(1000));

        // Update same connection with new time
        scheduler.update_connection(0, Some(2000));

        // Both entries are in the heap, but we'll pick the earliest one first
        // Note: This means we may have duplicate entries, but that's okay
        // The connection's actual policy will determine the real next send time
        assert_eq!(scheduler.len(), 2);
    }
}
