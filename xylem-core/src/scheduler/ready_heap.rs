//! ReadyHeap - Min-heap for efficient connection scheduling
//!
//! This is a data structure (not a scheduler) that maintains connections in a min-heap
//! ordered by their next send time. It enables O(log N) lookup of the next ready
//! connection instead of O(N) iteration.
//!
//! The actual scheduling decisions are made by each connection's `Policy` - this heap
//! just organizes them efficiently for lookup.
//!
//! ## Architecture
//!
//! ```text
//! ReadyHeap (data structure)
//! └── Min-Heap of (next_send_time, connection_id)
//!
//! ConnectionPool (owns the heap)
//! └── Connections (each with own Policy that decides next_send_time)
//!     ├── Connection 0: FixedRatePolicy(1000 req/s)
//!     ├── Connection 1: PoissonPolicy(500 req/s)
//!     └── Connection 2: ClosedLoopPolicy
//! ```

use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Entry in the ready heap
///
/// Orders connections by their next send time (earliest first).
/// Connections with `None` (closed-loop, always ready) have highest priority.
#[derive(Debug, Clone, Eq, PartialEq)]
struct HeapEntry {
    /// When this connection should send next (None = ready immediately)
    next_send_time_ns: Option<u64>,
    /// Connection index
    conn_idx: usize,
}

impl HeapEntry {
    fn new(next_send_time_ns: Option<u64>, conn_idx: usize) -> Self {
        Self { next_send_time_ns, conn_idx }
    }
}

impl Ord for HeapEntry {
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

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Min-heap of connections ordered by next send time
///
/// This is a data structure for efficient O(log N) lookup of the next ready connection.
/// Each connection's `Policy` decides when it should send - this heap just organizes
/// them for fast retrieval.
///
/// # Example
///
/// ```
/// use xylem_core::scheduler::ReadyHeap;
///
/// let mut heap = ReadyHeap::new();
///
/// // Add connections (with their ready times from their policies)
/// heap.push(0, Some(1000)); // conn 0 ready at t=1000ns
/// heap.push(1, Some(2000)); // conn 1 ready at t=2000ns
/// heap.push(2, None);       // conn 2 ready immediately (closed-loop)
///
/// // Pop next ready connection
/// let current_time = 500;
/// if let Some(conn_id) = heap.pop_ready(current_time) {
///     println!("Send on connection {}", conn_id);
/// }
/// ```
pub struct ReadyHeap {
    /// Min-heap of connections ordered by next send time
    heap: BinaryHeap<HeapEntry>,
}

impl ReadyHeap {
    /// Create a new ready heap
    pub fn new() -> Self {
        Self { heap: BinaryHeap::new() }
    }

    /// Add a connection to the heap with its next send time
    ///
    /// # Parameters
    /// - `connection_id`: Connection ID
    /// - `next_send_time_ns`: When this connection should send next (None = ready immediately)
    pub fn push(&mut self, connection_id: usize, next_send_time_ns: Option<u64>) {
        self.heap.push(HeapEntry::new(next_send_time_ns, connection_id));
    }

    /// Pop the next connection that is ready to send
    ///
    /// Returns the connection ID whose next send time is <= current_time,
    /// or None if no connection is ready.
    ///
    /// # Parameters
    /// - `current_time_ns`: Current time in nanoseconds
    ///
    /// # Returns
    /// - `Some(connection_id)`: Connection ID that is ready to send
    /// - `None`: No connection is ready yet
    pub fn pop_ready(&mut self, current_time_ns: u64) -> Option<usize> {
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

    /// Peek at the next ready time (when the next connection will be ready)
    ///
    /// Useful for sleep/busy-wait decisions.
    ///
    /// # Returns
    /// - `Some(time_ns)`: Next time when a connection will be ready
    /// - `None`: A connection is ready now (closed-loop) or heap is empty
    pub fn peek_next_time(&self) -> Option<u64> {
        self.heap.peek()?.next_send_time_ns
    }

    /// Check if any connection is ready to send now
    ///
    /// # Parameters
    /// - `current_time_ns`: Current time in nanoseconds
    pub fn has_ready(&self, current_time_ns: u64) -> bool {
        if let Some(entry) = self.heap.peek() {
            match entry.next_send_time_ns {
                None => true, // Closed-loop: always ready
                Some(ready_time) => ready_time <= current_time_ns,
            }
        } else {
            false
        }
    }

    /// Clear all entries from the heap
    pub fn clear(&mut self) {
        self.heap.clear();
    }

    /// Get the number of entries in the heap
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Check if the heap is empty
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

impl Default for ReadyHeap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heap_entry_ordering() {
        // None (closed-loop) should have highest priority
        let closed_loop = HeapEntry::new(None, 0);
        let timed = HeapEntry::new(Some(1000), 1);

        // In max-heap, greater means higher priority (popped first)
        assert!(closed_loop > timed);
    }

    #[test]
    fn test_heap_entry_ordering_times() {
        // Earlier times should have higher priority
        let early = HeapEntry::new(Some(1000), 0);
        let late = HeapEntry::new(Some(2000), 1);

        assert!(early > late); // early has higher priority
    }

    #[test]
    fn test_ready_heap_closed_loop() {
        let mut heap = ReadyHeap::new();

        // Add closed-loop connection (always ready)
        heap.push(0, None);

        // Should be ready immediately
        let ready = heap.pop_ready(0);
        assert_eq!(ready, Some(0));
    }

    #[test]
    fn test_ready_heap_timed() {
        let mut heap = ReadyHeap::new();

        // Add connection ready at t=1000
        heap.push(0, Some(1000));

        // Not ready at t=500
        let ready = heap.pop_ready(500);
        assert_eq!(ready, None);

        // Ready at t=1000
        heap.push(0, Some(1000));
        let ready = heap.pop_ready(1000);
        assert_eq!(ready, Some(0));

        // Ready at t=1500 (past ready time)
        heap.push(0, Some(1000));
        let ready = heap.pop_ready(1500);
        assert_eq!(ready, Some(0));
    }

    #[test]
    fn test_ready_heap_multiple_connections() {
        // Test 1: Pick earliest ready connection
        let mut heap = ReadyHeap::new();
        heap.push(0, Some(3000));
        heap.push(1, Some(1000)); // Earliest
        heap.push(2, Some(2000));

        // At t=500: none ready
        assert_eq!(heap.pop_ready(500), None);

        // At t=1000: conn 1 ready (earliest), heap is cleared after pick
        assert_eq!(heap.pop_ready(1000), Some(1));

        // Test 2: Pick second earliest
        heap.clear();
        heap.push(0, Some(3000));
        heap.push(2, Some(2000)); // Now earliest

        assert_eq!(heap.pop_ready(2000), Some(2));

        // Test 3: Pick last one
        heap.clear();
        heap.push(0, Some(3000));

        assert_eq!(heap.pop_ready(3000), Some(0));
    }

    #[test]
    fn test_ready_heap_closed_loop_priority() {
        let mut heap = ReadyHeap::new();

        // Add timed and closed-loop connections
        heap.push(0, Some(1000)); // Timed
        heap.push(1, None); // Closed-loop (highest priority)
        heap.push(2, Some(500)); // Timed (earlier)

        // Closed-loop should be picked first
        assert_eq!(heap.pop_ready(0), Some(1));
    }

    #[test]
    fn test_ready_heap_peek_next_time() {
        let mut heap = ReadyHeap::new();

        // Empty heap
        assert_eq!(heap.peek_next_time(), None);

        // Add connections
        heap.push(0, Some(3000));
        heap.push(1, Some(1000)); // Earliest
        heap.push(2, Some(2000));

        // Should return earliest time
        assert_eq!(heap.peek_next_time(), Some(1000));
    }

    #[test]
    fn test_ready_heap_peek_next_time_closed_loop() {
        let mut heap = ReadyHeap::new();

        // Add closed-loop connection
        heap.push(0, None);
        heap.push(1, Some(1000));

        // Closed-loop means ready now (None)
        assert_eq!(heap.peek_next_time(), None);
    }

    #[test]
    fn test_ready_heap_has_ready() {
        let mut heap = ReadyHeap::new();

        // Empty heap
        assert!(!heap.has_ready(0));

        // Add connection ready at t=1000
        heap.push(0, Some(1000));

        assert!(!heap.has_ready(500)); // Not ready
        assert!(heap.has_ready(1000)); // Ready
        assert!(heap.has_ready(1500)); // Ready (past)
    }

    #[test]
    fn test_ready_heap_clear() {
        let mut heap = ReadyHeap::new();

        heap.push(0, Some(1000));
        heap.push(1, Some(2000));

        assert_eq!(heap.len(), 2);

        heap.clear();

        assert_eq!(heap.len(), 0);
        assert!(heap.is_empty());
    }

    #[test]
    fn test_ready_heap_duplicate_entries() {
        let mut heap = ReadyHeap::new();

        // Add connection
        heap.push(0, Some(1000));

        // Add same connection with new time
        heap.push(0, Some(2000));

        // Both entries are in the heap, but we'll pick the earliest one first
        // Note: This means we may have duplicate entries, but that's okay
        // The connection's actual state will determine if it can actually send
        assert_eq!(heap.len(), 2);
    }
}
