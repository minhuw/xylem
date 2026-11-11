//! Per-Connection Traffic Model Scheduler
//!
//! This module provides a per-connection traffic model where each connection has its own
//! independent policy that determines when it should send requests.
//!
//! ## Architecture
//!
//! - **Policy**: Each connection has a `Policy` that decides when to send the next request
//!   (e.g., `ClosedLoopPolicy`, `FixedRatePolicy`, `PoissonPolicy`)
//! - **ReadyHeap**: A min-heap data structure for O(log N) lookup of the next ready connection
//! - **GroupDispatcher**: Under group dispatch, one arrival process shared by a
//!   thread's connections of a traffic group, which sends each arrival on the
//!   connection idle longest (see [`dispatch`])
//!
//! ## Example: Uniform Traffic
//!
//! ```
//! use xylem_core::scheduler::UniformPolicyScheduler;
//!
//! // All connections use Poisson arrivals at 1M req/s
//! let policy_scheduler = UniformPolicyScheduler::poisson(1_000_000.0).unwrap();
//! // This scheduler can be used with ConnectionPool to create uniform traffic
//! ```
//!
//! ## Example: Time-Varying Traffic
//!
//! ```
//! use xylem_core::workload::patterns::{RampPattern, LoadPattern};
//! use std::time::Duration;
//!
//! // Create a ramp pattern that goes from 1M to 10M req/s over 60 seconds
//! let pattern = RampPattern::new(1_000_000.0, 10_000_000.0, Duration::from_secs(60));
//!
//! // Get the rate at different time points
//! let rate_at_start = pattern.rate_at(Duration::from_secs(0));
//! let rate_at_30s = pattern.rate_at(Duration::from_secs(30));
//! let rate_at_60s = pattern.rate_at(Duration::from_secs(60));
//!
//! assert_eq!(rate_at_start, 1_000_000.0);
//! assert!((rate_at_30s - 5_500_000.0).abs() < 100_000.0); // Approximately 5.5M
//! assert_eq!(rate_at_60s, 10_000_000.0);
//! ```

pub mod dispatch;
pub mod policy;
pub mod ready_heap;

// Re-export main types
pub use policy::{
    AdaptivePolicy, ClosedLoopPolicy, FixedRatePolicy, GroupPolicyScheduler, PatternAwarePolicy,
    PatternPolicyScheduler, PoissonPolicy, Policy, PolicyScheduler, UniformPolicyScheduler,
};

pub use dispatch::{GroupDispatchScheduler, GroupDispatcher};
pub use ready_heap::ReadyHeap;
