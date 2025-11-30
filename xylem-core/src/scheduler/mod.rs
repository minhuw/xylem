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
//! ## Example: Heterogeneous Traffic (10% at 100K, 90% at 1M)
//!
//! ```
//! use xylem_core::scheduler::{FactoryPolicyScheduler, FixedRatePolicy};
//!
//! let policy_scheduler = FactoryPolicyScheduler::new(|conn_id| {
//!     if conn_id % 10 == 0 {
//!         Box::new(FixedRatePolicy::new(100_000.0))  // 10%: slow
//!     } else {
//!         Box::new(FixedRatePolicy::new(1_000_000.0))  // 90%: fast
//!     }
//! });
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

pub mod policy;
pub mod ready_heap;

// Re-export main types
pub use policy::{
    AdaptivePolicy, ClosedLoopPolicy, FactoryPolicyScheduler, FixedRatePolicy,
    PerConnectionPolicyScheduler, PoissonPolicy, Policy, PolicyScheduler, UniformPolicyScheduler,
};

pub use ready_heap::ReadyHeap;
