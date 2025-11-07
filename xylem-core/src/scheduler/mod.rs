//! Per-Connection Traffic Model Scheduler
//!
//! This module provides a per-connection traffic model where each connection has its own
//! independent policy that determines when it should send requests. The temporal scheduler
//! picks whichever connection's next request is due soonest (like an async runtime picking
//! the next ready coroutine).
//!
//! ## Architecture
//!
//! ```text
//! Per-Connection Model
//! ├── Policy (per-connection traffic pattern)
//! │   ├── ClosedLoopPolicy
//! │   ├── FixedRatePolicy
//! │   ├── PoissonPolicy
//! │   └── AdaptivePolicy
//! ├── PolicyScheduler (assigns policies to connections)
//! │   ├── UniformPolicyScheduler
//! │   ├── PerConnectionPolicyScheduler
//! │   └── FactoryPolicyScheduler
//! └── TemporalScheduler (min-heap picks next ready connection)
//! ```
//!
//! ## Key Benefits
//!
//! - **Truly independent connections**: No global timing state coupling
//! - **Flexible heterogeneous traffic**: Easy to configure (e.g., 10% at 100K req/s, 90% at 1M req/s)
//! - **Mix policy types**: Each connection can have different policy (ClosedLoop, FixedRate, Poisson, Adaptive)
//! - **Pure temporal scheduling**: No spatial concerns like connection selection
//! - **Efficient**: O(log n) scheduling with min-heap
//!
//! ## Example: Uniform Traffic
//!
//! ```ignore
//! use xylem_core::scheduler::{UniformPolicyScheduler, TemporalScheduler};
//!
//! // All connections use Poisson arrivals at 1M req/s
//! let policy_scheduler = UniformPolicyScheduler::poisson(1_000_000.0)?;
//! let pool = ConnectionPool::with_policy_scheduler(
//!     TcpTransport::new,
//!     target,
//!     100,  // 100 connections
//!     10,
//!     Box::new(policy_scheduler)
//! )?;
//!
//! // Pick next ready connection using temporal scheduler
//! if let Some(conn) = pool.pick_connection_temporal() {
//!     conn.send(data, req_id)?;
//! }
//! ```
//!
//! ## Example: Heterogeneous Traffic (10% at 100K, 90% at 1M)
//!
//! ```ignore
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
//! ```ignore
//! use xylem_core::workload::RampPattern;
//!
//! let pattern = RampPattern::new(1_000_000.0, 10_000_000.0, Duration::from_secs(60));
//!
//! loop {
//!     let target_rate = pattern.rate_at(start_time.elapsed());
//!     for conn in pool.connections_mut() {
//!         conn.policy.set_target_rate(target_rate);
//!     }
//!
//!     if let Some(conn) = pool.pick_connection_temporal() {
//!         // Send request...
//!     }
//! }
//! ```

pub mod policy;
pub mod temporal;

// Re-export main types
pub use policy::{
    AdaptivePolicy, ClosedLoopPolicy, FactoryPolicyScheduler, FixedRatePolicy,
    PerConnectionPolicyScheduler, PoissonPolicy, Policy, PolicyScheduler, UniformPolicyScheduler,
};

pub use temporal::TemporalScheduler;
