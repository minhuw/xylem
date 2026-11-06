//! Common test utilities for integration tests
//!
//! This module provides guards for Docker-based test services and shared test helpers.

#![allow(dead_code)]

// Docker compose fixture shared by the service guards
mod compose;

// Docker-based service guards
pub mod masstree;
pub mod memcached;
pub mod multi_protocol;
pub mod nginx;
pub mod redis;
pub mod redis_acl;
pub mod redis_auth;
pub mod redis_cluster;

use std::time::Duration;
use xylem_core::stats::{SamplingPolicy, TupleStatsCollector};

/// Create a TupleStatsCollector for testing with a single group (group_id=0)
///
/// The global view uses the default sampling policy; group 0 keeps up to
/// 1,000,000 latency samples per entry at full rate.
pub fn create_test_stats() -> TupleStatsCollector {
    let mut stats = TupleStatsCollector::new(SamplingPolicy::default(), Duration::from_secs(1));
    stats.register_group_policy(0, SamplingPolicy::Limited { max_samples: 1_000_000, rate: 1.0 });
    stats
}
