//! Common test utilities for integration tests
//!
//! This module provides guards for Docker-based test services and shared test helpers.

#![allow(dead_code)]

// Docker-based service guards
pub mod masstree;
pub mod memcached;
pub mod multi_protocol;
pub mod nginx;
pub mod redis;
pub mod redis_acl;
pub mod redis_auth;
pub mod redis_cluster;

use xylem_core::stats::GroupStatsCollector;

/// Create a GroupStatsCollector for testing with a single group (group_id=0)
///
/// This is a helper for tests that were written for single-group Workers
/// and need to be migrated to GroupStatsCollector.
pub fn create_test_stats() -> GroupStatsCollector {
    let mut stats = GroupStatsCollector::new();
    // Register group 0 with unlimited sampling
    stats.register_group_legacy(0, 1_000_000, 1.0);
    stats
}
