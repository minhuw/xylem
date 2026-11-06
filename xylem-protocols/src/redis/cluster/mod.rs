//! Redis Cluster support
//!
//! This module provides Redis Cluster support including:
//! - Topology management (slot-to-node mapping)
//! - MOVED/ASK redirect handling
//! - Hash slot routing
//! - Cluster-aware protocol with automatic routing
//! - Helper functions for cluster operations

pub mod helpers;
pub mod protocol;
pub mod redirect;
pub mod topology;

pub use helpers::calculate_slot_range;
pub use protocol::{ClusterRequestId, RedirectStats, RedisClusterProtocol};
pub use redirect::{
    generate_asking_command, generate_cluster_slots_command, parse_redirect, RedirectType,
};
pub use topology::{ClusterNode, ClusterTopology, SlotRange};
