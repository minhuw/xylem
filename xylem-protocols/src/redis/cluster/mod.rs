//! Redis Cluster support
//!
//! This module provides Redis Cluster support including:
//! - Topology management (slot-to-node mapping)
//! - MOVED/ASK redirect handling
//! - CLUSTER SLOTS command parsing
//! - Hash slot routing
//! - Cluster-aware protocol with automatic routing
//! - Helper functions for cluster operations

pub mod helpers;
pub mod protocol;
pub mod redirect;
pub mod slots_parser;
pub mod topology;

pub use helpers::{
    calculate_slot_range, extract_redirect, format_redirect, is_redirect_error, is_topology_stale,
    recommended_node_counts, redirect_strategy, validate_slot,
};
pub use protocol::{ClusterRequestId, RedirectStats, RedisClusterProtocol};
pub use redirect::{
    generate_asking_command, generate_cluster_slots_command, parse_redirect, RedirectType,
};
pub use slots_parser::{parse_cluster_slots, parse_simple_slot_config};
pub use topology::{ClusterNode, ClusterTopology, SlotRange};
