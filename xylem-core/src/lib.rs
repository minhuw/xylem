//! Xylem Core Library
//!
//! This crate provides the core functionality for the Xylem distributed latency
//! measurement tool, including transport layer, statistics engine, threading
//! runtime, and workload generation.

pub mod connection;
pub mod error;
pub mod request;
pub mod scheduler;
pub mod seed;
pub mod stats;
pub mod threading;
pub mod timing;
pub mod traffic_group;
pub mod workload;

// Re-export transport types from xylem-transport
pub use xylem_transport::{
    ConnectionGroup, GroupConnection, Timestamp as TransportTimestamp, Transport, TransportFactory,
};

pub use error::{Error, Result};
