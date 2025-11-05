//! Xylem Core Library
//!
//! This crate provides the core functionality for the Xylem distributed latency
//! measurement tool, including transport layer, statistics engine, threading
//! runtime, and workload generation.

pub mod config;
pub mod error;
pub mod request;
pub mod stats;
pub mod threading;
pub mod transport;
pub mod workload;

pub use error::{Error, Result};
