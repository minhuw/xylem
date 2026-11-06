//! Workload patterns and statistical distributions
//!
//! This module provides:
//! - Load patterns for adaptive rate control (ramp, spike, sinusoidal, sawtooth)
//! - Statistical distributions re-exported from `xylem-common` for use by protocols

pub mod patterns;

// Re-export distributions from xylem-common
pub use xylem_common::distributions;

// Re-export load patterns
pub use patterns::{LoadPattern, RampPattern, SawtoothPattern, SinusoidalPattern, SpikePattern};
