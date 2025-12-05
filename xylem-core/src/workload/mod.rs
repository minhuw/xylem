//! Workload patterns and statistical distributions
//!
//! This module provides:
//! - Load patterns for adaptive rate control (constant, ramp, spike, etc.)
//! - Statistical distributions re-exported from `xylem-common` for use by protocols

pub mod patterns;

// Re-export distributions from xylem-common
pub use xylem_common::distributions;
pub use xylem_common::{
    BimodalDistribution, Distribution, ExponentialDistribution, FacebookInterArrivalDistribution,
    FacebookKeySizeDistribution, FacebookValueSizeDistribution, GammaDistribution,
    GeneralizedParetoDistribution, GevDistribution, LognormalDistribution, NormalDistribution,
    ParetoDistribution, UniformDistribution, ZipfianDistribution,
};

// Re-export load patterns
pub use patterns::{
    ConstantPattern, LoadPattern, RampPattern, SawtoothPattern, SinusoidalPattern, SpikePattern,
    StepPattern,
};
