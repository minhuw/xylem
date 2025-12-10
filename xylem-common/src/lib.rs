//! Common utilities for xylem
//!
//! This crate provides shared utilities used by multiple xylem crates:
//! - `distributions`: Statistical distributions for workload generation

pub mod request;

pub mod distributions;

pub use distributions::{
    BimodalDistribution, Distribution, ExponentialDistribution, FacebookInterArrivalDistribution,
    FacebookKeySizeDistribution, FacebookValueSizeDistribution, GammaDistribution,
    GeneralizedParetoDistribution, GevDistribution, LognormalDistribution, NormalDistribution,
    ParetoDistribution, UniformDistribution, ZipfianDistribution,
};

pub use request::{Request, RequestMeta};
