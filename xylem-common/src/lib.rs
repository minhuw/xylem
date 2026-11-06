//! Common utilities for xylem
//!
//! This crate provides shared utilities used by multiple xylem crates:
//! - `distributions`: Statistical distributions for workload generation

pub mod request;

pub mod distributions;

pub use distributions::{
    Distribution, ExponentialDistribution, NormalDistribution, UniformDistribution,
    ZipfianDistribution,
};

pub use request::{Request, RequestMeta};
