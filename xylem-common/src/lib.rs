//! Common utilities for xylem
//!
//! This crate provides shared utilities used by multiple xylem crates:
//! - `distributions`: Statistical distributions for workload generation
//! - `protocol`: The `Protocol` trait shared by protocols and the worker

pub mod protocol;
pub mod request;

pub mod distributions;

pub use distributions::{
    Distribution, ExponentialDistribution, NormalDistribution, UniformDistribution,
    ZipfianDistribution,
};

pub use protocol::{ErrorReply, Parsed, Protocol};
pub use request::{Request, RequestMeta};
