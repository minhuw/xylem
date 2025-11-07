//! Workload generation

pub mod distributions;
pub mod generator;
pub mod patterns;

// Re-export main types
pub use distributions::{
    Distribution, ExponentialDistribution, NormalDistribution, UniformDistribution,
    ZipfianDistribution,
};
pub use generator::{KeyGeneration, RateControl, RequestGenerator};
pub use patterns::{
    ConstantPattern, LoadPattern, RampPattern, SawtoothPattern, SinusoidalPattern, SpikePattern,
    StepPattern,
};
