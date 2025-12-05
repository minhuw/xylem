//! Workload generation

pub mod data_import;
pub mod generator;
pub mod patterns;
pub mod value_size;

// Re-export distributions from xylem-common
pub use xylem_common::distributions;
pub use xylem_common::{
    BimodalDistribution, Distribution, ExponentialDistribution, FacebookInterArrivalDistribution,
    FacebookKeySizeDistribution, FacebookValueSizeDistribution, GammaDistribution,
    GeneralizedParetoDistribution, GevDistribution, LognormalDistribution, NormalDistribution,
    ParetoDistribution, UniformDistribution, ZipfianDistribution,
};

// Re-export main types
pub use data_import::{
    DataImporter, DataVerifier, ImportedEntry, VerificationMismatch, VerificationStats,
};
pub use generator::{KeyGeneration, KeyGeneratorTrait, RateControl, RequestGenerator};
pub use patterns::{
    ConstantPattern, LoadPattern, RampPattern, SawtoothPattern, SinusoidalPattern, SpikePattern,
    StepPattern,
};
pub use value_size::{FixedSize, NormalSize, PerCommandSize, UniformSize, ValueSizeGenerator};
