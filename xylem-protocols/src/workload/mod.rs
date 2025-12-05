//! Workload generation for protocol-specific request generation
//!
//! This module provides workload generation capabilities that protocols use
//! to generate request content. Each protocol embeds its own workload generator
//! and decides what to send when the core scheduler signals it's time to send.
//!
//! Key types:
//! - `KeyGeneration` - Key distribution strategies (sequential, random, zipfian, etc.)
//! - `ValueSizeGenerator` - Value size generation strategies
//! - `DataImporter` - Import test data from CSV files
//!
//! # Design Philosophy
//!
//! The core library (`xylem-core`) decides **when** to send requests (scheduling,
//! rate control, connection management). This module decides **what** to send
//! (request content, key distribution, value sizes).

pub mod data_import;
pub mod keys;
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
pub use keys::{KeyGeneration, KeyGeneratorTrait};
pub use value_size::{FixedSize, NormalSize, PerCommandSize, UniformSize, ValueSizeGenerator};
