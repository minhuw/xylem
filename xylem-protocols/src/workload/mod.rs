//! Workload generation for protocol-specific request generation
//!
//! This module provides workload generation capabilities that protocols use
//! to generate request content. Each protocol embeds its own workload generator
//! and decides what to send when the core scheduler signals it's time to send.
//!
//! Key types:
//! - `KeyGeneration` - Key distribution strategies (sequential, random, zipfian, etc.)
//! - `ValueSizeGenerator` - Value size generation strategies
//!
//! # Design Philosophy
//!
//! The core library (`xylem-core`) decides **when** to send requests (scheduling,
//! rate control, connection management). This module decides **what** to send
//! (request content, key distribution, value sizes).

pub mod keys;
pub mod value_size;

// Re-export distributions from xylem-common
pub use xylem_common::distributions;

// Re-export main types
pub use keys::{KeyGeneration, KeyGeneratorTrait};
pub use value_size::{FixedSize, NormalSize, PerCommandSize, UniformSize, ValueSizeGenerator};
