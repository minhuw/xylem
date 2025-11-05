//! Workload generation

use crate::config::{InterArrival, KeyGeneration, LoadPattern};

/// Request generator
#[allow(dead_code)]
pub struct RequestGenerator {
    pattern: LoadPattern,
    inter_arrival: InterArrival,
    key_gen: KeyGeneration,
}

impl RequestGenerator {
    pub fn new(pattern: LoadPattern, inter_arrival: InterArrival, key_gen: KeyGeneration) -> Self {
        Self { pattern, inter_arrival, key_gen }
    }
}

pub mod distributions;
pub mod generator;
pub mod patterns;
