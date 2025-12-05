//! Value size generation strategies

use std::collections::HashMap;
use xylem_common::{Distribution, NormalDistribution, UniformDistribution};

/// Trait for value size generation
pub trait ValueSizeGenerator: Send {
    /// Get the next value size
    fn next_size(&mut self) -> usize;

    /// Get the size for a specific command (if per-command sizing is supported)
    fn next_size_for_command(&mut self, _command: &str) -> usize {
        self.next_size()
    }

    /// Reset the generator state
    fn reset(&mut self) {
        // Default: no-op
    }
}

/// Fixed size generator - always returns the same size
#[derive(Debug, Clone)]
pub struct FixedSize {
    size: usize,
}

impl FixedSize {
    pub fn new(size: usize) -> Self {
        Self { size }
    }
}

impl ValueSizeGenerator for FixedSize {
    fn next_size(&mut self) -> usize {
        self.size
    }
}

/// Uniform random size generator - returns sizes in [min, max] range
pub struct UniformSize {
    #[allow(dead_code)]
    min: usize,
    #[allow(dead_code)]
    max: usize,
    dist: UniformDistribution,
}

impl UniformSize {
    /// Create a new uniform size generator with entropy-based seed
    pub fn new(min: usize, max: usize) -> anyhow::Result<Self> {
        Self::with_seed(min, max, None)
    }

    /// Create a new uniform size generator with explicit seed
    pub fn with_seed(min: usize, max: usize, seed: Option<u64>) -> anyhow::Result<Self> {
        if min > max {
            anyhow::bail!("UniformSize min ({}) must be <= max ({})", min, max);
        }

        let dist = UniformDistribution::with_seed(min as f64, max as f64, seed)?;
        Ok(Self { min, max, dist })
    }
}

impl ValueSizeGenerator for UniformSize {
    fn next_size(&mut self) -> usize {
        self.dist.sample() as usize
    }
}

/// Normal (Gaussian) size generator - returns sizes following a bell curve
pub struct NormalSize {
    #[allow(dead_code)]
    mean: f64,
    #[allow(dead_code)]
    std_dev: f64,
    min: usize,
    max: usize,
    dist: NormalDistribution,
}

impl NormalSize {
    /// Create a new normal size generator with entropy-based seed
    pub fn new(mean: f64, std_dev: f64, min: usize, max: usize) -> anyhow::Result<Self> {
        Self::with_seed(mean, std_dev, min, max, None)
    }

    /// Create a new normal size generator with explicit seed
    pub fn with_seed(
        mean: f64,
        std_dev: f64,
        min: usize,
        max: usize,
        seed: Option<u64>,
    ) -> anyhow::Result<Self> {
        if min > max {
            anyhow::bail!("NormalSize min ({}) must be <= max ({})", min, max);
        }

        let dist = NormalDistribution::with_seed(mean, std_dev, seed)?;
        Ok(Self { mean, std_dev, min, max, dist })
    }
}

impl ValueSizeGenerator for NormalSize {
    fn next_size(&mut self) -> usize {
        let sample = self.dist.sample();
        let clamped = sample.max(self.min as f64).min(self.max as f64);
        clamped as usize
    }
}

/// Per-command size generator - different sizes for different commands
pub struct PerCommandSize {
    /// Size generators for specific commands
    command_generators: HashMap<String, Box<dyn ValueSizeGenerator>>,
    /// Default generator for commands not in the map
    default_generator: Box<dyn ValueSizeGenerator>,
}

impl PerCommandSize {
    /// Create a new per-command size generator
    pub fn new(
        command_generators: HashMap<String, Box<dyn ValueSizeGenerator>>,
        default_generator: Box<dyn ValueSizeGenerator>,
    ) -> Self {
        Self { command_generators, default_generator }
    }
}

impl ValueSizeGenerator for PerCommandSize {
    fn next_size(&mut self) -> usize {
        self.default_generator.next_size()
    }

    fn next_size_for_command(&mut self, command: &str) -> usize {
        self.command_generators
            .get_mut(command)
            .map(|gen| gen.next_size())
            .unwrap_or_else(|| self.default_generator.next_size())
    }

    fn reset(&mut self) {
        self.default_generator.reset();
        for gen in self.command_generators.values_mut() {
            gen.reset();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_size() {
        let mut gen = FixedSize::new(128);
        for _ in 0..100 {
            assert_eq!(gen.next_size(), 128);
        }
    }

    #[test]
    fn test_uniform_size() {
        let mut gen = UniformSize::new(100, 200).expect("Failed to create UniformSize");
        for _ in 0..1000 {
            let size = gen.next_size();
            assert!((100..=200).contains(&size), "Size {} out of range [100, 200]", size);
        }
    }

    #[test]
    fn test_uniform_size_validation() {
        let result = UniformSize::new(200, 100);
        assert!(result.is_err(), "Should reject min > max");
    }

    #[test]
    fn test_normal_size() {
        let mut gen = NormalSize::new(150.0, 30.0, 50, 250).expect("Failed to create NormalSize");
        for _ in 0..1000 {
            let size = gen.next_size();
            assert!((50..=250).contains(&size), "Size {} out of range [50, 250]", size);
        }
    }

    #[test]
    fn test_normal_size_validation() {
        let result = NormalSize::new(100.0, 20.0, 200, 100);
        assert!(result.is_err(), "Should reject min > max");
    }

    #[test]
    fn test_per_command_size() {
        let mut command_gens: HashMap<String, Box<dyn ValueSizeGenerator>> = HashMap::new();
        command_gens.insert("get".to_string(), Box::new(FixedSize::new(64)));
        command_gens.insert("set".to_string(), Box::new(FixedSize::new(256)));

        let default_gen = Box::new(FixedSize::new(128));
        let mut gen = PerCommandSize::new(command_gens, default_gen);

        assert_eq!(gen.next_size_for_command("get"), 64);
        assert_eq!(gen.next_size_for_command("set"), 256);
        assert_eq!(gen.next_size_for_command("incr"), 128); // Uses default
        assert_eq!(gen.next_size(), 128); // Uses default
    }
}
