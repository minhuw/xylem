//! Value size generation strategies

use super::selector::{CommandSelector, WeightedCommandSelector};
use rand::{
    distr::{Distribution as RandDistribution, Uniform},
    rngs::SmallRng,
    SeedableRng,
};
use std::collections::HashMap;
use xylem_common::{Distribution, NormalDistribution};

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
    dist: Uniform<usize>,
    rng: SmallRng,
}

impl UniformSize {
    /// Create a new uniform size generator with explicit seed
    pub fn with_seed(min: usize, max: usize, seed: Option<u64>) -> anyhow::Result<Self> {
        if min > max {
            anyhow::bail!("UniformSize min ({}) must be <= max ({})", min, max);
        }

        let dist = Uniform::new_inclusive(min, max)?;
        let rng = seed.map(SmallRng::seed_from_u64).unwrap_or_else(SmallRng::from_os_rng);
        Ok(Self { dist, rng })
    }
}

impl ValueSizeGenerator for UniformSize {
    fn next_size(&mut self) -> usize {
        self.dist.sample(&mut self.rng)
    }
}

/// Normal (Gaussian) size generator - returns sizes following a bell curve
pub struct NormalSize {
    min: usize,
    max: usize,
    dist: NormalDistribution,
}

impl NormalSize {
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
        Ok(Self { min, max, dist })
    }
}

impl ValueSizeGenerator for NormalSize {
    fn next_size(&mut self) -> usize {
        let sample = self.dist.sample();
        let clamped = sample.max(self.min as f64).min(self.max as f64);
        clamped as usize
    }
}

/// Weighted size generator - draws each size from a weighted list, such as
/// an empirical size histogram
pub struct WeightedSize(WeightedCommandSelector<usize>);

impl WeightedSize {
    /// Create a weighted size generator over `(size, weight)` pairs; weights
    /// must be finite and non-negative, and not all zero
    pub fn with_seed(sizes: Vec<(usize, f64)>, seed: Option<u64>) -> anyhow::Result<Self> {
        anyhow::ensure!(!sizes.is_empty(), "Weighted value sizes need at least one size");
        anyhow::ensure!(
            sizes.iter().all(|(_, weight)| weight.is_finite() && *weight >= 0.0),
            "Weighted value size weights must be finite and >= 0"
        );
        Ok(Self(WeightedCommandSelector::with_seed(sizes, seed)?))
    }
}

impl ValueSizeGenerator for WeightedSize {
    fn next_size(&mut self) -> usize {
        self.0.next_command()
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
        Self {
            command_generators: command_generators
                .into_iter()
                .map(|(name, gen)| (name.to_ascii_uppercase(), gen))
                .collect(),
            default_generator,
        }
    }
}

impl ValueSizeGenerator for PerCommandSize {
    fn next_size(&mut self) -> usize {
        self.default_generator.next_size()
    }

    fn next_size_for_command(&mut self, command: &str) -> usize {
        self.command_generators
            .get_mut(&command.to_ascii_uppercase())
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
        let mut gen = UniformSize::with_seed(100, 200, None).expect("Failed to create UniformSize");
        for _ in 0..1000 {
            let size = gen.next_size();
            assert!((100..=200).contains(&size), "Size {} out of range [100, 200]", size);
        }
    }

    #[test]
    fn test_uniform_size_validation() {
        let result = UniformSize::with_seed(200, 100, None);
        assert!(result.is_err(), "Should reject min > max");
    }

    #[test]
    fn test_normal_size() {
        let mut gen =
            NormalSize::with_seed(150.0, 30.0, 50, 250, None).expect("Failed to create NormalSize");
        for _ in 0..1000 {
            let size = gen.next_size();
            assert!((50..=250).contains(&size), "Size {} out of range [50, 250]", size);
        }
    }

    #[test]
    fn test_normal_size_validation() {
        let result = NormalSize::with_seed(100.0, 20.0, 200, 100, None);
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

#[cfg(test)]
mod weighted_tests {
    use super::*;

    #[test]
    fn weighted_sizes_follow_their_weights_and_seed() {
        let sizes = vec![(16, 0.7), (1024, 0.2), (65536, 0.1)];
        let mut gen = WeightedSize::with_seed(sizes.clone(), Some(7)).unwrap();
        let mut again = WeightedSize::with_seed(sizes, Some(7)).unwrap();
        let mut counts = HashMap::new();
        let samples = 20_000;
        for _ in 0..samples {
            let size = gen.next_size();
            assert_eq!(size, again.next_size());
            *counts.entry(size).or_insert(0usize) += 1;
        }
        assert_eq!(counts.len(), 3);
        for (size, weight) in [(16, 0.7), (1024, 0.2), (65536, 0.1)] {
            let share = counts[&size] as f64 / samples as f64;
            assert!((share - weight).abs() < 0.02, "size {size}: {share}");
        }
    }

    #[test]
    fn a_zero_weight_size_never_appears() {
        let mut gen = WeightedSize::with_seed(vec![(1, 0.0), (2, 3.0)], None).unwrap();
        assert!((0..1000).all(|_| gen.next_size() == 2));
    }

    #[test]
    fn invalid_weighted_sizes_are_rejected() {
        for sizes in [
            vec![],
            vec![(1, 0.0)],
            vec![(1, -1.0), (2, 2.0)],
            vec![(1, f64::NAN)],
            vec![(1, f64::INFINITY)],
        ] {
            assert!(WeightedSize::with_seed(sizes.clone(), None).is_err(), "{sizes:?}");
        }
    }
}

#[cfg(test)]
mod integer_uniform_tests {
    use super::*;

    #[test]
    fn inclusive_integer_bounds_and_reproducibility() {
        for (min, max) in [(0, 0), (42, 42), (0, 1), (usize::MAX - 1, usize::MAX), (0, usize::MAX)]
        {
            let mut first = UniformSize::with_seed(min, max, Some(42)).unwrap();
            let mut second = UniformSize::with_seed(min, max, Some(42)).unwrap();
            let mut seen = std::collections::HashSet::new();
            for _ in 0..1000 {
                let size = first.next_size();
                assert_eq!(size, second.next_size());
                assert!((min..=max).contains(&size));
                seen.insert(size);
            }
            if max - min <= 1 {
                assert!(seen.contains(&min));
                assert!(seen.contains(&max));
            }
        }
        assert!(UniformSize::with_seed(2, 1, None).is_err());
    }
}
