//! Command selection strategies for protocols
//!
//! Provides abstractions for selecting which command to execute
//! on each request, supporting weighted random selection.

use rand::rngs::SmallRng;
use rand::SeedableRng;
use rand_distr::{weighted::WeightedIndex, Distribution as RandDistribution};

/// Trait for command selection strategies
pub trait CommandSelector<T>: Send
where
    T: Clone,
{
    /// Select the next command to execute
    fn next_command(&mut self) -> T;

    /// Reset the selector state
    fn reset(&mut self) {
        // Default: no-op for stateless selectors
    }
}

/// Fixed command selector - always returns the same command
pub struct FixedCommandSelector<T> {
    command: T,
}

impl<T: Clone> FixedCommandSelector<T> {
    pub fn new(command: T) -> Self {
        Self { command }
    }
}

impl<T: Clone + Send> CommandSelector<T> for FixedCommandSelector<T> {
    fn next_command(&mut self) -> T {
        self.command.clone()
    }
}

/// Weighted command selector - selects commands with specified probabilities
pub struct WeightedCommandSelector<T> {
    commands: Vec<T>,
    weights: WeightedIndex<f64>,
    rng: SmallRng,
}

impl<T: Clone> WeightedCommandSelector<T> {
    /// Create a new weighted command selector with entropy-based seed
    ///
    /// # Parameters
    /// - `commands_weights`: Vector of (command, weight) pairs
    ///
    /// # Returns
    /// Error if weights are invalid (empty, negative, or all zero)
    pub fn new(commands_weights: Vec<(T, f64)>) -> anyhow::Result<Self> {
        Self::with_seed(commands_weights, None)
    }

    /// Create a new weighted command selector with explicit seed
    ///
    /// # Parameters
    /// - `commands_weights`: Vector of (command, weight) pairs
    /// - `seed`: Optional seed for reproducibility (None = use entropy)
    ///
    /// # Returns
    /// Error if weights are invalid (empty, negative, or all zero)
    pub fn with_seed(commands_weights: Vec<(T, f64)>, seed: Option<u64>) -> anyhow::Result<Self> {
        if commands_weights.is_empty() {
            anyhow::bail!("WeightedCommandSelector requires at least one command");
        }

        let (commands, weights): (Vec<_>, Vec<_>) = commands_weights.into_iter().unzip();

        // Validate weights
        for (i, &weight) in weights.iter().enumerate() {
            if weight < 0.0 {
                anyhow::bail!("Weight {} is negative: {}", i, weight);
            }
        }

        let total_weight: f64 = weights.iter().sum();
        if total_weight == 0.0 {
            anyhow::bail!("All weights are zero");
        }

        let weights_idx = WeightedIndex::new(weights)?;
        let rng = match seed {
            Some(s) => SmallRng::seed_from_u64(s),
            None => SmallRng::from_os_rng(),
        };

        Ok(Self { commands, weights: weights_idx, rng })
    }
}

impl<T: Clone + Send> CommandSelector<T> for WeightedCommandSelector<T> {
    fn next_command(&mut self) -> T {
        let idx = self.weights.sample(&mut self.rng);
        self.commands[idx].clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    enum TestCommand {
        Get,
        Set,
        Delete,
    }

    #[test]
    fn test_fixed_selector() {
        let mut selector = FixedCommandSelector::new(TestCommand::Get);

        for _ in 0..100 {
            assert_eq!(selector.next_command(), TestCommand::Get);
        }
    }

    #[test]
    fn test_weighted_selector_basic() {
        let commands =
            vec![(TestCommand::Get, 0.7), (TestCommand::Set, 0.2), (TestCommand::Delete, 0.1)];

        let mut selector =
            WeightedCommandSelector::new(commands).expect("Failed to create selector");

        // Sample many times and verify all commands appear
        let mut seen_get = false;
        let mut seen_set = false;
        let mut seen_delete = false;

        for _ in 0..1000 {
            match selector.next_command() {
                TestCommand::Get => seen_get = true,
                TestCommand::Set => seen_set = true,
                TestCommand::Delete => seen_delete = true,
            }
        }

        assert!(seen_get, "Should have seen Get command");
        assert!(seen_set, "Should have seen Set command");
        assert!(seen_delete, "Should have seen Delete command");
    }

    #[test]
    fn test_weighted_selector_distribution() {
        let commands = vec![(TestCommand::Get, 0.8), (TestCommand::Set, 0.2)];

        let mut selector = WeightedCommandSelector::with_seed(commands, Some(42))
            .expect("Failed to create selector");

        let mut get_count = 0;
        let mut set_count = 0;
        let samples = 10000;

        for _ in 0..samples {
            match selector.next_command() {
                TestCommand::Get => get_count += 1,
                TestCommand::Set => set_count += 1,
                _ => {}
            }
        }

        let get_ratio = get_count as f64 / samples as f64;
        let set_ratio = set_count as f64 / samples as f64;

        // Should be close to 0.8 and 0.2 (allow 5% tolerance)
        assert!((get_ratio - 0.8).abs() < 0.05, "Get ratio {} not close to 0.8", get_ratio);
        assert!((set_ratio - 0.2).abs() < 0.05, "Set ratio {} not close to 0.2", set_ratio);
    }

    #[test]
    fn test_weighted_selector_reproducible() {
        let commands =
            vec![(TestCommand::Get, 0.5), (TestCommand::Set, 0.3), (TestCommand::Delete, 0.2)];

        let mut selector1 = WeightedCommandSelector::with_seed(commands.clone(), Some(123))
            .expect("Failed to create selector");
        let mut selector2 = WeightedCommandSelector::with_seed(commands, Some(123))
            .expect("Failed to create selector");

        for _ in 0..100 {
            assert_eq!(selector1.next_command(), selector2.next_command());
        }
    }

    #[test]
    fn test_weighted_selector_empty_commands() {
        let commands: Vec<(TestCommand, f64)> = vec![];
        let result = WeightedCommandSelector::new(commands);
        assert!(result.is_err());
    }

    #[test]
    fn test_weighted_selector_negative_weight() {
        let commands = vec![(TestCommand::Get, 0.8), (TestCommand::Set, -0.2)];
        let result = WeightedCommandSelector::new(commands);
        assert!(result.is_err());
    }

    #[test]
    fn test_weighted_selector_all_zero_weights() {
        let commands = vec![(TestCommand::Get, 0.0), (TestCommand::Set, 0.0)];
        let result = WeightedCommandSelector::new(commands);
        assert!(result.is_err());
    }

    #[test]
    fn test_weighted_selector_single_command() {
        let commands = vec![(TestCommand::Get, 1.0)];
        let mut selector =
            WeightedCommandSelector::new(commands).expect("Failed to create selector");

        for _ in 0..100 {
            assert_eq!(selector.next_command(), TestCommand::Get);
        }
    }
}
