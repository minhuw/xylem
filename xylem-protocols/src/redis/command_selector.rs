//! Command selection strategies, shared with the other key-value protocols
//! and defined in [`crate::workload::selector`]

pub use crate::workload::selector::{
    CommandSelector, FixedCommandSelector, KeyGenerator, WeightedCommandSelector,
};
