//! Memcached protocol implementations
//!
//! The ASCII and binary codecs share the operation enum, the insert-phase
//! state, and per-connection sequence numbering defined here.

use std::collections::HashMap;

pub mod ascii;
pub mod binary;
mod config;

pub use ascii::MemcachedAsciiProtocol;
pub use binary::MemcachedBinaryProtocol;

/// Operation sent in the measurement phase, shared by the ASCII and binary codecs
#[derive(Debug, Clone, Copy)]
pub enum MemcachedOp {
    Get,
    Set,
}

/// Insert phase state for data population before measurement
#[derive(Debug)]
pub struct InsertPhaseState {
    /// Total number of keys to insert
    key_count: u64,
    /// Number of keys inserted so far
    inserted: u64,
    /// Value size for inserts
    value_size: usize,
    /// Key generator for inserts (clone of workload generator to preserve distribution/seed)
    key_gen: Option<crate::workload::KeyGeneration>,
}

impl InsertPhaseState {
    /// Create new insert phase state
    pub fn new(
        key_count: u64,
        value_size: usize,
        key_gen: Option<crate::workload::KeyGeneration>,
    ) -> Self {
        Self {
            key_count,
            inserted: 0,
            value_size,
            key_gen,
        }
    }

    /// Check if insert phase is complete
    pub fn is_complete(&self) -> bool {
        self.inserted >= self.key_count
    }

    /// Get next key to insert, returns None if insert phase is complete
    pub fn next_key(&mut self) -> Option<u64> {
        if self.inserted >= self.key_count {
            None
        } else {
            self.inserted += 1;
            let key = if let Some(ref mut key_gen) = self.key_gen {
                key_gen.next_key()
            } else {
                self.inserted - 1 // Fallback to sequential if no generator is configured
            };
            Some(key)
        }
    }
}

/// Return the next sequence number for `conn_id` and advance it; numbering starts at 0
fn next_conn_seq(conn_seq: &mut HashMap<usize, u64>, conn_id: usize) -> u64 {
    let seq = conn_seq.entry(conn_id).or_insert(0);
    let result = *seq;
    *seq += 1;
    result
}
