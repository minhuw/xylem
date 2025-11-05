//! Xylem Protocol Implementations
//!
//! This crate provides implementations of various application-level protocols
//! for latency measurement.

use anyhow::Result;
use async_trait::async_trait;

/// Application protocol trait
#[async_trait]
pub trait Protocol: Send + Sync {
    /// Generate a request
    fn generate_request(&mut self, key: u64, value_size: usize) -> Vec<u8>;

    /// Parse a response (returns Ok(()) if valid)
    fn parse_response(&mut self, data: &[u8]) -> Result<()>;

    /// Protocol name
    fn name(&self) -> &'static str;

    /// Reset protocol state
    fn reset(&mut self);
}

pub mod echo;
pub mod http;
pub mod masstree;
pub mod memcached;
pub mod protocol;
pub mod redis;
pub mod synthetic;
