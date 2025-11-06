//! Xylem Protocol Implementations
//!
//! This crate provides implementations of various application-level protocols
//! for latency measurement.

use anyhow::Result;
use std::fmt::Debug;
use std::hash::Hash;

/// Application protocol trait with request ID tracking
pub trait Protocol: Send {
    /// Type of request ID used by this protocol
    type RequestId: Eq + Hash + Clone + Copy + Debug;

    /// Generate a request with an ID
    ///
    /// # Arguments
    /// * `conn_id` - Connection identifier (for protocols that need per-connection state)
    /// * `key` - Request key
    /// * `value_size` - Value size for the request
    ///
    /// Returns (request_data, request_id)
    fn generate_request(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
    ) -> (Vec<u8>, Self::RequestId);

    /// Parse a response and return the request ID it corresponds to
    ///
    /// # Arguments
    /// * `conn_id` - Connection identifier (for protocols that need per-connection state)
    /// * `data` - Response data buffer
    ///
    /// Returns Ok((bytes_consumed, Some(request_id))) if complete response found
    /// Returns Ok((0, None)) if response is incomplete
    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)>;

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
