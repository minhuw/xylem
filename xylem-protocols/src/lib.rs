//! Xylem Protocol Implementations
//!
//! This crate provides implementations of various application-level protocols
//! for latency measurement.

use anyhow::Result;
use std::fmt::Debug;
use std::hash::Hash;

/// Retry request information for protocol-level retries
///
/// This is used when a protocol needs to retry a request, typically
/// due to cluster redirects or other transient conditions.
#[derive(Debug, Clone)]
pub struct RetryRequest<ReqId> {
    /// Number of bytes consumed from the response buffer
    pub bytes_consumed: usize,
    /// Original request ID that triggered this retry
    pub original_request_id: ReqId,
    /// Key to use for the retry request
    pub key: u64,
    /// Value size for the retry request
    pub value_size: usize,
    /// Target connection ID for the retry (None = use routing logic)
    pub target_conn_id: Option<usize>,
    /// Preparation commands to send before the retry (e.g., ASKING for Redis Cluster)
    pub prepare_commands: Vec<Vec<u8>>,
    /// Retry attempt number (0 for first retry)
    pub attempt: usize,
}

/// Extended parse result that supports retries
///
/// This enum extends the traditional parse result to support protocol-level
/// retries, which is essential for distributed protocols like Redis Cluster.
#[derive(Debug)]
pub enum ParseResult<ReqId> {
    /// Complete response parsed successfully
    Complete {
        /// Number of bytes consumed from buffer
        bytes_consumed: usize,
        /// Request ID this response corresponds to
        request_id: ReqId,
    },
    /// Response incomplete, need more data
    Incomplete,
    /// Retry needed (e.g., cluster redirect)
    Retry(RetryRequest<ReqId>),
}

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

    /// Parse a response with retry support (extended version)
    ///
    /// This method provides an extended interface that supports protocol-level
    /// retries. The default implementation delegates to `parse_response()` for
    /// backward compatibility.
    ///
    /// # Arguments
    /// * `conn_id` - Connection identifier
    /// * `data` - Response data buffer
    ///
    /// # Returns
    /// * `ParseResult::Complete` - Full response parsed
    /// * `ParseResult::Incomplete` - Need more data
    /// * `ParseResult::Retry` - Retry needed (e.g., redirect)
    fn parse_response_extended(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<ParseResult<Self::RequestId>> {
        // Default implementation: delegate to parse_response
        match self.parse_response(conn_id, data)? {
            (bytes_consumed, Some(request_id)) => {
                Ok(ParseResult::Complete { bytes_consumed, request_id })
            }
            (0, None) => Ok(ParseResult::Incomplete),
            (bytes_consumed, None) => {
                // Consumed bytes but no request ID - treat as incomplete
                // This shouldn't happen in normal protocols
                if bytes_consumed > 0 {
                    Ok(ParseResult::Complete {
                        bytes_consumed,
                        request_id: unsafe { std::mem::zeroed() },
                    })
                } else {
                    Ok(ParseResult::Incomplete)
                }
            }
        }
    }

    /// Protocol name
    fn name(&self) -> &'static str;

    /// Reset protocol state
    fn reset(&mut self);
}

pub mod http;
pub mod key_generation_adapter;
pub mod masstree;
pub mod memcached;
pub mod protocol;
pub mod redis;
pub mod xylem_echo;

// Re-export commonly used types
pub use http::HttpMethod;
pub use http::HttpProtocol;
pub use masstree::MasstreeOp;
pub use memcached::{MemcachedAsciiProtocol, MemcachedBinaryProtocol};
pub use redis::cluster::{
    generate_asking_command, generate_cluster_slots_command, parse_redirect, ClusterNode,
    ClusterRequestId, ClusterTopology, RedirectStats, RedirectType, RedisClusterProtocol,
    SlotRange,
};
pub use redis::command_selector::{
    CommandSelector, FixedCommandSelector, KeyGenerator, WeightedCommandSelector,
};
pub use redis::command_template::CommandTemplate;
pub use redis::crc16::crc16;
pub use redis::slot::{
    calculate_slot, calculate_slot_str, extract_hash_tag, has_hash_tag, CLUSTER_SLOTS,
};
pub use redis::{RedisOp, RedisProtocol};
pub use xylem_echo::XylemEchoProtocol;
