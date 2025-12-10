//! Xylem Protocol Implementations
//!
//! This crate provides implementations of various application-level protocols
//! for latency measurement.

use anyhow::Result;
use std::fmt::Debug;
use std::hash::Hash;
pub use xylem_common::{Request, RequestMeta};

/// Retry request information for protocol-level retries
///
/// This is used when a protocol needs to retry a request, typically
/// due to cluster redirects or other transient conditions.
#[derive(Debug, Clone)]
pub struct RetryRequest<ReqId> {
    /// Number of bytes consumed from the response buffer
    pub bytes_consumed: usize,
    /// Original request ID that triggered this retry (protocol uses this to look up request data)
    pub original_request_id: ReqId,
    /// Whether the original request was warmup (used to preserve stats gating on retries)
    pub is_warmup: bool,
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

    /// Generate the next request for a connection
    ///
    /// The protocol generates the request based on its internal state
    /// (command selector, key generator, value size, etc.)
    ///
    /// # Arguments
    /// * `conn_id` - Connection identifier (for protocols that need per-connection state)
    ///
    /// # Returns
    /// A Request containing data, ID, and metadata where metadata indicates
    /// whether this is a warm-up request (stats should not be collected)
    fn next_request(&mut self, conn_id: usize) -> Request<Self::RequestId>;

    /// Regenerate a request for retry using the original request ID
    ///
    /// The protocol looks up any stored metadata for the original request
    /// and generates a new request with the same parameters.
    /// This is used for retry scenarios (e.g., cluster redirects).
    ///
    /// # Arguments
    /// * `conn_id` - Target connection ID for the retry
    /// * `original_request_id` - The request ID from the original request
    ///
    /// # Returns
    /// A new Request with data, ID, and metadata
    ///
    /// Default implementation just generates a new request (ignores original).
    /// Retries are always measurement requests (not warmup).
    fn regenerate_request(
        &mut self,
        conn_id: usize,
        _original_request_id: Self::RequestId,
    ) -> Request<Self::RequestId> {
        let request = self.next_request(conn_id);
        // Retries are always measurement requests
        Request::measurement(request.data, request.request_id)
    }

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

    /// Check if the protocol is ready to send a request on this connection
    ///
    /// This allows protocols to impose their own constraints beyond pipeline depth.
    /// For example, protocols that require handshakes (like Masstree) can return
    /// false until the handshake completes, preventing duplicate handshake requests
    /// from filling the pipeline with the same request ID.
    ///
    /// # Arguments
    /// * `conn_id` - Connection identifier
    ///
    /// # Returns
    /// true if the protocol is ready to generate a request for this connection
    ///
    /// Default implementation always returns true (no protocol-level constraints).
    fn can_send(&self, _conn_id: usize) -> bool {
        true
    }
}

pub mod configs;
pub mod factories;
pub mod factory;
pub mod http;
pub mod key_generation_adapter;
pub mod masstree;
pub mod memcached;
pub mod protocol;
pub mod redis;
pub mod workload;
pub mod xylem_echo;

// Re-export commonly used types
pub use configs::{
    CommandValueSizeConfig, DataImportConfig, HttpConfig, InsertPhaseConfig, KeysConfig,
    MasstreeConfig, MasstreeScanConfig, MemcachedConfig, RedisCommandParams, RedisCommandWeight,
    RedisConfig, RedisOperationsConfig, ValueSizeConfig, VerificationConfig, XylemEchoConfig,
};
pub use factories::{
    HttpFactory, MemcachedAsciiFactory, MemcachedBinaryFactory, ProtocolRegistry, RedisFactory,
    XylemEchoFactory,
};
pub use factory::{DynProtocol, DynProtocolFactory, ProtocolFactory};
pub use http::HttpMethod;
pub use http::HttpProtocol;
pub use masstree::{MasstreeOp, MasstreeProtocol, ResultCode};
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
