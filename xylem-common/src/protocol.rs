//! Protocol contract shared by protocol implementations and the worker.
//!
//! `xylem-protocols` and `xylem-core` both re-export these items, so a
//! protocol implementation can be passed to the worker directly.

use crate::Request;
use std::fmt::Debug;
use std::hash::Hash;

/// Application protocol trait for generating requests and parsing responses
/// with request ID tracking
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

    /// Parse a response and return the request ID it corresponds to
    ///
    /// # Arguments
    /// * `conn_id` - Connection identifier (for protocols that need per-connection state)
    /// * `data` - Response data buffer
    ///
    /// Returns Ok((bytes_consumed, Some(request_id))) if complete response found
    /// Returns Ok((0, None)) if response is incomplete
    ///
    /// # Byte streams
    ///
    /// On a stream transport (TCP, Unix, ntcp), `data` holds every byte not
    /// yet consumed, across reads. Consuming bytes without returning a
    /// request ID is an error.
    ///
    /// # Datagrams
    ///
    /// On a datagram transport (`ConnectionGroup::is_datagram`, e.g. UDP),
    /// `data` is one received datagram, or the part of it earlier calls left
    /// unconsumed; it never contains bytes of another datagram. Two things
    /// differ from byte streams:
    /// - `Ok((n, None))` with `n > 0` absorbs `n` bytes that complete no
    ///   request yet, such as one fragment of a multi-datagram response, a
    ///   duplicate, or a reply to an unknown request ID.
    /// - A datagram is never completed by a later one: bytes left when the
    ///   parser returns `Ok((0, _))` are an error.
    ///
    /// A returned request ID that is no longer pending (its request timed
    /// out) is dropped by the caller and counted as a late response.
    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> anyhow::Result<(usize, Option<Self::RequestId>)>;

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

    /// Check if the connection is still in connection setup: it has setup
    /// requests ([`crate::RequestMeta::is_setup`]) left to send, or a setup
    /// reply to wait for
    ///
    /// While this is true, the worker sends the connection's requests as soon
    /// as `can_send` allows, off its intended schedule, so a rate-driven
    /// connection does not wait for its intended send times to set up. Its
    /// schedule starts once setup is done. Return true only while the next
    /// request will be a setup request or a setup reply is pending.
    ///
    /// Default implementation returns false: any setup requests wait for the
    /// connection's intended send times like other requests.
    fn setup_pending(&self, _conn_id: usize) -> bool {
        false
    }

    /// Protocol name
    fn name(&self) -> &'static str;

    /// Reset protocol state
    fn reset(&mut self);
}
