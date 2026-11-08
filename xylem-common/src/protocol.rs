//! Protocol contract shared by protocol implementations and the worker.
//!
//! `xylem-protocols` and `xylem-core` both re-export these items, so a
//! protocol implementation can be passed to the worker directly.

use crate::Request;
use std::fmt::Debug;
use std::hash::Hash;

/// What [`Protocol::parse_response`] found at the start of its data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Parsed<'a, Id> {
    /// The response at the start is incomplete; nothing is consumed
    Incomplete,
    /// The first `n` bytes complete no request (datagram transports only;
    /// see [`Protocol::parse_response`])
    Absorbed(usize),
    /// The first `consumed` bytes complete the request `id`
    Complete {
        consumed: usize,
        id: Id,
        /// Set when the response is an error reply: the server answered,
        /// but the request failed
        error: Option<ErrorReply<'a>>,
    },
}

impl<'a, Id> Parsed<'a, Id> {
    /// A complete successful response of `consumed` bytes to request `id`
    pub fn ok(consumed: usize, id: Id) -> Self {
        Self::Complete { consumed, id, error: None }
    }

    /// A complete error reply of `consumed` bytes to request `id`
    pub fn error(consumed: usize, id: Id, error: ErrorReply<'a>) -> Self {
        Self::Complete { consumed, id, error: Some(error) }
    }

    /// Bytes consumed; zero when incomplete
    pub fn consumed(&self) -> usize {
        match *self {
            Self::Incomplete => 0,
            Self::Absorbed(consumed) | Self::Complete { consumed, .. } => consumed,
        }
    }

    /// Convert the request ID, keeping everything else
    pub fn map_id<T>(self, f: impl FnOnce(Id) -> T) -> Parsed<'a, T> {
        match self {
            Self::Incomplete => Parsed::Incomplete,
            Self::Absorbed(consumed) => Parsed::Absorbed(consumed),
            Self::Complete { consumed, id, error } => {
                Parsed::Complete { consumed, id: f(id), error }
            }
        }
    }
}

/// A complete response that reports a failed request, such as a Redis `-ERR`
/// reply or an HTTP 503
///
/// Both strings borrow from the parsed data or are static, so reporting an
/// error reply allocates nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorReply<'a> {
    /// Short, stable class of the error, by which error replies are counted:
    /// the Redis error code (`"WRONGTYPE"`), the HTTP status (`"503"`), ...
    pub kind: &'a str,
    /// The reply as text, for error messages: the Redis error line, the HTTP
    /// status line, ... It may be just the kind.
    pub message: &'a str,
}

impl<'a> ErrorReply<'a> {
    pub fn new(kind: &'a str, message: &'a str) -> Self {
        Self { kind, message }
    }

    /// An error reply from raw bytes, each cut to its longest valid UTF-8 prefix
    pub fn from_bytes(kind: &'a [u8], message: &'a [u8]) -> Self {
        Self::new(utf8_prefix(kind), utf8_prefix(message))
    }
}

/// The longest prefix of `bytes` that is valid UTF-8
fn utf8_prefix(bytes: &[u8]) -> &str {
    match std::str::from_utf8(bytes) {
        Ok(text) => text,
        Err(error) => std::str::from_utf8(&bytes[..error.valid_up_to()])
            .expect("the bytes before the first invalid one are valid UTF-8"),
    }
}

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

    /// Parse the response at the start of `data`
    ///
    /// # Arguments
    /// * `conn_id` - Connection identifier (for protocols that need per-connection state)
    /// * `data` - Response data buffer
    ///
    /// Returns [`Parsed::Complete`] with the bytes consumed and the request
    /// ID once a complete response is found, and [`Parsed::Incomplete`]
    /// while it is not.
    ///
    /// # Error replies
    ///
    /// A complete response that reports a failed request (a Redis error, an
    /// HTTP 4xx or 5xx, ...) is still `Complete`, with an [`ErrorReply`]
    /// whose kind and message borrow from `data` or are static. It completes
    /// its request like any response; the traffic group's `on_error` setting
    /// decides whether it is counted or fails the run. `Err` is for bytes
    /// that cannot be framed or understood, and for failed connection setup
    /// (a rejected AUTH, a failed handshake): either fails the run.
    ///
    /// # Byte streams
    ///
    /// On a stream transport (TCP, Unix, ntcp), `data` holds every byte not
    /// yet consumed, across reads. Consuming bytes without a request ID
    /// ([`Parsed::Absorbed`]) is an error.
    ///
    /// # Datagrams
    ///
    /// On a datagram transport (`ConnectionGroup::is_datagram`, e.g. UDP),
    /// `data` is one received datagram, or the part of it earlier calls left
    /// unconsumed; it never contains bytes of another datagram. Two things
    /// differ from byte streams:
    /// - `Absorbed(n)` with `n > 0` absorbs `n` bytes that complete no
    ///   request yet, such as one fragment of a multi-datagram response, a
    ///   duplicate, or a reply to an unknown request ID.
    /// - A datagram is never completed by a later one: bytes left when the
    ///   parser consumes nothing are an error.
    ///
    /// A returned request ID that is no longer pending (its request timed
    /// out) is dropped by the caller and counted as a late response.
    fn parse_response<'a>(
        &mut self,
        conn_id: usize,
        data: &'a [u8],
    ) -> anyhow::Result<Parsed<'a, Self::RequestId>>;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parsed_reports_consumed_bytes_and_maps_ids() {
        let error = ErrorReply::new("ERR", "ERR boom");
        assert_eq!(Parsed::<u8>::Incomplete.consumed(), 0);
        assert_eq!(Parsed::<u8>::Absorbed(4).consumed(), 4);
        assert_eq!(Parsed::ok(5, 1u8).consumed(), 5);
        assert_eq!(Parsed::error(7, 1u8, error).map_id(u64::from), Parsed::error(7, 1u64, error));
        assert_eq!(Parsed::<u8>::Absorbed(4).map_id(u64::from), Parsed::Absorbed(4));
    }

    #[test]
    fn error_reply_text_is_its_longest_utf8_prefix() {
        assert_eq!(ErrorReply::from_bytes(b"OOM", b"OOM full"), ErrorReply::new("OOM", "OOM full"));
        assert_eq!(ErrorReply::from_bytes(b"E\xff", b"\xffx"), ErrorReply::new("E", ""));
        // A character cut short is dropped whole
        assert_eq!(ErrorReply::from_bytes("é".as_bytes(), &"aé".as_bytes()[..2]).message, "a");
    }
}
