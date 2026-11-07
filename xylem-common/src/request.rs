//! Shared request types used across xylem crates.

/// Request metadata returned alongside request data.
///
/// This struct provides additional information about a request that the
/// protocol generates, allowing the worker to make decisions about how to
/// handle the request (e.g., whether to collect stats).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RequestMeta {
    /// True if this is a warm-up request (stats should not be collected).
    /// Warm-up requests are used for:
    /// - Protocol handshakes
    /// - Data population/insertion phases
    /// - Cache warming
    pub is_warmup: bool,
    /// True if this is a connection setup request (a handshake, AUTH, ...),
    /// sent once per connection before any other request. Setup requests are
    /// also warmup requests. The worker never records them in the request
    /// dump and they do not advance the connection's intended send schedule,
    /// which restarts once the connection's setup replies have arrived.
    pub is_setup: bool,
}

impl RequestMeta {
    /// Create metadata for a normal measurement request.
    #[inline]
    pub fn measurement() -> Self {
        Self { is_warmup: false, is_setup: false }
    }

    /// Create metadata for a warm-up request (stats not collected).
    #[inline]
    pub fn warmup() -> Self {
        Self { is_warmup: true, is_setup: false }
    }

    /// Create metadata for a connection setup request (stats not collected,
    /// not dumped, not scheduled).
    #[inline]
    pub fn setup() -> Self {
        Self { is_warmup: true, is_setup: true }
    }
}

/// A complete request containing data, ID, and metadata.
///
/// This struct encapsulates all information needed to send a request,
/// providing a cleaner interface than tuple destructuring.
#[derive(Debug, Clone, PartialEq)]
pub struct Request<ReqId> {
    /// The raw request data to send.
    pub data: Vec<u8>,
    /// Unique identifier for this request.
    pub request_id: ReqId,
    /// Additional metadata about the request.
    pub metadata: RequestMeta,
}

impl<ReqId> Request<ReqId> {
    /// Create a new request.
    pub fn new(data: Vec<u8>, request_id: ReqId, metadata: RequestMeta) -> Self {
        Self { data, request_id, metadata }
    }

    /// Create a measurement request (not warmup).
    pub fn measurement(data: Vec<u8>, request_id: ReqId) -> Self {
        Self {
            data,
            request_id,
            metadata: RequestMeta::measurement(),
        }
    }

    /// Create a warmup request (stats not collected).
    pub fn warmup(data: Vec<u8>, request_id: ReqId) -> Self {
        Self {
            data,
            request_id,
            metadata: RequestMeta::warmup(),
        }
    }

    /// Create a connection setup request (see [`RequestMeta::is_setup`]).
    pub fn setup(data: Vec<u8>, request_id: ReqId) -> Self {
        Self {
            data,
            request_id,
            metadata: RequestMeta::setup(),
        }
    }
}
