//! Request and response types

use std::time::Instant;

/// Timestamp for request/response tracking
#[derive(Debug, Clone, Copy)]
pub struct Timestamp {
    pub instant: Instant,
    pub nanos: u64,
}

impl Timestamp {
    /// Create a new timestamp from current time
    pub fn now() -> Self {
        let instant = Instant::now();
        Self {
            instant,
            nanos: instant.elapsed().as_nanos() as u64,
        }
    }

    /// Calculate duration since another timestamp
    pub fn duration_since(&self, earlier: &Timestamp) -> std::time::Duration {
        self.instant.duration_since(earlier.instant)
    }
}

/// Request metadata
#[derive(Debug, Clone)]
pub struct RequestMetadata {
    pub key: u64,
    pub value_size: usize,
    pub sent_at: Option<Timestamp>,
    pub received_at: Option<Timestamp>,
}

impl RequestMetadata {
    pub fn new(key: u64, value_size: usize) -> Self {
        Self {
            key,
            value_size,
            sent_at: None,
            received_at: None,
        }
    }

    pub fn latency(&self) -> Option<std::time::Duration> {
        if let (Some(sent), Some(received)) = (self.sent_at, self.received_at) {
            Some(received.duration_since(&sent))
        } else {
            None
        }
    }
}
