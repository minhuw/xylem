//! Xylem Transport Layer
//!
//! This crate provides transport layer implementations for the Xylem latency
//! measurement tool. Transports handle the low-level network I/O using non-blocking
//! sockets with epoll/kqueue for nanosecond-precision timing.
//!
//! ## Available Transports
//!
//! - **TCP**: Non-blocking TCP sockets with mio
//! - **UDP**: (TODO) Non-blocking UDP sockets
//! - **TLS**: (TODO) TLS over TCP using rustls
//!
//! ## Example
//!
//! ```rust,no_run
//! use xylem_transport::{TcpTransport, Transport, Timestamp};
//! use std::net::SocketAddr;
//!
//! let mut transport = TcpTransport::new();
//! let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
//!
//! transport.connect(&addr).unwrap();
//! let send_ts = transport.send(b"PING\r\n").unwrap();
//!
//! while !transport.poll_readable().unwrap() {
//!     std::hint::spin_loop();
//! }
//!
//! let (response, recv_ts) = transport.recv().unwrap();
//! let latency = recv_ts.duration_since(&send_ts);
//! println!("Latency: {:?}", latency);
//! ```

use std::fmt;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Result type for transport operations
pub type Result<T> = std::result::Result<T, Error>;

/// Transport layer error types
#[derive(Debug)]
pub enum Error {
    /// I/O errors from transport layer
    Io(std::io::Error),

    /// Connection errors
    Connection(String),

    /// Other errors
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Connection(msg) => write!(f, "Connection error: {msg}"),
            Error::Other(msg) => write!(f, "Error: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

/// Timestamp for request/response tracking
#[derive(Debug, Clone, Copy)]
pub struct Timestamp {
    pub instant: Instant,
    pub nanos: u64,
}

impl Timestamp {
    /// Create a new timestamp from current time
    ///
    /// Captures both a monotonic Instant and a nanosecond timestamp
    /// for high-precision latency measurements.
    pub fn now() -> Self {
        // Get nanosecond timestamp
        static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        let start = START.get_or_init(Instant::now);
        let nanos = start.elapsed().as_nanos() as u64;

        Self { instant: Instant::now(), nanos }
    }

    /// Calculate duration since another timestamp
    pub fn duration_since(&self, earlier: &Timestamp) -> Duration {
        self.instant.duration_since(earlier.instant)
    }
}

/// Transport protocol trait
///
/// This trait defines the interface for transport layer implementations.
/// All methods are blocking/synchronous to enable tight control over timing
/// and busy-waiting in the event loop.
pub trait Transport: Send {
    /// Connect to a target
    ///
    /// Establishes a connection to the specified target address.
    /// Returns immediately after connection is established.
    fn connect(&mut self, target: &SocketAddr) -> Result<()>;

    /// Send data and return timestamp
    ///
    /// Sends the provided data and captures a nanosecond-precision timestamp
    /// immediately after the send operation completes.
    ///
    /// For non-blocking transports, this may return immediately without
    /// guaranteeing the data has been written to the network.
    fn send(&mut self, data: &[u8]) -> Result<Timestamp>;

    /// Receive data and return timestamp
    ///
    /// Attempts to receive data from the transport. Returns the received data
    /// and a timestamp captured immediately after the receive operation.
    ///
    /// For non-blocking transports, this returns immediately even if no data
    /// is available (returning empty buffer or error).
    fn recv(&mut self) -> Result<(Vec<u8>, Timestamp)>;

    /// Check if there is data ready to read
    ///
    /// Non-blocking check for read readiness. Returns true if recv() will
    /// likely return data without blocking.
    fn poll_readable(&mut self) -> Result<bool>;

    /// Close the connection
    ///
    /// Closes the transport and releases resources.
    fn close(&mut self) -> Result<()>;
}

pub mod tcp;
pub mod tls;
pub mod udp;

#[cfg(unix)]
pub mod unix;

// Re-export transport implementations
pub use tcp::TcpTransport;
pub use tls::TlsTransport;
pub use udp::UdpTransport;

#[cfg(unix)]
pub use unix::UnixTransport;
