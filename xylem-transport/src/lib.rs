//! Xylem Transport Layer
//!
//! This crate provides transport layer implementations for the Xylem latency
//! measurement tool. Transports handle the low-level network I/O using non-blocking
//! sockets with runtime-selectable I/O multiplexing (epoll, poll, or select).
//!
//! ## Available Transports
//!
//! - **TCP**: Non-blocking TCP sockets
//! - **UDP**: Non-blocking UDP sockets
//! - **Unix**: Unix domain sockets (Unix only)
//!
//! ## I/O Multiplexer Selection
//!
//! The multiplexer backend can be selected at runtime:
//! - `Epoll`: Linux-only, most efficient for large numbers of connections
//! - `Poll`: Portable, no fd limit, good performance
//! - `Select`: Portable, but limited to ~1024 file descriptors
//!
//! ## Transport APIs
//!
//! This crate provides two APIs:
//!
//! ### Single-Connection API (`Transport` trait)
//!
//! Simple API for managing individual connections. Each transport owns its own
//! multiplexer instance, so polling N connections requires N syscalls.
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
//!
//! ### Connection Group API (`ConnectionGroup` trait)
//!
//! Efficient API for managing multiple connections. A single multiplexer is
//! shared across all connections in the group, so polling N connections requires
//! only 1 syscall.
//!
//! ```rust,no_run
//! use xylem_transport::{TcpTransportFactory, TransportFactory, ConnectionGroup, GroupConnection};
//! use std::net::SocketAddr;
//! use std::time::Duration;
//!
//! let factory = TcpTransportFactory::default();
//! let mut group = factory.create_group().unwrap();
//!
//! let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
//! let conn_id = group.add_connection(&addr).unwrap();
//!
//! // Send on connection
//! group.get_mut(conn_id).unwrap().send(b"PING\r\n").unwrap();
//!
//! // Poll all connections with one syscall
//! let ready = group.poll(Some(Duration::from_millis(100))).unwrap();
//! for id in ready {
//!     let (data, _ts) = group.get_mut(id).unwrap().recv().unwrap();
//!     println!("Received: {:?}", data);
//! }
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

    /// Configuration errors
    Config(String),

    /// Other errors
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Connection(msg) => write!(f, "Connection error: {msg}"),
            Error::Config(msg) => write!(f, "Configuration error: {msg}"),
            Error::Other(msg) => write!(f, "Error: {msg}"),
        }
    }
}

impl From<nix::Error> for Error {
    fn from(err: nix::Error) -> Self {
        Error::Io(std::io::Error::from(err))
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

/// Timestamp for request/response tracking
///
/// Uses RDTSC (Read Time-Stamp Counter) on x86_64 for minimal overhead.
/// Falls back to `Instant` on other architectures.
#[derive(Debug, Clone, Copy)]
pub struct Timestamp {
    cycles: u64,
}

/// Calibration data for converting RDTSC cycles to nanoseconds
struct TscCalibration {
    cycles_per_ns: f64,
}

impl TscCalibration {
    fn calibrate() -> Self {
        // Calibrate by measuring cycles over a known duration
        let start_cycles = rdtsc();
        let start_instant = Instant::now();

        // Sleep for a short calibration period
        std::thread::sleep(Duration::from_millis(10));

        let end_cycles = rdtsc();
        let elapsed_ns = start_instant.elapsed().as_nanos() as f64;

        let cycles = (end_cycles - start_cycles) as f64;
        let cycles_per_ns = cycles / elapsed_ns;

        Self { cycles_per_ns }
    }

    fn cycles_to_nanos(&self, cycles: u64) -> u64 {
        (cycles as f64 / self.cycles_per_ns) as u64
    }
}

/// Get calibration data (initialized once)
fn get_calibration() -> &'static TscCalibration {
    static CALIBRATION: std::sync::OnceLock<TscCalibration> = std::sync::OnceLock::new();
    CALIBRATION.get_or_init(TscCalibration::calibrate)
}

/// Read the CPU timestamp counter with serialization
///
/// Uses RDTSCP which includes a memory barrier to ensure the timestamp
/// is taken after all previous instructions complete.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn rdtsc() -> u64 {
    unsafe {
        let mut _aux: u32 = 0;
        core::arch::x86_64::__rdtscp(&mut _aux)
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
fn rdtsc() -> u64 {
    // Fallback for non-x86_64: use Instant
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

impl Timestamp {
    /// Create a new timestamp from current time
    ///
    /// Uses RDTSC for minimal overhead (~20 cycles vs ~200+ for clock_gettime).
    #[inline(always)]
    pub fn now() -> Self {
        Self { cycles: rdtsc() }
    }

    /// Calculate duration since another timestamp
    #[inline]
    pub fn duration_since(&self, earlier: &Timestamp) -> Duration {
        let delta_cycles = self.cycles.saturating_sub(earlier.cycles);
        let nanos = get_calibration().cycles_to_nanos(delta_cycles);
        Duration::from_nanos(nanos)
    }

    /// Get the raw cycle count (for comparisons)
    #[inline]
    pub fn cycles(&self) -> u64 {
        self.cycles
    }
}

// =============================================================================
// Single-Connection API (Transport trait)
// =============================================================================

/// Transport protocol trait for single connections
///
/// This trait defines the interface for transport layer implementations.
/// All methods are blocking/synchronous to enable tight control over timing
/// and busy-waiting in the event loop.
///
/// Each `Transport` instance owns its own poll/epoll instance. For managing
/// multiple connections efficiently, use the `ConnectionGroup` API instead.
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

// =============================================================================
// Connection Group API (ConnectionGroup trait)
// =============================================================================

/// A connection within a group
///
/// Represents a single connection managed by a `ConnectionGroup`. Provides
/// send/recv operations with timestamp tracking.
pub trait GroupConnection: Send {
    /// Send data and return timestamp
    ///
    /// Sends the provided data and captures a nanosecond-precision timestamp
    /// immediately after the send operation completes.
    fn send(&mut self, data: &[u8]) -> Result<Timestamp>;

    /// Receive data and return timestamp
    ///
    /// Attempts to receive data. Returns the received data and a timestamp
    /// captured immediately after the receive operation.
    ///
    /// Returns empty data if no data is available (non-blocking).
    fn recv(&mut self) -> Result<(Vec<u8>, Timestamp)>;

    /// Check if this connection is established
    fn is_connected(&self) -> bool;
}

/// A group of connections that can be polled together efficiently
///
/// This trait enables efficient I/O multiplexing across multiple connections
/// using a single poll/epoll instance. One `poll()` call checks all connections,
/// reducing syscall overhead compared to polling connections individually.
///
/// Typical usage pattern: create one `ConnectionGroup` per thread.
pub trait ConnectionGroup: Send {
    /// The connection type managed by this group
    type Conn: GroupConnection;

    /// Create and connect a new connection in this group
    ///
    /// Returns the connection ID that can be used to access the connection.
    fn add_connection(&mut self, target: &SocketAddr) -> Result<usize>;

    /// Get a connection by ID (immutable)
    fn get(&self, conn_id: usize) -> Option<&Self::Conn>;

    /// Get a connection by ID (mutable)
    fn get_mut(&mut self, conn_id: usize) -> Option<&mut Self::Conn>;

    /// Poll all connections for readability
    ///
    /// Returns a vector of connection IDs that have data ready to read.
    /// This is the key efficiency gain: one syscall checks all connections.
    ///
    /// # Arguments
    /// * `timeout` - Maximum time to wait. `None` means return immediately.
    fn poll(&mut self, timeout: Option<Duration>) -> Result<Vec<usize>>;

    /// Number of connections in the group
    fn len(&self) -> usize;

    /// Check if group is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Close a specific connection
    fn close(&mut self, conn_id: usize) -> Result<()>;

    /// Close all connections
    fn close_all(&mut self) -> Result<()>;
}

/// Factory for creating connection groups
///
/// Typically one connection group is created per thread. The factory is
/// `Send + Sync` so it can be shared across threads, with each thread
/// creating its own group.
pub trait TransportFactory: Send + Sync {
    /// The connection group type created by this factory
    type Group: ConnectionGroup;

    /// Create a new connection group
    ///
    /// Each group has its own poll/epoll instance. Create one per thread
    /// for optimal performance.
    fn create_group(&self) -> Result<Self::Group>;
}

// =============================================================================
// Module declarations
// =============================================================================

pub mod mux;
pub mod tcp;
pub mod udp;

#[cfg(unix)]
pub mod unix;

// =============================================================================
// Re-exports
// =============================================================================

pub use mux::{Multiplexer, MultiplexerType};

pub use tcp::TcpTransport;
pub use udp::UdpTransport;
#[cfg(unix)]
pub use unix::UnixTransport;

pub use tcp::{TcpConnectionGroup, TcpTransportFactory};
pub use udp::{UdpConnectionGroup, UdpTransportFactory};
#[cfg(unix)]
pub use unix::{UnixConnectionGroup, UnixTransportFactory};
