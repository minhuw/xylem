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
//! ## Hardware Timestamping (Linux only)
//!
//! On Linux, transports can use kernel timestamps (SO_TIMESTAMPING) for more
//! accurate latency measurement. Hardware timestamps are used when available,
//! with automatic fallback to software timestamps. This requires:
//! - Linux kernel >= 4.19.4 for best results
//! - NIC with hardware timestamping support for HW timestamps (e.g., Mellanox ConnectX, Intel i210)
//! - Root or CAP_NET_ADMIN capability for NIC-level timestamping
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
use std::time::Duration;

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

/// Information about a TX timestamp received from the kernel
///
/// TX timestamps arrive asynchronously via the socket's error queue.
/// This struct contains the timestamp and the byte offset (opt_id) needed
/// to correlate it back to the original send operation.
#[derive(Debug, Clone, Copy)]
pub struct TxTimestampInfo {
    /// Connection ID that sent the data
    pub conn_id: usize,
    /// Byte offset (opt_id) for correlating with the send operation
    pub tx_offset: u32,
    /// The kernel-provided TX timestamp
    pub timestamp: Timestamp,
}

/// A connection within a group
///
/// Represents a single connection managed by a `ConnectionGroup`. Provides
/// send/recv operations with timestamp tracking.
pub trait GroupConnection: Send {
    /// Send data and return software timestamp and TX byte/packet range
    ///
    /// Sends the provided data and returns a tuple of:
    /// - `Timestamp`: Software timestamp captured immediately after the send
    /// - `u32`: TX range start (byte offset for TCP, packet ID for UDP)
    /// - `u32`: TX range end (byte offset for TCP, packet ID for UDP)
    ///
    /// The range semantics differ by transport:
    /// - **TCP**: Byte offsets where end = start + data.len() (wrapping u32 counter)
    /// - **UDP**: Packet IDs where start == end (single packet per send)
    ///
    /// The actual hardware TX timestamp will arrive later via the error queue
    /// and can be retrieved using `ConnectionGroup::poll_tx_timestamps()`.
    /// A TX timestamp with offset >= end indicates the data has been transmitted.
    ///
    /// Note: The counters wrap at u32::MAX. Callers should use wrapping comparison
    /// when correlating TX timestamps with send operations.
    fn send(&mut self, data: &[u8]) -> Result<(Timestamp, u32, u32)>;

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

    /// Poll for TX timestamps from all connections (Linux only)
    ///
    /// TX timestamps arrive asynchronously via the socket's error queue after
    /// the data has been transmitted. This method checks all connections and
    /// returns any available TX timestamps.
    ///
    /// Returns a vector of `TxTimestampInfo` containing:
    /// - `conn_id`: The connection that sent the data
    /// - `tx_offset`: Byte offset for correlating with the original send
    /// - `timestamp`: The kernel-provided TX timestamp
    ///
    /// On non-Linux platforms, this returns an empty vector.
    fn poll_tx_timestamps(&mut self) -> Result<Vec<TxTimestampInfo>> {
        // Default implementation returns empty (no TX timestamp support)
        Ok(Vec::new())
    }

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

pub mod buffer_pool;
pub mod mux;
pub mod tcp;
mod timestamp;
pub mod udp;

#[cfg(unix)]
pub mod unix;

// Hardware timestamping helpers (Linux only)
// Used internally by tcp/udp modules for kernel timestamp support
#[cfg(target_os = "linux")]
pub mod hw_timestamp;

// =============================================================================
// Re-exports
// =============================================================================

pub use buffer_pool::{BufferPool, MaybePooledBuffer, PooledBuffer};
pub use mux::{Multiplexer, MultiplexerType};
pub use timestamp::Timestamp;

pub use tcp::TcpTransport;
pub use udp::UdpTransport;
#[cfg(unix)]
pub use unix::UnixTransport;

pub use tcp::{TcpConnectionGroup, TcpTransportFactory};
pub use udp::{UdpConnectionGroup, UdpTransportFactory};
#[cfg(unix)]
pub use unix::{UnixConnectionGroup, UnixTransportFactory};

// Timestamp configuration (Linux only)
#[cfg(target_os = "linux")]
pub use tcp::TcpTimestampConfig;
#[cfg(target_os = "linux")]
pub use udp::UdpTimestampConfig;

// Hardware timestamping utilities (Linux only)
#[cfg(target_os = "linux")]
pub use hw_timestamp::{disable_nic_timestamping, enable_nic_timestamping};
