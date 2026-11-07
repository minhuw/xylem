//! Xylem Transport Layer
//!
//! This crate provides transport layer implementations for the Xylem latency
//! measurement tool. Transports handle the low-level network I/O using non-blocking
//! sockets and one readiness multiplexer per connection group: epoll on Linux,
//! mio (kqueue) elsewhere.
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
//! ## Connection Group API
//!
//! Efficient API for managing multiple connections. A single multiplexer is
//! shared across all connections in the group, so polling N connections requires
//! only 1 syscall.
//!
//! ```rust,no_run
//! use xylem_transport::{ConnectionGroup, RecvOutcome, TcpTransportFactory, TransportFactory};
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
//! let receipt = group.send(conn_id, b"PING\r\n").unwrap();
//!
//! // Poll all connections with one syscall
//! let mut ready = Vec::new();
//! let mut buf = vec![0u8; 65536];
//! group.poll(Duration::from_millis(100), &mut ready).unwrap();
//! for &id in &ready {
//!     if let RecvOutcome::Data { len, timestamp } = group.recv(id, &mut buf).unwrap() {
//!         let latency = timestamp.duration_since(&receipt.timestamp);
//!         println!("Received {:?} after {:?}", &buf[..len], latency);
//!     }
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

#[cfg(target_os = "linux")]
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

/// What a send handed to the transport
///
/// The range semantics differ by transport:
/// - **TCP** (and Unix): Byte offsets where end = start + data.len() (wrapping u32 counter)
/// - **UDP**: Packet IDs where start == end (single packet per send)
///
/// The actual hardware TX timestamp will arrive later via the error queue
/// and can be retrieved using `ConnectionGroup::poll_tx_timestamps()`.
/// A TX timestamp with offset >= end indicates the data has been transmitted.
///
/// Note: The counters wrap at u32::MAX. Callers should use wrapping comparison
/// when correlating TX timestamps with send operations.
#[derive(Debug, Clone, Copy)]
pub struct SendReceipt {
    /// Software timestamp taken once the transport accepted every byte
    pub timestamp: Timestamp,
    /// TX range start (byte offset for TCP, packet id for UDP), wrapping
    pub tx_start: u32,
    /// TX range end (byte offset for TCP, packet id for UDP), wrapping
    pub tx_end: u32,
}

/// Result of one receive attempt
#[derive(Debug, Clone, Copy)]
pub enum RecvOutcome {
    /// `len` bytes were written to the front of the buffer; `timestamp` is the
    /// receive time (kernel/HW when available, else software)
    Data { len: usize, timestamp: Timestamp },
    /// No data available right now
    WouldBlock,
    /// The peer closed its sending side; no more data will arrive
    Eof,
}

/// A group of connections that can be polled together efficiently
///
/// This trait enables efficient I/O multiplexing across multiple connections
/// using a single poll/epoll instance. One `poll()` call checks all connections,
/// reducing syscall overhead compared to polling connections individually.
///
/// All I/O goes through the group by connection ID, so a group may own its
/// connections in any form (e.g., a user-space network stack). Groups need
/// not be `Send`: typical usage creates one `ConnectionGroup` per thread,
/// from a shared `TransportFactory`.
pub trait ConnectionGroup {
    /// Create and connect a new connection in this group
    ///
    /// Blocks until the connection is established. Returns the connection ID
    /// used for all later operations on the connection.
    fn add_connection(&mut self, target: &SocketAddr) -> Result<usize>;

    /// Send all of `data` on `conn_id`
    ///
    /// Either every byte is handed to the transport or an error is returned;
    /// the transport spins while it would block. Returns the software
    /// timestamp and TX range used to correlate TX timestamps.
    ///
    /// Unknown connection IDs return `Error::Connection`.
    fn send(&mut self, conn_id: usize, data: &[u8]) -> Result<SendReceipt>;

    /// Read the bytes available on `conn_id` into `buf`
    ///
    /// Non-blocking. Datagram transports read at most one datagram, which the
    /// kernel truncates to `buf`'s length.
    ///
    /// Unknown connection IDs return `Error::Connection`.
    fn recv(&mut self, conn_id: usize, buf: &mut [u8]) -> Result<RecvOutcome>;

    /// Wait for readable connections
    ///
    /// Clears `ready`, waits up to `timeout` (zero means do not wait), and
    /// fills it with the IDs of connections that are readable: data, EOF, or
    /// an error to report. This is the key efficiency gain: one syscall
    /// checks all connections.
    ///
    /// Level-triggered: a connection keeps being reported until a `recv`
    /// returns `RecvOutcome::WouldBlock`.
    fn poll(&mut self, timeout: Duration, ready: &mut Vec<usize>) -> Result<()>;

    /// Poll for TX timestamps from all connections (Linux only)
    ///
    /// TX timestamps arrive asynchronously via the socket's error queue after
    /// the data has been transmitted. This method clears `out` and fills it
    /// with the TX timestamps available now, each a `TxTimestampInfo` with:
    /// - `conn_id`: The connection that sent the data
    /// - `tx_offset`: Byte offset for correlating with the original send
    /// - `timestamp`: The kernel-provided TX timestamp
    ///
    /// The default implementation (no TX timestamp support) leaves `out` empty.
    fn poll_tx_timestamps(&mut self, out: &mut Vec<TxTimestampInfo>) -> Result<()> {
        out.clear();
        Ok(())
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

// Connection-group code shared by the tcp, udp, and unix modules
mod group;
mod mux;
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

pub use timestamp::{time_ns, Timestamp};

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
