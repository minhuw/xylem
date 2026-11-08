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
//! - **ntcp**: User-space TCP over a packet backend: a Linux TUN device
//!   (Linux only, `ntcp` feature), a DPDK port (`ntcp-dpdk` feature) or
//!   AF_XDP sockets (`ntcp-af-xdp` feature)
//!
//! ## Kernel Timestamping (Linux only)
//!
//! By default every timestamp is a user-space cycle count read when the send
//! or receive call completed. TCP and UDP groups use kernel timestamps
//! (SO_TIMESTAMPING) only when asked, through `TimestampConfig`:
//! - NIC hardware timestamps, when an interface is configured and has
//!   hardware timestamping switched on (e.g. with `enable_nic_timestamping`,
//!   which needs root or CAP_NET_ADMIN);
//! - otherwise kernel software timestamps, if requested explicitly.
//!
//! `TimestampConfig::require_hardware` never falls back: a group is not
//! created unless its interface has hardware timestamping switched on.
//!
//! Each group logs the source in effect once, at info level. A kernel
//! timestamp is used for a latency only when the send and the receive both
//! have one from the same clock; see [`Timestamp::duration_since`]. The
//! cycles of both stay available through
//! [`Timestamp::cycles_duration_since`].
//!
//! ## TX Timestamp Correlation
//!
//! Each connection counts what it sends in the units of its TX timestamp
//! counter: bytes for byte streams (TCP), datagrams for datagram transports
//! (UDP). Counters start at zero before the first send and wrap at
//! `u32::MAX`; compare them with wrapping arithmetic.
//!
//! - A send returns [`SendReceipt::tx`] `= Some(TxRange { start, end })`, the
//!   half-open range of counter values it used. `end` is exclusive: two
//!   10-byte TCP sends give `[0, 10)` and `[10, 20)`; two UDP sends give
//!   `[0, 1)` and `[1, 2)`. It is `None` when the connection reports no TX
//!   timestamps.
//! - A TX timestamp ([`TxTimestampInfo`]) carries `tx_end`, the exclusive end
//!   of what it covers, in the same units. Transports normalise the kernel's
//!   value to this: with `SOF_TIMESTAMPING_OPT_ID`, Linux reports the offset
//!   of the last byte for TCP (`seq + len - 1`) and the datagram index for
//!   UDP, both `tx_end - 1`.
//! - If `cumulative` (TCP), the stamp covers every not yet stamped send with
//!   `end <= tx_end`: the kernel stamps only the last segment of what it
//!   transmits together. Otherwise (UDP) it covers only the send with
//!   `end == tx_end`, so a missing or reordered stamp is never given to
//!   another send.
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

/// Half-open range `[start, end)` of TX counter values used by one send
///
/// See [TX timestamp correlation](crate#tx-timestamp-correlation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxRange {
    /// First counter value of the send (wrapping)
    pub start: u32,
    /// Exclusive end: one past the last counter value of the send (wrapping)
    pub end: u32,
}

/// Information about a TX timestamp received from the kernel
///
/// TX timestamps arrive asynchronously via the socket's error queue.
/// `tx_end` and `cumulative` say which sends the stamp belongs to.
#[derive(Debug, Clone, Copy)]
pub struct TxTimestampInfo {
    /// Connection ID that sent the data
    pub conn_id: usize,
    /// Exclusive end of the TX counter range the stamp covers; equal to the
    /// `TxRange::end` of the last send it belongs to
    pub tx_end: u32,
    /// Whether the stamp also covers earlier sends not yet stamped (byte
    /// streams), rather than only the send ending at `tx_end` (datagrams)
    pub cumulative: bool,
    /// The kernel-provided TX timestamp; attach it to the send's own
    /// `Timestamp` with [`Timestamp::with_kernel`]
    pub timestamp: KernelTimestamp,
}

/// What a send handed to the transport
///
/// The kernel TX timestamp, if any, arrives later via
/// `ConnectionGroup::poll_tx_timestamps()` and is matched with `tx`.
#[derive(Debug, Clone, Copy)]
pub struct SendReceipt {
    /// Software timestamp taken once the transport accepted every byte
    pub timestamp: Timestamp,
    /// TX counter range of this send, or `None` if the connection reports no
    /// TX timestamps
    pub tx: Option<TxRange>,
}

/// How a group converted NIC timestamps to the [`time_ns`] clock
///
/// The group fits the NIC clock to `time_ns` from paired readings of both,
/// taken about every 100 ms over a sliding window, and converts each RX
/// hardware timestamp with the fit in effect when it was read.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClockCalibrationReport {
    /// Nanoseconds of `time_ns` per NIC clock tick, by the last fit
    pub ns_per_tick: Option<f64>,
    /// RMS residual of the last fit's inliers, in nanoseconds
    pub residual_rms_ns: Option<f64>,
    /// Largest inlier residual of any fit during the run, in nanoseconds
    pub max_residual_ns: Option<f64>,
    /// Paired readings in the last fit's window that it used and rejected
    pub inliers: usize,
    pub outliers: usize,
    /// Paired readings refused because a clock did not move forward
    pub rejected_samples: u64,
    /// Receives whose hardware timestamp was converted
    pub converted: u64,
    /// Receives without a hardware timestamp of the NIC clock, or read
    /// before the first fit: their latencies have no kernel split
    pub unconverted: u64,
}

/// Result of one receive attempt
#[derive(Debug, Clone, Copy)]
pub enum RecvOutcome {
    /// `len` bytes were written to the front of the buffer; `timestamp` holds
    /// the cycles read when the receive completed, plus the kernel/HW receive
    /// timestamp when one was reported
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
    /// timestamp and, when TX timestamps are on, the TX range used to
    /// correlate them.
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

    /// Whether the group's connections carry datagrams rather than a byte stream
    ///
    /// A datagram group's `recv` returns exactly one whole message, so its
    /// boundaries are meaningful: the connection pool then parses each
    /// received datagram on its own instead of appending it to earlier
    /// unparsed bytes. Byte-stream groups (the default) return `false`.
    fn is_datagram(&self) -> bool {
        false
    }

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

    /// Poll for TX timestamps from all connections
    ///
    /// TX timestamps arrive asynchronously (via the socket's error queue on
    /// Linux) after the data has been transmitted. This method clears `out`
    /// and fills it with the TX timestamps available now. Connections whose
    /// sends are all stamped already, or that report no TX timestamps, cost
    /// nothing.
    ///
    /// The default implementation (no TX timestamp support) leaves `out` empty.
    fn poll_tx_timestamps(&mut self, out: &mut Vec<TxTimestampInfo>) -> Result<()> {
        out.clear();
        Ok(())
    }

    /// How the group converted NIC timestamps to the [`time_ns`] clock, for
    /// groups that report [`KernelClock::NicCalibrated`] timestamps
    ///
    /// The default implementation (no such timestamps) returns `None`.
    fn clock_calibration(&self) -> Option<ClockCalibrationReport> {
        None
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

// User-space TCP transport (Linux only, opt-in)
#[cfg(all(target_os = "linux", feature = "ntcp"))]
pub mod ntcp;

// =============================================================================
// Re-exports
// =============================================================================

pub use timestamp::{time_ns, KernelClock, KernelTimestamp, Timestamp};

#[cfg(target_os = "linux")]
pub use group::TimestampSource;

#[cfg(all(target_os = "linux", feature = "ntcp-af-xdp"))]
pub use ntcp::af_xdp::{
    NtcpAfXdpConfig, NtcpAfXdpFactory, NtcpAfXdpMember, NtcpAfXdpPort, XdpMode,
};
#[cfg(all(target_os = "linux", feature = "ntcp-dpdk"))]
pub use ntcp::dpdk::{NtcpDpdkConfig, NtcpDpdkFactory, NtcpDpdkMember, NtcpDpdkPort};
#[cfg(all(target_os = "linux", feature = "ntcp"))]
pub use ntcp::{NtcpBackend, NtcpConnectionGroup, NtcpTunFactory};
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
pub use hw_timestamp::{
    disable_nic_timestamping, enable_nic_timestamping, nic_timestamping_enabled,
};
