//! Connection-group code shared by the TCP, UDP, and Unix transports
//!
//! Each transport's connection group keeps its own `add_connection` and its
//! connection send/receive code, and delegates the rest to a `GroupCore`: the
//! shared multiplexer, the map of live connections, and the id counter.
//!
//! This module also holds the kernel timestamping configuration used by the
//! TCP and UDP groups (Linux only).

use crate::mux::{Interest, Multiplexer};
use crate::{Error, Result, TxRange};
use std::os::fd::{AsRawFd, RawFd};
use std::time::Duration;

#[cfg(target_os = "linux")]
use crate::{hw_timestamp, TxTimestampInfo};
#[cfg(target_os = "linux")]
use std::io;

/// Source of the timestamps a connection group reports (Linux only)
#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampSource {
    /// NIC hardware timestamps (`SO_TIMESTAMPING` raw hardware), TX and RX
    Hardware,
    /// Kernel software timestamps (`SO_TIMESTAMPING` software), TX and RX
    KernelSoftware,
    /// User-space cycle counts only: no `SO_TIMESTAMPING`, no error queue
    UserSpace,
}

#[cfg(target_os = "linux")]
impl std::fmt::Display for TimestampSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TimestampSource::Hardware => "hardware",
            TimestampSource::KernelSoftware => "kernel software",
            TimestampSource::UserSpace => "user-space",
        })
    }
}

/// Configuration for TCP and UDP timestamping (Linux only)
///
/// A group uses NIC hardware timestamps only when `interface` is set and
/// that interface has hardware timestamping switched on (checked once per
/// group with `SIOCGHWTSTAMP`). Otherwise it uses kernel software
/// timestamps if `software_timestamps` is set, and user-space timestamps by
/// default. See [`TimestampSource`].
///
/// With `require_hardware`, a group never falls back: creating it fails
/// unless the interface has hardware timestamping switched on, and adding a
/// connection fails unless its socket is bound to the interface and has
/// hardware timestamping enabled.
#[derive(Debug, Clone)]
pub struct TimestampConfig {
    /// Network interface to bind to and take hardware timestamps from (e.g., "eth0")
    /// If None, no interface binding is performed and no hardware timestamps are used.
    #[cfg(target_os = "linux")]
    pub interface: Option<String>,

    /// Whether kernel timestamping may be used at all
    /// Default: true on Linux
    #[cfg(target_os = "linux")]
    pub enable_timestamps: bool,

    /// Use kernel software timestamps when hardware timestamps are not in effect
    /// Default: false (user-space timestamps)
    #[cfg(target_os = "linux")]
    pub software_timestamps: bool,

    /// Fail instead of falling back when `interface` cannot provide hardware
    /// timestamps. Never switches NIC timestamping on.
    /// Default: false
    #[cfg(target_os = "linux")]
    pub require_hardware: bool,
}

// Not derivable on Linux, where the fields have non-default defaults
#[allow(clippy::derivable_impls)]
impl Default for TimestampConfig {
    fn default() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            interface: None,
            #[cfg(target_os = "linux")]
            enable_timestamps: true,
            #[cfg(target_os = "linux")]
            software_timestamps: false,
            #[cfg(target_os = "linux")]
            require_hardware: false,
        }
    }
}

impl TimestampConfig {
    /// Create the default config: hardware timestamps only with an interface
    #[cfg(target_os = "linux")]
    pub fn with_timestamps() -> Self {
        Self::default()
    }

    /// Create config with interface binding and its hardware timestamps
    #[cfg(target_os = "linux")]
    pub fn with_interface(interface: impl Into<String>) -> Self {
        Self {
            interface: Some(interface.into()),
            ..Self::default()
        }
    }

    /// Create config that uses kernel software timestamps (no interface binding)
    #[cfg(target_os = "linux")]
    pub fn with_software_timestamps() -> Self {
        Self {
            software_timestamps: true,
            ..Self::default()
        }
    }

    /// Create config that uses the hardware timestamps of `interface` and
    /// never falls back to another source (see `require_hardware`)
    #[cfg(target_os = "linux")]
    pub fn require_hardware(interface: impl Into<String>) -> Self {
        Self {
            require_hardware: true,
            ..Self::with_interface(interface)
        }
    }

    /// Check that the required hardware timestamps are available
    ///
    /// Without `require_hardware`, always succeeds. With it, reads the
    /// interface's configuration with `SIOCGHWTSTAMP` and fails unless
    /// hardware timestamping is switched on.
    #[cfg(target_os = "linux")]
    pub fn check(&self) -> Result<()> {
        if !self.require_hardware {
            return Ok(());
        }
        let Some(interface) = &self.interface else {
            return Err(Error::Config(
                "Hardware timestamps are required, but no interface is configured".to_string(),
            ));
        };
        match hw_timestamp::nic_timestamping_enabled(interface) {
            Ok(true) => Ok(()),
            Ok(false) => Err(Error::Config(format!(
                "Hardware timestamping is not enabled on '{interface}': SIOCGHWTSTAMP reports \
                 TX timestamping off or no RX filter. Switch it on for the NIC (e.g. \
                 `hwstamp_ctl -i {interface} -t 1 -r 1`, as root), or use kernel software \
                 timestamps"
            ))),
            Err(Error::Config(message)) => Err(Error::Config(format!(
                "{message}; '{interface}' cannot provide hardware timestamps"
            ))),
            Err(e) => Err(e),
        }
    }

    /// Decide the timestamp source for a new group and log it at info level
    ///
    /// `transport` names the transport in log messages (e.g., "TCP"). Fails
    /// only when hardware timestamps are required and unavailable.
    #[cfg(target_os = "linux")]
    pub(crate) fn resolve_source(&self, transport: &str) -> Result<TimestampSource> {
        if self.require_hardware {
            self.check()?;
            tracing::info!("{} connection group timestamp source: hardware", transport);
            return Ok(TimestampSource::Hardware);
        }
        let hardware = match (&self.interface, self.enable_timestamps) {
            (Some(interface), true) => match hw_timestamp::nic_timestamping_enabled(interface) {
                Ok(true) => true,
                Ok(false) => {
                    tracing::warn!(
                        "Hardware timestamping is not enabled on '{}'; not using it",
                        interface
                    );
                    false
                }
                Err(e) => {
                    tracing::warn!("{}; not using hardware timestamps", e);
                    false
                }
            },
            _ => false,
        };
        let source = if hardware {
            TimestampSource::Hardware
        } else if self.enable_timestamps && self.software_timestamps {
            TimestampSource::KernelSoftware
        } else {
            TimestampSource::UserSpace
        };
        tracing::info!("{} connection group timestamp source: {}", transport, source);
        Ok(source)
    }

    /// Bind a new socket to the interface and enable `source` on it (Linux only)
    ///
    /// Returns true if kernel timestamping is enabled on the socket. Fails
    /// only when hardware timestamps are required and either step fails.
    #[cfg(target_os = "linux")]
    pub(crate) fn configure_socket(
        &self,
        fd: RawFd,
        source: TimestampSource,
        transport: &str,
        conn_id: usize,
    ) -> Result<bool> {
        if let (Some(interface), true) = (&self.interface, self.enable_timestamps) {
            if let Err(e) = hw_timestamp::bind_to_device(fd, interface) {
                if self.require_hardware {
                    return Err(e);
                }
                tracing::warn!("Could not bind to device '{}': {}", interface, e);
            }
        }

        let result = match source {
            TimestampSource::Hardware => hw_timestamp::enable_socket_timestamping(fd),
            TimestampSource::KernelSoftware => hw_timestamp::enable_software_timestamping(fd),
            TimestampSource::UserSpace => return Ok(false),
        };
        match result {
            Ok(()) => Ok(true),
            Err(e) if self.require_hardware => Err(e),
            Err(e) => {
                tracing::warn!(
                    "Could not enable {} timestamps on {} connection {}: {}",
                    source,
                    transport,
                    conn_id,
                    e
                );
                Ok(false)
            }
        }
    }
}

/// Wrapping less-than for u32 counters (valid while they differ by < 2^31)
#[cfg(target_os = "linux")]
fn wrapping_lt(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) < 0
}

/// Send counter correlating kernel TX timestamps with sends
///
/// Counts in the units of the kernel's `SOF_TIMESTAMPING_OPT_ID` counter:
/// bytes for TCP, datagrams for UDP. Both start at zero when timestamping is
/// enabled, before the first send.
#[derive(Debug, Default)]
pub(crate) struct TxCounter {
    /// Exclusive end of everything sent so far
    sent: u32,
    /// Highest exclusive end that a received TX timestamp covered
    #[cfg(target_os = "linux")]
    stamped: u32,
}

impl TxCounter {
    /// Count `units` more sent; returns their range
    pub(crate) fn advance(&mut self, units: u32) -> TxRange {
        let start = self.sent;
        self.sent = start.wrapping_add(units);
        TxRange { start, end: self.sent }
    }

    /// Whether some send has not been covered by a TX timestamp yet
    #[cfg(target_os = "linux")]
    fn pending(&self) -> bool {
        self.stamped != self.sent
    }

    /// Note a TX timestamp covering up to `end` (exclusive)
    #[cfg(target_os = "linux")]
    fn on_stamp(&mut self, end: u32) {
        if wrapping_lt(self.stamped, end) {
            self.stamped = end;
        }
    }
}

/// Socket access that `GroupCore` needs from a transport's connection type
pub(crate) trait GroupSocket {
    /// The file descriptor registered with the group's multiplexer
    fn socket_fd(&self) -> RawFd;

    /// The TX counter, when kernel TX timestamps are enabled on the socket
    ///
    /// Connections without kernel timestamping (Unix sockets) keep the default.
    #[cfg(target_os = "linux")]
    fn tx_counter(&mut self) -> Option<&mut TxCounter> {
        None
    }
}

/// Bookkeeping shared by every connection group
///
/// Connection ids start at 0 and increase by one for every connection the
/// group tries to open. An id is never handed out again, even after its
/// connection closes or fails to connect.
pub(crate) struct GroupCore<C> {
    mux: Multiplexer,
    /// Live connections by id; `None` for an id that closed or never connected
    connections: Vec<Option<C>>,
    /// Number of live connections
    live: usize,
    next_id: usize,
    /// Number of connections with kernel TX timestamps enabled
    #[cfg(target_os = "linux")]
    tx_stamping: usize,
}

impl<C: GroupSocket> GroupCore<C> {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self {
            mux: Multiplexer::new()?,
            connections: Vec::new(),
            live: 0,
            next_id: 0,
            #[cfg(target_os = "linux")]
            tx_stamping: 0,
        })
    }

    /// Reserve the id for the next connection
    pub(crate) fn reserve_id(&mut self) -> usize {
        let conn_id = self.next_id;
        self.next_id += 1;
        conn_id
    }

    /// Wait for a non-blocking connect to complete (the socket becomes writable)
    ///
    /// Uses a temporary multiplexer for connection establishment to avoid
    /// mixing connection events with other connections in the group. Does not
    /// check the socket for a connection error.
    pub(crate) fn wait_for_connect<S: AsRawFd>(&self, socket: &S) -> Result<()> {
        let mut temp_mux = Multiplexer::new()?;
        temp_mux.register_fd(socket, 0, Interest::BOTH)?;

        // Poll for connection complete
        for _ in 0..50 {
            let events = temp_mux.wait(Some(Duration::from_millis(100)))?;

            let connected = events.iter().any(|e| e.id == 0 && e.writable);
            if connected {
                let _ = temp_mux.deregister_fd(socket);
                return Ok(());
            }
        }

        Err(Error::Connection("Connection timeout".to_string()))
    }

    /// Register a connected socket with the group's multiplexer for reads
    pub(crate) fn register_readable<S: AsRawFd>(
        &mut self,
        socket: &S,
        conn_id: usize,
    ) -> Result<()> {
        self.mux.register_fd(socket, conn_id, Interest::READABLE)
    }

    /// Start tracking a connection whose socket is already registered
    // Only Linux reads the TX counter
    #[cfg_attr(not(target_os = "linux"), allow(unused_mut))]
    pub(crate) fn insert(&mut self, conn_id: usize, mut conn: C) {
        #[cfg(target_os = "linux")]
        if conn.tx_counter().is_some() {
            self.tx_stamping += 1;
        }
        if self.connections.len() <= conn_id {
            self.connections.resize_with(conn_id + 1, || None);
        }
        debug_assert!(self.connections[conn_id].is_none(), "ids are never reused");
        self.connections[conn_id] = Some(conn);
        self.live += 1;
    }

    /// A live connection
    pub(crate) fn get(&self, conn_id: usize) -> Option<&C> {
        self.connections.get(conn_id)?.as_ref()
    }

    /// Look up a live connection for I/O
    pub(crate) fn conn_mut(&mut self, conn_id: usize) -> Result<&mut C> {
        self.connections
            .get_mut(conn_id)
            .and_then(Option::as_mut)
            .ok_or_else(|| Error::Connection("Connection not found".to_string()))
    }

    fn is_live(&self, conn_id: usize) -> bool {
        self.connections.get(conn_id).is_some_and(Option::is_some)
    }

    /// Wait for readable sockets, and writable ones registered for writes
    /// (see [`Self::set_write_interest`]), and collect the ids of live
    /// connections into `ready`
    pub(crate) fn poll(&mut self, timeout: Duration, ready: &mut Vec<usize>) -> Result<()> {
        ready.clear();
        let events = self.mux.wait(Some(timeout))?;
        ready.extend(
            events
                .iter()
                .filter(|e| e.readable || e.writable)
                .map(|e| e.id)
                .filter(|&id| self.is_live(id)),
        );
        Ok(())
    }

    /// Whether `poll` also reports `conn_id` when its socket is writable
    pub(crate) fn set_write_interest(&mut self, conn_id: usize, interested: bool) -> Result<()> {
        let fd = self.conn_mut(conn_id)?.socket_fd();
        let interest = if interested {
            Interest::BOTH
        } else {
            Interest::READABLE
        };
        self.mux.modify(fd, conn_id, interest)
    }

    /// Drain TX timestamps from the error queue of every connection with
    /// sends not yet covered by one (Linux only, non-blocking)
    ///
    /// Normalises the kernel's `SOF_TIMESTAMPING_OPT_ID` value to the
    /// exclusive end of what the stamp covers: Linux reports the offset of
    /// the last byte for TCP and the datagram index for UDP, so both become
    /// `ee_data + 1`, comparable with `SendReceipt::tx`'s `end`.
    ///
    /// `cumulative` is passed through to `TxTimestampInfo`; `transport`
    /// names the transport in log messages (e.g., "TCP").
    #[cfg(target_os = "linux")]
    pub(crate) fn poll_tx_timestamps(
        &mut self,
        cumulative: bool,
        transport: &str,
        out: &mut Vec<TxTimestampInfo>,
    ) {
        out.clear();
        if self.tx_stamping == 0 {
            return;
        }

        for (conn_id, conn) in self.connections.iter_mut().enumerate() {
            let Some(conn) = conn else {
                continue;
            };
            let fd = conn.socket_fd();
            let Some(counter) = conn.tx_counter() else {
                continue;
            };
            if !counter.pending() {
                continue;
            }

            // Poll error queue for TX timestamps (non-blocking)
            loop {
                match hw_timestamp::recv_tx_timestamp(fd) {
                    Ok(Some((timestamp, opt_id))) => {
                        let tx_end = opt_id.wrapping_add(1);
                        counter.on_stamp(tx_end);
                        out.push(TxTimestampInfo { conn_id, tx_end, cumulative, timestamp });
                    }
                    Ok(None) => break, // No more timestamps available
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => {
                        tracing::trace!(
                            "Error polling TX timestamp for {} conn {}: {}",
                            transport,
                            conn_id,
                            e
                        );
                        break;
                    }
                }
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.live
    }

    /// Close a connection, deregistering its socket before it is dropped
    #[cfg_attr(not(target_os = "linux"), allow(unused_mut))]
    pub(crate) fn close(&mut self, conn_id: usize) -> Result<()> {
        if let Some(mut conn) = self.connections.get_mut(conn_id).and_then(Option::take) {
            self.live -= 1;
            let _ = self.mux.deregister(conn.socket_fd());
            #[cfg(target_os = "linux")]
            if conn.tx_counter().is_some() {
                self.tx_stamping -= 1;
            }
        }
        Ok(())
    }

    pub(crate) fn close_all(&mut self) -> Result<()> {
        for conn_id in 0..self.connections.len() {
            self.close(conn_id)?;
        }
        Ok(())
    }
}
