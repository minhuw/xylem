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
    /// Returns true if kernel timestamping is enabled on the socket, which
    /// it is for every source but user-space. Failing to enable `source`
    /// fails, naming the connection, the socket option and its errno; so
    /// does failing to bind to the interface for hardware timestamps.
    #[cfg(target_os = "linux")]
    pub(crate) fn configure_socket(
        &self,
        fd: RawFd,
        source: TimestampSource,
        transport: &str,
        conn_id: usize,
    ) -> Result<bool> {
        self.configure_socket_with(fd, source, transport, conn_id, hw_timestamp::set_timestamping)
    }

    /// [`Self::configure_socket`], setting `SO_TIMESTAMPING` with `set_timestamping`
    #[cfg(target_os = "linux")]
    fn configure_socket_with(
        &self,
        fd: RawFd,
        source: TimestampSource,
        transport: &str,
        conn_id: usize,
        set_timestamping: impl FnOnce(RawFd, u32) -> io::Result<()>,
    ) -> Result<bool> {
        if let (Some(interface), true) = (&self.interface, self.enable_timestamps) {
            if let Err(e) = hw_timestamp::bind_to_device(fd, interface) {
                if self.require_hardware || source == TimestampSource::Hardware {
                    let cause = match e {
                        Error::Config(message) => message,
                        e => e.to_string(),
                    };
                    return Err(Error::Config(format!(
                        "Could not bind {transport} connection {conn_id} to '{interface}' for \
                         its hardware timestamps: {cause}"
                    )));
                }
                tracing::warn!("Could not bind to device '{}': {}", interface, e);
            }
        }

        let ts_flags = match source {
            TimestampSource::Hardware => hw_timestamp::HARDWARE_FLAGS,
            TimestampSource::KernelSoftware => hw_timestamp::SOFTWARE_FLAGS,
            TimestampSource::UserSpace => return Ok(false),
        };
        set_timestamping(fd, ts_flags).map_err(|e| {
            Error::Config(format!(
                "Could not enable {source} timestamps on {transport} connection {conn_id}: \
                 setsockopt(SOL_SOCKET, SO_TIMESTAMPING, {ts_flags:#x}) failed: {e}"
            ))
        })?;
        Ok(true)
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
    /// names the transport in error messages (e.g., "TCP"). Fails on any
    /// error reading an error queue but an empty one, naming the connection.
    #[cfg(target_os = "linux")]
    pub(crate) fn poll_tx_timestamps(
        &mut self,
        cumulative: bool,
        transport: &str,
        out: &mut Vec<TxTimestampInfo>,
    ) -> Result<()> {
        out.clear();
        if self.tx_stamping == 0 {
            return Ok(());
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
                    // The error queue is empty (EAGAIN)
                    Ok(None) => break,
                    Err(e) => {
                        return Err(Error::Io(io::Error::new(
                            e.kind(),
                            format!(
                                "Could not read the TX timestamps of {transport} connection \
                                 {conn_id}: recvmsg(MSG_ERRQUEUE): {e}"
                            ),
                        )));
                    }
                }
            }
        }
        Ok(())
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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::os::fd::OwnedFd;

    #[test]
    fn a_socket_that_cannot_enable_its_timestamps_fails_naming_option_and_errno() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let fd = socket.as_raw_fd();
        let config = TimestampConfig::with_software_timestamps();
        for (source, flags) in [
            (TimestampSource::KernelSoftware, hw_timestamp::SOFTWARE_FLAGS),
            (TimestampSource::Hardware, hw_timestamp::HARDWARE_FLAGS),
        ] {
            let mut requested = None;
            let error = config
                .configure_socket_with(fd, source, "TCP", 3, |_, ts_flags| {
                    requested = Some(ts_flags);
                    Err(io::Error::from_raw_os_error(libc::EOPNOTSUPP))
                })
                .unwrap_err()
                .to_string();
            assert_eq!(requested, Some(flags));
            assert!(
                error.starts_with(&format!(
                    "Configuration error: Could not enable {source} timestamps on TCP \
                     connection 3: setsockopt(SOL_SOCKET, SO_TIMESTAMPING, {flags:#x}) failed: "
                )),
                "{error}"
            );
            assert!(error.ends_with(&format!("(os error {})", libc::EOPNOTSUPP)), "{error}");
        }
        // User-space timestamps set no socket option
        let user =
            config.configure_socket_with(fd, TimestampSource::UserSpace, "TCP", 3, |_, _| {
                panic!("set SO_TIMESTAMPING for user-space timestamps")
            });
        assert!(!user.unwrap());
        // The real option succeeds on a UDP socket
        let source = TimestampSource::KernelSoftware;
        assert!(config.configure_socket(fd, source, "UDP", 0).unwrap());
    }

    #[test]
    fn hardware_timestamps_fail_when_the_socket_cannot_bind_to_the_interface() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let config = TimestampConfig::with_interface("xylem-no-such-if");
        let error = config
            .configure_socket_with(
                socket.as_raw_fd(),
                TimestampSource::Hardware,
                "UDP",
                1,
                |_, _| panic!("enabled timestamps on an unbound socket"),
            )
            .unwrap_err()
            .to_string();
        assert!(
            error.starts_with(
                "Configuration error: Could not bind UDP connection 1 to 'xylem-no-such-if' for \
                 its hardware timestamps: setsockopt(SOL_SOCKET, SO_BINDTODEVICE, \
                 'xylem-no-such-if') failed: "
            ),
            "{error}"
        );
        assert!(error.contains("(os error"), "{error}");
    }

    /// A connection whose "socket" is any file descriptor
    struct FdConn {
        fd: OwnedFd,
        tx: TxCounter,
    }

    impl GroupSocket for FdConn {
        fn socket_fd(&self) -> RawFd {
            self.fd.as_raw_fd()
        }

        fn tx_counter(&mut self) -> Option<&mut TxCounter> {
            Some(&mut self.tx)
        }
    }

    #[test]
    fn an_error_queue_read_error_fails_naming_the_connection() {
        // Not a socket: recvmsg fails with ENOTSOCK, not EAGAIN
        let (reader, _writer) = std::io::pipe().unwrap();
        let mut core = GroupCore::new().unwrap();
        let conn_id = core.reserve_id();
        let mut tx = TxCounter::default();
        tx.advance(10);
        core.insert(conn_id, FdConn { fd: reader.into(), tx });
        let mut out = Vec::new();
        let error = core.poll_tx_timestamps(true, "TCP", &mut out).unwrap_err().to_string();
        assert!(
            error.starts_with(
                "I/O error: Could not read the TX timestamps of TCP connection 0: \
                 recvmsg(MSG_ERRQUEUE): "
            ),
            "{error}"
        );
        assert!(error.ends_with(&format!("(os error {})", libc::ENOTSOCK)), "{error}");
    }

    #[test]
    fn an_empty_error_queue_is_no_error() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let mut core = GroupCore::new().unwrap();
        let conn_id = core.reserve_id();
        let mut tx = TxCounter::default();
        tx.advance(1);
        core.insert(conn_id, FdConn { fd: socket.into(), tx });
        let mut out = vec![];
        core.poll_tx_timestamps(false, "UDP", &mut out).unwrap();
        assert!(out.is_empty());
    }
}
