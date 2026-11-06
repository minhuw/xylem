//! Connection-group code shared by the TCP, UDP, and Unix transports
//!
//! Each transport's connection group keeps its own `add_connection` and its
//! connection send/receive code, and delegates the rest to a `GroupCore`: the
//! shared multiplexer, the map of live connections, and the id counter.
//!
//! This module also holds the kernel timestamping configuration used by the
//! TCP and UDP groups (Linux only).

use crate::mux::{Interest, Multiplexer, MultiplexerType};
use crate::{Error, Result};
use std::collections::HashMap;
use std::os::fd::{AsRawFd, RawFd};
use std::time::Duration;

#[cfg(target_os = "linux")]
use crate::{hw_timestamp, TxTimestampInfo};
#[cfg(target_os = "linux")]
use std::io;

/// Configuration for TCP and UDP timestamping (Linux only)
///
/// On Linux, this configures kernel-level timestamping for more accurate
/// latency measurement. Falls back to software timestamps if HW not available.
///
/// By default, kernel timestamps are enabled on Linux.
#[derive(Debug, Clone)]
pub struct TimestampConfig {
    /// Network interface to bind to for hardware timestamping (e.g., "eth0")
    /// If None, no interface binding is performed.
    #[cfg(target_os = "linux")]
    pub interface: Option<String>,

    /// Whether to enable kernel timestamping on sockets
    /// When true, uses SO_TIMESTAMPING for RX timestamps
    /// Default: true on Linux
    #[cfg(target_os = "linux")]
    pub enable_timestamps: bool,
}

impl Default for TimestampConfig {
    fn default() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            interface: None,
            #[cfg(target_os = "linux")]
            enable_timestamps: true, // Enable by default on Linux
        }
    }
}

impl TimestampConfig {
    /// Create config with timestamping enabled (no interface binding)
    #[cfg(target_os = "linux")]
    pub fn with_timestamps() -> Self {
        Self { interface: None, enable_timestamps: true }
    }

    /// Create config with timestamping and interface binding
    #[cfg(target_os = "linux")]
    pub fn with_interface(interface: impl Into<String>) -> Self {
        Self {
            interface: Some(interface.into()),
            enable_timestamps: true,
        }
    }

    /// Configure kernel timestamping on a socket (Linux only)
    ///
    /// Tries hardware timestamping first, falls back to software if unavailable.
    /// Returns true if any form of kernel timestamping was enabled.
    /// `transport` names the transport in log messages (e.g., "TCP").
    #[cfg(target_os = "linux")]
    pub(crate) fn configure_timestamping(
        &self,
        fd: RawFd,
        transport: &str,
        conn_id: usize,
    ) -> bool {
        // Bind to interface if specified
        if let Some(ref interface) = self.interface {
            if let Err(e) = hw_timestamp::bind_to_device(fd, interface) {
                tracing::warn!("Could not bind to device '{}': {}", interface, e);
            }
        }

        // Try hardware timestamping first
        if hw_timestamp::enable_socket_timestamping(fd).is_ok() {
            tracing::debug!(
                "Hardware timestamping enabled for {} connection {}",
                transport,
                conn_id
            );
            return true;
        }

        // Fallback to software timestamps
        tracing::debug!("Hardware timestamping not available, trying software");
        match hw_timestamp::enable_software_timestamping(fd) {
            Ok(()) => {
                tracing::debug!(
                    "Software timestamping enabled for {} connection {}",
                    transport,
                    conn_id
                );
                true
            }
            Err(e) => {
                tracing::warn!("Could not enable kernel timestamping: {}", e);
                false
            }
        }
    }
}

/// Socket access that `GroupCore` needs from a transport's connection type
pub(crate) trait GroupSocket {
    /// The file descriptor registered with the group's multiplexer
    fn socket_fd(&self) -> RawFd;

    /// Whether kernel timestamping is enabled on the socket (Linux only)
    ///
    /// Connections without kernel timestamping (Unix sockets) keep the default.
    #[cfg(target_os = "linux")]
    fn timestamps_enabled(&self) -> bool {
        false
    }
}

/// Bookkeeping shared by every connection group
///
/// Connection ids start at 0 and increase by one for every connection the
/// group tries to open. An id is never handed out again, even after its
/// connection closes or fails to connect.
pub(crate) struct GroupCore<C> {
    mux: Multiplexer,
    connections: HashMap<usize, C>,
    next_id: usize,
}

impl<C: GroupSocket> GroupCore<C> {
    pub(crate) fn new(mux_type: MultiplexerType) -> Result<Self> {
        Ok(Self {
            mux: Multiplexer::new(mux_type)?,
            connections: HashMap::new(),
            next_id: 0,
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
        let mut temp_mux = Multiplexer::new(self.mux.mux_type())?;
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
    pub(crate) fn insert(&mut self, conn_id: usize, conn: C) {
        self.connections.insert(conn_id, conn);
    }

    pub(crate) fn get(&self, conn_id: usize) -> Option<&C> {
        self.connections.get(&conn_id)
    }

    pub(crate) fn get_mut(&mut self, conn_id: usize) -> Option<&mut C> {
        self.connections.get_mut(&conn_id)
    }

    /// Wait for readable sockets and return the ids of live connections
    pub(crate) fn poll(&mut self, timeout: Option<Duration>) -> Result<Vec<usize>> {
        let events = self.mux.wait(timeout)?;
        let ready: Vec<usize> = events
            .iter()
            .filter(|e| e.readable)
            .map(|e| e.id)
            .filter(|id| self.connections.contains_key(id))
            .collect();
        Ok(ready)
    }

    /// Drain TX timestamps from the error queue of every connection with
    /// kernel timestamping enabled (Linux only, non-blocking)
    ///
    /// `transport` names the transport in log messages (e.g., "TCP").
    #[cfg(target_os = "linux")]
    pub(crate) fn poll_tx_timestamps(&self, transport: &str) -> Vec<TxTimestampInfo> {
        let mut results = Vec::new();

        for (&conn_id, conn) in &self.connections {
            if !conn.timestamps_enabled() {
                continue;
            }

            let fd = conn.socket_fd();

            // Poll error queue for TX timestamps (non-blocking)
            loop {
                match hw_timestamp::recv_tx_timestamp(fd) {
                    Ok(Some((timestamp, opt_id))) => {
                        results.push(TxTimestampInfo { conn_id, tx_offset: opt_id, timestamp });
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

        results
    }

    pub(crate) fn len(&self) -> usize {
        self.connections.len()
    }

    /// Close a connection, deregistering its socket before it is dropped
    pub(crate) fn close(&mut self, conn_id: usize) -> Result<()> {
        if let Some(conn) = self.connections.remove(&conn_id) {
            let _ = self.mux.deregister(conn.socket_fd());
        }
        Ok(())
    }

    pub(crate) fn close_all(&mut self) -> Result<()> {
        let conn_ids: Vec<usize> = self.connections.keys().copied().collect();
        for conn_id in conn_ids {
            self.close(conn_id)?;
        }
        Ok(())
    }
}
