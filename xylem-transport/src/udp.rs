//! UDP transport implementation using non-blocking I/O
//!
//! This module provides two APIs:
//! - `UdpTransport`: Single-connection API (one multiplexer per connection)
//! - `UdpConnectionGroup`: Multi-connection API (one multiplexer per group)
//!
//! ## Hardware Timestamping (Linux only)
//!
//! On Linux, this transport can use kernel timestamps (SO_TIMESTAMPING) for
//! more accurate latency measurement. Hardware timestamps are used when the
//! NIC supports them, with automatic fallback to software timestamps.
//! Configure via `UdpTimestampConfig`.

use crate::mux::{Interest, Multiplexer, MultiplexerType};
use crate::{
    ConnectionGroup, Error, GroupConnection, Result, Timestamp, Transport, TransportFactory,
};
use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

#[cfg(target_os = "linux")]
use crate::hw_timestamp;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

const DEFAULT_UDP_BUFFER_SIZE: usize = 65535;

/// Configuration for UDP timestamping (Linux only)
///
/// On Linux, this configures kernel-level timestamping for more accurate
/// latency measurement. Falls back to software timestamps if HW not available.
///
/// By default, kernel timestamps are enabled on Linux.
#[derive(Debug, Clone)]
pub struct UdpTimestampConfig {
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

impl Default for UdpTimestampConfig {
    fn default() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            interface: None,
            #[cfg(target_os = "linux")]
            enable_timestamps: true, // Enable by default on Linux
        }
    }
}

impl UdpTimestampConfig {
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
}

pub struct UdpTransport {
    socket: Option<UdpSocket>,
    target: Option<SocketAddr>,
    mux: Multiplexer,
    recv_buffer: Vec<u8>,
}

impl UdpTransport {
    pub fn new() -> Self {
        Self::with_multiplexer(MultiplexerType::default())
    }

    pub fn with_multiplexer(mux_type: MultiplexerType) -> Self {
        Self {
            socket: None,
            target: None,
            mux: Multiplexer::new(mux_type).expect("Failed to create multiplexer"),
            recv_buffer: vec![0u8; DEFAULT_UDP_BUFFER_SIZE],
        }
    }
}

impl Default for UdpTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl Transport for UdpTransport {
    fn connect(&mut self, target: &SocketAddr) -> Result<()> {
        let bind_addr: SocketAddr = if target.is_ipv4() {
            "0.0.0.0:0".parse().unwrap()
        } else {
            "[::]:0".parse().unwrap()
        };

        let socket = UdpSocket::bind(bind_addr)?;
        socket.set_nonblocking(true)?;

        self.mux.register_fd(&socket, 0, Interest::READABLE)?;

        self.socket = Some(socket);
        self.target = Some(*target);

        Ok(())
    }

    fn send(&mut self, data: &[u8]) -> Result<Timestamp> {
        let socket = self
            .socket
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".to_string()))?;

        let target = self
            .target
            .ok_or_else(|| Error::Connection("No target address set".to_string()))?;

        loop {
            match socket.send_to(data, target) {
                Ok(_) => break,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::hint::spin_loop();
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(Timestamp::now())
    }

    fn recv(&mut self) -> Result<(Vec<u8>, Timestamp)> {
        let socket = self
            .socket
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected".to_string()))?;

        match socket.recv_from(&mut self.recv_buffer) {
            Ok((n, _src_addr)) => {
                let timestamp = Timestamp::now();
                let data = self.recv_buffer[..n].to_vec();
                Ok((data, timestamp))
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok((Vec::new(), Timestamp::now())),
            Err(e) => Err(e.into()),
        }
    }

    fn poll_readable(&mut self) -> Result<bool> {
        let events = self.mux.wait(Some(Duration::ZERO))?;
        Ok(!events.is_empty())
    }

    fn close(&mut self) -> Result<()> {
        if let Some(socket) = self.socket.take() {
            let _ = self.mux.deregister_fd(&socket);
        }
        self.target = None;
        Ok(())
    }
}

// =============================================================================
// Connection Group API
// =============================================================================

/// A single UDP "connection" within a connection group
pub struct UdpConn {
    socket: UdpSocket,
    target: SocketAddr,
    recv_buffer: Vec<u8>,
    /// TX packet counter for correlating TX timestamps (wrapping)
    /// For UDP, each datagram gets its own timestamp, so we use a simple counter
    tx_packet_counter: u32,
    /// Whether kernel timestamping is enabled for this connection
    #[cfg(target_os = "linux")]
    timestamps_enabled: bool,
}

impl UdpConn {
    #[allow(dead_code)] // Used on non-Linux or when timestamps not configured
    fn new(socket: UdpSocket, target: SocketAddr) -> Self {
        Self {
            socket,
            target,
            recv_buffer: vec![0u8; DEFAULT_UDP_BUFFER_SIZE],
            tx_packet_counter: 0,
            #[cfg(target_os = "linux")]
            timestamps_enabled: false,
        }
    }

    #[cfg(target_os = "linux")]
    fn with_timestamps(socket: UdpSocket, target: SocketAddr, timestamps_enabled: bool) -> Self {
        Self {
            socket,
            target,
            recv_buffer: vec![0u8; DEFAULT_UDP_BUFFER_SIZE],
            tx_packet_counter: 0,
            timestamps_enabled,
        }
    }

    /// Get the raw file descriptor (for polling TX timestamps)
    #[cfg(target_os = "linux")]
    pub(crate) fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
        self.socket.as_raw_fd()
    }

    /// Check if timestamps are enabled
    #[cfg(target_os = "linux")]
    pub(crate) fn timestamps_enabled(&self) -> bool {
        self.timestamps_enabled
    }
}

impl GroupConnection for UdpConn {
    fn send(&mut self, data: &[u8]) -> Result<(Timestamp, u32, u32)> {
        // For UDP, kernel's SOF_TIMESTAMPING_OPT_ID returns a packet counter (not byte offset).
        // The kernel reports packet ID N after the Nth sendmsg (0-indexed).
        // Unlike TCP where opt_id = cumulative bytes sent, UDP opt_id = packet sequence number.
        //
        // We store tx_end = packet_id so that when kernel reports opt_id = N, the comparison
        // `tx_end <= offset` (i.e., `N <= N`) is true and we match correctly.
        let packet_id = self.tx_packet_counter;
        let tx_start = packet_id;
        let tx_end = packet_id;

        loop {
            match self.socket.send_to(data, self.target) {
                Ok(_) => break,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::hint::spin_loop();
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        // Increment packet counter for next send
        self.tx_packet_counter = packet_id.wrapping_add(1);

        // Return software timestamp and packet ID for correlation
        Ok((Timestamp::now(), tx_start, tx_end))
    }

    fn recv(&mut self) -> Result<(Vec<u8>, Timestamp)> {
        // On Linux with timestamps enabled, use recvmsg to get kernel timestamp
        #[cfg(target_os = "linux")]
        if self.timestamps_enabled {
            let fd = self.socket.as_raw_fd();
            match hw_timestamp::recvmsg_with_timestamp(fd, &mut self.recv_buffer, 0) {
                Ok((n, ts_opt)) => {
                    let data = self.recv_buffer[..n].to_vec();
                    // Use hardware timestamp if available, otherwise software
                    let timestamp = ts_opt.unwrap_or_else(Timestamp::now);
                    return Ok((data, timestamp));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    return Ok((Vec::new(), Timestamp::now()));
                }
                Err(e) => return Err(e.into()),
            }
        }

        // Standard recv path (non-Linux or timestamps disabled)
        match self.socket.recv_from(&mut self.recv_buffer) {
            Ok((n, _src_addr)) => {
                let timestamp = Timestamp::now();
                let data = self.recv_buffer[..n].to_vec();
                Ok((data, timestamp))
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok((Vec::new(), Timestamp::now())),
            Err(e) => Err(e.into()),
        }
    }

    fn is_connected(&self) -> bool {
        true // UDP is always "connected"
    }
}

/// A group of UDP sockets sharing a single multiplexer
pub struct UdpConnectionGroup {
    mux: Multiplexer,
    connections: HashMap<usize, UdpConn>,
    next_id: usize,
    /// Timestamp configuration for new connections
    #[cfg(target_os = "linux")]
    timestamp_config: UdpTimestampConfig,
}

impl UdpConnectionGroup {
    pub fn new() -> Result<Self> {
        Self::with_multiplexer(MultiplexerType::default())
    }

    pub fn with_multiplexer(mux_type: MultiplexerType) -> Result<Self> {
        Ok(Self {
            mux: Multiplexer::new(mux_type)?,
            connections: HashMap::new(),
            next_id: 0,
            #[cfg(target_os = "linux")]
            timestamp_config: UdpTimestampConfig::default(),
        })
    }

    /// Create with timestamp configuration (Linux only)
    #[cfg(target_os = "linux")]
    pub fn with_timestamp_config(
        mux_type: MultiplexerType,
        timestamp_config: UdpTimestampConfig,
    ) -> Result<Self> {
        Ok(Self {
            mux: Multiplexer::new(mux_type)?,
            connections: HashMap::new(),
            next_id: 0,
            timestamp_config,
        })
    }

    /// Configure kernel timestamping on a socket (Linux only)
    ///
    /// Tries hardware timestamping first, falls back to software if unavailable.
    /// Returns true if any form of kernel timestamping was enabled.
    #[cfg(target_os = "linux")]
    fn configure_timestamping(&self, fd: i32, conn_id: usize) -> bool {
        // Bind to interface if specified
        if let Some(ref interface) = self.timestamp_config.interface {
            if let Err(e) = hw_timestamp::bind_to_device(fd, interface) {
                tracing::warn!("Could not bind to device '{}': {}", interface, e);
            }
        }

        // Try hardware timestamping first
        if hw_timestamp::enable_socket_timestamping(fd).is_ok() {
            tracing::debug!("Hardware timestamping enabled for UDP connection {}", conn_id);
            return true;
        }

        // Fallback to software timestamps
        tracing::debug!("Hardware timestamping not available, trying software");
        match hw_timestamp::enable_software_timestamping(fd) {
            Ok(()) => {
                tracing::debug!("Software timestamping enabled for UDP connection {}", conn_id);
                true
            }
            Err(e) => {
                tracing::warn!("Could not enable kernel timestamping: {}", e);
                false
            }
        }
    }
}

impl Default for UdpConnectionGroup {
    fn default() -> Self {
        Self::new().expect("Failed to create UdpConnectionGroup")
    }
}

impl ConnectionGroup for UdpConnectionGroup {
    type Conn = UdpConn;

    fn add_connection(&mut self, target: &SocketAddr) -> Result<usize> {
        let conn_id = self.next_id;
        self.next_id += 1;

        let bind_addr: SocketAddr = if target.is_ipv4() {
            "0.0.0.0:0".parse().unwrap()
        } else {
            "[::]:0".parse().unwrap()
        };

        let socket = UdpSocket::bind(bind_addr)?;
        socket.set_nonblocking(true)?;
        self.mux.register_fd(&socket, conn_id, Interest::READABLE)?;

        // On Linux, configure timestamping
        #[cfg(target_os = "linux")]
        let conn = {
            let timestamps_enabled = if self.timestamp_config.enable_timestamps {
                self.configure_timestamping(socket.as_raw_fd(), conn_id)
            } else {
                false
            };

            UdpConn::with_timestamps(socket, *target, timestamps_enabled)
        };

        #[cfg(not(target_os = "linux"))]
        let conn = UdpConn::new(socket, *target);

        self.connections.insert(conn_id, conn);

        Ok(conn_id)
    }

    fn get(&self, conn_id: usize) -> Option<&Self::Conn> {
        self.connections.get(&conn_id)
    }

    fn get_mut(&mut self, conn_id: usize) -> Option<&mut Self::Conn> {
        self.connections.get_mut(&conn_id)
    }

    fn poll(&mut self, timeout: Option<Duration>) -> Result<Vec<usize>> {
        let events = self.mux.wait(timeout)?;
        let ready: Vec<usize> = events
            .iter()
            .filter(|e| e.readable)
            .map(|e| e.id)
            .filter(|id| self.connections.contains_key(id))
            .collect();
        Ok(ready)
    }

    #[cfg(target_os = "linux")]
    fn poll_tx_timestamps(&mut self) -> Result<Vec<crate::TxTimestampInfo>> {
        let mut results = Vec::new();

        for (&conn_id, conn) in &self.connections {
            if !conn.timestamps_enabled() {
                continue;
            }

            let fd = conn.as_raw_fd();

            // Poll error queue for TX timestamps (non-blocking)
            loop {
                match hw_timestamp::recv_tx_timestamp(fd) {
                    Ok(Some((timestamp, opt_id))) => {
                        results.push(crate::TxTimestampInfo {
                            conn_id,
                            tx_offset: opt_id,
                            timestamp,
                        });
                    }
                    Ok(None) => break, // No more timestamps available
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => {
                        tracing::trace!(
                            "Error polling TX timestamp for UDP conn {}: {}",
                            conn_id,
                            e
                        );
                        break;
                    }
                }
            }
        }

        Ok(results)
    }

    fn len(&self) -> usize {
        self.connections.len()
    }

    fn close(&mut self, conn_id: usize) -> Result<()> {
        if let Some(conn) = self.connections.remove(&conn_id) {
            let _ = self.mux.deregister_fd(&conn.socket);
        }
        Ok(())
    }

    fn close_all(&mut self) -> Result<()> {
        let conn_ids: Vec<usize> = self.connections.keys().copied().collect();
        for conn_id in conn_ids {
            self.close(conn_id)?;
        }
        Ok(())
    }
}

/// Factory for creating UDP connection groups
#[derive(Clone, Default)]
pub struct UdpTransportFactory {
    mux_type: MultiplexerType,
    #[cfg(target_os = "linux")]
    timestamp_config: UdpTimestampConfig,
}

impl UdpTransportFactory {
    pub fn new(mux_type: MultiplexerType) -> Self {
        Self {
            mux_type,
            #[cfg(target_os = "linux")]
            timestamp_config: UdpTimestampConfig::default(),
        }
    }

    /// Create factory with timestamp configuration (Linux only)
    #[cfg(target_os = "linux")]
    pub fn with_timestamp_config(
        mux_type: MultiplexerType,
        timestamp_config: UdpTimestampConfig,
    ) -> Self {
        Self { mux_type, timestamp_config }
    }

    /// Create factory with timestamping enabled (uses default interface binding)
    #[cfg(target_os = "linux")]
    pub fn with_timestamps(mux_type: MultiplexerType) -> Self {
        Self::with_timestamp_config(mux_type, UdpTimestampConfig::with_timestamps())
    }
}

impl TransportFactory for UdpTransportFactory {
    type Group = UdpConnectionGroup;

    fn create_group(&self) -> Result<Self::Group> {
        #[cfg(target_os = "linux")]
        return UdpConnectionGroup::with_timestamp_config(
            self.mux_type,
            self.timestamp_config.clone(),
        );

        #[cfg(not(target_os = "linux"))]
        UdpConnectionGroup::with_multiplexer(self.mux_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket as StdUdpSocket;
    use std::thread;

    #[test]
    fn test_udp_connect() {
        let server = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();

        let mut transport = UdpTransport::new();
        assert!(transport.connect(&addr).is_ok());
        assert!(transport.target.is_some());
        assert_eq!(transport.target.unwrap(), addr);
    }

    #[test]
    fn test_udp_send_recv() {
        let server = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();

        thread::spawn(move || {
            let mut buf = vec![0u8; 65535];
            while let Ok((n, src)) = server.recv_from(&mut buf) {
                server.send_to(&buf[..n], src).unwrap();
            }
        });

        thread::sleep(Duration::from_millis(10));

        let mut transport = UdpTransport::new();
        transport.connect(&addr).unwrap();

        let test_data = b"Hello, UDP!";
        let send_ts = transport.send(test_data).unwrap();

        thread::sleep(Duration::from_millis(10));

        for _ in 0..100 {
            if transport.poll_readable().unwrap() {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }

        let (recv_data, recv_ts) = transport.recv().unwrap();
        assert_eq!(recv_data, test_data);
        assert!(recv_ts.cycles() >= send_ts.cycles());
    }

    #[test]
    fn test_udp_send_without_connect() {
        let mut transport = UdpTransport::new();
        let result = transport.send(b"test");
        assert!(result.is_err());
    }

    #[test]
    fn test_udp_recv_without_connect() {
        let mut transport = UdpTransport::new();
        let result = transport.recv();
        assert!(result.is_err());
    }

    #[test]
    fn test_udp_close() {
        let server = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();

        let mut transport = UdpTransport::new();
        transport.connect(&addr).unwrap();
        assert!(transport.close().is_ok());
        assert!(transport.socket.is_none());
        assert!(transport.target.is_none());
    }

    #[test]
    fn test_udp_multiple_packets() {
        let server = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();

        thread::spawn(move || {
            let mut buf = vec![0u8; 65535];
            for _ in 0..5 {
                if let Ok((n, src)) = server.recv_from(&mut buf) {
                    server.send_to(&buf[..n], src).unwrap();
                } else {
                    break;
                }
            }
        });

        thread::sleep(Duration::from_millis(10));

        let mut transport = UdpTransport::new();
        transport.connect(&addr).unwrap();

        for i in 0..5 {
            let test_data = format!("Packet {}", i);
            transport.send(test_data.as_bytes()).unwrap();

            thread::sleep(Duration::from_millis(5));

            for _ in 0..100 {
                if transport.poll_readable().unwrap() {
                    break;
                }
                thread::sleep(Duration::from_millis(1));
            }

            let (recv_data, _) = transport.recv().unwrap();
            assert_eq!(recv_data, test_data.as_bytes());
        }
    }

    // =========================================================================
    // Connection Group Tests
    // =========================================================================

    #[test]
    fn test_udp_group_create() {
        let group = UdpConnectionGroup::new();
        assert!(group.is_ok());
        let group = group.unwrap();
        assert_eq!(group.len(), 0);
        assert!(group.is_empty());
    }

    #[test]
    fn test_udp_group_add_connection() {
        let server = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();

        let mut group = UdpConnectionGroup::new().unwrap();

        let conn_id = group.add_connection(&addr);
        assert!(conn_id.is_ok());
        let conn_id = conn_id.unwrap();
        assert_eq!(conn_id, 0);
        assert_eq!(group.len(), 1);

        let conn_id2 = group.add_connection(&addr).unwrap();
        assert_eq!(conn_id2, 1);
        assert_eq!(group.len(), 2);

        assert!(group.get(0).is_some());
        assert!(group.get(1).is_some());
        assert!(group.get(99).is_none());
    }

    #[test]
    fn test_udp_group_send_recv() {
        let server = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();

        thread::spawn(move || {
            let mut buf = vec![0u8; 65535];
            while let Ok((n, src)) = server.recv_from(&mut buf) {
                server.send_to(&buf[..n], src).unwrap();
            }
        });

        thread::sleep(Duration::from_millis(10));

        let mut group = UdpConnectionGroup::new().unwrap();
        let conn_id = group.add_connection(&addr).unwrap();

        let test_data = b"Hello, UDP Group!";
        let (send_ts, _tx_start, _tx_end) =
            group.get_mut(conn_id).unwrap().send(test_data).unwrap();

        thread::sleep(Duration::from_millis(10));

        let ready = group.poll(Some(Duration::from_millis(100))).unwrap();
        assert!(!ready.is_empty());
        assert!(ready.contains(&conn_id));

        let (recv_data, recv_ts) = group.get_mut(conn_id).unwrap().recv().unwrap();
        assert_eq!(recv_data, test_data);
        assert!(recv_ts.cycles() >= send_ts.cycles());
    }

    #[test]
    fn test_udp_group_close() {
        let server = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();

        let mut group = UdpConnectionGroup::new().unwrap();
        let conn1 = group.add_connection(&addr).unwrap();
        let conn2 = group.add_connection(&addr).unwrap();

        assert_eq!(group.len(), 2);

        group.close(conn1).unwrap();
        assert_eq!(group.len(), 1);
        assert!(group.get(conn1).is_none());
        assert!(group.get(conn2).is_some());

        group.close_all().unwrap();
        assert_eq!(group.len(), 0);
    }

    #[test]
    fn test_udp_transport_factory() {
        let factory = UdpTransportFactory::default();
        let group = factory.create_group();
        assert!(group.is_ok());
    }
}
