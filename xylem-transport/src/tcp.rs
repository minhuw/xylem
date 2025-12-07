//! TCP transport implementation using non-blocking I/O
//!
//! This module provides two APIs:
//! - `TcpTransport`: Single-connection API (one multiplexer per connection)
//! - `TcpConnectionGroup`: Multi-connection API (one multiplexer per group)
//!
//! ## Hardware Timestamping (Linux only)
//!
//! On Linux, this transport can use kernel timestamps (SO_TIMESTAMPING) for
//! more accurate latency measurement. Hardware timestamps are used when the
//! NIC supports them, with automatic fallback to software timestamps.
//! Configure via `TcpTimestampConfig`.

use crate::mux::{Interest, Multiplexer, MultiplexerType};
use crate::{
    ConnectionGroup, Error, GroupConnection, Result, Timestamp, Transport, TransportFactory,
};
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

#[cfg(target_os = "linux")]
use crate::hw_timestamp;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

const DEFAULT_RECV_BUFFER_SIZE: usize = 8192;

/// Configuration for TCP timestamping (Linux only)
///
/// On Linux, this configures kernel-level timestamping for more accurate
/// latency measurement. Falls back to software timestamps if HW not available.
///
/// By default, kernel timestamps are enabled on Linux.
#[derive(Debug, Clone)]
pub struct TcpTimestampConfig {
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

impl Default for TcpTimestampConfig {
    fn default() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            interface: None,
            #[cfg(target_os = "linux")]
            enable_timestamps: true, // Enable by default on Linux
        }
    }
}

impl TcpTimestampConfig {
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

pub struct TcpTransport {
    stream: Option<TcpStream>,
    mux: Multiplexer,
    recv_buffer: Vec<u8>,
}

impl TcpTransport {
    pub fn new() -> Self {
        Self::with_multiplexer(MultiplexerType::default())
    }

    pub fn with_multiplexer(mux_type: MultiplexerType) -> Self {
        Self {
            stream: None,
            mux: Multiplexer::new(mux_type).expect("Failed to create multiplexer"),
            recv_buffer: vec![0u8; DEFAULT_RECV_BUFFER_SIZE],
        }
    }
}

impl Default for TcpTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl Transport for TcpTransport {
    fn connect(&mut self, target: &SocketAddr) -> Result<()> {
        // Create socket with socket2 for more control
        let domain = if target.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_nonblocking(true)?;

        // Start non-blocking connect
        match socket.connect(&(*target).into()) {
            Ok(()) => {}
            Err(e) if e.raw_os_error() == Some(libc::EINPROGRESS) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(e.into()),
        }

        let stream: TcpStream = socket.into();

        // Register for write events to detect connection completion
        self.mux.register_fd(&stream, 0, Interest::BOTH)?;

        // Poll for connection complete (writable means connected)
        for _ in 0..50 {
            let events = self.mux.wait(Some(Duration::from_millis(100)))?;

            let connected = events.iter().any(|e| e.id == 0 && e.writable);
            if !connected {
                continue;
            }

            // Check for connection errors
            if let Some(err) = stream.take_error()? {
                let _ = self.mux.deregister_fd(&stream);
                return Err(Error::Connection(format!("Connection failed: {err}")));
            }

            // Re-register for read events only
            self.mux.modify_fd(&stream, 0, Interest::READABLE)?;
            self.stream = Some(stream);
            return Ok(());
        }

        // Timeout - deregister before returning error
        let _ = self.mux.deregister_fd(&stream);
        Err(Error::Connection("Connection timeout".to_string()))
    }

    fn send(&mut self, data: &[u8]) -> Result<Timestamp> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| Error::Connection("Not connected".to_string()))?;

        // Non-blocking write
        let mut total_written = 0;
        while total_written < data.len() {
            match stream.write(&data[total_written..]) {
                Ok(n) => total_written += n,
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
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| Error::Connection("Not connected".to_string()))?;

        match stream.read(&mut self.recv_buffer) {
            Ok(0) => Err(Error::Connection("Connection closed by peer".to_string())),
            Ok(n) => {
                let timestamp = Timestamp::now();
                let data = self.recv_buffer[..n].to_vec();
                Ok((data, timestamp))
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok((Vec::new(), Timestamp::now())),
            Err(e) => Err(e.into()),
        }
    }

    fn poll_readable(&mut self) -> Result<bool> {
        let events = self.mux.wait(Some(Duration::from_micros(1)))?;
        Ok(!events.is_empty())
    }

    fn close(&mut self) -> Result<()> {
        if let Some(stream) = self.stream.take() {
            let _ = self.mux.deregister_fd(&stream);
        }
        Ok(())
    }
}

// =============================================================================
// Connection Group API
// =============================================================================

/// A single TCP connection within a connection group
pub struct TcpConn {
    stream: TcpStream,
    recv_buffer: Vec<u8>,
    connected: bool,
    /// TX byte counter for correlating TX timestamps (wrapping)
    tx_byte_counter: u32,
    /// Whether kernel timestamping is enabled for this connection
    #[cfg(target_os = "linux")]
    timestamps_enabled: bool,
}

impl TcpConn {
    #[allow(dead_code)] // Used on non-Linux or when timestamps not configured
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            recv_buffer: vec![0u8; DEFAULT_RECV_BUFFER_SIZE],
            connected: true,
            tx_byte_counter: 0,
            #[cfg(target_os = "linux")]
            timestamps_enabled: false,
        }
    }

    #[cfg(target_os = "linux")]
    fn with_timestamps(stream: TcpStream, timestamps_enabled: bool) -> Self {
        Self {
            stream,
            recv_buffer: vec![0u8; DEFAULT_RECV_BUFFER_SIZE],
            connected: true,
            tx_byte_counter: 0,
            timestamps_enabled,
        }
    }

    /// Get the raw file descriptor (for polling TX timestamps)
    #[cfg(target_os = "linux")]
    pub(crate) fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
        self.stream.as_raw_fd()
    }

    /// Check if timestamps are enabled
    #[cfg(target_os = "linux")]
    pub(crate) fn timestamps_enabled(&self) -> bool {
        self.timestamps_enabled
    }
}

impl GroupConnection for TcpConn {
    fn send(&mut self, data: &[u8]) -> Result<(Timestamp, u32, u32)> {
        if !self.connected {
            return Err(Error::Connection("Connection closed".to_string()));
        }

        // Record TX byte range [tx_start, tx_end)
        let tx_start = self.tx_byte_counter;
        let tx_end = tx_start.wrapping_add(data.len() as u32);

        let mut total_written = 0;
        while total_written < data.len() {
            match self.stream.write(&data[total_written..]) {
                Ok(n) => total_written += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::hint::spin_loop();
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        // Update byte counter after successful send
        self.tx_byte_counter = tx_end;

        // Return software timestamp and TX byte range for correlation
        // The actual HW TX timestamp will arrive via error queue with offset >= tx_end
        Ok((Timestamp::now(), tx_start, tx_end))
    }

    fn recv(&mut self) -> Result<(Vec<u8>, Timestamp)> {
        if !self.connected {
            return Err(Error::Connection("Connection closed".to_string()));
        }

        // Use kernel timestamps when available
        #[cfg(target_os = "linux")]
        if self.timestamps_enabled {
            return self.recv_with_timestamp();
        }

        // Standard recv without kernel timestamps
        self.recv_standard()
    }

    fn is_connected(&self) -> bool {
        self.connected
    }
}

impl TcpConn {
    /// Standard receive using read() - no kernel timestamps
    fn recv_standard(&mut self) -> Result<(Vec<u8>, Timestamp)> {
        match self.stream.read(&mut self.recv_buffer) {
            Ok(0) => {
                self.connected = false;
                Err(Error::Connection("Connection closed by peer".to_string()))
            }
            Ok(n) => {
                let timestamp = Timestamp::now();
                let data = self.recv_buffer[..n].to_vec();
                Ok((data, timestamp))
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok((Vec::new(), Timestamp::now())),
            Err(e) => Err(e.into()),
        }
    }

    /// Receive using recvmsg() with kernel timestamps
    #[cfg(target_os = "linux")]
    fn recv_with_timestamp(&mut self) -> Result<(Vec<u8>, Timestamp)> {
        let fd = self.stream.as_raw_fd();

        match hw_timestamp::recvmsg_with_timestamp(fd, &mut self.recv_buffer, 0) {
            Ok((0, _)) => {
                self.connected = false;
                Err(Error::Connection("Connection closed by peer".to_string()))
            }
            Ok((n, ts_opt)) => {
                let data = self.recv_buffer[..n].to_vec();
                // Use hardware timestamp if available, otherwise software
                let timestamp = ts_opt.unwrap_or_else(Timestamp::now);
                Ok((data, timestamp))
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok((Vec::new(), Timestamp::now())),
            Err(e) => Err(e.into()),
        }
    }
}

/// A group of TCP connections sharing a single multiplexer
pub struct TcpConnectionGroup {
    mux: Multiplexer,
    connections: HashMap<usize, TcpConn>,
    next_id: usize,
    /// Timestamp configuration for new connections
    #[cfg(target_os = "linux")]
    timestamp_config: TcpTimestampConfig,
}

impl TcpConnectionGroup {
    pub fn new() -> Result<Self> {
        Self::with_multiplexer(MultiplexerType::default())
    }

    pub fn with_multiplexer(mux_type: MultiplexerType) -> Result<Self> {
        Ok(Self {
            mux: Multiplexer::new(mux_type)?,
            connections: HashMap::new(),
            next_id: 0,
            #[cfg(target_os = "linux")]
            timestamp_config: TcpTimestampConfig::default(),
        })
    }

    /// Create with timestamp configuration (Linux only)
    #[cfg(target_os = "linux")]
    pub fn with_timestamp_config(
        mux_type: MultiplexerType,
        timestamp_config: TcpTimestampConfig,
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
            tracing::debug!("Hardware timestamping enabled for connection {}", conn_id);
            return true;
        }

        // Fallback to software timestamps
        tracing::debug!("Hardware timestamping not available, trying software");
        match hw_timestamp::enable_software_timestamping(fd) {
            Ok(()) => {
                tracing::debug!("Software timestamping enabled for connection {}", conn_id);
                true
            }
            Err(e) => {
                tracing::warn!("Could not enable kernel timestamping: {}", e);
                false
            }
        }
    }

    fn connect_with_timeout(&mut self, target: &SocketAddr, conn_id: usize) -> Result<TcpStream> {
        let domain = if target.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_nonblocking(true)?;

        match socket.connect(&(*target).into()) {
            Ok(()) => {}
            Err(e) if e.raw_os_error() == Some(libc::EINPROGRESS) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(e.into()),
        }

        let stream: TcpStream = socket.into();

        // Use a temporary multiplexer for connection establishment to avoid
        // mixing connection events with other connections in the group
        let mut temp_mux = Multiplexer::new(self.mux.mux_type())?;
        temp_mux.register_fd(&stream, 0, Interest::BOTH)?;

        // Poll for connection complete
        for _ in 0..50 {
            let events = temp_mux.wait(Some(Duration::from_millis(100)))?;

            let connected = events.iter().any(|e| e.id == 0 && e.writable);
            if !connected {
                continue;
            }

            if let Some(err) = stream.take_error()? {
                return Err(Error::Connection(format!("Connection failed: {err}")));
            }

            // Connection successful - deregister from temp and register with main mux
            let _ = temp_mux.deregister_fd(&stream);
            self.mux.register_fd(&stream, conn_id, Interest::READABLE)?;
            return Ok(stream);
        }

        Err(Error::Connection("Connection timeout".to_string()))
    }
}

impl Default for TcpConnectionGroup {
    fn default() -> Self {
        Self::new().expect("Failed to create TcpConnectionGroup")
    }
}

impl ConnectionGroup for TcpConnectionGroup {
    type Conn = TcpConn;

    fn add_connection(&mut self, target: &SocketAddr) -> Result<usize> {
        let conn_id = self.next_id;
        self.next_id += 1;

        let stream = self.connect_with_timeout(target, conn_id)?;

        // Configure timestamping if enabled
        #[cfg(target_os = "linux")]
        let conn = {
            let timestamps_enabled = if self.timestamp_config.enable_timestamps {
                self.configure_timestamping(stream.as_raw_fd(), conn_id)
            } else {
                false
            };

            TcpConn::with_timestamps(stream, timestamps_enabled)
        };

        #[cfg(not(target_os = "linux"))]
        let conn = TcpConn::new(stream);

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
                        tracing::trace!("Error polling TX timestamp for conn {}: {}", conn_id, e);
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
        if let Some(mut conn) = self.connections.remove(&conn_id) {
            conn.connected = false;
            let _ = self.mux.deregister_fd(&conn.stream);
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

/// Factory for creating TCP connection groups
#[derive(Clone, Default)]
pub struct TcpTransportFactory {
    mux_type: MultiplexerType,
    #[cfg(target_os = "linux")]
    timestamp_config: TcpTimestampConfig,
}

impl TcpTransportFactory {
    pub fn new(mux_type: MultiplexerType) -> Self {
        Self {
            mux_type,
            #[cfg(target_os = "linux")]
            timestamp_config: TcpTimestampConfig::default(),
        }
    }

    /// Create factory with timestamp configuration (Linux only)
    #[cfg(target_os = "linux")]
    pub fn with_timestamp_config(
        mux_type: MultiplexerType,
        timestamp_config: TcpTimestampConfig,
    ) -> Self {
        Self { mux_type, timestamp_config }
    }

    /// Create factory with timestamping enabled (uses default interface binding)
    #[cfg(target_os = "linux")]
    pub fn with_timestamps(mux_type: MultiplexerType) -> Self {
        Self::with_timestamp_config(mux_type, TcpTimestampConfig::with_timestamps())
    }
}

impl TransportFactory for TcpTransportFactory {
    type Group = TcpConnectionGroup;

    fn create_group(&self) -> Result<Self::Group> {
        #[cfg(target_os = "linux")]
        {
            TcpConnectionGroup::with_timestamp_config(self.mux_type, self.timestamp_config.clone())
        }

        #[cfg(not(target_os = "linux"))]
        {
            TcpConnectionGroup::with_multiplexer(self.mux_type)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn test_tcp_connect() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buf = vec![0u8; 1024];
            loop {
                let n = socket.read(&mut buf).unwrap_or(0);
                if n == 0 {
                    break;
                }
                socket.write_all(&buf[..n]).unwrap();
            }
        });

        thread::sleep(Duration::from_millis(10));

        let mut transport = TcpTransport::new();
        assert!(transport.connect(&addr).is_ok());
    }

    #[test]
    fn test_tcp_send_recv() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buf = vec![0u8; 1024];
            loop {
                let n = socket.read(&mut buf).unwrap_or(0);
                if n == 0 {
                    break;
                }
                socket.write_all(&buf[..n]).unwrap();
            }
        });

        thread::sleep(Duration::from_millis(10));

        let mut transport = TcpTransport::new();
        transport.connect(&addr).unwrap();

        let test_data = b"Hello, World!";
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
    fn test_tcp_send_without_connect() {
        let mut transport = TcpTransport::new();
        let result = transport.send(b"test");
        assert!(result.is_err());
    }

    #[test]
    fn test_tcp_recv_without_connect() {
        let mut transport = TcpTransport::new();
        let result = transport.recv();
        assert!(result.is_err());
    }

    #[test]
    fn test_tcp_close() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            let _ = listener.accept();
        });

        thread::sleep(Duration::from_millis(10));

        let mut transport = TcpTransport::new();
        transport.connect(&addr).unwrap();
        assert!(transport.close().is_ok());
        assert!(transport.stream.is_none());
    }

    #[test]
    fn test_tcp_with_poll_multiplexer() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buf = vec![0u8; 1024];
            let n = socket.read(&mut buf).unwrap_or(0);
            if n > 0 {
                socket.write_all(&buf[..n]).unwrap();
            }
        });

        thread::sleep(Duration::from_millis(10));

        let mut transport = TcpTransport::with_multiplexer(MultiplexerType::Poll);
        transport.connect(&addr).unwrap();
        transport.send(b"hello").unwrap();

        thread::sleep(Duration::from_millis(10));
        for _ in 0..100 {
            if transport.poll_readable().unwrap() {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }

        let (data, _) = transport.recv().unwrap();
        assert_eq!(data, b"hello");
    }

    // =========================================================================
    // Connection Group Tests
    // =========================================================================

    #[test]
    fn test_tcp_group_create() {
        let group = TcpConnectionGroup::new();
        assert!(group.is_ok());
        let group = group.unwrap();
        assert_eq!(group.len(), 0);
        assert!(group.is_empty());
    }

    #[test]
    fn test_tcp_group_add_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            for _ in 0..3 {
                let _ = listener.accept();
            }
        });

        thread::sleep(Duration::from_millis(10));

        let mut group = TcpConnectionGroup::new().unwrap();

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
    fn test_tcp_group_send_recv() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buf = vec![0u8; 1024];
            loop {
                let n = socket.read(&mut buf).unwrap_or(0);
                if n == 0 {
                    break;
                }
                socket.write_all(&buf[..n]).unwrap();
            }
        });

        thread::sleep(Duration::from_millis(10));

        let mut group = TcpConnectionGroup::new().unwrap();
        let conn_id = group.add_connection(&addr).unwrap();

        let test_data = b"Hello, Group!";
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

    fn echo_handler(mut socket: TcpStream) {
        let mut buf = vec![0u8; 1024];
        while let Ok(n) = socket.read(&mut buf) {
            if n == 0 {
                break;
            }
            let _ = socket.write_all(&buf[..n]);
        }
    }

    #[test]
    fn test_tcp_group_multiple_connections_poll() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            while let Ok((socket, _)) = listener.accept() {
                thread::spawn(|| echo_handler(socket));
            }
        });

        thread::sleep(Duration::from_millis(10));

        let mut group = TcpConnectionGroup::new().unwrap();
        let conn1 = group.add_connection(&addr).unwrap();
        let conn2 = group.add_connection(&addr).unwrap();
        let conn3 = group.add_connection(&addr).unwrap();

        group.get_mut(conn1).unwrap().send(b"msg1").unwrap();
        group.get_mut(conn2).unwrap().send(b"msg2").unwrap();
        group.get_mut(conn3).unwrap().send(b"msg3").unwrap();

        thread::sleep(Duration::from_millis(50));

        let ready = group.poll(Some(Duration::from_millis(100))).unwrap();
        assert!(!ready.is_empty());

        for id in ready {
            let (data, _) = group.get_mut(id).unwrap().recv().unwrap();
            assert!(!data.is_empty());
        }
    }

    #[test]
    fn test_tcp_group_close() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            for _ in 0..2 {
                let _ = listener.accept();
            }
        });

        thread::sleep(Duration::from_millis(10));

        let mut group = TcpConnectionGroup::new().unwrap();
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
    fn test_tcp_transport_factory() {
        let factory = TcpTransportFactory::default();
        let group = factory.create_group();
        assert!(group.is_ok());
    }

    #[test]
    fn test_tcp_transport_factory_with_poll() {
        let factory = TcpTransportFactory::new(MultiplexerType::Poll);
        let group = factory.create_group();
        assert!(group.is_ok());
    }
}
