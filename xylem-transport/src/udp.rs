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

use crate::group::{GroupCore, GroupSocket, TimestampConfig};
use crate::mux::{Interest, Multiplexer, MultiplexerType};
use crate::{
    ConnectionGroup, Error, RecvOutcome, Result, SendReceipt, Timestamp, Transport,
    TransportFactory,
};
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Duration;

#[cfg(target_os = "linux")]
use crate::hw_timestamp;

const DEFAULT_UDP_BUFFER_SIZE: usize = 65535;

/// Configuration for UDP timestamping (Linux only)
///
/// The same configuration type as `TcpTimestampConfig`.
pub type UdpTimestampConfig = TimestampConfig;

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
pub(crate) struct UdpConn {
    socket: UdpSocket,
    target: SocketAddr,
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
            tx_packet_counter: 0,
            timestamps_enabled,
        }
    }
}

impl GroupSocket for UdpConn {
    fn socket_fd(&self) -> RawFd {
        self.socket.as_raw_fd()
    }

    #[cfg(target_os = "linux")]
    fn timestamps_enabled(&self) -> bool {
        self.timestamps_enabled
    }
}

impl UdpConn {
    fn send(&mut self, data: &[u8]) -> Result<SendReceipt> {
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
        Ok(SendReceipt {
            timestamp: Timestamp::now(),
            tx_start,
            tx_end,
        })
    }

    /// Read one datagram into `buf`; the kernel truncates it to `buf`'s length
    fn recv(&mut self, buf: &mut [u8]) -> Result<RecvOutcome> {
        // On Linux with timestamps enabled, use recvmsg to get kernel timestamp
        #[cfg(target_os = "linux")]
        if self.timestamps_enabled {
            let fd = self.socket.as_raw_fd();
            return match hw_timestamp::recvmsg_with_timestamp(fd, buf, 0) {
                Ok((len, ts_opt)) => {
                    // Use hardware timestamp if available, otherwise software
                    let timestamp = ts_opt.unwrap_or_else(Timestamp::now);
                    Ok(RecvOutcome::Data { len, timestamp })
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(RecvOutcome::WouldBlock),
                Err(e) => Err(e.into()),
            };
        }

        // Standard recv path (non-Linux or timestamps disabled)
        match self.socket.recv_from(buf) {
            Ok((len, _src_addr)) => Ok(RecvOutcome::Data { len, timestamp: Timestamp::now() }),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(RecvOutcome::WouldBlock),
            Err(e) => Err(e.into()),
        }
    }
}

/// A group of UDP sockets sharing a single multiplexer
pub struct UdpConnectionGroup {
    core: GroupCore<UdpConn>,
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
            core: GroupCore::new(mux_type)?,
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
            core: GroupCore::new(mux_type)?,
            timestamp_config,
        })
    }
}

impl Default for UdpConnectionGroup {
    fn default() -> Self {
        Self::new().expect("Failed to create UdpConnectionGroup")
    }
}

impl ConnectionGroup for UdpConnectionGroup {
    fn add_connection(&mut self, target: &SocketAddr) -> Result<usize> {
        let conn_id = self.core.reserve_id();

        let bind_addr: SocketAddr = if target.is_ipv4() {
            "0.0.0.0:0".parse().unwrap()
        } else {
            "[::]:0".parse().unwrap()
        };

        let socket = UdpSocket::bind(bind_addr)?;
        socket.set_nonblocking(true)?;
        self.core.register_readable(&socket, conn_id)?;

        // On Linux, configure timestamping
        #[cfg(target_os = "linux")]
        let conn = {
            let timestamps_enabled = if self.timestamp_config.enable_timestamps {
                self.timestamp_config.configure_timestamping(socket.as_raw_fd(), "UDP", conn_id)
            } else {
                false
            };

            UdpConn::with_timestamps(socket, *target, timestamps_enabled)
        };

        #[cfg(not(target_os = "linux"))]
        let conn = UdpConn::new(socket, *target);

        self.core.insert(conn_id, conn);

        Ok(conn_id)
    }

    fn send(&mut self, conn_id: usize, data: &[u8]) -> Result<SendReceipt> {
        self.core.conn_mut(conn_id)?.send(data)
    }

    fn recv(&mut self, conn_id: usize, buf: &mut [u8]) -> Result<RecvOutcome> {
        self.core.conn_mut(conn_id)?.recv(buf)
    }

    fn poll(&mut self, timeout: Duration, ready: &mut Vec<usize>) -> Result<()> {
        self.core.poll(timeout, ready)
    }

    #[cfg(target_os = "linux")]
    fn poll_tx_timestamps(&mut self, out: &mut Vec<crate::TxTimestampInfo>) -> Result<()> {
        self.core.poll_tx_timestamps("UDP", out);
        Ok(())
    }

    fn len(&self) -> usize {
        self.core.len()
    }

    fn close(&mut self, conn_id: usize) -> Result<()> {
        self.core.close(conn_id)
    }

    fn close_all(&mut self) -> Result<()> {
        self.core.close_all()
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

        assert!(group.core.get(0).is_some());
        assert!(group.core.get(1).is_some());
        assert!(group.core.get(99).is_none());
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
        let receipt = group.send(conn_id, test_data).unwrap();

        thread::sleep(Duration::from_millis(10));

        let mut ready = vec![usize::MAX];
        group.poll(Duration::from_millis(100), &mut ready).unwrap();
        assert!(!ready.is_empty());
        assert!(ready.contains(&conn_id));
        assert!(!ready.contains(&usize::MAX));

        let mut buf = vec![0u8; 65536];
        let RecvOutcome::Data { len, timestamp } = group.recv(conn_id, &mut buf).unwrap() else {
            panic!("expected data");
        };
        assert_eq!(&buf[..len], test_data);
        assert!(timestamp.cycles() >= receipt.timestamp.cycles());
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
        assert!(group.core.get(conn1).is_none());
        assert!(group.core.get(conn2).is_some());

        group.close_all().unwrap();
        assert_eq!(group.len(), 0);
    }

    #[test]
    fn test_udp_group_ids_are_not_reused() {
        let server = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();

        let mut group = UdpConnectionGroup::new().unwrap();
        assert_eq!(group.add_connection(&addr).unwrap(), 0);
        assert_eq!(group.add_connection(&addr).unwrap(), 1);
        assert_eq!(group.add_connection(&addr).unwrap(), 2);

        // Closing a middle id must not let the next connection reuse it or
        // the current length
        group.close(1).unwrap();
        assert!(group.core.get(1).is_none());
        assert!(group.core.get(1).is_none());
        assert_eq!(group.len(), 2);

        assert_eq!(group.add_connection(&addr).unwrap(), 3);
        assert_eq!(group.len(), 3);
        assert!(group.core.get(0).is_some());
        assert!(group.core.get(1).is_none());
        assert!(group.core.get(2).is_some());
        assert!(group.core.get(3).is_some());

        // Closing the highest id must not let the next connection reuse it
        group.close(3).unwrap();
        assert!(group.core.get(3).is_none());
        assert!(group.core.get(3).is_none());
        assert_eq!(group.len(), 2);

        assert_eq!(group.add_connection(&addr).unwrap(), 4);
        assert_eq!(group.len(), 3);
        assert!(group.core.get(0).is_some());
        assert!(group.core.get(1).is_none());
        assert!(group.core.get(2).is_some());
        assert!(group.core.get(3).is_none());
        assert!(group.core.get(4).is_some());
    }

    #[test]
    fn test_udp_group_recv_outcomes() {
        let server = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();

        let mut group = UdpConnectionGroup::new().unwrap();
        let conn_id = group.add_connection(&addr).unwrap();
        let mut buf = vec![0u8; 65536];
        let mut ready = Vec::new();

        group.poll(Duration::ZERO, &mut ready).unwrap();
        assert!(ready.is_empty());
        assert!(matches!(group.recv(conn_id, &mut buf).unwrap(), RecvOutcome::WouldBlock));

        // Each send is one packet id
        let first = group.send(conn_id, b"one").unwrap();
        let second = group.send(conn_id, b"two").unwrap();
        assert_eq!((first.tx_start, first.tx_end), (0, 0));
        assert_eq!((second.tx_start, second.tx_end), (1, 1));

        // One datagram per recv
        let mut server_buf = [0u8; 16];
        let (_, client) = server.recv_from(&mut server_buf).unwrap();
        server.send_to(b"first", client).unwrap();
        server.send_to(b"second", client).unwrap();
        thread::sleep(Duration::from_millis(10));
        group.poll(Duration::from_millis(100), &mut ready).unwrap();
        assert_eq!(ready, vec![conn_id]);
        assert!(matches!(
            group.recv(conn_id, &mut buf).unwrap(),
            RecvOutcome::Data { len: 5, .. }
        ));
        assert_eq!(&buf[..5], b"first");
        assert!(matches!(
            group.recv(conn_id, &mut buf).unwrap(),
            RecvOutcome::Data { len: 6, .. }
        ));
        assert_eq!(&buf[..6], b"second");

        assert!(matches!(group.recv(99, &mut buf), Err(Error::Connection(_))));
        assert!(matches!(group.send(99, b"x"), Err(Error::Connection(_))));
    }

    #[test]
    fn test_udp_transport_factory() {
        let factory = UdpTransportFactory::default();
        let group = factory.create_group();
        assert!(group.is_ok());
    }
}
