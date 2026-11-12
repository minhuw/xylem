//! UDP transport implementation using non-blocking I/O
//!
//! `UdpConnectionGroup` manages many UDP sockets behind one multiplexer.
//!
//! ## Kernel Timestamping (Linux only)
//!
//! On Linux, this transport can use kernel timestamps (SO_TIMESTAMPING):
//! hardware timestamps from a configured interface that has them enabled, or
//! kernel software timestamps on request. Configure via `UdpTimestampConfig`.

use crate::group::{GroupCore, GroupSocket, TimestampConfig, TxCounter};
use crate::{ConnectionGroup, RecvOutcome, Result, SendReceipt, Timestamp, TransportFactory};
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Duration;

#[cfg(target_os = "linux")]
use crate::group::TimestampSource;
#[cfg(target_os = "linux")]
use crate::hw_timestamp;

/// Configuration for UDP timestamping (Linux only)
///
/// The same configuration type as `TcpTimestampConfig`.
pub type UdpTimestampConfig = TimestampConfig;

// =============================================================================
// Connection Group API
// =============================================================================

/// A single UDP "connection" within a connection group
pub(crate) struct UdpConn {
    socket: UdpSocket,
    target: SocketAddr,
    /// TX datagram counter, present only when kernel timestamping is enabled
    tx: Option<TxCounter>,
}

impl UdpConn {
    /// `timestamps_enabled`: kernel timestamping is enabled on `socket`
    fn new(socket: UdpSocket, target: SocketAddr, timestamps_enabled: bool) -> Self {
        Self {
            socket,
            target,
            tx: timestamps_enabled.then(TxCounter::default),
        }
    }
}

impl GroupSocket for UdpConn {
    fn socket_fd(&self) -> RawFd {
        self.socket.as_raw_fd()
    }

    #[cfg(target_os = "linux")]
    fn tx_counter(&mut self) -> Option<&mut TxCounter> {
        self.tx.as_mut()
    }
}

impl UdpConn {
    fn send(&mut self, data: &[u8]) -> Result<SendReceipt> {
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

        let timestamp = Timestamp::now();
        // One datagram: [id, id + 1). The kernel reports datagram index id,
        // which the group normalises to tx_end = id + 1.
        let tx = self.tx.as_mut().map(|counter| counter.advance(1));
        Ok(SendReceipt { timestamp, tx })
    }

    /// Read one datagram into `buf`; the kernel truncates it to `buf`'s length
    fn recv(&mut self, buf: &mut [u8]) -> Result<RecvOutcome> {
        // On Linux with timestamps enabled, use recvmsg to get kernel timestamp
        #[cfg(target_os = "linux")]
        if self.tx.is_some() {
            let fd = self.socket.as_raw_fd();
            return match hw_timestamp::recvmsg_with_timestamp(fd, buf, 0) {
                Ok((len, ts_opt)) => {
                    // Cycles of this read, plus the kernel timestamp if attached
                    let timestamp = Timestamp::now();
                    let timestamp =
                        ts_opt.map_or(timestamp, |kernel| timestamp.with_kernel(kernel));
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
    /// Timestamp source resolved from the configuration at creation
    #[cfg(target_os = "linux")]
    timestamp_source: TimestampSource,
}

impl UdpConnectionGroup {
    pub fn new() -> Result<Self> {
        #[cfg(target_os = "linux")]
        return Self::with_timestamp_config(UdpTimestampConfig::default());

        #[cfg(not(target_os = "linux"))]
        Ok(Self { core: GroupCore::new()? })
    }

    /// Create with timestamp configuration (Linux only)
    #[cfg(target_os = "linux")]
    pub fn with_timestamp_config(timestamp_config: UdpTimestampConfig) -> Result<Self> {
        Ok(Self {
            core: GroupCore::new()?,
            timestamp_source: timestamp_config.resolve_source("UDP")?,
            timestamp_config,
        })
    }

    /// The timestamp source this group's connections use (Linux only)
    #[cfg(target_os = "linux")]
    pub fn timestamp_source(&self) -> TimestampSource {
        self.timestamp_source
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

        // Configure timestamping before the first send, so the kernel's TX
        // counter starts at zero like the connection's
        #[cfg(target_os = "linux")]
        let timestamps_enabled = self.timestamp_config.configure_socket(
            socket.as_raw_fd(),
            self.timestamp_source,
            "UDP",
            conn_id,
        )?;
        #[cfg(not(target_os = "linux"))]
        let timestamps_enabled = false;

        self.core.insert(conn_id, UdpConn::new(socket, *target, timestamps_enabled));

        Ok(conn_id)
    }

    fn send(&mut self, conn_id: usize, data: &[u8]) -> Result<SendReceipt> {
        self.core.conn_mut(conn_id)?.send(data)
    }

    fn is_datagram(&self) -> bool {
        true
    }

    fn recv(&mut self, conn_id: usize, buf: &mut [u8]) -> Result<RecvOutcome> {
        self.core.conn_mut(conn_id)?.recv(buf)
    }

    fn poll(&mut self, timeout: Duration, ready: &mut Vec<usize>) -> Result<()> {
        self.core.poll(timeout, ready)
    }

    fn local_addr(&self, conn_id: usize) -> Option<SocketAddr> {
        self.core.get(conn_id)?.socket.local_addr().ok()
    }

    #[cfg(target_os = "linux")]
    fn poll_tx_timestamps(&mut self, out: &mut Vec<crate::TxTimestampInfo>) -> Result<()> {
        // One stamp per datagram: exact
        self.core.poll_tx_timestamps(false, "UDP", out);
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
    #[cfg(target_os = "linux")]
    timestamp_config: UdpTimestampConfig,
}

impl UdpTransportFactory {
    /// Create factory with timestamp configuration (Linux only)
    #[cfg(target_os = "linux")]
    pub fn with_timestamp_config(timestamp_config: UdpTimestampConfig) -> Self {
        Self { timestamp_config }
    }

    /// Create factory with timestamping enabled (uses default interface binding)
    #[cfg(target_os = "linux")]
    pub fn with_timestamps() -> Self {
        Self::with_timestamp_config(UdpTimestampConfig::with_timestamps())
    }
}

impl TransportFactory for UdpTransportFactory {
    type Group = UdpConnectionGroup;

    fn create_group(&self) -> Result<Self::Group> {
        #[cfg(target_os = "linux")]
        return UdpConnectionGroup::with_timestamp_config(self.timestamp_config.clone());

        #[cfg(not(target_os = "linux"))]
        UdpConnectionGroup::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    #[cfg(target_os = "linux")]
    use crate::{KernelClock, TxRange, TxTimestampInfo};
    use std::net::UdpSocket as StdUdpSocket;
    use std::thread;

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
        assert!(group.is_datagram());
        assert!(!crate::TcpConnectionGroup::new().unwrap().is_datagram());
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
    fn local_addr_has_the_source_port_the_server_sees() {
        let server = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();
        let mut group = UdpConnectionGroup::new().unwrap();
        let conn_id = group.add_connection(&addr).unwrap();
        group.send(conn_id, b"ping").unwrap();
        let mut buf = [0u8; 8];
        let (_, source) = server.recv_from(&mut buf).unwrap();
        let local = group.local_addr(conn_id).unwrap();
        assert_eq!(local.port(), source.port());
        group.close(conn_id).unwrap();
        assert_eq!(group.local_addr(conn_id), None);
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

        // No TX timestamps by default, so no TX range
        assert_eq!(group.send(conn_id, b"one").unwrap().tx, None);
        assert_eq!(group.send(conn_id, b"two").unwrap().tx, None);

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
    fn test_udp_group_echoes_datagrams_in_order() {
        let server = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();
        thread::spawn(move || {
            let mut buf = vec![0u8; 65535];
            while let Ok((n, src)) = server.recv_from(&mut buf) {
                server.send_to(&buf[..n], src).unwrap();
            }
        });

        let mut group = UdpConnectionGroup::new().unwrap();
        let conn_id = group.add_connection(&addr).unwrap();
        let mut buf = vec![0u8; 65536];
        let mut ready = Vec::new();
        for i in 0..5 {
            let packet = format!("Packet {i}");
            let receipt = group.send(conn_id, packet.as_bytes()).unwrap();
            group.poll(Duration::from_secs(1), &mut ready).unwrap();
            assert_eq!(ready, vec![conn_id]);
            let RecvOutcome::Data { len, timestamp } = group.recv(conn_id, &mut buf).unwrap()
            else {
                panic!("expected data");
            };
            assert_eq!(&buf[..len], packet.as_bytes());
            assert!(timestamp.cycles() >= receipt.timestamp.cycles());
        }
    }

    /// Wait until `group` has reported `count` TX timestamps
    #[cfg(target_os = "linux")]
    fn wait_tx_stamps(group: &mut UdpConnectionGroup, count: usize) -> Vec<TxTimestampInfo> {
        let mut all = Vec::new();
        let mut out = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while all.len() < count && std::time::Instant::now() < deadline {
            group.poll_tx_timestamps(&mut out).unwrap();
            all.append(&mut out);
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(all.len(), count, "TX timestamps within 5 s");
        all
    }

    /// Kernel datagram IDs 0, 1, 2 (pinned in hw_timestamp's tests) reach
    /// the caller as exact exclusive ends 1, 2, 3, equal to the receipts'
    #[cfg(target_os = "linux")]
    #[test]
    fn test_udp_group_loopback_tx_stamps_match_receipts() {
        let server = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();
        let config = UdpTimestampConfig::with_software_timestamps();
        let mut group = UdpConnectionGroup::with_timestamp_config(config).unwrap();
        assert_eq!(group.timestamp_source(), TimestampSource::KernelSoftware);
        let conn_id = group.add_connection(&addr).unwrap();

        let ends: Vec<_> = (0..3)
            .map(|i| {
                let receipt = group.send(conn_id, &[0; 10]).unwrap();
                assert_eq!(receipt.tx, Some(TxRange { start: i, end: i + 1 }));
                receipt.tx.unwrap().end
            })
            .collect();
        let stamps = wait_tx_stamps(&mut group, 3);
        assert_eq!(stamps.iter().map(|s| s.tx_end).collect::<Vec<_>>(), ends);
        for stamp in &stamps {
            assert_eq!(stamp.conn_id, conn_id);
            assert!(!stamp.cumulative);
            assert_eq!(stamp.timestamp.clock, KernelClock::Realtime);
        }

        let mut out = Vec::new();
        group.poll_tx_timestamps(&mut out).unwrap();
        assert!(out.is_empty());
    }

    /// Required hardware timestamps fail the group rather than fall back
    #[cfg(target_os = "linux")]
    #[test]
    fn test_udp_group_required_hardware_never_falls_back() {
        // lo has no PHC: its driver rejects the query or reports it off
        for interface in ["lo", "xylem-no-such-if"] {
            let config = UdpTimestampConfig::require_hardware(interface);
            assert!(config.check().is_err());
            let Err(error) = UdpTransportFactory::with_timestamp_config(config).create_group()
            else {
                panic!("{interface}: created a group without hardware timestamps");
            };
            let message = error.to_string();
            assert!(message.contains(&format!("'{interface}'")), "{message}");
            assert_eq!(message.matches("Configuration error").count(), 1, "{message}");
        }
    }

    #[test]
    fn test_udp_transport_factory() {
        let factory = UdpTransportFactory::default();
        let group = factory.create_group();
        assert!(group.is_ok());
    }
}
