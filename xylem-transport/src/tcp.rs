//! TCP transport implementation using non-blocking I/O
//!
//! `TcpConnectionGroup` manages many TCP connections behind one multiplexer.
//!
//! ## Kernel Timestamping (Linux only)
//!
//! On Linux, this transport can use kernel timestamps (SO_TIMESTAMPING):
//! hardware timestamps from a configured interface that has them enabled, or
//! kernel software timestamps on request. Configure via `TcpTimestampConfig`.

use crate::group::{GroupCore, GroupSocket, TimestampConfig, TxCounter};
use crate::mux::Interest;
use crate::{
    ConnectionGroup, Error, RecvOutcome, Result, SendProgress, SendReceipt, Timestamp,
    TransportFactory,
};
use socket2::{Domain, Protocol, Socket, Type};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Duration;

#[cfg(target_os = "linux")]
use crate::group::TimestampSource;
#[cfg(target_os = "linux")]
use crate::hw_timestamp;

/// Configuration for TCP timestamping (Linux only)
///
/// The same configuration type as `UdpTimestampConfig`.
pub type TcpTimestampConfig = TimestampConfig;

// =============================================================================
// Connection Group API
// =============================================================================

/// A single TCP connection within a connection group
pub(crate) struct TcpConn {
    stream: TcpStream,
    connected: bool,
    connecting: bool,
    /// TX byte counter, present only when kernel timestamping is enabled
    tx: Option<TxCounter>,
}

impl TcpConn {
    /// `timestamps_enabled`: kernel timestamping is enabled on `stream`
    fn new(stream: TcpStream, timestamps_enabled: bool) -> Self {
        Self {
            stream,
            connected: true,
            connecting: false,
            tx: timestamps_enabled.then(TxCounter::default),
        }
    }

    /// A socket whose non-blocking TCP handshake is still in progress
    fn new_connecting(stream: TcpStream) -> Self {
        Self {
            stream,
            connected: false,
            connecting: true,
            tx: None,
        }
    }
}

impl GroupSocket for TcpConn {
    fn socket_fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }

    #[cfg(target_os = "linux")]
    fn tx_counter(&mut self) -> Option<&mut TxCounter> {
        self.tx.as_mut()
    }
}

impl TcpConn {
    /// Write `data`, spinning while the socket would block if `spin`, else
    /// stopping there
    fn write(&mut self, data: &[u8], spin: bool) -> Result<SendProgress> {
        if !self.connected {
            return Err(Error::Connection("Connection closed".to_string()));
        }

        let mut total_written = 0;
        while total_written < data.len() {
            match self.stream.write(&data[total_written..]) {
                Ok(n) => total_written += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock && spin => {
                    std::hint::spin_loop();
                    continue;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e.into()),
            }
        }

        let timestamp = Timestamp::now();
        // Byte range [start, end) of this send; its kernel TX timestamp
        // arrives later with a cumulative tx_end >= end
        let tx = match total_written {
            0 if !data.is_empty() => None,
            n => self.tx.as_mut().map(|counter| counter.advance(n as u32)),
        };
        Ok(SendProgress {
            accepted: total_written,
            receipt: SendReceipt { timestamp, tx },
        })
    }

    fn recv(&mut self, buf: &mut [u8]) -> Result<RecvOutcome> {
        // The peer closed the stream earlier; nothing more will arrive
        if !self.connected {
            return Ok(RecvOutcome::Eof);
        }

        // Use kernel timestamps when available
        #[cfg(target_os = "linux")]
        if self.tx.is_some() {
            return self.recv_with_timestamp(buf);
        }

        // Standard recv without kernel timestamps
        self.recv_standard(buf)
    }

    /// Standard receive using read() - no kernel timestamps
    fn recv_standard(&mut self, buf: &mut [u8]) -> Result<RecvOutcome> {
        match self.stream.read(buf) {
            Ok(0) => {
                self.connected = false;
                Ok(RecvOutcome::Eof)
            }
            Ok(len) => Ok(RecvOutcome::Data { len, timestamp: Timestamp::now() }),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(RecvOutcome::WouldBlock),
            Err(e) => Err(e.into()),
        }
    }

    /// Receive using recvmsg() with kernel timestamps
    #[cfg(target_os = "linux")]
    fn recv_with_timestamp(&mut self, buf: &mut [u8]) -> Result<RecvOutcome> {
        let fd = self.stream.as_raw_fd();

        match hw_timestamp::recvmsg_with_timestamp(fd, buf, 0) {
            Ok((0, _)) => {
                self.connected = false;
                Ok(RecvOutcome::Eof)
            }
            Ok((len, ts_opt)) => {
                // Cycles of this read, plus the kernel timestamp if attached
                let timestamp = Timestamp::now();
                let timestamp = ts_opt.map_or(timestamp, |kernel| timestamp.with_kernel(kernel));
                Ok(RecvOutcome::Data { len, timestamp })
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(RecvOutcome::WouldBlock),
            Err(e) => Err(e.into()),
        }
    }
}

/// A group of TCP connections sharing a single multiplexer
pub struct TcpConnectionGroup {
    core: GroupCore<TcpConn>,
    /// Timestamp configuration for new connections
    #[cfg(target_os = "linux")]
    timestamp_config: TcpTimestampConfig,
    /// Timestamp source resolved from the configuration at creation
    #[cfg(target_os = "linux")]
    timestamp_source: TimestampSource,
}

impl TcpConnectionGroup {
    pub fn new() -> Result<Self> {
        #[cfg(target_os = "linux")]
        return Self::with_timestamp_config(TcpTimestampConfig::default());

        #[cfg(not(target_os = "linux"))]
        Ok(Self { core: GroupCore::new()? })
    }

    /// Create with timestamp configuration (Linux only)
    #[cfg(target_os = "linux")]
    pub fn with_timestamp_config(timestamp_config: TcpTimestampConfig) -> Result<Self> {
        Ok(Self {
            core: GroupCore::new()?,
            timestamp_source: timestamp_config.resolve_source("TCP")?,
            timestamp_config,
        })
    }

    /// The timestamp source this group's connections use (Linux only)
    #[cfg(target_os = "linux")]
    pub fn timestamp_source(&self) -> TimestampSource {
        self.timestamp_source
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

        // Requests are small and often pipelined: without TCP_NODELAY, Nagle's
        // algorithm holds a request back until the previous one is
        // acknowledged, which inflates the measured latency.
        socket.set_tcp_nodelay(true)?;

        let stream: TcpStream = socket.into();

        self.core.wait_for_connect(&stream)?;

        if let Some(err) = stream.take_error()? {
            return Err(Error::Connection(format!("Connection failed: {err}")));
        }

        // Connection successful - register with main mux
        self.core.register_readable(&stream, conn_id)?;
        Ok(stream)
    }
}

impl Default for TcpConnectionGroup {
    fn default() -> Self {
        Self::new().expect("Failed to create TcpConnectionGroup")
    }
}

impl ConnectionGroup for TcpConnectionGroup {
    fn add_connection(&mut self, target: &SocketAddr) -> Result<usize> {
        let conn_id = self.core.reserve_id();

        let stream = self.connect_with_timeout(target, conn_id)?;

        // Configure timestamping before the first send, so the kernel's TX
        // counter starts at zero like the connection's
        #[cfg(target_os = "linux")]
        let timestamps_enabled = self.timestamp_config.configure_socket(
            stream.as_raw_fd(),
            self.timestamp_source,
            "TCP",
            conn_id,
        )?;
        #[cfg(not(target_os = "linux"))]
        let timestamps_enabled = false;

        self.core.insert(conn_id, TcpConn::new(stream, timestamps_enabled));

        Ok(conn_id)
    }

    fn send(&mut self, conn_id: usize, data: &[u8]) -> Result<SendReceipt> {
        Ok(self.core.conn_mut(conn_id)?.write(data, true)?.receipt)
    }

    fn try_send(&mut self, conn_id: usize, data: &[u8]) -> Result<SendProgress> {
        self.core.conn_mut(conn_id)?.write(data, false)
    }

    fn set_write_interest(&mut self, conn_id: usize, interested: bool) -> Result<()> {
        self.core.set_write_interest(conn_id, interested)
    }

    fn recv(&mut self, conn_id: usize, buf: &mut [u8]) -> Result<RecvOutcome> {
        self.core.conn_mut(conn_id)?.recv(buf)
    }

    fn poll(&mut self, timeout: Duration, ready: &mut Vec<usize>) -> Result<()> {
        self.core.poll(timeout, ready)
    }

    fn local_addr(&self, conn_id: usize) -> Option<SocketAddr> {
        self.core.get(conn_id)?.stream.local_addr().ok()
    }

    #[cfg(target_os = "linux")]
    fn poll_tx_timestamps(&mut self, out: &mut Vec<crate::TxTimestampInfo>) -> Result<()> {
        // The kernel stamps the last segment sent together: cumulative
        self.core.poll_tx_timestamps(true, "TCP", out)
    }

    #[cfg(target_os = "linux")]
    fn poll_tx_timestamps_for(
        &mut self,
        conn_id: usize,
        out: &mut Vec<crate::TxTimestampInfo>,
    ) -> Result<()> {
        self.core.poll_tx_timestamps_for(conn_id, true, "TCP", out)
    }

    fn start_reconnect(&mut self, conn_id: usize, target: &SocketAddr) -> Result<bool> {
        // Graceful FIN on any previous socket; frees slot `conn_id`.
        self.core.close(conn_id)?;

        let domain = if target.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))
            .map_err(|e| Error::Connection(format!("Connection failed: {e}")))?;
        socket.set_nonblocking(true)?;
        socket.set_tcp_nodelay(true)?;

        match socket.connect(&(*target).into()) {
            Ok(()) => {
                let stream: TcpStream = socket.into();
                #[cfg(target_os = "linux")]
                let timestamps_enabled = self.timestamp_config.configure_socket(
                    stream.as_raw_fd(),
                    self.timestamp_source,
                    "TCP",
                    conn_id,
                )?;
                #[cfg(not(target_os = "linux"))]
                let timestamps_enabled = false;

                self.core.register_readable(&stream, conn_id)?;
                self.core.insert(conn_id, TcpConn::new(stream, timestamps_enabled));
                Ok(true)
            }
            Err(e)
                if e.raw_os_error() == Some(libc::EINPROGRESS)
                    || e.kind() == io::ErrorKind::WouldBlock =>
            {
                let stream: TcpStream = socket.into();
                self.core.register_writable(&stream, conn_id)?;
                self.core.insert(conn_id, TcpConn::new_connecting(stream));
                Ok(false)
            }
            Err(e) => Err(Error::Connection(format!("Connection failed: {e}"))),
        }
    }

    fn finish_reconnect(&mut self, conn_id: usize) -> Result<bool> {
        {
            let conn = self.core.conn_mut(conn_id)?;
            if !conn.connecting {
                return Ok(true);
            }
            if let Some(err) = conn.stream.take_error()? {
                let _ = self.core.close(conn_id);
                return Err(Error::Connection(format!("Connection failed: {err}")));
            }
            match conn.stream.peer_addr() {
                Ok(_) => {}
                Err(e)
                    if e.raw_os_error() == Some(libc::ENOTCONN)
                        || e.raw_os_error() == Some(libc::EINPROGRESS)
                        || e.kind() == io::ErrorKind::NotConnected =>
                {
                    if let Some(err) = conn.stream.take_error()? {
                        let _ = self.core.close(conn_id);
                        return Err(Error::Connection(format!("Connection failed: {err}")));
                    }
                    return Ok(false);
                }
                Err(e) => {
                    let _ = self.core.close(conn_id);
                    return Err(Error::Connection(format!("Connection failed: {e}")));
                }
            }
        }

        // Handshake succeeded: configure timestamping before the first send so
        // the kernel's TX counter starts at zero.
        #[cfg(target_os = "linux")]
        let timestamps_enabled = {
            let fd = self.core.conn_mut(conn_id)?.stream.as_raw_fd();
            match self
                .timestamp_config
                .configure_socket(fd, self.timestamp_source, "TCP", conn_id)
            {
                Ok(enabled) => enabled,
                Err(e) => {
                    let _ = self.core.close(conn_id);
                    return Err(e);
                }
            }
        };
        #[cfg(not(target_os = "linux"))]
        let timestamps_enabled = false;

        if let Err(e) = self.core.set_interest(conn_id, Interest::READABLE) {
            let _ = self.core.close(conn_id);
            return Err(e);
        }

        let conn = self.core.conn_mut(conn_id)?;
        conn.connecting = false;
        conn.connected = true;
        if timestamps_enabled {
            conn.tx = Some(TxCounter::default());
            #[cfg(target_os = "linux")]
            self.core.note_tx_stamping_enabled();
        }
        Ok(true)
    }

    fn pause_io(&mut self, conn_id: usize) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            self.core.set_interest(conn_id, Interest::NONE)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = conn_id;
            Ok(())
        }
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

/// Factory for creating TCP connection groups
#[derive(Clone, Default)]
pub struct TcpTransportFactory {
    #[cfg(target_os = "linux")]
    timestamp_config: TcpTimestampConfig,
}

impl TcpTransportFactory {
    /// Create factory with timestamp configuration (Linux only)
    #[cfg(target_os = "linux")]
    pub fn with_timestamp_config(timestamp_config: TcpTimestampConfig) -> Self {
        Self { timestamp_config }
    }

    /// Create factory with timestamping enabled (uses default interface binding)
    #[cfg(target_os = "linux")]
    pub fn with_timestamps() -> Self {
        Self::with_timestamp_config(TcpTimestampConfig::with_timestamps())
    }
}

impl TransportFactory for TcpTransportFactory {
    type Group = TcpConnectionGroup;

    fn create_group(&self) -> Result<Self::Group> {
        #[cfg(target_os = "linux")]
        {
            TcpConnectionGroup::with_timestamp_config(self.timestamp_config.clone())
        }

        #[cfg(not(target_os = "linux"))]
        {
            TcpConnectionGroup::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    use crate::{KernelClock, TxRange, TxTimestampInfo};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

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
    fn group_connections_disable_nagle() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut group = TcpConnectionGroup::new().unwrap();
        let ids = [group.add_connection(&addr).unwrap(), group.add_connection(&addr).unwrap()];
        for id in ids {
            assert!(group.core.conn_mut(id).unwrap().stream.nodelay().unwrap(), "connection {id}");
        }
    }

    #[test]
    fn local_addr_is_the_address_the_peer_sees_while_open() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut group = TcpConnectionGroup::new().unwrap();
        for _ in 0..2 {
            let conn_id = group.add_connection(&addr).unwrap();
            let (_, peer) = listener.accept().unwrap();
            assert_eq!(group.local_addr(conn_id), Some(peer));
        }
        group.close(0).unwrap();
        assert_eq!(group.local_addr(0), None);
        assert_eq!(group.local_addr(99), None);
    }

    #[test]
    fn try_send_takes_what_fits_and_write_interest_reports_room() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut group = TcpConnectionGroup::new().unwrap();
        let conn_id = group.add_connection(&addr).unwrap();
        let (mut peer, _) = listener.accept().unwrap();

        // The peer reads nothing, so the socket buffers fill: a non-blocking
        // send takes a part, then nothing
        let data = vec![7u8; 64 << 20];
        let first = group.try_send(conn_id, &data).unwrap();
        assert!(first.accepted > 0 && first.accepted < data.len(), "{}", first.accepted);
        let full = group.try_send(conn_id, &data[first.accepted..]).unwrap();
        assert_eq!(full.accepted, 0);

        // With write interest, a poll reports the connection only once the
        // peer made room
        group.set_write_interest(conn_id, true).unwrap();
        let mut ready = Vec::new();
        group.poll(Duration::ZERO, &mut ready).unwrap();
        assert!(ready.is_empty());
        // The peer reads everything sent so far, so the client's buffer empties
        let sent = first.accepted;
        peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let reader = thread::spawn(move || {
            let mut buf = vec![0; 1 << 20];
            let mut total = 0;
            while total < sent {
                total += peer.read(&mut buf).unwrap();
            }
            peer
        });
        let _peer = reader.join().unwrap();
        group.poll(Duration::from_secs(5), &mut ready).unwrap();
        assert_eq!(ready, [conn_id]);
        assert!(group.try_send(conn_id, &data[first.accepted..]).unwrap().accepted > 0);

        // Without it, a writable connection is not reported
        group.set_write_interest(conn_id, false).unwrap();
        group.poll(Duration::ZERO, &mut ready).unwrap();
        assert!(ready.is_empty());
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

        assert!(group.core.get(0).is_some());
        assert!(group.core.get(1).is_some());
        assert!(group.core.get(99).is_none());
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

        group.send(conn1, b"msg1").unwrap();
        group.send(conn2, b"msg2").unwrap();
        group.send(conn3, b"msg3").unwrap();

        thread::sleep(Duration::from_millis(50));

        let mut ready = Vec::new();
        group.poll(Duration::from_millis(100), &mut ready).unwrap();
        assert!(!ready.is_empty());

        let mut buf = vec![0u8; 65536];
        for id in ready {
            let outcome = group.recv(id, &mut buf).unwrap();
            assert!(matches!(outcome, RecvOutcome::Data { len, .. } if len > 0));
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
        assert!(group.core.get(conn1).is_none());
        assert!(group.core.get(conn2).is_some());

        group.close_all().unwrap();
        assert_eq!(group.len(), 0);
    }

    #[test]
    fn test_tcp_group_ids_are_not_reused() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            for _ in 0..5 {
                let _ = listener.accept();
            }
        });

        thread::sleep(Duration::from_millis(10));

        let mut group = TcpConnectionGroup::new().unwrap();
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
    fn test_tcp_group_recv_outcomes() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (peer_tx, peer_rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            peer_tx.send(socket).unwrap();
        });

        let mut group = TcpConnectionGroup::new().unwrap();
        let conn_id = group.add_connection(&addr).unwrap();
        let mut peer = peer_rx.recv().unwrap();
        let mut buf = vec![0u8; 4];
        let mut ready = Vec::new();

        // Nothing sent yet: a zero timeout returns at once and recv would block
        group.poll(Duration::ZERO, &mut ready).unwrap();
        assert!(ready.is_empty());
        assert!(matches!(group.recv(conn_id, &mut buf).unwrap(), RecvOutcome::WouldBlock));

        // Level-triggered: still reported while bytes remain unread
        peer.write_all(b"abcdef").unwrap();
        group.poll(Duration::from_millis(100), &mut ready).unwrap();
        assert_eq!(ready, vec![conn_id]);
        assert!(matches!(
            group.recv(conn_id, &mut buf).unwrap(),
            RecvOutcome::Data { len: 4, .. }
        ));
        assert_eq!(&buf, b"abcd");
        group.poll(Duration::ZERO, &mut ready).unwrap();
        assert_eq!(ready, vec![conn_id]);
        assert!(matches!(
            group.recv(conn_id, &mut buf).unwrap(),
            RecvOutcome::Data { len: 2, .. }
        ));
        assert_eq!(&buf[..2], b"ef");

        // Peer close is reported as EOF, also on later calls
        drop(peer);
        group.poll(Duration::from_millis(100), &mut ready).unwrap();
        assert_eq!(ready, vec![conn_id]);
        assert!(matches!(group.recv(conn_id, &mut buf).unwrap(), RecvOutcome::Eof));
        assert!(matches!(group.recv(conn_id, &mut buf).unwrap(), RecvOutcome::Eof));

        // Unknown and closed ids are connection errors
        assert!(matches!(group.recv(99, &mut buf), Err(Error::Connection(_))));
        assert!(matches!(group.send(99, b"x"), Err(Error::Connection(_))));
        group.close(conn_id).unwrap();
        assert!(matches!(group.send(conn_id, b"x"), Err(Error::Connection(_))));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_tcp_group_tx_range_tracks_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || echo_handler(listener.accept().unwrap().0));

        let mut group = TcpConnectionGroup::new().unwrap();
        let conn_id = group.add_connection(&addr).unwrap();
        // No TX timestamps by default, so no TX range
        assert_eq!(group.timestamp_source(), TimestampSource::UserSpace);
        assert_eq!(group.send(conn_id, b"hello").unwrap().tx, None);

        let mut group = TcpConnectionGroup::with_timestamp_config(
            TcpTimestampConfig::with_software_timestamps(),
        )
        .unwrap();
        let conn_id = group.add_connection(&addr).unwrap();
        let first = group.send(conn_id, b"hello").unwrap();
        let second = group.send(conn_id, b"abc").unwrap();
        assert_eq!(first.tx, Some(TxRange { start: 0, end: 5 }));
        assert_eq!(second.tx, Some(TxRange { start: 5, end: 8 }));
    }

    /// Wait for the next TX timestamps from `group`
    #[cfg(target_os = "linux")]
    fn wait_tx_stamps(group: &mut TcpConnectionGroup) -> Vec<TxTimestampInfo> {
        let mut out = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            group.poll_tx_timestamps(&mut out).unwrap();
            if !out.is_empty() {
                return out;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("no TX timestamp within 5 s");
    }

    /// Kernel IDs 9 and 19 (last byte of each 10-byte write, pinned in
    /// hw_timestamp's tests) reach the caller as exclusive ends 10 and 20,
    /// equal to the receipts' ends: each stamp maps to its own send.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_tcp_group_loopback_tx_stamps_match_receipts() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (peer_tx, peer_rx) = std::sync::mpsc::channel();
        thread::spawn(move || peer_tx.send(listener.accept().unwrap().0).unwrap());

        let config = TcpTimestampConfig::with_software_timestamps();
        let mut group = TcpConnectionGroup::with_timestamp_config(config).unwrap();
        assert_eq!(group.timestamp_source(), TimestampSource::KernelSoftware);
        let conn_id = group.add_connection(&addr).unwrap();
        let mut peer = peer_rx.recv().unwrap();
        group.core.conn_mut(conn_id).unwrap().stream.set_nodelay(true).unwrap();

        for expected in [TxRange { start: 0, end: 10 }, TxRange { start: 10, end: 20 }] {
            let receipt = group.send(conn_id, &[0; 10]).unwrap();
            assert_eq!(receipt.tx, Some(expected));
            let stamps = wait_tx_stamps(&mut group);
            assert_eq!(stamps.len(), 1);
            assert_eq!(stamps[0].conn_id, conn_id);
            assert_eq!(stamps[0].tx_end, expected.end);
            assert!(stamps[0].cumulative);
            assert_eq!(stamps[0].timestamp.clock, KernelClock::Realtime);
            peer.read_exact(&mut [0; 10]).unwrap();
        }

        // Everything stamped: polling does no work and finds nothing
        let mut out = Vec::new();
        group.poll_tx_timestamps(&mut out).unwrap();
        assert!(out.is_empty());

        // RX carries a kernel timestamp from the same clock
        peer.write_all(b"pong").unwrap();
        let mut ready = Vec::new();
        group.poll(Duration::from_secs(5), &mut ready).unwrap();
        let mut buf = [0u8; 16];
        let RecvOutcome::Data { timestamp, .. } = group.recv(conn_id, &mut buf).unwrap() else {
            panic!("expected data");
        };
        assert_eq!(timestamp.kernel().map(|k| k.clock), Some(KernelClock::Realtime));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_tcp_group_default_uses_user_space_timestamps() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || echo_handler(listener.accept().unwrap().0));

        let mut group = TcpTransportFactory::default().create_group().unwrap();
        assert_eq!(group.timestamp_source(), TimestampSource::UserSpace);
        let conn_id = group.add_connection(&addr).unwrap();
        assert!(group.core.conn_mut(conn_id).unwrap().tx.is_none());
        assert_eq!(group.send(conn_id, b"ping").unwrap().tx, None);

        let mut ready = Vec::new();
        group.poll(Duration::from_secs(5), &mut ready).unwrap();
        let mut buf = [0u8; 16];
        let RecvOutcome::Data { timestamp, .. } = group.recv(conn_id, &mut buf).unwrap() else {
            panic!("expected data");
        };
        assert_eq!(timestamp.kernel(), None);
        let mut out = Vec::new();
        group.poll_tx_timestamps(&mut out).unwrap();
        assert!(out.is_empty());

        // An interface without hardware timestamping does not switch it on
        let config = TcpTimestampConfig::with_interface("lo");
        let group = TcpConnectionGroup::with_timestamp_config(config).unwrap();
        assert_eq!(group.timestamp_source(), TimestampSource::UserSpace);
    }

    /// Required hardware timestamps fail the group rather than fall back
    #[cfg(target_os = "linux")]
    #[test]
    fn test_tcp_group_required_hardware_never_falls_back() {
        // lo has no PHC: its driver rejects the query or reports it off
        for interface in ["lo", "xylem-no-such-if"] {
            let config = TcpTimestampConfig::require_hardware(interface);
            assert!(config.check().is_err());
            let Err(error) = TcpTransportFactory::with_timestamp_config(config).create_group()
            else {
                panic!("{interface}: created a group without hardware timestamps");
            };
            let message = error.to_string();
            assert!(message.contains(&format!("'{interface}'")), "{message}");
            assert_eq!(message.matches("Configuration error").count(), 1, "{message}");
        }
        // Without the requirement, an interface without them is not an error
        assert!(TcpTimestampConfig::with_interface("lo").check().is_ok());
    }

    #[test]
    fn test_tcp_transport_factory() {
        let factory = TcpTransportFactory::default();
        let group = factory.create_group();
        assert!(group.is_ok());
    }

    fn complete_reconnect(
        group: &mut TcpConnectionGroup,
        conn_id: usize,
        addr: &SocketAddr,
        ready: &mut Vec<usize>,
    ) -> Result<()> {
        if group.start_reconnect(conn_id, addr)? {
            return Ok(());
        }
        loop {
            group.poll(Duration::from_millis(100), ready)?;
            if ready.contains(&conn_id) && group.finish_reconnect(conn_id)? {
                return Ok(());
            }
        }
    }

    fn recv_payload(
        group: &mut TcpConnectionGroup,
        conn_id: usize,
        ready: &mut Vec<usize>,
        buf: &mut [u8],
    ) -> usize {
        loop {
            group.poll(Duration::from_millis(100), ready).unwrap();
            if !ready.contains(&conn_id) {
                continue;
            }
            match group.recv(conn_id, buf).unwrap() {
                RecvOutcome::Data { len, .. } => return len,
                RecvOutcome::WouldBlock => continue,
                RecvOutcome::Eof => panic!("unexpected EOF before data"),
            }
        }
    }

    #[test]
    fn test_tcp_group_start_and_finish_reconnect_reuses_slot_and_reports_refused() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // Accept 4 sequential connections, echo 4 bytes, and immediately close (FIN)
        let server = thread::spawn(move || {
            let mut peer_ports = Vec::new();
            for _ in 0..4 {
                let (mut stream, peer) = listener.accept().unwrap();
                peer_ports.push(peer.port());
                let mut buf = [0u8; 4];
                stream.read_exact(&mut buf).unwrap();
                stream.write_all(&buf).unwrap();
                // Drop stream immediately -> FIN
            }
            peer_ports
        });

        let mut group = TcpConnectionGroup::new().unwrap();
        let conn_id = group.add_connection(&addr).unwrap();
        let mut ready = Vec::new();
        let mut buf = [0u8; 16];

        for round in 0u8..4 {
            if round > 0 {
                complete_reconnect(&mut group, conn_id, &addr, &mut ready).unwrap();
            }
            assert_eq!(group.len(), 1);
            let payload = [b'a' + round; 4];
            group.send(conn_id, &payload).unwrap();
            let len = recv_payload(&mut group, conn_id, &mut ready, &mut buf);
            assert_eq!(&buf[..len], &payload);
        }

        let peer_ports = server.join().unwrap();
        assert_eq!(peer_ports.len(), 4);

        // Reconnecting to a closed port must surface Error::Connection
        let closed_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let closed_addr = closed_listener.local_addr().unwrap();
        drop(closed_listener);

        let outcome = complete_reconnect(&mut group, conn_id, &closed_addr, &mut ready);
        assert!(
            matches!(outcome, Err(Error::Connection(_))),
            "expected Connection error, got {outcome:?}"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_tcp_group_pause_io_and_poll_tx_timestamps_for_after_reconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                let mut buf = [0u8; 8];
                stream.read_exact(&mut buf).unwrap();
                stream.write_all(&buf).unwrap();
                // Immediately close -> sends FIN while client may still drain TX stamp
            }
        });

        let config = TcpTimestampConfig::with_software_timestamps();
        let mut group = TcpConnectionGroup::with_timestamp_config(config).unwrap();
        let conn_id = group.add_connection(&addr).unwrap();
        let mut ready = Vec::new();
        let mut buf = [0u8; 16];
        let mut stamps = Vec::new();

        for round in 0..2 {
            if round > 0 {
                complete_reconnect(&mut group, conn_id, &addr, &mut ready).unwrap();
            }
            let receipt = group.send(conn_id, b"12345678").unwrap();
            // Fresh socket restarts TX byte counter at 0
            assert_eq!(receipt.tx, Some(TxRange { start: 0, end: 8 }));
            let len = recv_payload(&mut group, conn_id, &mut ready, &mut buf);
            assert_eq!(len, 8);
            // Pause I/O so peer FIN doesn't spin epoll, then drain TX timestamp for conn_id
            group.pause_io(conn_id).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            loop {
                group.poll_tx_timestamps_for(conn_id, &mut stamps).unwrap();
                if !stamps.is_empty() {
                    break;
                }
                assert!(std::time::Instant::now() < deadline, "no TX timestamp for conn {conn_id}");
                thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(stamps.len(), 1);
            assert_eq!(stamps[0].conn_id, conn_id);
            assert_eq!(stamps[0].tx_end, 8);
        }

        server.join().unwrap();
    }
}
