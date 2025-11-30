//! UDP transport implementation using non-blocking I/O
//!
//! This module provides two APIs:
//! - `UdpTransport`: Single-connection API (one multiplexer per connection)
//! - `UdpConnectionGroup`: Multi-connection API (one multiplexer per group)

use crate::mux::{Interest, Multiplexer, MultiplexerType};
use crate::{
    ConnectionGroup, Error, GroupConnection, Result, Timestamp, Transport, TransportFactory,
};
use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

const DEFAULT_UDP_BUFFER_SIZE: usize = 65535;

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
}

impl UdpConn {
    fn new(socket: UdpSocket, target: SocketAddr) -> Self {
        Self {
            socket,
            target,
            recv_buffer: vec![0u8; DEFAULT_UDP_BUFFER_SIZE],
        }
    }
}

impl GroupConnection for UdpConn {
    fn send(&mut self, data: &[u8]) -> Result<Timestamp> {
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

        Ok(Timestamp::now())
    }

    fn recv(&mut self) -> Result<(Vec<u8>, Timestamp)> {
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
        })
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
#[derive(Clone, Copy, Default)]
pub struct UdpTransportFactory {
    mux_type: MultiplexerType,
}

impl UdpTransportFactory {
    pub fn new(mux_type: MultiplexerType) -> Self {
        Self { mux_type }
    }
}

impl TransportFactory for UdpTransportFactory {
    type Group = UdpConnectionGroup;

    fn create_group(&self) -> Result<Self::Group> {
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
        assert!(recv_ts.instant >= send_ts.instant);
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
        let send_ts = group.get_mut(conn_id).unwrap().send(test_data).unwrap();

        thread::sleep(Duration::from_millis(10));

        let ready = group.poll(Some(Duration::from_millis(100))).unwrap();
        assert!(!ready.is_empty());
        assert!(ready.contains(&conn_id));

        let (recv_data, recv_ts) = group.get_mut(conn_id).unwrap().recv().unwrap();
        assert_eq!(recv_data, test_data);
        assert!(recv_ts.instant >= send_ts.instant);
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
